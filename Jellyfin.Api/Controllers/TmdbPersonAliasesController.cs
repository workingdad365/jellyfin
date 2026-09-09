using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Jellyfin.Api.Models;
using MediaBrowser.Common.Api;
using MediaBrowser.Controller.Providers;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Jellyfin.Api.Controllers;

/// <summary>
/// 관리자 전용 TMDB 인물 별칭 API.
/// </summary>
[Authorize(Policy = Policies.RequiresElevation)]
public class TmdbPersonAliasesController : BaseJellyfinApiController
{
    private readonly ITmdbPersonAliasService _aliases;
    private readonly ITmdbPersonSearchService _people;

    /// <summary>
    /// 별칭 API를 초기화한다.
    /// </summary>
    /// <param name="aliases">메인 DB와 분리된 별칭 저장소.</param>
    /// <param name="people">실제 TMDB 인물 검색 및 상세 조회 서비스.</param>
    public TmdbPersonAliasesController(ITmdbPersonAliasService aliases, ITmdbPersonSearchService people)
    {
        _aliases = aliases;
        _people = people;
    }

    /// <summary>
    /// 등록된 별칭을 TMDB ID 순서로 조회한다.
    /// </summary>
    /// <returns>TMDB 인물 ID와 로컬 이름 목록.</returns>
    [HttpGet]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public ActionResult<IEnumerable<TmdbPersonAliasDto>> GetAliases()
    {
        return Ok(_aliases.GetAliases().OrderBy(alias => alias.Key)
            .Select(alias => new TmdbPersonAliasDto { TmdbId = alias.Key, Name = alias.Value }));
    }

    /// <summary>
    /// 이름으로 TMDB 인물 후보와 구분용 약력을 조회한다.
    /// </summary>
    /// <param name="name">검색할 인물명.</param>
    /// <param name="page">1부터 500까지의 검색 페이지.</param>
    /// <param name="cancellationToken">요청 취소 토큰.</param>
    /// <returns>인물 후보 목록. TMDB 통신 실패 시 502.</returns>
    [HttpGet("Search")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status502BadGateway)]
    public async Task<ActionResult<TmdbPersonSearchResult>> SearchPeople(
        [FromQuery, Required, StringLength(200, MinimumLength = 1)] string name,
        [FromQuery, Range(1, 500)] int page = 1,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return BadRequest();
        }

        try
        {
            return Ok(await _people.Search(name.Trim(), page, cancellationToken).ConfigureAwait(false));
        }
        catch (HttpRequestException)
        {
            return Problem(statusCode: StatusCodes.Status502BadGateway, detail: "TMDB 인물 검색에 실패했습니다. 잠시 후 다시 시도하세요.");
        }
    }

    /// <summary>
    /// 기존 별칭 수정 시 실제 TMDB 인물 이름을 조회한다.
    /// </summary>
    /// <param name="tmdbId">TMDB 인물 ID.</param>
    /// <param name="cancellationToken">요청 취소 토큰.</param>
    /// <returns>TMDB 인물 정보. 인물이 없으면 404.</returns>
    [HttpGet("People/{tmdbId}")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status502BadGateway)]
    public async Task<ActionResult<TmdbPersonCandidate>> GetPerson(
        [FromRoute, Range(1, int.MaxValue)] int tmdbId,
        CancellationToken cancellationToken)
    {
        try
        {
            var person = await _people.GetPerson(tmdbId, cancellationToken).ConfigureAwait(false);
            if (person is null)
            {
                return NotFound();
            }

            return Ok(person);
        }
        catch (HttpRequestException)
        {
            return Problem(statusCode: StatusCodes.Status502BadGateway, detail: "TMDB 인물 조회에 실패했습니다. 잠시 후 다시 시도하세요.");
        }
    }

    /// <summary>
    /// 실제 TMDB 이름과 다른 고유 별칭을 추가하거나 변경한다.
    /// </summary>
    /// <param name="alias">양의 TMDB 인물 ID와 200자 이내의 고유 이름.</param>
    /// <param name="cancellationToken">TMDB 인물 확인 요청의 취소 토큰.</param>
    /// <returns>저장 성공 시 204, 잘못된 입력 시 400, 이름 충돌 시 409.</returns>
    [HttpPut]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    [ProducesResponseType(StatusCodes.Status502BadGateway)]
    public async Task<ActionResult> SaveAlias([FromBody, Required] TmdbPersonAliasDto alias, CancellationToken cancellationToken = default)
    {
        try
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(alias.Name);
            var person = await _people.GetPerson(alias.TmdbId, cancellationToken).ConfigureAwait(false);
            if (person is null)
            {
                return Problem(statusCode: StatusCodes.Status400BadRequest, detail: "TMDB에 존재하지 않는 인물 ID입니다.");
            }

            if (string.Equals(alias.Name.Trim().Normalize(NormalizationForm.FormC), person.Name.Trim().Normalize(NormalizationForm.FormC), StringComparison.OrdinalIgnoreCase))
            {
                return Problem(statusCode: StatusCodes.Status400BadRequest, detail: "TMDB 원래 이름과 다른 인물 별칭을 입력하세요.");
            }

            _aliases.Save(alias.TmdbId, alias.Name);
            return NoContent();
        }
        catch (ArgumentException exception)
        {
            return Problem(statusCode: StatusCodes.Status400BadRequest, detail: exception.Message);
        }
        catch (InvalidOperationException exception)
        {
            return Problem(statusCode: StatusCodes.Status409Conflict, detail: exception.Message);
        }
        catch (HttpRequestException)
        {
            return Problem(statusCode: StatusCodes.Status502BadGateway, detail: "TMDB 인물 확인에 실패하여 저장하지 않았습니다. 잠시 후 다시 시도하세요.");
        }
    }

    /// <summary>
    /// 별칭을 삭제한다. 라이브러리의 기존 인물과 연결은 변경하지 않는다.
    /// </summary>
    /// <param name="tmdbId">삭제할 양의 TMDB 인물 ID.</param>
    /// <returns>삭제 성공 시 204.</returns>
    [HttpDelete("{tmdbId}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public ActionResult DeleteAlias([FromRoute, Range(1, int.MaxValue)] int tmdbId)
    {
        _aliases.Delete(tmdbId);
        return NoContent();
    }
}
