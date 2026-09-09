using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MediaBrowser.Controller.Providers;

namespace MediaBrowser.Providers.Plugins.Tmdb.People;

/// <summary>
/// 기존 TMDB 클라이언트와 캐시를 사용하는 관리자 인물 검색 서비스.
/// </summary>
public sealed class TmdbPersonSearchService : ITmdbPersonSearchService
{
    private readonly TmdbClientManager _client;

    /// <summary>
    /// 인물 검색 서비스를 초기화한다.
    /// </summary>
    /// <param name="client">TMDB 인증과 응답 캐시를 관리하는 클라이언트.</param>
    public TmdbPersonSearchService(TmdbClientManager client)
    {
        _client = client;
    }

    /// <inheritdoc />
    public async Task<TmdbPersonSearchResult> Search(string name, int page, CancellationToken cancellationToken)
    {
        var result = await _client.SearchPersonPageAsync(name, page, cancellationToken).ConfigureAwait(false);
        var people = new List<TmdbPersonCandidate>();
        if (result?.Results is not null)
        {
            foreach (var batch in result.Results.Where(person => person.Id > 0).DistinctBy(person => person.Id).Chunk(4))
            {
                var details = await Task.WhenAll(batch.Select(person => GetPerson(person.Id, cancellationToken))).ConfigureAwait(false);
                people.AddRange(details.OfType<TmdbPersonCandidate>());
            }
        }

        return new TmdbPersonSearchResult
        {
            Items = people,
            TotalPages = Math.Min(result?.TotalPages ?? 0, 500)
        };
    }

    /// <inheritdoc />
    public async Task<TmdbPersonCandidate?> GetPerson(int tmdbId, CancellationToken cancellationToken)
    {
        var person = await _client.GetPersonAsync(tmdbId, "ko-KR", "KR", cancellationToken).ConfigureAwait(false);
        var name = person?.Name;
        if (person is null || string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        var biography = person.Biography;
        if (string.IsNullOrWhiteSpace(biography))
        {
            var fallback = await _client.GetPersonAsync(tmdbId, "en-US", "US", cancellationToken).ConfigureAwait(false);
            biography = fallback?.Biography;
        }

        return new TmdbPersonCandidate
        {
            TmdbId = person.Id,
            Name = name,
            Biography = biography,
            Birthday = person.Birthday,
            PlaceOfBirth = person.PlaceOfBirth,
            ImageUrl = _client.GetProfileUrl(person.ProfilePath)
        };
    }
}
