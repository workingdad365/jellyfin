using System.Threading;
using System.Threading.Tasks;

namespace MediaBrowser.Controller.Providers;

/// <summary>
/// 관리자 별칭 편집에 필요한 TMDB 인물 검색과 상세 조회를 제공한다.
/// </summary>
public interface ITmdbPersonSearchService
{
    /// <summary>
    /// 이름으로 인물을 검색하고 구분에 필요한 상세 정보를 조회한다.
    /// </summary>
    /// <param name="name">검색할 인물명.</param>
    /// <param name="page">1부터 시작하는 검색 페이지.</param>
    /// <param name="cancellationToken">요청 취소 토큰.</param>
    /// <returns>인물 후보와 전체 페이지 수.</returns>
    Task<TmdbPersonSearchResult> Search(string name, int page, CancellationToken cancellationToken);

    /// <summary>
    /// ID로 실제 TMDB 인물을 조회한다.
    /// </summary>
    /// <param name="tmdbId">양의 TMDB 인물 ID.</param>
    /// <param name="cancellationToken">요청 취소 토큰.</param>
    /// <returns>인물 정보. 존재하지 않으면 null.</returns>
    Task<TmdbPersonCandidate?> GetPerson(int tmdbId, CancellationToken cancellationToken);
}
