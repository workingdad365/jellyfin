using System.Collections.Generic;

namespace MediaBrowser.Controller.Providers;

/// <summary>
/// TMDB 인물 검색 페이지.
/// </summary>
public sealed class TmdbPersonSearchResult
{
    /// <summary>현재 페이지의 인물 후보를 가져오거나 설정한다.</summary>
    public IReadOnlyList<TmdbPersonCandidate> Items { get; set; } = [];

    /// <summary>전체 검색 페이지 수를 가져오거나 설정한다.</summary>
    public int TotalPages { get; set; }
}
