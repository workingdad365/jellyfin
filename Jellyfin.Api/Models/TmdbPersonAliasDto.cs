using System.ComponentModel.DataAnnotations;

namespace Jellyfin.Api.Models;

/// <summary>
/// TMDB 인물 ID에 대응하는 로컬 인물명.
/// </summary>
public sealed class TmdbPersonAliasDto
{
    /// <summary>
    /// 양의 TMDB 인물 ID를 가져오거나 설정한다.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int TmdbId { get; set; }

    /// <summary>
    /// Jellyfin에서 사용할 고유 인물명을 가져오거나 설정한다.
    /// </summary>
    [Required]
    [StringLength(200, MinimumLength = 1)]
    public string Name { get; set; } = string.Empty;
}
