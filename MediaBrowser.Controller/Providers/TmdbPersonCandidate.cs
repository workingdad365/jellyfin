using System;

namespace MediaBrowser.Controller.Providers;

/// <summary>
/// 동명이인 비교를 위한 TMDB 인물 정보.
/// </summary>
public sealed class TmdbPersonCandidate
{
    /// <summary>TMDB 인물 ID를 가져오거나 설정한다.</summary>
    public int TmdbId { get; set; }

    /// <summary>TMDB에 등록된 이름을 가져오거나 설정한다.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>약력을 가져오거나 설정한다.</summary>
    public string? Biography { get; set; }

    /// <summary>생년월일을 가져오거나 설정한다.</summary>
    public DateTime? Birthday { get; set; }

    /// <summary>출생지를 가져오거나 설정한다.</summary>
    public string? PlaceOfBirth { get; set; }

    /// <summary>TMDB 인물 사진 URL을 가져오거나 설정한다.</summary>
    public string? ImageUrl { get; set; }
}
