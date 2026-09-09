using System.Collections.Generic;

namespace MediaBrowser.Controller.Providers;

/// <summary>
/// 메인 데이터베이스와 분리된 TMDB 인물 별칭 저장소를 제공한다.
/// </summary>
public interface ITmdbPersonAliasService
{
    /// <summary>
    /// 현재 별칭의 읽기 전용 스냅샷을 가져온다. 저장소 오류는 호출자에게 전파한다.
    /// </summary>
    /// <returns>양의 TMDB 인물 ID와 Jellyfin에서 사용할 이름의 사전.</returns>
    IReadOnlyDictionary<int, string> GetAliases();

    /// <summary>
    /// 스냅샷에서 별칭을 적용하고 미등록 인물이 예약된 별칭을 침범하는지 검사한다.
    /// </summary>
    /// <param name="aliases">현재 메타데이터 수집에 사용할 별칭 스냅샷.</param>
    /// <param name="tmdbId">TMDB 인물 ID.</param>
    /// <param name="originalName">TMDB에서 받은 이름.</param>
    /// <returns>등록된 별칭 또는 앞뒤 공백을 제거한 원래 이름.</returns>
    /// <exception cref="System.InvalidOperationException">다른 인물의 별칭과 충돌하는 경우.</exception>
    string ResolveName(IReadOnlyDictionary<int, string> aliases, int tmdbId, string originalName);

    /// <summary>
    /// 이름 충돌을 검사하고 별칭을 저장한다. 기존 라이브러리 항목은 변경하지 않는다.
    /// </summary>
    /// <param name="tmdbId">양의 TMDB 인물 ID.</param>
    /// <param name="name">앞뒤 공백을 제거하여 저장할 고유 인물명.</param>
    /// <exception cref="System.ArgumentException">ID 또는 이름이 유효하지 않은 경우.</exception>
    /// <exception cref="System.InvalidOperationException">다른 인물의 이름과 충돌하는 경우.</exception>
    void Save(int tmdbId, string name);

    /// <summary>
    /// 별칭만 삭제한다. 기존 라이브러리 항목은 변경하지 않는다.
    /// </summary>
    /// <param name="tmdbId">삭제할 TMDB 인물 ID.</param>
    void Delete(int tmdbId);
}
