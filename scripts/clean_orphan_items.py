"""라이브러리 트리에서 분리된 고아 항목을 조회하고 정리하는 스크립트.

ParentId 가 NULL 인 미디어 항목은 어떤 라이브러리에도 속하지 않아 웹 UI 에서 조회도 삭제도 불가능하지만,
스캔과 이미지 처리는 계속 이 항목들을 대상으로 삼는다.

기본은 조회만 수행하며, --apply 를 붙이면 API 로 삭제를 요청한다.
미디어 파일이 실제로 존재하는 항목은 원본 삭제 위험이 있어 자동으로 제외한다.

사용 예:
    python scripts/clean_orphan_items.py
    $env:JELLYFIN_API_KEY = "<API 키>"
    python scripts/clean_orphan_items.py --apply
"""

import argparse
import os
import sqlite3
import sys
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_DB = Path(os.environ.get("LOCALAPPDATA", "")) / "jellyfin" / "data" / "jellyfin.db"
DEFAULT_URL = "http://localhost:8096"

# ParentId 가 반드시 있어야 하는 타입. Person/Studio/Genre 등 ItemByName 과 시스템 폴더는 제외한다
ORPHAN_CANDIDATE_TYPES = (
    "MediaBrowser.Controller.Entities.Movies.Movie",
    "MediaBrowser.Controller.Entities.TV.Series",
    "MediaBrowser.Controller.Entities.TV.Season",
    "MediaBrowser.Controller.Entities.TV.Episode",
    "MediaBrowser.Controller.Entities.Video",
    "MediaBrowser.Controller.Entities.Movies.Trailer",
    "MediaBrowser.Controller.Entities.Audio.MusicAlbum",
    "MediaBrowser.Controller.Entities.Audio.Audio",
    "MediaBrowser.Controller.Entities.AudioBook",
    "MediaBrowser.Controller.Entities.Book",
    "MediaBrowser.Controller.Entities.Photo",
    "MediaBrowser.Controller.Entities.PhotoAlbum",
    "MediaBrowser.Controller.Entities.MusicVideo",
)


def find_orphans(db_path: Path) -> list[dict]:
    """ParentId 가 NULL 인 미디어 항목과 그에 딸린 하위 항목 수를 조회한다.

    Args:
        db_path: jellyfin.db 경로. 읽기 전용으로 연다.

    Returns:
        id, name, type, path, media_exists, descendants 를 담은 딕셔너리 리스트.
    """
    con = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row

    placeholders = ",".join("?" * len(ORPHAN_CANDIDATE_TYPES))
    rows = con.execute(
        f"""
        select Id, Name, Type, Path, ProductionYear, DateCreated
        from BaseItems
        where ParentId is null and IsVirtualItem = 0 and Type in ({placeholders})
        order by Name
        """,
        ORPHAN_CANDIDATE_TYPES,
    ).fetchall()

    orphans = []
    for row in rows:
        descendants = 0
        if row["Path"]:
            prefix = row["Path"] if row["Path"].endswith("\\") else row["Path"] + "\\"
            descendants = con.execute(
                "select count(*) from BaseItems where Path like ? and Id != ?",
                (prefix + "%", row["Id"]),
            ).fetchone()[0]

        orphans.append(
            {
                "id": row["Id"],
                "name": row["Name"],
                "type": row["Type"].rsplit(".", 1)[-1],
                "path": row["Path"],
                "year": row["ProductionYear"],
                "media_exists": bool(row["Path"]) and os.path.exists(row["Path"]),
                "descendants": descendants,
            }
        )

    con.close()
    return orphans


def delete_item(base_url: str, api_key: str, item_id: str, timeout: float) -> None:
    """API 로 항목 삭제를 요청한다.

    Args:
        base_url: Jellyfin 서버 주소.
        api_key: API 키.
        item_id: 삭제할 아이템 GUID.
        timeout: 요청 타임아웃(초).

    Raises:
        urllib.error.HTTPError: 서버가 4xx/5xx 를 반환한 경우.
    """
    url = f"{base_url.rstrip('/')}/Items/{item_id}"
    request = urllib.request.Request(url, method="DELETE")
    request.add_header("Authorization", f'MediaBrowser Token="{api_key}"')

    with urllib.request.urlopen(request, timeout=timeout) as response:
        response.read()


def main() -> int:
    parser = argparse.ArgumentParser(description="고아 항목 조회 및 정리")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="jellyfin.db 경로")
    parser.add_argument("--url", default=os.environ.get("JELLYFIN_URL", DEFAULT_URL), help="서버 주소")
    parser.add_argument("--apply", action="store_true", help="실제로 삭제를 요청한다")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"DB 를 찾을 수 없음: {args.db}", file=sys.stderr)
        return 1

    orphans = find_orphans(args.db)
    if not orphans:
        print("고아 항목이 없습니다.")
        return 0

    deletable = [o for o in orphans if not o["media_exists"]]
    protected = [o for o in orphans if o["media_exists"]]

    print(f"고아 항목: {len(orphans)}건 (삭제 가능 {len(deletable)}, 보호 {len(protected)})\n")
    for item in orphans:
        mark = "보호" if item["media_exists"] else "삭제대상"
        print(f"  [{mark}] {item['type']} {item['name']} ({item['year']}) 하위 {item['descendants']}건")
        print(f"           {item['path']}")

    if protected:
        print("\n'보호' 항목은 미디어 파일이 실제로 존재하므로 삭제하지 않습니다.")
        print("라이브러리 스캔으로 트리에 다시 편입되는지 먼저 확인하세요.")

    if not args.apply:
        print("\ndry-run 입니다. 실제로 삭제하려면 --apply 를 붙이세요.")
        print("서버가 수정본으로 실행 중이라면 예약 작업 '데이터베이스 정리'로도 제거됩니다.")
        return 0

    api_key = os.environ.get("JELLYFIN_API_KEY")
    if not api_key:
        print("환경변수 JELLYFIN_API_KEY 가 설정되지 않았습니다.", file=sys.stderr)
        return 1

    succeeded = 0
    for item in deletable:
        try:
            delete_item(args.url, api_key, item["id"], timeout=60.0)
            succeeded += 1
            print(f"삭제 요청 완료: {item['name']}")
        except urllib.error.HTTPError as exc:
            print(f"삭제 실패({exc.code}): {item['name']}", file=sys.stderr)
        except urllib.error.URLError as exc:
            print(f"연결 실패: {item['name']} - {exc.reason}", file=sys.stderr)

    print(f"\n{succeeded}/{len(deletable)}건 처리")
    return 0


if __name__ == "__main__":
    sys.exit(main())
