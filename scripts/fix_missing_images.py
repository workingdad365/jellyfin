"""이미지 파일이 유실된 항목을 찾아 Jellyfin API로 강제 재수집하는 스크립트.

DB(읽기 전용)에서 BaseItemImageInfos 레코드는 존재하지만 실제 파일이 없는 항목을 추출한 뒤,
각 항목에 대해 replaceAllImages=true 로 새로고침을 요청한다. 이는 웹 UI에서
"메타데이터 새로 고침 > 현재 이미지 교체" 를 누르는 것과 동일한 동작이다.

사용 예:
    $env:JELLYFIN_API_KEY = "<대시보드에서 발급한 API 키>"
    python scripts/fix_missing_images.py            # 대상만 출력 (dry-run)
    python scripts/fix_missing_images.py --apply    # 실제 새로고침 요청
"""

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

DEFAULT_DB = Path(os.environ.get("LOCALAPPDATA", "")) / "jellyfin" / "data" / "jellyfin.db"
DEFAULT_URL = "http://localhost:8096"

# 대상 타입: 영화, 시리즈, 시즌, 에피소드
TARGET_TYPES = (
    "MediaBrowser.Controller.Entities.Movies.Movie",
    "MediaBrowser.Controller.Entities.TV.Series",
    "MediaBrowser.Controller.Entities.TV.Season",
    "MediaBrowser.Controller.Entities.TV.Episode",
)

# 포스터가 반드시 있어야 하는 타입. 에피소드는 원본에 썸네일이 없는 경우가 흔해 제외한다
PRIMARY_REQUIRED_TYPES = (
    "MediaBrowser.Controller.Entities.Movies.Movie",
    "MediaBrowser.Controller.Entities.TV.Series",
)


def classify_media(media_path: str | None) -> str:
    """미디어 경로의 상태를 판정한다.

    Args:
        media_path: BaseItems.Path 값.

    Returns:
        present(미디어 존재), ghost(조상 경로는 접근되나 대상이 없음), unreachable(경로 전체가 접근 불가).
    """
    if not media_path:
        return "unreachable"

    if os.path.exists(media_path):
        return "present"

    # 조상 경로 중 하나라도 살아 있으면 저장소는 정상이고 대상만 삭제된 것으로 본다
    current = os.path.dirname(media_path)
    while current and current not in (os.path.dirname(current), "\\\\"):
        if os.path.isdir(current):
            return "ghost"

        current = os.path.dirname(current)

    return "unreachable"


def collect_broken_items(db_path: Path) -> dict[str, list[dict]]:
    """이미지가 깨진 항목을 수집하고 미디어 상태별로 분류한다.

    다음 두 유형을 모두 대상으로 한다.
      - stale: 이미지 레코드는 있으나 실제 파일이 없는 경우 (UI 에서 빈 영역으로 표시)
      - absent: 포스터 레코드 자체가 없는 경우 (UI 에서 필름 아이콘 플레이스홀더로 표시)

    Args:
        db_path: jellyfin.db 경로. 서버 실행 중에도 안전하도록 읽기 전용으로 연다.

    Returns:
        present/ghost/unreachable 키를 갖는 딕셔너리. 각 값은 항목 리스트이며
        항목은 id, name, year, media, kind, missing(유실된 이미지 경로 목록)을 가진다.
    """
    uri = f"file:{db_path.as_posix()}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    con.row_factory = sqlite3.Row

    placeholders = ",".join("?" * len(TARGET_TYPES))
    rows = con.execute(
        f"""
        select b.Id, b.Name, b.ProductionYear, b.Path as MediaPath, i.ImageType, i.Path
        from BaseItems b
        join BaseItemImageInfos i on i.ItemId = b.Id
        where b.IsVirtualItem = 0 and b.Type in ({placeholders})
        """,
        TARGET_TYPES,
    ).fetchall()

    broken: dict[str, dict] = {}
    for row in rows:
        path = row["Path"]
        if path.startswith("http") or os.path.exists(path):
            continue

        entry = broken.setdefault(
            row["Id"],
            {
                "id": row["Id"],
                "name": row["Name"],
                "year": row["ProductionYear"],
                "media": row["MediaPath"],
                "kind": "stale",
                "missing": [],
            },
        )
        entry["missing"].append(path)

    # 포스터 레코드 자체가 없는 항목은 위 조인에 잡히지 않으므로 별도로 수집한다
    absent_placeholders = ",".join("?" * len(PRIMARY_REQUIRED_TYPES))
    absent_rows = con.execute(
        f"""
        select b.Id, b.Name, b.ProductionYear, b.Path as MediaPath
        from BaseItems b
        where b.IsVirtualItem = 0 and b.ParentId is not null
          and b.Type in ({absent_placeholders})
          and not exists (
            select 1 from BaseItemImageInfos i where i.ItemId = b.Id and i.ImageType = 0
          )
        """,
        PRIMARY_REQUIRED_TYPES,
    ).fetchall()
    con.close()

    for row in absent_rows:
        broken.setdefault(
            row["Id"],
            {
                "id": row["Id"],
                "name": row["Name"],
                "year": row["ProductionYear"],
                "media": row["MediaPath"],
                "kind": "absent",
                "missing": [],
            },
        )

    result: dict[str, list[dict]] = {"present": [], "ghost": [], "unreachable": []}
    media_status: dict[str, str] = {}
    for entry in broken.values():
        media = entry["media"]
        if media not in media_status:
            media_status[media] = classify_media(media)

        result[media_status[media]].append(entry)

    return result


def refresh_item(base_url: str, api_key: str, item_id: str, timeout: float) -> None:
    """단일 항목에 대해 이미지 강제 교체 새로고침을 요청한다.

    Args:
        base_url: Jellyfin 서버 주소.
        api_key: 대시보드에서 발급한 API 키.
        item_id: 하이픈이 포함된 형태의 아이템 GUID.
        timeout: 요청 타임아웃(초).

    Raises:
        urllib.error.HTTPError: 서버가 4xx/5xx 를 반환한 경우.
    """
    query = urllib.parse.urlencode(
        {
            "metadataRefreshMode": "FullRefresh",
            "imageRefreshMode": "FullRefresh",
            "replaceAllMetadata": "false",
            "replaceAllImages": "true",
        }
    )
    url = f"{base_url.rstrip('/')}/Items/{item_id}/Refresh?{query}"
    request = urllib.request.Request(url, method="POST", data=b"")
    request.add_header("Authorization", f'MediaBrowser Token="{api_key}"')

    with urllib.request.urlopen(request, timeout=timeout) as response:
        response.read()


def main() -> int:
    parser = argparse.ArgumentParser(description="이미지 유실 항목 일괄 복구")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="jellyfin.db 경로")
    parser.add_argument("--url", default=os.environ.get("JELLYFIN_URL", DEFAULT_URL), help="서버 주소")
    parser.add_argument("--apply", action="store_true", help="실제로 새로고침을 요청한다")
    parser.add_argument("--delay", type=float, default=1.5, help="요청 간 대기 시간(초)")
    parser.add_argument("--limit", type=int, default=0, help="처리할 최대 항목 수 (0=전체)")
    parser.add_argument("--out", type=Path, help="대상 목록을 JSON 으로 저장할 경로")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"DB 를 찾을 수 없음: {args.db}", file=sys.stderr)
        return 1

    groups = collect_broken_items(args.db)
    items = sorted(groups["present"], key=lambda i: (i["name"] or ""))
    ghosts = sorted(groups["ghost"], key=lambda i: (i["name"] or ""))
    unreachable = groups["unreachable"]

    print(f"재수집 대상(미디어 존재): {len(items)}건")
    print(f"제외 - 미디어가 없는 유령 항목: {len(ghosts)}건")
    print(f"제외 - 경로 접근 불가(연결 확인 필요): {len(unreachable)}건\n")

    if ghosts:
        print("[유령 항목 - 라이브러리에서 정리 필요]")
        for item in ghosts:
            print(f"  {item['name']} ({item['year']}) <- {item['media']}")
        print()

    if unreachable:
        print("[경로 접근 불가 - 네트워크/드라이브 연결 확인 후 재실행 권장]")
        for item in unreachable[:20]:
            print(f"  {item['name']} ({item['year']}) <- {item['media']}")
        print()

    if args.limit:
        items = items[: args.limit]

    print(f"[재수집 대상 {len(items)}건]")
    for item in items:
        detail = "포스터 레코드 없음" if item["kind"] == "absent" else f"유실 {len(item['missing'])}개"
        print(f"  {item['name']} ({item['year']}) - {detail}")

    if args.out:
        payload = {"targets": items, "ghosts": ghosts, "unreachable": unreachable}
        args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"목록 저장: {args.out}")

    if not args.apply:
        print("\ndry-run 입니다. 실제로 실행하려면 --apply 를 붙이세요.")
        return 0

    api_key = os.environ.get("JELLYFIN_API_KEY")
    if not api_key:
        print("환경변수 JELLYFIN_API_KEY 가 설정되지 않았습니다.", file=sys.stderr)
        return 1

    succeeded = 0
    failed: list[tuple[str, str]] = []
    for index, item in enumerate(items, start=1):
        try:
            refresh_item(args.url, api_key, item["id"], timeout=30.0)
            succeeded += 1
            print(f"[{index}/{len(items)}] 요청 완료: {item['name']}")
        except urllib.error.HTTPError as exc:
            failed.append((item["name"] or item["id"], f"HTTP {exc.code}"))
            print(f"[{index}/{len(items)}] 실패({exc.code}): {item['name']}", file=sys.stderr)
        except urllib.error.URLError as exc:
            failed.append((item["name"] or item["id"], str(exc.reason)))
            print(f"[{index}/{len(items)}] 연결 실패: {item['name']}", file=sys.stderr)

        time.sleep(args.delay)

    print(f"\n요청 성공 {succeeded}건, 실패 {len(failed)}건")
    for name, reason in failed:
        print(f"  실패: {name} - {reason}")
    print("새로고침은 서버에서 비동기로 처리됩니다. 완료 후 다시 실행해 잔여 건수를 확인하세요.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
