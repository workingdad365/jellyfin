"""Jellyfin DB 이미지 누락 진단 스크립트 (읽기 전용)."""

import sqlite3
import sys

DB = "file:C:/Users/drasys/AppData/Local/jellyfin/data/jellyfin.db?mode=ro"


def dump(rows) -> None:
    for r in rows:
        print(dict(r))


def main() -> None:
    con = sqlite3.connect(DB, uri=True)
    con.row_factory = sqlite3.Row
    cmd = sys.argv[1] if len(sys.argv) > 1 else "tables"

    if cmd == "tables":
        for r in con.execute("select name from sqlite_master where type='table' order by name"):
            print(r[0])
    elif cmd == "schema":
        for r in con.execute(
            "select sql from sqlite_master where name = ?", (sys.argv[2],)
        ):
            print(r[0])
    elif cmd == "find":
        dump(
            con.execute(
                """
                select Id, Name, Type, Path, ProductionYear, DateCreated,
                       DateLastRefreshed, DateLastSaved, IsFolder, IsVirtualItem
                from BaseItems
                where Name like ?
                order by Name
                """,
                (f"%{sys.argv[2]}%",),
            )
        )
    elif cmd == "imgs":
        dump(
            con.execute(
                """
                select ImageType, Path, Width, Height, DateModified,
                       Blurhash is not null as hasBlur
                from BaseItemImageInfos
                where ItemId = ?
                order by ImageType
                """,
                (sys.argv[2],),
            )
        )
    elif cmd == "sql":
        dump(con.execute(sys.argv[2]))
    elif cmd == "audit":
        # 영화/시리즈 전체를 대상으로 이미지 레코드 유무 및 실제 파일 존재 여부 점검
        import os

        rows = con.execute(
            """
            select b.Id, b.Name, b.ProductionYear, b.Type, b.Path, b.DateLastRefreshed
            from BaseItems b
            where b.Type in (
                'MediaBrowser.Controller.Entities.Movies.Movie',
                'MediaBrowser.Controller.Entities.TV.Series')
              and b.IsVirtualItem = 0
            """
        ).fetchall()

        no_record = []
        missing_file = []
        zero_byte = []
        total = 0
        for r in rows:
            total += 1
            imgs = con.execute(
                "select ImageType, Path from BaseItemImageInfos where ItemId = ?",
                (r["Id"],),
            ).fetchall()
            primary = [i for i in imgs if i["ImageType"] == 0]
            if not primary:
                no_record.append((r["Name"], r["ProductionYear"], r["DateLastRefreshed"]))
                continue
            for i in imgs:
                p = i["Path"]
                if p.startswith("http"):
                    continue
                if not os.path.exists(p):
                    missing_file.append((r["Name"], r["ProductionYear"], i["ImageType"], p))
                elif os.path.getsize(p) == 0:
                    zero_byte.append((r["Name"], r["ProductionYear"], i["ImageType"], p))

        print(f"총 아이템: {total}")
        print(f"\n[A] Primary(포스터) 레코드 자체가 없음: {len(no_record)}건")
        for n in no_record[:60]:
            print("   ", n)
        print(f"\n[B] DB 레코드는 있으나 파일 없음: {len(missing_file)}건")
        for n in missing_file[:60]:
            print("   ", n)
        print(f"\n[C] 파일이 0바이트: {len(zero_byte)}건")
        for n in zero_byte[:60]:
            print("   ", n)
    elif cmd == "dirs":
        # 파일이 없는 아이템의 메타데이터 디렉토리 실제 상태 조사
        import os
        from collections import Counter

        rows = con.execute(
            """
            select b.Id, b.Name, b.ProductionYear, b.DateCreated, b.DateLastRefreshed,
                   i.ImageType, i.Path
            from BaseItems b join BaseItemImageInfos i on i.ItemId = b.Id
            where b.Type in (
                'MediaBrowser.Controller.Entities.Movies.Movie',
                'MediaBrowser.Controller.Entities.TV.Series')
              and b.IsVirtualItem = 0 and i.ImageType = 0
            """
        ).fetchall()

        stat = Counter()
        samples = []
        refresh_missing = Counter()
        refresh_ok = Counter()
        for r in rows:
            p = r["Path"]
            d = os.path.dirname(p)
            month = (r["DateLastRefreshed"] or "?")[:7]
            if os.path.exists(p):
                stat["파일 있음"] += 1
                refresh_ok[month] += 1
                continue
            refresh_missing[month] += 1
            if not os.path.isdir(d):
                stat["디렉토리 자체가 없음"] += 1
                if len(samples) < 10:
                    samples.append(("NO_DIR", r["Name"], d))
            else:
                listing = os.listdir(d)
                if not listing:
                    stat["디렉토리는 있으나 비어있음"] += 1
                else:
                    stat["디렉토리에 다른 파일 존재"] += 1
                    if len(samples) < 10:
                        samples.append(("OTHER_FILES", r["Name"], d, listing[:10]))

        print("=== 상태 분포 (Primary 기준) ===")
        for k, v in stat.items():
            print(f"  {k}: {v}")
        print("\n=== 샘플 ===")
        for s in samples:
            print("  ", s)
        print("\n=== 마지막 새로고침 월별: 파일없음 / 정상 ===")
        for m in sorted(set(refresh_missing) | set(refresh_ok)):
            print(f"  {m}: 없음 {refresh_missing.get(m, 0):5d} / 정상 {refresh_ok.get(m, 0):5d}")
    elif cmd == "detail":
        # 누락 186건의 정확한 시각 분포와 이미지 경로 GUID 일치 여부 검증
        import os
        from collections import Counter

        rows = con.execute(
            """
            select b.Id, b.Name, b.ProductionYear, b.Type, b.Path as ItemPath,
                   b.DateCreated, b.DateLastRefreshed, b.DateLastSaved, b.DateModified,
                   i.Path as ImgPath
            from BaseItems b join BaseItemImageInfos i on i.ItemId = b.Id
            where b.IsVirtualItem = 0 and i.ImageType = 0
              and b.Type in (
                'MediaBrowser.Controller.Entities.Movies.Movie',
                'MediaBrowser.Controller.Entities.TV.Series')
            """
        ).fetchall()

        missing = [r for r in rows if not os.path.exists(r["ImgPath"])]
        print(f"누락 건수: {len(missing)}")

        mismatch = 0
        for r in missing:
            guid_in_path = os.path.basename(os.path.dirname(r["ImgPath"])).lower()
            item_guid = r["Id"].replace("-", "").lower()
            if guid_in_path != item_guid:
                mismatch += 1
                if mismatch <= 5:
                    print(f"  [GUID 불일치] {r['Name']} item={item_guid} path={guid_in_path}")
        print(f"이미지 경로 GUID 불일치: {mismatch}건")

        print("\n=== DateLastRefreshed 분(minute) 단위 분포 ===")
        c = Counter(r["DateLastRefreshed"][:16] for r in missing)
        for k in sorted(c):
            print(f"  {k}  {c[k]}건")

        print("\n=== 라이브러리(경로 최상위)별 분포 ===")
        c2 = Counter((r["ItemPath"] or "?").split("\\")[3] if r["ItemPath"] else "?" for r in missing)
        for k, v in c2.most_common():
            print(f"  {k}: {v}")

        print("\n=== 샘플 10건 상세 ===")
        for r in missing[:10]:
            print(
                f"  {r['Name']} ({r['ProductionYear']}) created={r['DateCreated']} "
                f"refreshed={r['DateLastRefreshed']} saved={r['DateLastSaved']} modified={r['DateModified']}"
            )
    elif cmd == "blur":
        # blurhash 실제 값 유무로 과거 파일 존재 여부 판별
        import os

        rows = con.execute(
            """
            select b.Name, i.Path, i.Blurhash, i.Width, i.Height, i.DateModified
            from BaseItems b join BaseItemImageInfos i on i.ItemId = b.Id
            where b.IsVirtualItem = 0 and i.ImageType = 0
              and b.Type = 'MediaBrowser.Controller.Entities.Movies.Movie'
            """
        ).fetchall()

        miss_empty = miss_valued = ok_empty = ok_valued = 0
        miss_samples = []
        ok_samples = []
        for r in rows:
            blur = r["Blurhash"]
            has_value = blur is not None and len(blur) > 0
            if os.path.exists(r["Path"]):
                if has_value:
                    ok_valued += 1
                    if len(ok_samples) < 3:
                        ok_samples.append((r["Name"], blur[:30], r["Width"], r["DateModified"]))
                else:
                    ok_empty += 1
            else:
                if has_value:
                    miss_valued += 1
                    if len(miss_samples) < 5:
                        miss_samples.append((r["Name"], blur[:30], r["Width"], r["DateModified"]))
                else:
                    miss_empty += 1

        print("=== Primary 이미지 blurhash 상태 (영화) ===")
        print(f"  파일있음 + blurhash 값 있음 : {ok_valued}")
        print(f"  파일있음 + blurhash 비어있음 : {ok_empty}")
        print(f"  파일없음 + blurhash 값 있음 : {miss_valued}  <- 과거엔 파일이 존재했다는 증거")
        print(f"  파일없음 + blurhash 비어있음 : {miss_empty}  <- 애초에 저장된 적 없음")
        print("\n  [파일없음 샘플]")
        for s in miss_samples:
            print("   ", s)
        print("  [정상 샘플]")
        for s in ok_samples:
            print("   ", s)
    con.close()


if __name__ == "__main__":
    main()
