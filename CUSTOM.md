# Jellyfin 서버 커스텀 변경 사항

## 정리 기준

- 작성일: 2026-09-18.
- 대상 브랜치: `feature/custom`.
- 로컬 `master`와의 공통 조상 이후 최종 코드 차이 및 커스텀 커밋을 기준으로 정리한다. 업스트림 병합 자체는 커스텀 기능으로 계산하지 않는다.
- 웹의 대응 브랜치는 `feature/custom_ui`다. TMDb 인물 별칭 관리 화면은 서버와 웹 변경을 함께 적용해야 한다.
- 이번 작업은 문서 작성만 수행했다. 아래에 언급하는 테스트는 저장소에 추가된 테스트이며, 이번 작성 중 빌드·테스트·운영 데이터 변경은 수행하지 않았다.

## 1. 사용자 및 홈 화면 기본값

- 신규 사용자 권한의 기본값에서 미디어 변환, 동기화 트랜스코딩, 오디오·비디오 재생 트랜스코딩, 공개 공유, 라이브 TV 접근을 비활성화했다. 라이브 TV 관리도 비활성화 상태로 설정한다.
- 일반 미디어 재생과 리먹싱은 허용하는 구성을 유지한다. 따라서 재인코딩이 필요한 미디어의 재생 가능 여부는 사용자 권한과 클라이언트 지원 형식에 영향을 받는다.
- 최대 동시 세션 기본값을 `3`으로 변경했다.
- 로그인 실패 잠금 횟수는 현재 사용자 엔티티 생성자에서 `3`, `UserPolicy` 생성자에서 `0`으로 설정된다. 두 경로의 값이 다르므로 모든 계정에 같은 값이 적용된다고 해석하지 않는다.
- 홈 화면 기본 순서를 작은 라이브러리 타일 → 이어서 시청 → 최근 추가 미디어 → 다음 에피소드로 변경하고 나머지 기본 섹션은 `None`으로 설정했다.
- 표시 설정 조회·저장 및 잘못된 보기 유형에 대한 진단 로그를 보강했다.
- 기본값 변경이며 기존 사용자 설정과 권한을 일괄 덮어쓰는 기능은 아니다.

관련 파일:

- [Jellyfin.Data/UserEntityExtensions.cs](Jellyfin.Data/UserEntityExtensions.cs)
- [MediaBrowser.Model/Users/UserPolicy.cs](MediaBrowser.Model/Users/UserPolicy.cs)
- [src/Jellyfin.Database/Jellyfin.Database.Implementations/Entities/User.cs](src/Jellyfin.Database/Jellyfin.Database.Implementations/Entities/User.cs)
- [Jellyfin.Api/Controllers/DisplayPreferencesController.cs](Jellyfin.Api/Controllers/DisplayPreferencesController.cs)

## 2. 라이브러리 스캔과 실시간 감시

### 스캔 제외 규칙

- 기존 제외 목록에 Windows·NAS 휴지통 이름 변형을 추가했다. 추가 항목은 `Recycled`, `RECYCLER`, `.Trash`, `Trash`, `#Recycle`, `#RECYCLE`, `recycle`, `RECYCLE` 및 그 하위 경로다.
- 경로 구분자 `\`를 `/`로 정규화한 뒤 패턴을 검사하여 Windows 경로에서도 동일한 제외 규칙을 사용한다.
- 휴지통 경로가 제외되는 경우 확인용 로그를 추가하고 관련 테스트를 보강했다.

### 파일 감시 복구

- 시작 시 감시 경로가 없거나 아직 접근되지 않으면 30초 간격으로 최대 10회 재시도한다. 시작 시 연결이 늦는 UNC 공유 경로 등을 위한 보완이다.
- 파일 감시기 오류가 발생하면 기존 감시기를 해제하고 5초 후 재시작한다.
- 권한 오류는 로그를 남기고 반환하며 자동 재시작 대상에서 제외한다. 감시기 재시작 자체가 누락된 모든 변경 이벤트를 재생하는 것은 아니다.
- 외부에서 파일 변경을 알릴 수 있도록 `/Library/Media/Updated` 호출용 PowerShell 스크립트를 추가했다.

관련 파일:

- [Emby.Server.Implementations/Library/IgnorePatterns.cs](Emby.Server.Implementations/Library/IgnorePatterns.cs)
- [Emby.Server.Implementations/Library/CoreResolutionIgnoreRule.cs](Emby.Server.Implementations/Library/CoreResolutionIgnoreRule.cs)
- [Emby.Server.Implementations/IO/LibraryMonitor.cs](Emby.Server.Implementations/IO/LibraryMonitor.cs)
- [scripts/Trigger-JellyfinMediaUpdated.ps1](scripts/Trigger-JellyfinMediaUpdated.ps1)

## 3. 썸네일과 TMDb 이미지

### 라이브러리 썸네일 갱신

- 라이브러리 폴더의 합성 썸네일 갱신 판단을 기존 7일 기준에서 파일의 마지막 수정 시각 기준 24시간 초과로 변경했다.
- 이미지 기록과 실제 파일의 수정 시각이 달라도 변경된 것으로 판단한다.
- 별도 24시간 타이머를 추가한 것은 아니다. 이미지 제공자의 갱신 검사 흐름에서 재생성 여부를 결정한다.

### 이미지 언어 및 캐시

- TMDb 이미지 요청 언어 목록을 선호 언어 → 영어 → 언어 미지정(`null`) 순서로 구성했다. 한국어 설정 시 한국어 이미지가 없을 때 영어 이미지를 대체 후보에 포함한다.
- 영화, 컬렉션, 시리즈, 시즌, 에피소드 이미지 제공자가 선호 언어·국가 및 이미지 언어 목록을 전달하도록 수정했다.
- TMDb 메모리 캐시 키에 이미지 언어 목록을 포함하여 서로 다른 이미지 언어 조건의 응답이 섞이지 않도록 했다.

### 파일이 유실된 이미지 기록 정리

- 라이브러리 항목의 로컬 이미지 파일이 사라졌는데 이미지 DB 기록만 남아 있는 경우 해당 기록을 제거하고 저장한다.
- 이미지가 이미 있다고 판단하여 누락 이미지 재수집을 건너뛰던 원인을 보정한다. 파일 재다운로드 자체는 이후 제공자 갱신 과정에서 수행한다.
- 내부 메타데이터 경로 밖의 이미지는 상위 디렉터리도 존재하는지 확인한다. UNC·이동식 저장소가 일시적으로 끊어진 것을 파일 삭제로 오인하지 않기 위한 보호다.

관련 파일:

- [Emby.Server.Implementations/Images/CollectionFolderImageProvider.cs](Emby.Server.Implementations/Images/CollectionFolderImageProvider.cs)
- [Emby.Server.Implementations/Library/LibraryManager.cs](Emby.Server.Implementations/Library/LibraryManager.cs)
- [MediaBrowser.Providers/Plugins/Tmdb/TmdbUtils.cs](MediaBrowser.Providers/Plugins/Tmdb/TmdbUtils.cs)
- [MediaBrowser.Providers/Plugins/Tmdb/TmdbClientManager.cs](MediaBrowser.Providers/Plugins/Tmdb/TmdbClientManager.cs)
- [MediaBrowser.Providers/Plugins/Tmdb/Movies/TmdbMovieImageProvider.cs](MediaBrowser.Providers/Plugins/Tmdb/Movies/TmdbMovieImageProvider.cs)

## 4. HLS 리먹싱 탐색 시 오디오·자막 동기화 보정

- 두 번째 오디오 트랙 등을 사용하는 리먹싱 재생에서 탐색 후 오디오·영상·자막 시각이 어긋나는 문제를 보완했다.
- 비디오까지 스트림 복사하고 분할 라이브 스트림이 아닌 경우, 복사 오디오 출력에 `-copypriorss:a:0 0`을 추가하여 시작 시각 이전 패킷을 제외한다.
- HLS 리먹싱 탐색 입력에 `-noaccurate_seek`를 복원했다. 이 옵션이 탐색을 방해하는 `wtv` 컨테이너는 제외한다.
- 비디오 트랜스코딩 경로 전체를 바꾸거나 모든 형식의 동기화 문제를 해결하는 변경은 아니다.

관련 파일:

- [Jellyfin.Api/Controllers/DynamicHlsController.cs](Jellyfin.Api/Controllers/DynamicHlsController.cs)
- [MediaBrowser.Controller/MediaEncoding/EncodingHelper.cs](MediaBrowser.Controller/MediaEncoding/EncodingHelper.cs)

## 5. TMDb 인물 별칭

### 목적과 저장 방식

- TMDb 인물 ID별로 로컬 표시 이름을 지정하여, 동명이인이 이름 기반 인물 병합으로 섞이는 문제를 방지한다.
- 별칭은 서버 데이터 디렉터리의 별도 SQLite 파일 `tmdb-person-aliases.db`에 저장한다. 메인 Jellyfin DB에 별칭 테이블이나 마이그레이션을 추가하지 않는다.
- 최초 접근 시 DB를 생성하고 불변 메모리 스냅샷으로 조회한다. 저장 성공 후 스냅샷을 교체하며 쓰기 실패 시 마지막 정상 스냅샷을 유지한다.
- 실행 중 DB를 직접 수정하면 메모리 상태에 즉시 반영되지 않는다. 관리자 API로 편집하고 백업 시 별칭 DB도 별도로 포함한다.

### 관리자 API

모든 엔드포인트에 관리자 권한이 필요하다.

| 메서드와 경로 | 동작 |
| --- | --- |
| `GET /TmdbPersonAliases` | 등록된 별칭 목록 |
| `GET /TmdbPersonAliases/Search?name=...&page=1` | TMDb 인물 검색 및 페이지 조회 |
| `GET /TmdbPersonAliases/People/{tmdbId}` | 인물 상세 정보 조회 |
| `PUT /TmdbPersonAliases` | `{ "TmdbId": 123, "Name": "인물 별칭" }` 저장 |
| `POST /TmdbPersonAliases/RefreshItems` | 기존 인물명 배열을 받아 관련 작품 갱신 예약 |
| `DELETE /TmdbPersonAliases/{tmdbId}` | 별칭 삭제 |

- 검색·상세 조회는 서버에서 TMDb에 요청한다. 한국어 이름·약력을 요청하고 약력이 없으면 영문 약력을 대체 조회한다.
- 실제 TMDb 인물 존재 여부, 이름 길이·제어 문자, 원래 이름과 동일한 별칭, 다른 ID의 별칭 및 기존 라이브러리 인물과의 충돌을 검사한다.
- 공백·유니코드 정규화·대소문자 비교뿐 아니라 Jellyfin의 이름 기반 인물 ID 충돌도 검사한다.
- TMDb 전체 인물의 이름을 미리 검사하지는 않는다. 추후 수집된 다른 인물이 예약된 별칭과 충돌하면 제공자 갱신에 오류를 반환해 조용히 병합되는 것을 방지한다.

### 메타데이터 반영과 갱신

- 영화·시리즈·시즌·에피소드의 배우, 게스트, 제작진 및 시리즈 창작자에 적용한다.
- 이름 기반 `AddPerson` 병합 전에 이름만 바꾸고 TMDb ID, 사진, 배역 등 나머지 정보는 유지한다.
- 작품 하나를 처리할 때 동일한 별칭 스냅샷을 사용한다. 전체 라이브러리 스캔 시작 시점의 스냅샷으로 고정하는 방식은 아니다.
- 웹에서 별칭 저장을 모두 완료하면 검색어·원래 이름·변경 전 별칭으로 관련 작품 갱신을 요청한다. 작품 제목이 아니라 저장된 인물명으로 조회하고 작품 ID 중복을 제거한다.
- 갱신은 높은 우선순위의 수동 요청으로 예약한다. 메타데이터·이미지 `FullRefresh`, `ReplaceAllMetadata`, `ReplaceAllImages`, `RemoveOldMetadata`, `ForceSave`를 적용하며 트릭플레이는 재생성하지 않는다.
- 기존 사용자 지정 메타데이터와 이미지도 교체 대상이다. 반환된 건수는 예약 건수이며 실제 수집 성공 건수가 아니다.
- `PUT`만 직접 호출하면 작품을 자동 갱신하지 않는다. 별도 `RefreshItems` 호출이 필요하다. 별칭 삭제도 기존 인물 연결이나 작품을 자동 갱신하지 않는다.

관련 파일:

- [Emby.Server.Implementations/Library/TmdbPersonAliasService.cs](Emby.Server.Implementations/Library/TmdbPersonAliasService.cs)
- [Jellyfin.Api/Controllers/TmdbPersonAliasesController.cs](Jellyfin.Api/Controllers/TmdbPersonAliasesController.cs)
- [MediaBrowser.Controller/Providers/ITmdbPersonAliasService.cs](MediaBrowser.Controller/Providers/ITmdbPersonAliasService.cs)
- [MediaBrowser.Providers/Plugins/Tmdb/People/TmdbPersonSearchService.cs](MediaBrowser.Providers/Plugins/Tmdb/People/TmdbPersonSearchService.cs)

## 6. 고아 미디어 및 연결 없는 인물 처리

- 기존에 부모 ID가 가리키는 항목이 없는 경우뿐 아니라, 부모 ID 자체가 `NULL`인 미디어 항목도 고아 항목 조회 대상으로 확장했다.
- 영화·시리즈·시즌·에피소드·오디오·사진 등 부모가 필요한 미디어 종류에 한정한다. 부모가 없어도 정상인 인물·스튜디오·장르와 시스템 폴더는 제외한다.
- 인물 조회에서는 작품 연결 테이블에 참조가 남아 있는 인물만 반환하도록 했다. 별칭 적용 후 작품 연결이 끊긴 이전 인물이 조회 결과에 남는 현상을 보정한다.
- 연결 없는 인물 레코드를 이 조회에서 즉시 삭제하는 것은 아니다.

관련 파일:

- [Jellyfin.Server.Implementations/Item/BaseItemRepository.cs](Jellyfin.Server.Implementations/Item/BaseItemRepository.cs)
- [Jellyfin.Server.Implementations/Item/BaseItemRepository.TranslateQuery.cs](Jellyfin.Server.Implementations/Item/BaseItemRepository.TranslateQuery.cs)
- [Jellyfin.Server.Implementations/Item/PeopleRepository.cs](Jellyfin.Server.Implementations/Item/PeopleRepository.cs)

## 7. SQLite 성능 설정

- DB 제공자에 `OptimizeDatabasePerformance`를 추가하고 서버 시작 시 호출하도록 연결했다.
- 예약 최적화에서도 성능 PRAGMA 적용 경로를 사용한다. 개별 PRAGMA 실패는 경고 로그로 남긴다.
- 설정값은 `page_size=4096`, `cache_size=20000`, `locking_mode=EXCLUSIVE`, `synchronous=NORMAL`, `temp_store=MEMORY`, `mmap_size=30000000000`이다.
- 미디어 항목 모델에 `Type`, `TopParentId`, `IsVirtualItem`, `DateCreated`, `SortName` 복합 인덱스 정의를 추가했다. 이름은 `IX_BaseItems_TVShow_DateCreated_Performance`다.
- 코드에 설정·인덱스 정의가 추가된 사실과 운영 DB에서의 실제 적용·성능 향상 여부는 별개다. PRAGMA는 연결 및 기존 DB 상태에 영향을 받으며, 이번 문서 작성에서 운영 DB 상태나 성능을 측정하지 않았다.

관련 파일:

- [Jellyfin.Server/Program.cs](Jellyfin.Server/Program.cs)
- [src/Jellyfin.Database/Jellyfin.Database.Providers.Sqlite/SqliteDatabaseProvider.cs](src/Jellyfin.Database/Jellyfin.Database.Providers.Sqlite/SqliteDatabaseProvider.cs)
- [src/Jellyfin.Database/Jellyfin.Database.Implementations/ModelConfiguration/BaseItemConfiguration.cs](src/Jellyfin.Database/Jellyfin.Database.Implementations/ModelConfiguration/BaseItemConfiguration.cs)

## 8. 유지보수 도구와 개발 환경

| 파일 | 용도 및 주의사항 |
| --- | --- |
| [scripts/Trigger-JellyfinMediaUpdated.ps1](scripts/Trigger-JellyfinMediaUpdated.ps1) | 파일 생성·수정·삭제 경로를 서버에 알려 갱신을 유도한다. |
| [scripts/diag_images.py](scripts/diag_images.py) | 이미지 관련 DB 및 파일 상태 진단 도구다. |
| [scripts/fix_missing_images.py](scripts/fix_missing_images.py) | 읽기 전용 DB 조회로 이미지 복구 대상을 찾는다. 기본은 dry-run이며 `--apply` 시 API로 이미지 교체 갱신을 요청한다. |
| [scripts/clean_orphan_items.py](scripts/clean_orphan_items.py) | 부모가 없는 미디어를 조회한다. 기본은 dry-run이며 `--apply` 시 API 삭제를 요청한다. 실제 미디어가 존재한다고 판정된 항목은 제외하지만, 실행 전 공유 경로 연결 상태와 백업을 확인해야 한다. |

- 이미지 진단 산출물 `broken_images.json`을 Git 제외 목록에 추가했다.
- VS Code Release 빌드 작업과 웹 클라이언트를 함께 사용하는 실행 프로필을 추가했다. 웹 경로는 `c:/jellyfin/jellyfin-web/dist`로 고정되어 있다.
- VS Code 실행 프로필의 서버 DLL 경로에는 `net9.0`이 명시되어 있으므로 실제 빌드 대상 프레임워크와 일치하는지 확인해야 한다.
- 솔루션 Release 구성에서 Keyframes·Hls 프로젝트가 Debug로 빌드되던 매핑을 Release로 수정했다.
- TMDb 별칭 저장소·API, 인물 조회, 스캔 제외 규칙 관련 테스트를 추가·보강했다.

관련 파일: [.vscode/launch.json](.vscode/launch.json), [.vscode/tasks.json](.vscode/tasks.json), [Jellyfin.Server/Properties/launchSettings.json](Jellyfin.Server/Properties/launchSettings.json), [Jellyfin.sln](Jellyfin.sln).

## 주요 커스텀 커밋

| 커밋 | 변경 내용 |
| --- | --- |
| `46d5ce982b`, `11c1d12732` | 초기 설정 기본값 변경 |
| `a7f843fc77` | 스캔 제외 패턴 확장 |
| `3fda568c73` | 라이브러리 썸네일 24시간 갱신 기준 |
| `1c283d0c65`, `1b80b4aa1f` | TMDb 이미지 언어 및 영어 대체 이미지 처리 |
| `9ae934c2cd` | 파일 감시 누락 보완 |
| `6e6d2348d4` | 누락 이미지 메타데이터 관련 보정 |
| `b393642e2d` | 오디오 트랙 동기화 관련 리먹싱 보정 |
| `da189b552a` | Keyframes·Hls Release 구성 수정 |
| `4e1dcc2f69` | TMDb 인물 별칭 저장소·API·제공자 연동 |
| `3de009f189` | 별칭 저장 후 기존 작품 메타데이터 갱신 |
| `45d2f6e80a` | 작품 연결이 없는 인물의 조회 제외 |

표는 커스텀 커밋의 요약이며, 업스트림 병합 과정에서 조정된 내용까지 포함한 현재 동작은 위 기능별 설명을 기준으로 한다.
