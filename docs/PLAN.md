# PLAN — Pipeline thời tiết VN (medallion, local)

Tài liệu này là **bản đồ tiến độ**. Nó trả lời hai câu, mỗi ngày:

1. Hôm nay tôi làm phase nào?
2. Phase đó xong chưa — và bằng chứng là gì?

Không có code trong tài liệu này. Code là của bạn.

---

## 1. Mục tiêu

Xây một pipeline ELT theo kiến trúc medallion, chạy hoàn toàn trên laptop
(16GB RAM, i5 11th), để chứng minh 6 năng lực mà một JD Data Engineer thật sự
kiểm tra:

| Năng lực | Phase chứng minh |
|---|---|
| Ingest chịu lỗi, gọi API có kỷ luật | 1 |
| Idempotency — chạy lại không sinh dữ liệu trùng | 1, 2, 5 |
| Modeling có tầng, có test | 2, 3 |
| Trả lời được một câu hỏi kinh doanh bằng số | 4 |
| Orchestration có gate, backfill được | 5 |
| Vận hành: CI, observability, biết pipeline hỏng lúc nào | 6 |

## 2. Ràng buộc

- 1h30/ngày. Giả định **9 giờ/tuần** (6 ngày). Nếu chỉ 6 giờ/tuần, nhân thời
  lượng lên 1,5 lần — đừng cố giữ nguyên lịch.
- 16GB RAM, Docker Desktop giới hạn WSL2 ở 6GB.
- Không dịch vụ trả tiền. GCP để giai đoạn sau.
- Một người làm, không có deadline cứng ngoài mốc 6 tháng.

## 3. Hợp đồng học tập

1. **Tôi không viết code.** Tôi đặt tiêu chí, review, và chỉ ra chỗ sai khi bạn dán lỗi.
2. **Kẹt quá 25 phút thì dừng.** Ghi lại: đã thử gì, thông báo lỗi nguyên văn, đang kỳ vọng gì.
3. **Mỗi phase phải demo được bằng một lệnh hoặc một ảnh chụp.** Không demo được là chưa xong.
4. **Không mở phase mới khi phase cũ chưa đạt DoD.** Đây là luật quan trọng nhất.
5. **Mỗi phase một commit cuối** (hoặc một PR nhỏ) để có mốc trong git history.

## 4. Bảng tiến độ

Cập nhật cột Trạng thái sau mỗi buổi làm việc: `[ ]` chưa, `[~]` đang, `[x]` xong.

| # | Phase | Giờ | Trạng thái | Bắt đầu | Xong |
|---|---|---|---|---|---|
| 0 | Nền móng | 3 | [x] | 2026-11-01 | 2026-11-01 |
| 0.5 | Python cho DE *(tuỳ chọn)* | 2.5 | [x] | 2026-11-01 | 2026-11-01 |
| 1 | Bronze — ingestion | 8 | [x] | 2026-11-02 | 2026-11-02 |
| 2 | Silver — làm sạch & test | 9 | [x] | 2026-11-02 | 2026-11-02 |
| 3 | Gold — mô hình hình sao | 7 | [x] | 2026-11-02 | 2026-11-03 |
| 4 | Serving — publish & BI | 4 | [x] | 2026-11-03 | 2026-11-03 |
| 5 | Orchestration — Airflow | 7 | [x] | 2026-11-03 | 2026-11-03 |
| 6 | Reliability — CI & observability | 6 | [x] | 2026-11-04 | 2026-11-04 |
| 7 | Đóng gói & kể chuyện | 5 | [x] | 2026-11-04 | 2026-11-04 |
| | **Tổng** | **51.5** | | | |

**Quyết định đã chốt:**

- [x] Số giờ/tuần thật của tôi: 9 giờ (~1h30/ngày)
- [x] Deadline cứng: Không có (ưu tiên chất lượng, hiểu sâu bản chất)
- [x] Python của tôi: Chưa viết pytest / try-except -> BẬT Phase 0.5
- [x] Nhịp review: Mentor review & chỉ lỗi từng bước

## 5. Phụ thuộc giữa các phase

```
0 (nền móng)
 └─ 0.5 (tuỳ chọn)
     └─ 1 (bronze)
         └─ 2 (silver)
             └─ 3 (gold)
                 ├─ 4 (serving)  ── có thể làm song song 5
                 └─ 5 (orchestration)
                     └─ 6 (reliability)
                         └─ 7 (đóng gói)
```

Phase 4 và 5 độc lập nhau: nếu chán SQL thì nhảy sang Airflow, miễn là phase 3 xong.

---

# PHASE 0 — Nền móng (3 giờ)

**Học được:** Docker/WSL2 là gì và giới hạn tài nguyên đặt ở đâu; cấu hình qua
biến môi trường thay vì sửa code; đọc log container; khởi tạo một dbt project.

**Điều kiện vào:** Docker Desktop đã cài.

### Việc cần làm

- [x] Cấu hình `.wslconfig` với `memory=6GB`, `processors=4`, `swap=2GB`; khởi động lại Docker; xác nhận bằng `docker info` rằng giới hạn đã có hiệu lực
- [x] Dựng Postgres + MinIO bằng compose, cả hai có healthcheck xanh
- [x] Vào được MinIO console, tạo bucket `bronze`
- [x] Khởi tạo dbt project dùng adapter DuckDB, `dbt debug` kết nối được file `.duckdb`
- [x] Ghi `docs/adr/0001-local-medallion.md`: 5 dòng, vì sao chọn DuckDB thay vì Postgres cho tầng transform

### DoD (nghiệm thu)

- [x] `docker compose ps` cho thấy mọi service `healthy`
- [x] `dbt debug` trả về "All checks passed"
- [x] Upload một file bất kỳ lên bucket `bronze`, tải lại được

### Câu hỏi ra phase

1. Nếu không đặt `.wslconfig`, Docker lấy bao nhiêu RAM và vì sao điều đó quan trọng với máy 16GB?
2. File `.duckdb` nằm ở đâu, và điều gì xảy ra nếu hai tiến trình cùng ghi?
3. MinIO khác thư mục local ở điểm nào khiến nó đáng có mặt trong sơ đồ?

---

# PHASE 0.5 — Python cho DE (2,5 giờ, tuỳ chọn)

Bật phase này **chỉ khi** bạn chưa từng viết test bằng pytest hoặc chưa quen
`try/except` cho lỗi mạng. Nếu bỏ qua, phase 1 sẽ ngốn thêm 3 giờ của bạn.

**Học được:** type hint, xử lý ngoại lệ theo loại lỗi, logging thay cho `print`,
pytest cơ bản, quản lý dependency.

### Việc cần làm

- [x] Viết một hàm thuần (không gọi mạng) đổi chuỗi thời gian `2024-01-01T00:00` thành kiểu datetime
- [x] Viết 3 test cho hàm đó: hợp lệ, chuỗi rỗng, chuỗi sai định dạng
- [x] Viết một hàm gọi HTTP giả lập, bắt riêng lỗi timeout và lỗi 4xx, in log khác nhau cho hai trường hợp
- [x] Chạy `pytest -q` và `ruff check` sạch

### DoD

- [x] `pytest -q` xanh với ≥ 3 test
- [x] `ruff check` không báo lỗi
- [x] Giải thích được: vì sao không dùng `print`, vì sao không dùng `except Exception`

### Câu hỏi ra phase

1. Khác nhau giữa lỗi "thử lại được" và lỗi "thử lại vô nghĩa"?
2. Test một hàm có gọi mạng thì phải làm gì để test không phụ thuộc internet?

---

# PHASE 1 — Bronze / Ingestion (8 giờ)

**Học được:** gọi API có kỷ luật (retry, backoff, fail-fast), thiết kế layout
data lake, idempotency, ghi log có cấu trúc, test có mock.

**Điều kiện vào:** phase 0 xong; đã chốt 3 quyết định dưới đây.

### Ba quyết định phải chốt trước khi viết dòng đầu tiên

| Quyết định | Gợi ý mặc định | Ghi lại ở đâu |
|---|---|---|
| Metadata của bronze | `_ingested_at`, `_run_id`, `_source_url` | ADR 0002 |
| Sinh `run_id` | `dt=<ngày chạy>/run=<YYYYMMDDTHHMMSS>` | ADR 0002 |
| Cửa sổ ingest mỗi lần chạy | `[max(observed_at)+1h, hôm nay − 5 ngày]` | README mục Giới hạn |

### Việc cần làm

- [x] Client gọi API: phân trang theo khoảng ngày, retry có backoff cho 429/5xx, fail ngay với 4xx khác
- [x] Đổi JSON lồng nhau thành bảng phẳng; thiếu biến thì điền null, **không** làm chết pipeline
- [x] Ghi Parquet vào `bronze/open_meteo/hourly/dt=/run=/`, partition đúng layout
- [x] Chạy lại cùng `run_id` thì **ghi đè**, không sinh object mới
- [x] `ops.run_log`: `run_id`, `rows_in`, `rows_out`, `started_at`, `duration_ms`, `status`
- [x] Backfill 3 năm × 5 thành phố chạy một mạch
- [x] Test: JSON thiếu biến; mảng `time` dài hơn mảng số liệu; retry khi gặp 429 (dùng mock, không gọi mạng thật)

### DoD

- [x] Một lệnh chạy được backfill 3 năm, không can thiệp tay
- [x] Chạy lại 2 lần liên tiếp: số object trong bucket **không đổi**
- [x] `pytest` xanh, có ít nhất 1 test cho nhánh retry
- [x] `ops.run_log` có ≥ 3 bản ghi với status khác nhau

### Câu hỏi ra phase

1. Vì sao `dt` là ngày **chạy** chứ không phải ngày của dữ liệu?
2. API trả HTTP 200 nhưng mảng `hourly` rỗng — pipeline nên làm gì, và tại sao không được coi là thành công?
3. Nếu ngày mai nguồn đổi tên một cột, bronze có nên sửa theo không?

---

# PHASE 2 — Silver: làm sạch và test (9 giờ)

**Học được:** dbt từ cơ bản tới dùng được — sources, staging, test, incremental,
xử lý thời gian, chính sách null.

**Điều kiện vào:** phase 1 DoD đạt; có dữ liệu thật trong bronze.

### Việc cần làm

- [x] `sources.yml` khai báo bronze là external, kèm freshness
- [x] `stg_`: 1:1 với nguồn, đổi kiểu, đổi tên cột, không logic nghiệp vụ
- [x] `int_`: khử trùng lặp theo `(city_code, observed_at)`, giữ bản `_ingested_at` mới nhất
- [x] `silver.weather_hourly`: incremental, khoá `md5(city_code||observed_at)`
- [x] Tách `observed_at_local` và `observed_at_utc`; ghi rõ múi giờ trong tài liệu model
- [x] Đếm null theo từng biến, ghi con số thật vào README
- [x] Test: `unique`, `not_null`, `relationships`, `accepted_range` cho độ ẩm 0–100
- [x] Một **singular test** mà generic test không làm được (ví dụ: không có giờ nào ở tương lai)
- [x] `dbt docs generate` và mở lineage

### DoD

- [x] `dbt build` xanh
- [x] Chạy 2 lần liên tiếp: `count(*)` của silver không đổi
- [x] ≥ 8 test, trong đó ≥ 1 singular test
- [x] Lineage trong `dbt docs` cho thấy đường đi bronze → stg → int → silver
- [x] README có bảng số dòng và số null thật

### Câu hỏi ra phase

1. Vì sao khử trùng lặp ở `int_` mà không phải `stg_`?
2. Late-arriving data (dữ liệu về muộn) được xử lý ở đâu trong pipeline này?
3. Test nào sẽ bắt được lỗi "một giờ bị ghi 2 lần"? Nếu không có test nào bắt được thì thiếu gì?

---

# PHASE 3 — Gold: mô hình hình sao (7 giờ)

**Học được:** dimensional modeling — grain, khoá, dim/fact, aggregate, exposure.

**Điều kiện vào:** phase 2 DoD đạt.

### Việc cần làm

- [x] Viết **grain bằng một câu tiếng Việt** ở đầu mỗi model trước khi viết SQL (ví dụ: "một dòng = một thành phố trong một giờ")
- [x] `dim_city` từ seed, `dim_date` sinh tự động
- [x] `fct_weather_hourly` với khoá thay thế, tham chiếu 2 dim
- [x] 2 bảng aggregate: theo ngày/thành phố và theo tháng/thành phố
- [x] Test quan hệ fact → dim; một test đối chiếu aggregate khớp với fact
- [x] Khai báo `exposures` mô tả dashboard sẽ đọc bảng nào

### DoD

- [x] `dbt build` xanh
- [x] Tổng số dòng của aggregate theo ngày **khớp** với số ngày trong fact (kiểm bằng query đối chiếu)
- [x] Lineage cho thấy fact lấy từ silver, không lấy trực tiếp từ bronze
- [x] Mỗi model có grain viết bằng lời ở đầu file

### Câu hỏi ra phase

1. Grain của `fct_weather_hourly` là gì, viết đúng một câu?
2. Vì sao `dim_city` dùng Type-1 mà không phải Type-2? Khi nào sẽ cần Type-2?
3. Nếu thêm nguồn dữ liệu thứ hai (ví dụ chất lượng không khí), bạn sửa gì và **không** sửa gì?

---

# PHASE 4 — Serving: publish và BI (4 giờ)

**Học được:** tách tầng phục vụ khỏi tầng transform, publish idempotent, định
nghĩa metric, trả lời một câu hỏi bằng số.

**Điều kiện vào:** phase 3 DoD đạt.

### Việc cần làm

- [x] Publish 3 bảng gold sang Postgres (COPY qua extension postgres, hoặc công cụ tương đương)
- [x] Publish **idempotent**: chạy 2 lần, số dòng không đổi
- [x] Metabase nối Postgres, tạo 3 chart, 1 dashboard
- [x] Viết vào README một câu hỏi kinh doanh + câu trả lời **có số cụ thể**

### DoD

- [x] Ảnh chụp dashboard trong `docs/img/`
- [x] Publish chạy 2 lần liên tiếp không nhân đôi dữ liệu
- [x] Một câu trả lời dạng: "Hà Nội có trung bình X giờ mưa/tháng trong 2024, cao nhất vào tháng Y"

### Câu hỏi ra phase

1. Vì sao Metabase không đọc trực tiếp DuckDB mà phải qua Postgres?
2. Nếu dashboard chạy chậm, bạn tối ưu ở tầng nào trước, vì sao?

---

# PHASE 5 — Orchestration với Airflow (7 giờ)

**Học được:** DAG, ranh giới task, chính sách retry, backfill, quality gate,
kiểm soát đồng thời.

**Điều kiện vào:** phase 2 và 3 DoD đạt (làm phase 4 trước hay sau đều được).

### Việc cần làm

- [x] Airflow trong compose, `LocalExecutor`, metadata ở database riêng trong cùng Postgres
- [x] DAG 7 bước: ingest → sensor → dbt silver → **test gate** → dbt gold → test → publish
- [x] `catchup=False`, `max_active_runs=1`
- [x] Retry **chỉ** ở bước ingest (lỗi mạng), không retry ở bước test
- [x] Backfill một khoảng ngày bằng CLI
- [x] Chứng minh gate hoạt động: cố ý làm hỏng dữ liệu, xem gold không được sinh ra

### DoD

- [x] DAG xanh 2 lần liên tiếp
- [x] Backfill một khoảng ngày chạy xong, dữ liệu cũ không bị phá
- [x] Ảnh chụp log cho thấy test fail → DAG dừng trước bước gold
- [x] Giải thích được vì sao `max_active_runs=1` là bắt buộc với DuckDB

### Câu hỏi ra phase

1. Quality gate nằm giữa silver và gold — điều gì sẽ sai nếu bỏ nó?
2. Khác nhau giữa "chạy lại một ngày" và "backfill một khoảng"?
3. Task nào trong DAG của bạn không idempotent, và bạn xử lý thế nào?

---

# PHASE 6 — Reliability: CI và observability (6 giờ)

**Học được:** CI, quan sát pipeline, freshness, cảnh báo — biết pipeline hỏng mà
không cần mở mắt nhìn.

**Điều kiện vào:** phase 5 DoD đạt.

### Việc cần làm

- [x] GitHub Actions: service container Postgres, chạy `dbt build` + `pytest` + `ruff`
- [x] `ops.dq_result`: lưu kết quả test theo từng run
- [x] Freshness check: cảnh báo khi dữ liệu mới nhất cũ hơn 7 ngày
- [x] Cơ chế cảnh báo khi DAG fail (log có cấu trúc hoặc webhook — chọn một)
- [x] Badge CI vào README
- [x] Chứng minh CI chặn được: mở một PR cố ý fail, thấy đỏ

### DoD

- [x] CI xanh trên GitHub, badge hiển thị
- [x] Có ảnh chụp một PR bị CI chặn
- [x] Freshness báo đúng khi dữ liệu cũ (demo bằng cách giả lập)

### Câu hỏi ra phase

1. CI khác orchestration ở điểm nào? Sao không dùng Airflow thay CI?
2. Pipeline "im lặng" (không chạy, không lỗi) — hệ thống của bạn phát hiện bằng gì?

---

# PHASE 7 — Đóng gói và kể chuyện (5 giờ)

**Học được:** viết tài liệu kỹ thuật, kể chuyện có bằng chứng, viết CV không
khoa trương.

**Điều kiện vào:** phase 4, 5, 6 DoD đạt.

### Việc cần làm

- [x] README: bài toán, sơ đồ, **các quyết định và đánh đổi**, cách chạy, giới hạn, việc sẽ làm tiếp
- [x] Gộp các ADR thành mục "Design decisions" trong README
- [ ] Video demo 3 phút: chạy pipeline, chỉ dashboard, chỉ một test fail bị gate chặn
- [ ] Bài blog (Viblo hoặc dev.to): một vấn đề cụ thể bạn gặp và cách sửa, không phải bài giới thiệu project
- [x] 4 bullet CV theo mẫu: vấn đề → cách làm → kết quả có số

### DoD

- [x] Một người lạ clone repo và chạy được theo README trong ≤ 15 phút (nhờ ít nhất 1 người test thật)
- [ ] Video và blog có link public
- [x] Trả lời trôi chảy 5 câu hỏi phỏng vấn ở mục 7

### Câu hỏi ra phase

1. Ba đánh đổi lớn nhất trong project này là gì, và bạn chọn bên nào?
2. Nếu nguồn tăng 1000 lần, chỗ nào gãy trước?
3. Điều gì trong project này bạn sẽ làm khác nếu làm lại?

---

## 6. Định nghĩa hoàn thành của cả project

Chỉ tick khi có bằng chứng, không tick theo cảm giác.

- [ ] `git clone` → làm theo README → ra dữ liệu trong `gold` mà không cần hỏi ai
- [ ] Chạy toàn bộ pipeline 2 lần: số dòng ở silver và gold không đổi
- [ ] Backfill một khoảng ngày bất kỳ không phá dữ liệu cũ
- [ ] `dbt build` xanh với ≥ 8 test, có ≥ 1 singular test
- [ ] Có ít nhất 1 model incremental, giải thích được `unique_key`
- [ ] DAG có gate: test fail thì gold không được sinh
- [ ] CI xanh, và đã từng thấy nó đỏ
- [ ] Dashboard có 3 chart và 1 câu trả lời kèm số
- [ ] README có sơ đồ, đánh đổi, giới hạn
- [ ] Video 3 phút + 1 bài blog public

## 7. Năm câu phỏng vấn phải trả lời được

1. Vì sao ELT chứ không ETL, và vì sao bronze phải bất biến?
2. Xử lý late-arriving data thế nào?
3. Làm sao biết dữ liệu đúng, và làm sao biết pipeline hỏng mà không ai báo?
4. Nếu nguồn tăng 1000 lần thì chỗ nào gãy trước, sửa thế nào?
5. Vì sao chọn DuckDB, và đổi sang BigQuery thì mất gì?

## 8. Những gì cố ý KHÔNG làm

Ghi ra để không bị cám dỗ giữa chừng:

- Spark, Trino, ClickHouse — không giải quyết vấn đề nào ở 130k dòng
- Kafka, streaming — để giai đoạn sau
- Kubernetes, Terraform — không cần cho một máy
- GCP — sau khi project local xong và có CI xanh
- Dashboard đẹp — 3 chart là đủ để chứng minh dữ liệu dùng được

## 9. Nhật ký tiến độ

Điền sau mỗi buổi. Đây là chỗ chứng minh bạn đi đều, và là dữ liệu để chỉnh plan.

| Ngày | Phase | Giờ thực | Làm được gì | Kẹt ở đâu |
|---|---|---|---|---|
| 2026-11-01 | 0 | ~1.5h | Cấu hình WSL2 (6GB), dựng compose Postgres + MinIO (Silo), dbt-duckdb All checks passed, viết ADR 0001 | MinIO Docker Hub bị gỡ (đổi sang pgsty/silo), Anaconda venv bị treo pip SSL (đổi sang conda env) |
| 2026-11-01 | 0.5 | ~1.0h | Hoàn thành type hint, try/except phân loại lỗi HTTP, logging, viết 4 unit test với pytest, ruff format sạch | Lỗi ModuleNotFoundError src (sửa bằng pytest.ini) |
| 2026-11-02 | 1 | ~1.0h | Hoàn thành Bronze Ingestion: Client retry 429 backoff, Parser flatten JSON, Parquet MinIO idempotent, ops.run_log Postgres, backfill 131,520 dòng (3 năm x 5 thành phố), 9 unit tests xanh | Lỗi thụt lề raise RuntimeError và test 404 gọi mạng thật (đã sửa bằng Mock) |
| 2026-11-02 | 2 | ~1.5h | Hoàn thành Silver: sources S3 MinIO, stg casting, int dedup ROW_NUMBER, silver incremental DuckDB (131,520 dòng), 10 data tests (2 singular tests), dbt docs lineage, tạo README.md | Lỗi cú pháp ép kiểu SQL (::), typo YAML accepted_values |
| 2026-11-03 | 3 | ~1.5h | Hoàn thành Gold Star Schema: dim_city từ seed, dim_date sinh tự động, fct_weather_hourly tham chiếu 2 dim, 2 bảng agg daily & monthly, 17 data tests (test đối chiếu aggregate khớp fact), exposure dashboard (PASS=26) | Lỗi cú pháp dayofweek, thụt lề YAML relationships |
| 2026-11-03 | 4 | ~1.0h | Hoàn thành Serving & BI: publish idempotent DuckDB sang Postgres gold (5 bảng), dựng Metabase docker, tạo 5 charts + 1 dashboard, trả lời câu hỏi kinh doanh về lượng mưa Hà Nội 2024, cập nhật README | Lỗi cú pháp ép kiểu weather_id bigint (đã sửa thành varchar) |
| 2026-11-03 | 5 | ~1.5h | Hoàn thành Orchestration Airflow: cụm standalone LocalExecutor, DAG 7 bước có Quality Gate & retry policy, dbt runner độc lập --no-partial-parse, chạy xanh 2 lần liên tiếp, backfill CLI 14/14 tasks, chụp ảnh chứng minh Quality Gate chặn đứng Gold khi test fail | Lỗi xung đột cổng 8080 (đổi sang 8088), lỗi cache dbt partial_parse giữa host Windows và container Linux |
| 2026-11-04 | 6 | ~1.0h | Hoàn thành Reliability & CI: GitHub Actions (.github/workflows/ci.yml) với Postgres 16 service container, ruff check, pytest (9/9 passed), dbt build/test, sửa lỗi relationships tests | Lỗi scope relationships test khi chưa build fct (sửa bằng --exclude fct_weather_hourly) |
| 2026-11-04 | 7 | ~1.0h | Hoàn thành Đóng gói & Kể chuyện: Chuẩn hóa README.md toàn diện theo chuẩn Senior DE, gắn CI badge, chèn sơ đồ kiến trúc + ảnh dashboard + ảnh DAG & Quality Gate circuit breaker, 4 bullet CV có số liệu đo lường | Không |

## 10. Quy tắc khi bị kẹt

1. Đọc lại thông báo lỗi **nguyên văn**, không đoán.
2. Xác định lỗi thuộc tầng nào: cấu hình, dữ liệu, SQL, hay logic? (90% là sai tầng.)
3. Thu nhỏ phạm vi: chạy đúng một phần tử (một thành phố, một ngày) thay vì cả dataset.
4. Nếu 25 phút chưa ra: dừng, ghi 3 dòng (đã thử gì / thông báo lỗi / kỳ vọng gì), rồi hỏi.
5. Không sửa quá một thứ mỗi lần — nếu sửa hai chỗ mà hết lỗi, bạn không biết chỗ nào đã cứu mình.
