# OCEAN wardrobe — H&M shopping behavior & market basket recommendations

Web thời trang tông xanh biển, logo móc áo + sóng biển, tham khảo bố cục tìm kiếm/danh mục/thẻ sản phẩm của Shopee. Bấm vào sản phẩm để xem thông tin và **Khách hàng cũng thường mua**. Có tìm kiếm, lọc, phân trang, giỏ trên thiết bị, gợi ý theo cả giỏ và dashboard phân tích.

Stack: **HTML/CSS/JavaScript + Python API + MongoDB + PySpark FP-Growth**. Dùng Python thay PHP để pipeline và API dùng cùng logic gợi ý. API local dùng thư viện chuẩn Python, không cần framework. Đây là ứng dụng phục vụ đề tài, chưa có tài khoản, đơn hàng hoặc thanh toán.

## Trạng thái bàn giao

- Mô hình đang dùng: `c95bac01b5b4470baeaf58b18620f5d7`, khai phá cấp mẫu `product_code` trên toàn bộ nguồn 31.788.324 giao dịch; 31.292.772 giao dịch trước cutoff và 9.014.490 giỏ huấn luyện. Hoàn tất exit=0, OOMKilled=false.
- Validation chọn support=0.00002, confidence=0.10, lift>1 từ sáu cấu hình: 4.141 luật cấp mẫu và 27.817 tập phổ biến. Có 6.620 SKU áp dụng được luật khi xem riêng (6,272% danh mục), so với 965 SKU ở mô hình trước; web ánh xạ gợi ý về SKU cụ thể và loại mẫu đã chọn.
- Trên 10.000 giỏ test cấp mẫu sau 15/09, FP-Growth có 306 hit (Recall@6=3,06%), popularity 143 hit (1,43%), hybrid 400 hit (4,00%). Coverage giỏ FP-Growth=24,50%; hybrid=100% nhờ dự phòng. Các chỉ số cấp mẫu không so trực tiếp với chỉ số SKU cũ. Đánh giá chi tiết và giới hạn: `artifacts/danh-gia-mo-hinh-cap-mau.md`.
- Trang danh mục có nút **Có gợi ý mua kèm**, thẻ sản phẩm có nhãn tương ứng. API `/api/products?buy_together=1` chỉ lấy sản phẩm có luật vế trái một item; luật nhiều item chỉ áp dụng khi giỏ đáp ứng đầy đủ điều kiện. Dashboard có độ phủ danh mục và bảng so sánh validation. Kết quả lưu tại `artifacts/training-improved.json`.

- Demo chạy được ngay, không cần MongoDB/Spark. 16 sản phẩm, ảnh SVG tự vẽ, giá và 49 luật **tổng hợp minh họa**, không phải kết quả H&M.
- Đã viết pipeline MongoDB và notebook thay thế; đã kiểm tra tích hợp CSV → MongoDB thật → Spark → FP-Growth → MongoDB kết quả → API Store trên 14 giao dịch tổng hợp trong database riêng. Dataset thật đã nhập đủ 31.788.324 giao dịch vào MongoDB. Lần huấn luyện toàn bộ đầu tiên đã hoàn tất: 9.096.428 giỏ huấn luyện và 366 luật với support=0.0001, confidence=0.3, cutoff=2020-09-15. Lỗi Java heap trước đó được xử lý bằng phân vùng và lưu tạm trên đĩa. Chỉ số cũ dùng protocol ẩn item cuối; không so trực tiếp với protocol seed mới.
- Không sửa notebook gốc ở Downloads. Notebook mới: `training/hm_mongodb_fpgrowth.ipynb`, tự chứa mã pipeline để có thể chạy trên Kaggle hoặc Jupyter.
- Đã đạt 10 kiểm tra API/Spark và hai luồng tích hợp MongoDB: API, áp dụng đầy đủ antecedent/gợi ý trùng, làm sạch Spark/giỏ theo kênh, FP-Growth và đánh giá trên dữ liệu nhỏ. Notebook hợp lệ về schema và cú pháp. Dùng các lệnh dưới đây để huấn luyện hoặc chọn cấu hình mới. Các bài kiểm tra mở rộng thêm seed, temporal split, popularity/hybrid và dự phòng hiển thị riêng.

## Cấu trúc

```text
backend/                 API + truy vấn MongoDB + áp dụng luật
frontend/                HTML, CSS, JS, logo và ảnh minh họa
data/demo.json           Dữ liệu tổng hợp, chỉ dùng ở APP_MODE=demo
data/raw/                Dataset và images tải từ Kaggle (không đưa vào Git)
training/download.py     Tải dataset bằng kagglehub
training/import_data.py  CSV → MongoDB, batch và phiên bản dữ liệu
training/pipeline.py     MongoDB → Spark → FP-Growth → MongoDB
training/hm_mongodb_fpgrowth.ipynb  Notebook đã sửa, không có output cũ
tests/                   Kiểm tra logic gợi ý và API
compose.yaml             MongoDB + web + training trên Docker
.env.example             Cấu hình mẫu, không có mật khẩu
```

## Chạy demo

Python 3.11 hoặc 3.12:

```powershell
python -m backend.app
```

Mở **http://127.0.0.1:8000**. Chế độ mặc định là demo, dùng thư viện chuẩn và không cần cài toàn bộ requirements. Có thể dùng `./start-demo.ps1` sau khi có Python.

## Chạy thật — khuyến nghị Docker trên Windows

Docker Desktop cần được cài và chạy trước. Dataset đầy đủ có nhiều giao dịch và ảnh: cần đủ dung lượng lưu dataset, MongoDB nguồn và các phiên bản kết quả; chạy thử nhỏ trước. Không dùng một MongoDB có dung lượng thấp cho toàn bộ dataset. Chỉ mở cổng MongoDB trên localhost trong compose.

```powershell
# 1. Tạo cấu hình (đổi APP_MODE=demo thành mongo sau khi train thành công)
Copy-Item .env.example .env

# 2. MongoDB và image ứng dụng
docker compose up -d mongo
docker compose build

# 3. Tải dataset: dùng tài khoản Kaggle của bạn, đã chấp nhận quy tắc competition
# Có thể tải bằng notebook Kaggle rồi chép CSV và images vào data/raw.
# Hoặc cài requirements trên máy và chạy:
python -m training.download --output data/raw

# 4. Kiểm tra trước với 100.000 giao dịch đầu (không dùng kết quả này cho báo cáo cuối)
docker compose run --rm training python -m training.import_data --limit 100000
docker compose run --rm training python -m training.pipeline --min-support 0.001

# 5. Nếu chạy thử thành công, nhập toàn bộ và huấn luyện với temporal holdout
docker compose run --rm training python -m training.import_data
docker compose run --rm training python -m training.pipeline --min-support 0.0001 --min-confidence 0.3 --cutoff 2020-09-15

# 6. Đặt APP_MODE=mongo trong .env rồi khởi động web
docker compose up -d web
```

Nếu Kaggle cần xác thực, thực hiện đăng nhập trong môi trường của bạn theo hướng dẫn KaggleHub. Không gửi API key vào chat hoặc commit vào repository. Nếu dùng lệnh download trong container, cần cung cấp xác thực Kaggle đúng cách; tải ngoài container và chép vào `data/raw` là cách đơn giản hơn.

## Chạy thật bằng Python / MongoDB cục bộ hoặc Atlas

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
Copy-Item .env.example .env
# Sửa URI/database/data path trong .env, cài Java 17.
.venv\Scripts\python -m training.download
.venv\Scripts\python -m training.import_data --limit 100000
.venv\Scripts\python -m training.pipeline --min-support 0.001
# Sau khi thử, nhập lại toàn bộ để có mô hình chính thức.
.venv\Scripts\python -m training.import_data
.venv\Scripts\python -m training.pipeline --cutoff 2020-09-15
# Sửa APP_MODE=mongo trong .env trước khi chạy web
.venv\Scripts\python -m backend.app
```

Spark 3.5.3 + Java 17 + Mongo Connector `org.mongodb.spark:mongo-spark-connector_2.12:10.4.1`. Lần đầu cần Internet để tải Connector từ Maven. Chạy Spark trên Windows có thể cần thiết lập Hadoop/Python phù hợp; Docker Linux là lựa chọn ổn định hơn. Đường dẫn tương đối được hiểu từ thư mục dự án.

### Chạy full data khi Spark ngừng phản hồi

`Answer from Java side is empty` / `ConnectionRefusedError` là lỗi kết nối sau khi JVM ngừng phản hồi, không tự xác định được nguyên nhân. Đọc lỗi đầu tiên phía trên và giữ container nếu cần kiểm tra `State.OOMKilled`.

Cấu hình mặc định dùng `SPARK_MASTER=local[4]`, `SPARK_PARTITIONS=64`, driver 4 GB, các bảng giao dịch/giỏ lớn lưu tạm trên đĩa và MongoDB WiredTiger cache 1 GB. Riêng `.env` của máy này đã đặt driver 6 GB theo tài nguyên Docker khoảng 11,35 GB. `MONGO_CACHE_GB` chỉ giới hạn cache WiredTiger, không phải toàn bộ RAM MongoDB. FP-Growth cũng dùng số partition đã cấu hình. Những thay đổi này vẫn xử lý toàn bộ dữ liệu, không sampling. Chạy build lại để container dùng code mới:

```powershell
docker compose build training
docker compose up -d mongo
New-Item -ItemType Directory -Force logs | Out-Null
docker compose run --name hm-training-full training python -u -m training.pipeline --min-support 0.0001 --min-confidence 0.3 --cutoff 2020-09-15 2>&1 | Tee-Object logs/training-full.log
```

Lệnh training không có `--rm` để giữ trạng thái container nếu lỗi. Dùng tên khác nếu `hm-training-full` đã tồn tại. Không cần nhập lại raw data đã có `active_source`. Các bảng lớn lưu tạm dưới `/tmp/hm-spark` trong Docker: cần đủ dung lượng đĩa Docker cho cache và shuffle; dữ liệu tạm không được dùng để resume sau khi container mất. Bản model chỉ được kích hoạt sau khi ghi thành công toàn bộ kết quả.

```powershell
docker inspect hm-training-full --format '{{.State.OOMKilled}} {{.State.ExitCode}} {{.State.Error}}'
```

OOMKilled=true chứng tỏ container bị OOM-kill; false không loại trừ lỗi Java heap, ổ đĩa hoặc nguyên nhân khác. Không tăng heap lên vượt tài nguyên Docker; MongoDB và bộ nhớ ngoài heap cũng cần RAM.

Với Atlas: URI trong `.env`, database giống notebook, quyền đọc/ghi đúng DB và kết nối từ môi trường chạy Spark/API. Notebook trên Kaggle có thể dùng Secret `MONGODB_URI`. Không mở rộng quyền truy cập mạng một cách máy móc; chỉ cho phép môi trường thực sự chạy chương trình.

## Pipeline và các sửa đổi so với notebook cũ

1. **Thu thập/lưu trữ:** đọc CSV giữ mọi cột dưới dạng chuỗi, lưu raw articles/customers/transactions vào collection theo run. Nạp 5.000 dòng mỗi batch. Không dùng `collect()` cho toàn bộ tập dữ liệu. Manifest `active_source` chỉ đổi sau khi cả ba bảng đã nhập xong.
2. **Làm sạch Spark:** parse ngày, giá double, kênh 1/2; loại ID sản phẩm không hợp lệ, giá thiếu/NaN/không dương/vô hạn và sản phẩm không có trong articles. ID có 1–10 chữ số được đệm thành 10 chữ số. Dòng lặp có thể là mua nhiều đơn vị, nên không xóa khỏi dữ liệu giao dịch; giỏ dùng `collect_set`.
3. **Tạo giỏ:** `customer_id + t_dat + sales_channel_id`. Không có invoice ID nên đây là quy ước; nhiều đơn cùng ngày và kênh có thể bị gộp. Chưa thể tuyên bố đây là hóa đơn thật.
4. **FP-Growth:** giữ cả singleton baskets trong mẫu số để tránh tăng giả tỷ lệ mua cùng. Chạy một cấu hình có tham số; mặc định support 0.0001, confidence 0.3, lift > 1. Ngưỡng cần điều chỉnh bằng thực nghiệm. Giữ đầy đủ mảng antecedent/consequent.
5. **Đánh giá:** nếu có `--cutoff`, train chỉ dùng ngày <= cutoff. Gợi ý từ các item còn lại trong giỏ tương lai sau khi ẩn một item theo hash của giỏ + item + seed (mặc định 42). Tối đa 10.000 giỏ >=2 items, chọn xác định theo hash + seed. Baseline popularity dùng tần suất xuất hiện trong giỏ huấn luyện; hybrid dùng luật và chỉ dự phòng khi không có luật. Không dùng tần suất từ các ngày tương lai. Báo cáo precision@6 (mẫu số 6), recall@6 và tỷ lệ giỏ có gợi ý. Không đánh giá cold-start riêng; protocol này không tương đương kiểm nghiệm người dùng thực tế. Không có cutoff thì evaluation=null và UI ghi chưa đánh giá.
6. **Lưu kết quả:** products, rules, itemsets, baskets, clean_transactions vào collection phiên bản mới; tạo index rồi công bố `active_model`. Giữ lịch sử `model_runs`. API chọn products và rules theo cùng manifest. Cần quản lý dung lượng/lưu giữ các run cũ; không tự xóa dữ liệu của bạn.
7. **Gợi ý:** chỉ áp dụng khi `antecedent ⊆ giỏ`; loại item trong giỏ, hợp nhất gợi ý trùng và xếp confidence → lift → support. Trang một sản phẩm chỉ dùng được antecedent một item; giỏ đầy đủ dùng được luật nhiều item. Không có luật thì trả rỗng, không thay bằng bestseller dưới nhãn mua kèm.

Số liệu notebook cũ không thể giữ nguyên sau khi đổi quy ước giỏ/mẫu số. Không ghi cứng số giao dịch/luật vào báo cáo. Đây là **market basket analysis**, không phải cá nhân hóa theo từng customer như mục tiêu xếp hạng của competition Kaggle.

## Schema MongoDB và ảnh

MongoDB trong Docker được mở ra Windows tại `mongodb://127.0.0.1:27018` để tránh trùng MongoDB cài sẵn ở cổng 27017. Compass hoặc Python chạy ngoài Docker cần dùng URI cổng 27018; nếu dùng Python thì sửa `MONGODB_URI` trong `.env`. Các container web/training vẫn kết nối nội bộ qua `mongodb://mongo:27017`. Có thể đổi cổng Windows bằng `MONGO_HOST_PORT` trong `.env`.

`metadata.active_source`: run_id, collections, counts, sample, imported_at.

`metadata.active_model`: run_id, source_run, collections, parameters, trained_at, counts, evaluation, cutoff, top_categories.

`products_<run>`: article_id, name, category, product_type, colour, description, purchase_count, price_normalized, image_url.

`rules_<run>`: antecedent[], consequent[], support, confidence, lift.

`itemsets_<run>`: items[], freq, support. Raw/clean transactions và baskets không được công khai qua API.

Ảnh giữ trên ổ đĩa: `data/raw/images/010/0108775015.jpg`; MongoDB lưu URL. Chọn `article_id=0108775015` sẽ tải `/images/010/0108775015.jpg`. Nếu thiếu ảnh, UI báo rõ thay vì vỡ bố cục.

Giá H&M trong dữ liệu đã chuẩn hóa, không tự quy đổi ra VNĐ. Web thật hiển thị **trung bình giá lịch sử chuẩn hóa** trong tập train. VNĐ chỉ xuất hiện ở sản phẩm tổng hợp demo. Không suy diễn sizes/stock/review/discount/shipping nếu dataset không có.

## API

| Endpoint | Mục đích |
| --- | --- |
| GET /api/health | Trạng thái và mode |
| GET /api/products?q=&category=&sort=popular&page=1&page_size=12 | Danh mục, search, phân trang |
| GET /api/products/{article_id} | Chi tiết sản phẩm |
| GET /api/recommendations?items=id1,id2 | Gợi ý top 6 cho 1–30 sản phẩm |
| GET /api/categories | Các nhóm sản phẩm |
| GET /api/metrics | Thống kê, tham số và đánh giá |

Chế độ mongo không tự rơi về demo khi mất kết nối: API trả 503 và UI hiển thị lỗi. Chế độ nhập mẫu có cờ `sample` được ghi rõ trên web.

## Kiểm tra

```powershell
python -m unittest discover -s tests -v
python -m compileall -q backend training tests
node --check frontend/app.js
```

Kiểm tra Spark (cần dependencies + Java, không cần MongoDB):

```powershell
python -m unittest discover -s tests -p test_pipeline.py -v
```

Chạy end-to-end thật bằng bước nhập mẫu + pipeline + APP_MODE=mongo. Thành công phải có `active_model`, products/rules có index và `/api/health` trả mode mongo. Không coi demo là bằng chứng pipeline đã chạy.

## Nguồn tham khảo

- [Dataset H&M](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data)
- [Apache Spark 3.5.3 FP-Growth](https://spark.apache.org/docs/3.5.3/ml-frequent-pattern-mining.html)
- [MongoDB Spark Connector 10.4](https://www.mongodb.com/docs/spark-connector/v10.4/)
- [Shopee Việt Nam — tham khảo bố cục](https://shopee.vn/)

## Cải thiện mô hình và gợi ý (02/10/2026)

Thử ba cấu hình (support/confidence): `0.0001/0.30`, `0.0001/0.15`, `0.00005/0.15`, giữ lift > 1. Train đến 08/09, chọn cấu hình trên 09–15/09, kiểm tra cuối sau 15/09. Tập test chỉ được đánh giá cho cấu hình chọn bằng validation; không lấy nhiều luật làm tiêu chí chất lượng. Seed và giỏ đánh giá giống nhau giữa các cấu hình.

```powershell
docker compose build training web
docker compose run --name hm-training-improved training python -u -m training.pipeline --cutoff 2020-09-08 --validation-end 2020-09-15 --seed 42 --no-activate
```

`--no-activate` lưu mô hình ứng viên trong `model_runs` để kiểm tra trước; bỏ cờ này để kích hoạt sau khi tất cả kết quả/index đã lưu. Web đang dùng mô hình cũ trong lúc training. Kết quả chạy sau được lưu theo phiên bản. Không cần nhập lại 31 triệu giao dịch: pipeline tái sử dụng giao dịch sạch và giỏ SKU khi cùng `source_run`, giữ tập phổ biến và bộ luật trên đĩa để tránh khai phá lại khi đếm/xuất; `--no-reuse` buộc xây lại. Khi chia thời gian, dữ liệu sạch chứa đầy đủ ngày nhưng fit, độ phổ biến và giá chỉ dùng ngày huấn luyện.

Tuỳ chọn `--item-level product_code` gộp các biến thể trước khi khai phá FP-Growth. Web ánh xạ luật cấp mẫu sang SKU cụ thể mua nhiều nhất, không gợi ý lại mẫu đã chọn. Chỉ số cấp mẫu và cấp SKU khác đơn vị đánh giá nên không so trực tiếp. Các collection giỏ được lưu vẫn ở cấp SKU để dùng lại cho cả hai cách.

API trả `items` là luật mua kèm và `suggestions` là dự phòng popularity riêng. Khi không có luật, giao diện sản phẩm và giỏ hiển thị **Có thể bạn thích**. Phần dự phòng không có confidence/lift. Với mô hình cũ chưa lưu tần suất giỏ, dự phòng dùng lượt mua lịch sử; mô hình mới dùng `recommendation_popularity` từ giỏ huấn luyện. Dashboard hiển thị riêng FP-Growth, popularity, hybrid và bảng validation.

Nếu gặp cảnh báo BlockManager khi kết thúc, kiểm tra lỗi đầu tiên và mã thoát. Cảnh báo không tìm thấy block lúc dọn cache không tự chứng minh job lỗi. `Java heap space` là thiếu heap JVM: giữ local[4], 64 partitions, DISK_ONLY; trên máy hiện tại cấu hình heap là 6g. Docker cần thêm RAM cho MongoDB, Python và vùng ngoài heap. Không dùng local[*] hay tăng heap vượt RAM Docker.

## Rà soát trước khi nộp

Giao diện mua sắm đã bỏ giá chuẩn hóa và các dòng giải thích thuật toán; số liệu kỹ thuật giữ trong Góc dữ liệu. Rà soát hiện tại: `artifacts/danh-gia-mo-hinh-cap-mau.md`. Audit đọc dữ liệu: `training/audit_model.py` (APP_MODE=mongo); kết quả `artifacts/family-audit.json`. Audit kiểm tra toàn bộ luật nhưng chỉ lấy mẫu giao dịch/giỏ và ảnh; không thay cho kiểm tra bản báo cáo viết. `danh-gia-truoc-khi-nop.md` và `final-audit.json` giữ kết quả SKU cũ để đối chiếu lịch sử.

## Thử nghiệm mở rộng theo mẫu sản phẩm

`--item-level product_code` gộp biến thể thành mẫu, loại trùng mẫu trong giỏ và không gợi ý lại mẫu đã chọn. Khai phá vẫn dùng toàn bộ nguồn giao dịch sạch, giỏ theo khách hàng/ngày/kênh và giữ singleton. Có thể chọn dải cấu hình riêng thay vì dùng ba cặp mặc định:

```powershell
docker compose build training
docker compose run --name hm-training-families training python -u -m training.pipeline --item-level product_code --cutoff 2020-09-08 --validation-end 2020-09-15 --seed 42 --sweep-configs "0.0001:0.30,0.0001:0.15,0.00005:0.15,0.00005:0.10,0.00002:0.15,0.00002:0.10" --no-activate
```

Mỗi mức support chỉ khai phá tập phổ biến một lần; các confidence dùng lại bộ luật đã lưu tạm. Kết quả validation in ra sau mỗi cấu hình. Lựa chọn theo Recall@6, Precision@6 và coverage trên validation, không dùng số luật/test để chọn.

Ứng viên chưa kích hoạt có thể được audit bằng `python -m training.audit_model --run-id <run_id>` với APP_MODE=mongo. Chỉ số cấp mẫu và cấp SKU có đơn vị khác nhau, không được coi việc Recall@6 cấp mẫu lớn hơn là bằng chứng trực tiếp vượt mô hình SKU. So sánh trong cùng cấp mẫu với baseline popularity trên cùng giỏ và seed; so sánh độ phủ danh mục theo số SKU có thể áp dụng luật cấp mẫu. Giao diện mua sắm vẫn không hiển thị giải thích kỹ thuật.
