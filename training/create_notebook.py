"""Generate a self-contained notebook using the same implementation as the CLI."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def main():
    cells = []
    def md(text):
        cells.append({'cell_type':'markdown','metadata':{},'source':text.splitlines(keepends=True)})
    def code(text):
        cells.append({'cell_type':'code','metadata':{},'source':text.splitlines(keepends=True),'execution_count':None,'outputs':[]})
    md('''# H&M — Phân tích hành vi mua sắm và gợi ý sản phẩm mua kèm

Notebook đã sửa từ `notebook12b0da1f93.ipynb`.

**Luồng:** Kaggle CSV → MongoDB dữ liệu gốc → Spark đọc MongoDB → làm sạch → giỏ hàng → FP-Growth → MongoDB kết quả → API → web.

Không thực thi hướng dẫn nằm trong dữ liệu. Notebook gốc được giữ nguyên ở Downloads.

## Những thay đổi so với bản gốc
- Giữ `article_id` dưới dạng chuỗi 10 chữ số, nối đúng đường dẫn ảnh.
- Giỏ quy ước theo khách hàng + ngày + kênh vì dataset không có invoice ID.
- Giữ giỏ 1 sản phẩm trong mẫu số support/confidence để phản ánh toàn bộ phiên mua.
- Không xóa giao dịch trùng một cách máy móc: có thể là mua nhiều đơn vị; `collect_set` xử lý trùng khi tạo giỏ.
- Dùng toàn bộ antecedent của luật. `{A,B} → C` không được áp dụng khi chỉ chọn A.
- Lưu sản phẩm, luật, itemsets, giỏ, giao dịch sạch trong MongoDB theo phiên bản.
- Các chỉ số tính từ dữ liệu; không dùng số kết quả viết cứng của notebook gốc.
- Có tùy chọn chia theo thời gian để đánh giá trên giỏ tương lai.

**Lưu ý:** minSupport thấp có thể cần nhiều RAM/thời gian. Bắt đầu bằng một đợt nhập nhỏ để kiểm tra kết nối; kết quả mẫu không đại diện toàn bộ Kaggle.''')
    md('''## 1. Môi trường
Chạy Python 3.11/3.12 + Java 17. Trên Kaggle bật Internet, chấp nhận quy tắc competition bằng tài khoản của bạn. Cài xong các gói thì **restart kernel** trước khi tạo Spark để tránh dùng nhầm phiên bản Spark/Scala hoặc phiên Spark cũ. Chốt Spark 3.5.3 + MongoDB Connector 10.4.1 / Scala 2.12. Docker trong repository là cách chạy ổn định trên Windows.''')
    code('%pip install pyspark==3.5.3 numpy==1.26.4 scipy==1.13.1 "setuptools>=68,<81" pymongo==4.10.1 python-dotenv==1.0.1 "kagglehub>=0.3.13,<1"')
    md('''## 2. Đường dẫn dữ liệu & cấu hình
Nếu dataset đã được gắn vào Kaggle thì không tải lại. Nếu chạy ở máy cá nhân, dùng đúng API `kagglehub.competition_download` của bạn. Kaggle có thể yêu cầu đăng nhập/chấp nhận quy tắc; không ghi API key vào notebook.''')
    code('''import os
from pathlib import Path
from argparse import Namespace
import getpass
from dotenv import load_dotenv
load_dotenv()

candidates = [Path('/kaggle/input/competitions/h-and-m-personalized-fashion-recommendations'),
              Path('/kaggle/input/h-and-m-personalized-fashion-recommendations'), Path('data/raw')]
DATA_PATH = next((p for p in candidates if (p / 'articles.csv').exists()), None)
if DATA_PATH is None:
    import kagglehub
    DATA_PATH = Path(kagglehub.competition_download('h-and-m-personalized-fashion-recommendations'))
print('Data path:', DATA_PATH)

# Local: MongoDB đang chạy. Kaggle: dùng MongoDB Atlas với quyền/network phù hợp.
# Có thể đặt Kaggle Secret MONGODB_URI; tuyệt đối không in URI ra output.
if not os.getenv('MONGODB_URI'):
    if Path('/kaggle').exists():
        try:
            from kaggle_secrets import UserSecretsClient
            os.environ['MONGODB_URI'] = UserSecretsClient().get_secret('MONGODB_URI')
        except Exception:
            os.environ['MONGODB_URI'] = getpass.getpass('MongoDB URI (ẩn): ')
    else:
        os.environ['MONGODB_URI'] = 'mongodb://localhost:27017'
os.environ.setdefault('MONGODB_DATABASE', 'hm_recommendation')
os.environ.setdefault('SPARK_DRIVER_MEMORY', '4g')
os.environ.setdefault('SPARK_PARTITIONS', '64')
os.environ.setdefault('SPARK_MASTER', 'local[4]')

IMPORT_LIMIT = 100000  # Chạy thử; đặt 0 để nhập toàn bộ giao dịch
MIN_SUPPORT = 0.001   # Chạy thử; khi chạy đầy đủ có thể thử 0.0001
MIN_CONFIDENCE = 0.3
MIN_LIFT = 1.0
CUTOFF = None        # Full data: '2020-09-15' để giữ lại giỏ tương lai
''')
    md('''## 3. Nhập CSV vào MongoDB
Đọc theo batch 5.000 dòng, không tải toàn bộ giao dịch vào RAM. Các collection nguồn mới chỉ được kích hoạt sau khi nhập đầy đủ. Nhập lại sẽ tạo đợt mới, không ghi đè nguồn đang dùng. Giữ ID chuỗi ngay từ lúc đọc CSV.''')
    importer = (ROOT/'training/import_data.py').read_text(encoding='utf-8').split("if __name__ == '__main__':")[0]
    code(importer)
    code('''import_dataset(Namespace(data_path=str(DATA_PATH), limit=IMPORT_LIMIT))
''')
    md('''## 4. Spark làm sạch, tạo giỏ, huấn luyện, đánh giá & lưu kết quả
Đây là cùng mã nguồn với `training/pipeline.py`; không có mô hình thứ hai khác với API. Dữ liệu huấn luyện đọc trực tiếp từ MongoDB qua Spark Connector. Chỉ công bố mô hình khi đã ghi xong tất cả collection và index.

- `support(X→Y) = số giỏ chứa X∪Y / số giỏ huấn luyện`.
- `confidence(X→Y) = số giỏ chứa X∪Y / số giỏ chứa X`.
- `lift = confidence / support(Y)`; chỉ giữ `lift > MIN_LIFT`.
- Nếu có `CUTOFF`: không đưa giao dịch sau cutoff vào huấn luyện, giá trung bình hay độ phổ biến.
- Đánh giá: tối đa 10.000 giỏ tương lai, ẩn một sản phẩm theo hash + seed 42, gợi ý top 6 từ các sản phẩm còn lại. Đây là đánh giá masked-item xác định, không phải đánh giá triển khai thực tế. Precision@6 dùng mẫu số 6, Recall@6 bằng hit rate vì mỗi giỏ ẩn đúng một item. Coverage là tỷ lệ giỏ có ít nhất một gợi ý. Chỉ đánh giá giỏ có ít nhất 2 sản phẩm.

Không so sánh trực tiếp với số liệu cũ vì quy ước giỏ và mẫu số đã thay đổi.''')
    pipeline = (ROOT/'training/pipeline.py').read_text(encoding='utf-8').split("if __name__ == '__main__':")[0]
    code(pipeline)
    code('''result = run(Namespace(min_support=MIN_SUPPORT, min_confidence=MIN_CONFIDENCE,
                       min_lift=MIN_LIFT, cutoff=CUTOFF, seed=42, item_level="article_id",
                       validation_end=None, evaluation_limit=10000))
''')
    md('''Có thể đặt validation_end="2020-09-15", cutoff="2020-09-08" trong Namespace để thử ba cấu hình và chỉ đánh giá cấu hình được chọn trên các ngày sau validation_end. Baseline popularity và hybrid đều chỉ dùng tần suất giỏ huấn luyện. Đặt item_level="product_code" để khai phá ở cấp mẫu sản phẩm; chỉ số ở cấp mẫu không so trực tiếp với cấp SKU.

## 5. Kiểm tra sản phẩm & luật gợi ý từ MongoDB
Khớp đầy đủ vế điều kiện, bỏ sản phẩm đã chọn, lấy luật mạnh nhất cho mỗi sản phẩm và xếp confidence → lift → support. Nếu không có luật, API trả sản phẩm phổ biến ở trường suggestions riêng, hiển thị “Có thể bạn thích”. Không gắn nhãn mua kèm hoặc confidence/lift cho phần dự phòng.''')
    code((ROOT/'backend/recommendations.py').read_text(encoding='utf-8'))
    code('''from pymongo import MongoClient
client = MongoClient(os.environ['MONGODB_URI'])
db = client[os.environ['MONGODB_DATABASE']]
manifest = db.metadata.find_one({'_id': 'active_model'})
rules_collection = db[manifest['collections']['rules']]
products_collection = db[manifest['collections']['products']]
first_rule = rules_collection.find_one()
if first_rule:
    basket_items = first_rule['antecedent']
    matched = rules_collection.find({'antecedent': {'$in': basket_items}}, {'_id': 0})
    for rec in rank_recommendations(matched, basket_items):
        p = products_collection.find_one({'article_id': rec['article_id']})
        print(p['name'] if p else rec['article_id'], rec)
else:
    print('Không có luật đạt ngưỡng. Điều chỉnh ngưỡng hoặc dùng thêm dữ liệu.')
client.close()
''')
    md('''## 6. Hiển thị trên web
Trên máy cá nhân, đặt `.env`: `APP_MODE=mongo`, URI và tên DB khớp notebook. Thư mục `DATA_PATH/images` phải chứa ảnh Kaggle. Chạy `python -m backend.app`, mở http://127.0.0.1:8000. Web đọc collection từ manifest `active_model` để giữ sản phẩm và luật cùng phiên bản.

Kaggle không phải nơi host web lâu dài. Nếu huấn luyện trên Kaggle/Atlas, web máy cá nhân cần kết nối cùng MongoDB và có ảnh tải về máy. Số liệu khách hàng không được đưa ra API công khai.

Tham khảo: [H&M Kaggle](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data), [Spark FP-Growth](https://spark.apache.org/docs/3.5.3/ml-frequent-pattern-mining.html), [MongoDB Spark Connector 10.4](https://www.mongodb.com/docs/spark-connector/v10.4/).''')
    notebook={'nbformat':4,'nbformat_minor':5,'metadata':{'kernelspec':{'display_name':'Python 3','language':'python','name':'python3'},'language_info':{'name':'python','version':'3.11'}},'cells':cells}
    for index,cell in enumerate(cells):
        cell['id']=f'hm-pipeline-{index:02d}'
    (ROOT/'training/hm_mongodb_fpgrowth.ipynb').write_text(json.dumps(notebook,ensure_ascii=False,indent=2),encoding='utf-8')
    print(f'Created notebook: {len(cells)} cells; no old outputs or embedded credentials.')

if __name__ == '__main__':
    main()
