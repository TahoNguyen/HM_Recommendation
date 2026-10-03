"""Run: python -m backend.app. No web framework needed for the local app."""
import json
import logging
import mimetypes
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit
from backend.store import ROOT, Store

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env')
except ImportError:
    pass

class Handler(BaseHTTPRequestHandler):
    store = None

    def json_response(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False, default=str).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlsplit(self.path)
        path = unquote(parsed.path)
        args = parse_qs(parsed.query)
        def arg(key, default=''):
            return args.get(key, [default])[0]
        try:
            if path == '/api/health':
                meta = self.store.manifest()
                return self.json_response({'status': 'ok', 'mode': self.store.mode, 'run_id': meta.get('run_id')})
            if path == '/api/products':
                page = max(1, min(100000, int(arg('page', '1'))))
                size = max(1, min(48, int(arg('page_size', '12'))))
                together = arg('buy_together','0')
                if together not in ('0','1'):
                    raise ValueError('Invalid buy_together')
                return self.json_response(self.store.products(arg('q')[:100], arg('category')[:100], arg('sort'), page, size, together=='1'))
            if path == '/api/categories':
                return self.json_response({'items': self.store.categories()})
            if path == '/api/metrics':
                return self.json_response(self.store.metrics())
            if path.startswith('/api/products/'):
                article_id = path.rsplit('/', 1)[-1]
                if not re.fullmatch(r'\d{10}', article_id):
                    return self.json_response({'error': 'Mã sản phẩm phải gồm 10 chữ số.'}, 400)
                product = self.store.product(article_id)
                return self.json_response(product if product else {'error': 'Không tìm thấy sản phẩm'}, 200 if product else 404)
            if path == '/api/recommendations':
                items = list(dict.fromkeys(arg('items').split(',')))
                if not 1 <= len(items) <= 30 or any(not re.fullmatch(r'\d{10}', i) for i in items):
                    return self.json_response({'error': 'Cần 1–30 mã sản phẩm hợp lệ.'}, 400)
                return self.json_response(self.store.recommend(items))
            if path.startswith('/api/'):
                return self.json_response({'error': 'Không tìm thấy API'}, 404)
            base = Path(os.getenv('DATA_PATH', str(ROOT / 'data/raw'))).resolve() / 'images' if path.startswith('/images/') else ROOT / 'frontend'
            relative = path[len('/images/'):] if path.startswith('/images/') else path.lstrip('/') or 'index.html'
            target = (base / relative).resolve()
            if not target.is_relative_to(base.resolve()) or not target.is_file():
                return self.json_response({'error': 'Không tìm thấy tệp'}, 404)
            content = target.read_bytes()
            self.send_response(200)
            self.send_header('Content-Type', mimetypes.guess_type(target.name)[0] or 'application/octet-stream')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Content-Length', str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except ValueError:
            self.json_response({'error': 'Tham số không hợp lệ'}, 400)
        except Exception:
            logging.exception('Request failed')
            self.json_response({'error': 'Không kết nối được dữ liệu. Kiểm tra MongoDB và mô hình đã huấn luyện.'}, 503)

def main():
    Handler.store = Store()
    address = (os.getenv('HOST', '127.0.0.1'), int(os.getenv('PORT', '8000')))
    print(f'OCEAN wardrobe: http://{address[0]}:{address[1]} | mode={Handler.store.mode}', flush=True)
    ThreadingHTTPServer(address, Handler).serve_forever()

if __name__ == '__main__':
    main()
