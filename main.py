import requests
import json
import time
import os
from typing import List, Dict, Any, Optional
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


API_URL = "https://api.tiki.vn/product-detail/api/v1/products/{product_id}"
BATCH_SIZE = 1000 #mỗi file ~1000 sản phẩm
OUTPUT_DIR = "output_products"
DELAY_BETWEEN_REQUESTS = 0.1  # giây, để giảm rủi ro bị chặn (10 req/s)
MAX_WORKERS = 10          # số thread chạy song song
RETRY_TOTAL = 3            # số lần retry tối đa cho 1 request
fail_product_ids = []

#load product Ids
def load_product_ids_from_csv(path: str):
    """
    Đọc product_id từ file CSV có duy nhất 1 cột và có header.
    """
    df = pd.read_csv(path)
    # duplicate_rows = df[df.duplicated()]
    # print(duplicate_rows) #empty df
    product_ids = df['id'].tolist()
    return product_ids


#Xử lý dữ liệu sản phẩm

def extract_product_fields(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Lọc các field cần: id, name, url_key, price, description, images (list url)
    Cấu trúc thật sự của API có thể hơi khác -> bạn chỉnh lại nếu cần.
    """
    # Lấy price (tuỳ cấu trúc API)
    price = None
    if isinstance(raw.get("price"), (int, float, str)):
        price = raw.get("price")
    elif isinstance(raw.get("price"), dict):
        price = (
            raw["price"].get("value")
            or raw["price"].get("original_price")
            or raw["price"].get("final_price")
        )

    # Lấy images
    images_url = []
    images_raw = raw.get("images") or []
    if isinstance(images_raw, list):
        for img in images_raw:
            if isinstance(img, dict):
                url = img.get("base_url") or img.get("url")
                if url:
                    images_url.append(url)

    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "url_key": raw.get("url_key"),
        "price": price,
        "description": raw.get("description"),
        "images": images_url,
    }


# ================== HTTP LAYER: SESSION + RETRY ================== #

def create_session_with_retry() -> requests.Session:
    """
    Tạo 1 Session:
    - Reuse kết nối (connection pool)
    - Gắn sẵn headers
    - Có retry tự động khi gặp 429 / 5xx / lỗi tạm thời
    """
    session = requests.Session()

    # Header mặc định cho mọi request
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    })

    # Cấu hình retry
    retry_strategy = Retry(
        total=RETRY_TOTAL,
        backoff_factor=1,                 # delay: 1s, 2s, 4s, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],  # các method được retry
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def fetch_product_detail(session: requests.Session, product_id: str) -> Optional[Dict[str, Any]]:
    url = API_URL.format(product_id=product_id)
    try:
        resp = session.get(url, timeout=10)

        if resp.status_code != 200:
            # Ghi nhận các lỗi status code khác 200
            print(f"[WARN] ID {product_id} status {resp.status_code}")
            fail_product_ids.append(product_id)
            return None

        data = resp.json()
        time.sleep(DELAY_BETWEEN_REQUESTS)
        return extract_product_fields(data)

    except requests.exceptions.Timeout:
        # Ghi nhận lỗi Timeout rõ ràng
        print(f"[ERROR] ID {product_id} gặp lỗi Timeout sau {RETRY_TOTAL + 1} lần thử.")
        return None
    except requests.exceptions.RequestException as e:
        # Ghi nhận các lỗi request khác (ConnectionError, HTTPError, ...)
        print(f"[ERROR] ID {product_id} gặp lỗi RequestException: {e}")
        return None
    except Exception as e:
        # Ghi nhận các lỗi khác (JSONDecodeError, v.v.)
        print(f"[ERROR] Lỗi không phân loại khi fetch ID {product_id}: {e}")
        return None


# ================== LƯU FILE THEO BATCH ================== #

def save_batch_to_file(batch_data: List[Dict[str, Any]], batch_index: int) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = os.path.join(OUTPUT_DIR, f"products_{batch_index:03d}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(batch_data, f, ensure_ascii=False, indent=2)
    print(f"Đã lưu {len(batch_data)} sản phẩm vào {filename}")


def chunk_iterable(lst: List[Any], size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def main():

    # TODO: thay bằng chỗ bạn load list id
    # Ví dụ: từ file txt
    product_ids = load_product_ids_from_csv("product_ids.csv")

    print(f"Tổng số product_id: {len(product_ids)}")

    batch_index = 1

    # 1 Session duy nhất cho toàn bộ chương trình (reuse connection)
    session = create_session_with_retry()

    try:
        for ids_chunk in chunk_iterable(product_ids, BATCH_SIZE):
            batch_results: List[Dict[str, Any]] = []

            # Multi-thread cho 1 chunk
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_pid = {
                    executor.submit(fetch_product_detail, session, pid): pid
                    for pid in ids_chunk
                }

                for future in as_completed(future_to_pid):
                    pid = future_to_pid[future]
                    try:
                        data = future.result()
                        if data is not None:
                            batch_results.append(data)
                    except Exception as e:
                        # Tránh crash toàn bộ nếu 1 thread lỗi
                        print(f"[ERROR] Lỗi không mong đợi với ID {pid}: {e}")

            if batch_results:
                save_batch_to_file(batch_results, batch_index)
            else:
                print(f"[WARN] Chunk batch_index={batch_index} không có sản phẩm hợp lệ.")

            batch_index += 1
            #delay giữa các batch, tránh lỗi 429
            time.sleep(2)
    finally:
        session.close()

    print("Hoàn thành!")


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))

    print(f"Số sản phẩm không thành công = {len(fail_product_ids)}")
    with open("fail_ids.txt", "w", encoding="utf-8") as f:
        f.write("--- %s seconds ---\n" % (time.time() - start_time))
        f.write(f"Số sản phẩm không thành công = {len(fail_product_ids)}\n")
        for pid in fail_product_ids:
            f.write(f"{pid}\n")
    print(f"Đã lưu {len(fail_product_ids)} ID lỗi vào fail_ids.txt")


