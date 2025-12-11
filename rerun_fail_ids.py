import requests
import json
import time
import os
from typing import List, Dict, Any, Optional
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

API_URL = "https://api.tiki.vn/product-detail/api/v1/products/{product_id}"
BATCH_SIZE = 1000        # mỗi file ~1000 sản phẩm
OUTPUT_DIR = "output_products"
DELAY_BETWEEN_REQUESTS = 0.1  # giây, để giảm rủi ro bị chặn (10 req/s)
RETRY_TOTAL = 3          # số lần retry tối đa cho 1 request

# Global list lưu các ID lỗi cho từng vòng chạy
fail_product_ids: List[str] = []


# =============== LOAD PRODUCT IDS TỪ TXT =============== #

def load_product_ids_from_txt(path: str) -> List[str]:
    """
    Đọc product_id từ file TXT.
    Mỗi dòng là 1 product_id. Bỏ qua dòng trống.
    """
    product_ids: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            pid = line.strip()
            if pid:
                product_ids.append(pid)

    product_ids = product_ids[2:]
    return product_ids


# =============== XỬ LÝ DỮ LIỆU SẢN PHẨM =============== #

def extract_product_fields(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Lọc các field cần: id, name, url_key, price, description, images (list url)
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


# =============== HTTP LAYER: SESSION + RETRY =============== #

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
        allowed_methods=["GET"],          # các method được retry
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def fetch_product_detail(session: requests.Session, product_id: str) -> Optional[Dict[str, Any]]:
    """
    Gọi API lấy chi tiết 1 sản phẩm.
    Nếu lỗi hoặc status != 200:
      - log ra
      - thêm product_id vào fail_product_ids
      - return None
    """
    url = API_URL.format(product_id=product_id)
    try:
        resp = session.get(url, timeout=10)

        if resp.status_code != 200:
            print(f"[WARN] ID {product_id} status {resp.status_code}")
            fail_product_ids.append(product_id)
            return None

        data = resp.json()
        # Delay nhỏ giữa các request để giảm nguy cơ bị chặn
        time.sleep(DELAY_BETWEEN_REQUESTS)
        return extract_product_fields(data)

    except requests.exceptions.Timeout:
        print(f"[ERROR] ID {product_id} gặp lỗi Timeout sau {RETRY_TOTAL + 1} lần thử.")
        fail_product_ids.append(product_id)
        return None
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] ID {product_id} gặp lỗi RequestException: {e}")
        fail_product_ids.append(product_id)
        return None
    except Exception as e:
        print(f"[ERROR] Lỗi không phân loại khi fetch ID {product_id}: {e}")
        fail_product_ids.append(product_id)
        return None


# =============== LƯU FILE THEO BATCH =============== #

def save_batch_to_file(batch_data: List[Dict[str, Any]], batch_index: int) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = os.path.join(OUTPUT_DIR, f"products_{batch_index:03d}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(batch_data, f, ensure_ascii=False, indent=2)
    print(f"Đã lưu {len(batch_data)} sản phẩm vào {filename}")


def chunk_iterable(lst: List[Any], size: int):
    """Cắt list thành các chunk size phần tử."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def get_last_batch_index(folder: str) -> int:
    """
    Trả về batch index *tiếp theo* dựa trên các file products_XXX.json trong folder.
    Nếu folder trống → trả về 1.
    """
    if not os.path.exists(folder):
        return 1

    max_index = 0
    for filename in os.listdir(folder):
        if filename.startswith("products_") and filename.endswith(".json"):
            try:
                index = int(filename.replace("products_", "").replace(".json", ""))
                max_index = max(max_index, index)
            except:
                pass

    return max_index + 1 if max_index > 0 else 1



# =============== MAIN: ĐƠN LUỒNG, NHẬN FILE INPUT =============== #

def main(input_file: str):
    """
    Chạy crawl 1 vòng:
    - Đọc product_ids từ input_file
    - Crawl từng ID
    - Lưu output theo batch
    - Cập nhật global fail_product_ids
    """
    product_ids = load_product_ids_from_txt(input_file)
    print(f"[RUN] Input file: {input_file}, tổng số product_id: {len(product_ids)}")

    batch_index = get_last_batch_index(OUTPUT_DIR)
    print(f"Khởi động với batch_index = {batch_index}")
    session = create_session_with_retry()

    try:
        for ids_chunk in chunk_iterable(product_ids, BATCH_SIZE):
            batch_results: List[Dict[str, Any]] = []

            print(f"=== Bắt đầu batch {batch_index}, số ID: {len(ids_chunk)} ===")

            for pid in ids_chunk:
                data = fetch_product_detail(session, pid)
                if data is not None:
                    batch_results.append(data)

            if batch_results:
                save_batch_to_file(batch_results, batch_index)
            else:
                print(f"[WARN] Batch {batch_index} không có sản phẩm hợp lệ.")

            print(f"=== Kết thúc batch {batch_index}, đã ghi {len(batch_results)} sản phẩm ===")
            batch_index += 1

            # Delay giữa các batch, tránh bị 429
            time.sleep(2)

    finally:
        session.close()

    print("Hoàn thành 1 vòng crawl!")


# =============== VÒNG LẶP NHIỀU LẦN =============== #

if __name__ == "__main__":
    NUM_RUNS = 3

    current_input = "fail_ids.txt"

    for iteration in range(1, NUM_RUNS + 1):
        print(f"\n===== BẮT ĐẦU VÒNG {iteration}, input = {current_input} =====")

        # Reset danh sách fail cho vòng này
        fail_product_ids.clear()

        start_time = time.time()
        main(current_input)
        crawl_time = time.time() - start_time

        print(f"--- Thời gian vòng {iteration}: {crawl_time:.2f} giây ---")
        print(f"Số sản phẩm không thành công ở vòng {iteration} = {len(fail_product_ids)}")

        # Ghi đè fail_ids.txt bằng các ID lỗi của vòng này
        with open("fail_ids_rerun.txt", "w", encoding="utf-8") as f:
            f.write(f"--- Thời gian vòng {iteration}: {crawl_time:.2f} giây ---\n")
            f.write(f"Số sản phẩm không thành công ở vòng {iteration} = {len(fail_product_ids)}\n")
            for pid in fail_product_ids:
                f.write(f"{pid}\n")

        print(f"Đã lưu {len(fail_product_ids)} ID lỗi vào fail_ids.txt")
        # Các vòng sau luôn dùng fail_ids.txt làm input
        current_input = "fail_ids_rerun.txt"


    print("\n===== HOÀN TẤT TẤT CẢ CÁC VÒNG =====")
