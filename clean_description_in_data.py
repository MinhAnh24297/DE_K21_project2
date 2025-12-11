import json
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import os

FOLDER = "output_products"


def clean_description(html_text):
    # Parse HTML
    soup = BeautifulSoup(html_text, "html.parser")

    # Lấy toàn bộ nội dung text, tự loại bỏ tag
    text = soup.get_text(separator="\n")

    # Loại bỏ khoảng trắng thừa
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    # Gộp lại thành văn bản sạch
    return "\n".join(lines)


def process_file(path):
    print("Xử lý:", path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for item in data:
        item["description"] = clean_description(item.get("description", ""))

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

files = [os.path.join(FOLDER, file) for file in os.listdir(FOLDER) if file.endswith(".json")]

with ThreadPoolExecutor(max_workers=5) as exe:
    exe.map(process_file, files)

print("Xong toàn bộ!")
