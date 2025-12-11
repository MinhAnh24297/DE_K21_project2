# DE_K21_project2
Name: Nguyễn Minh Anh

# TIKI PRODUCT SCRAPER
This is a high-performance web scraper for Tiki.vn product details, featuring multithreading, automatic retry, HTML description cleaning, and batch-based JSON exporting.

# FEATURES
- Fast multithreaded scraping
- Automatic retry on 429 / 5xx errors
- Saves data in JSON batches (~1000 items/file)
- Cleans HTML descriptions using BeautifulSoup
- Reuses HTTP session for improved performance
- Generates fail_ids.txt for unreachable products

# PROJECT STRUCTURE
- main.py -> Scrapes product info and saves to JSON
- clean_description_in_data.py -> Cleans HTML tags in descriptions
- product_ids.csv -> Input: product ID list
- output_products/ -> JSON batch output
- fail_ids.txt -> Failed IDs log
- rerun_fail_ids.py -> Re-crawl the products in the failed-product ID list

# INPUT FORMAT
The file `product_ids.csv` must contain a single column named `"id"`:<br>
Example:<br>
id<br>
123456<br>
234567<br>
345678<br>

# RUNNING THE SCRAPER
- Run the main scraper:
python main.py
- After that, clean descriptions:
python clean_description_in_data.py

# CONFIGURATION
Editable values in main.py:
- BATCH_SIZE = 1000
- MAX_WORKERS = 10
- DELAY_BETWEEN_REQUESTS = 0.1
- RETRY_TOTAL = 3
- OUTPUT_DIR = "output_products"

# OUTPUT EXAMPLE AFTER TWO PROCESSES
{
"id": 123,
"name": "Product Name",
"url_key": "product-name",
"price": 150000,
"description": "Clean product description...",
"images": [
"https://salt.tikicdn.com/cache/...jpg
",
"https://salt.tikicdn.com/cache/...jpg
"
]
}
