import json
import os
import sys
import re
import asyncio
import aiohttp
import urllib.parse
from urllib.parse import quote_plus
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from tqdm.asyncio import tqdm

# ================= 1. CONFIGURATION & ENV =================
load_dotenv(dotenv_path='.env', override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
APP_NAME = os.getenv("APP_NAME", "Cluster0")
DB_NAME = os.getenv("DB_NAME", "my_database")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "articles")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TARGET_MOJOK_URL = "https://mojok.co/"
TARGET_TERMINAL_URL = "https://mojok.co/terminal/"
TEMP_SLUGS_FILE = "temp_all_slugs.txt"
PROXY_LIST_URL = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt"

CONCURRENCY_LIMIT_CATEGORY = 15
CONCURRENCY_LIMIT_ARTICLE = 15

IGNORED_PATHS = {
    'video', 'terminal', 'kirim-artikel', 'kirim-tulisan', 'tentang', 'kru-mojok',
    'kontak', 'pedoman-media-siber', 'kebijakan-privasi', 'page', 'ketentuan', 'faq', 'topik', 'wp-json'
}

RE_PUB = re.compile(r'(?i)<meta\s+property=["\']article:published_time["\']\s+content=["\']([^"\']+)["\']')
RE_MOD = re.compile(r'(?i)<meta\s+property=["\']article:modified_time["\']\s+content=["\']([^"\']+)["\']')

# ================= 2. HELPER & DB FUNCTIONS =================
def get_db_collection():
    try:
        encoded_password = quote_plus(DB_PASSWORD) if DB_PASSWORD else ""
        mongo_uri = f"mongodb+srv://{DB_USER}:{encoded_password}@{DB_HOST}/?appName={APP_NAME}"
        
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        collection.create_index("url", unique=True)
        return collection
    except Exception as e:
        print(f"❌ Gagal koneksi MongoDB Atlas: {e}")
        sys.exit(1)

def cleanup_database_pages(collection):
    try:
        res1 = collection.delete_many({"sub_sub_slug": "page"})
        res2 = collection.delete_many({"article_slug": {"$regex": "^[0-9]+$"}})
        res3 = collection.delete_many({"slug": "video"})
        total = res1.deleted_count + res2.deleted_count + res3.deleted_count
        if total > 0:
            print(f"🧹 [DB Cleanup] Berhasil menghapus {total} dokumen sampah dari database!\n")
    except Exception as e:
        print(f"⚠️ [DB Cleanup] Gagal menjalankan pembersihan DB: {e}\n")

def extract_slugs_dynamic(url_str):
    try:
        parsed = urllib.parse.urlparse(url_str)
        path_parts = [p for p in parsed.path.strip('/').split('/') if p]
        
        if not path_parts:
            return {}

        if path_parts[0] == 'terminal':
            article_slug = path_parts[-1]
            category_parts = [c for c in path_parts[:-1] if c != 'topik']

            return {
                "slug": category_parts[0] if len(category_parts) > 0 else "terminal",
                "sub_slug": category_parts[1] if len(category_parts) > 1 else "",
                "sub_sub_slug": category_parts[2] if len(category_parts) > 2 else "",
                "article_slug": article_slug
            }

        article_slug = path_parts[-1]
        category_parts = [c for c in path_parts[:-1] if c != 'topik']

        return {
            "slug": category_parts[0] if len(category_parts) > 0 else "",
            "sub_slug": category_parts[1] if len(category_parts) > 1 else "",
            "sub_sub_slug": category_parts[2] if len(category_parts) > 2 else "",
            "article_slug": article_slug
        }
    except Exception:
        return {"slug": "", "sub_slug": "", "sub_sub_slug": "", "article_slug": ""}

def is_valid_article_url(url_str):
    if not url_str or "mojok.co" not in url_str:
        return False
    
    parsed = urllib.parse.urlparse(url_str)
    parts = [p for p in parsed.path.strip('/').split('/') if p]
    
    if not parts:
        return False
    
    if 'page' in parts or 'video' in parts or 'wp-json' in parts:
        return False

    if parts[-1].isdigit():
        return False

    if parts[0] in IGNORED_PATHS or parts[-1] in IGNORED_PATHS:
        return False
    
    if parts[-1] in {'topik', 'kuliner', 'hiburan', 'gaya-hidup', 'film', 'anime', 'musik', 'sinetron', 'serial', 'game', 'gadget', 'kampus', 'pendidikan', 'nusantara', 'ekonomi', 'teknologi', 'olahraga', 'otomotif'}:
        return False

    if len(parts) == 1 and parts[0] in {'esai', 'tajuk', 'pojokan', 'kilas', 'cuan', 'otomojok', 'maljum', 'liputan', 'terminal'}:
        return False
        
    return True

def send_telegram_raw(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    try:
        import requests
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"⚠️ Gagal kirim Telegram: {e}")

def send_telegram_notification(added_articles, total_new, success_count):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    header_msg = (
        f"✅ *Bot Sync Mojok Selesai!*\n\n"
        f"📊 *Hasil Ingest:*\n"
        f" ├─ Total Terdeteksi Baru: `{total_new}`\n"
        f" └─ Berhasil Disimpan DB: `{success_count}`\n\n"
        f"⏰ Waktu: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n"
        f"👇 *Daftar Artikel Baru:* "
    )
    send_telegram_raw(header_msg)

    CHUNK_SIZE = 20
    for i in range(0, len(added_articles), CHUNK_SIZE):
        chunk = added_articles[i:i + CHUNK_SIZE]
        chunk_text = ""
        for idx, (title, url, is_headline) in enumerate(chunk, start=i+1):
            tag = "🔥 *[HEADLINE]* " if is_headline else ""
            chunk_text += f"{idx}. {tag}*{title}*\n🔗 {url}\n\n"
        send_telegram_raw(chunk_text)

def clean_existing_slugs_file():
    if not os.path.exists(TEMP_SLUGS_FILE):
        return set()

    with open(TEMP_SLUGS_FILE, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    valid_slugs = set(s for s in lines if is_valid_article_url(s))

    with open(TEMP_SLUGS_FILE, "w", encoding="utf-8") as f:
        for slug in sorted(valid_slugs):
            f.write(f"{slug}\n")

    return valid_slugs

async def fetch_proxy_list(session):
    try:
        async with session.get(PROXY_LIST_URL, timeout=aiohttp.ClientTimeout(total=8), ssl=False) as resp:
            if resp.status == 200:
                text = await resp.text()
                return [
                    line.strip() if line.strip().startswith("http") else f"http://{line.strip()}"
                    for line in text.splitlines() if line.strip()
                ]
    except Exception:
        pass
    return []

async def fetch_html_loop(session, url, proxies, batch_size=5, timeout=5):
    for attempt in range(3):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=6), ssl=False) as resp:
                if resp.status == 200:
                    return await resp.text()
        except Exception:
            pass

        if proxies:
            import random
            batch = random.sample(proxies, min(batch_size, len(proxies)))
            tasks = [try_proxy(session, url, p, timeout) for p in batch]
            
            try:
                results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=8.0)
                for html in results:
                    if isinstance(html, str) and html:
                        return html
            except asyncio.TimeoutError:
                pass
            
        await asyncio.sleep(0.2)
    return None

async def try_proxy(session, url, proxy, timeout=5):
    try:
        async with session.get(url, proxy=proxy, timeout=aiohttp.ClientTimeout(total=timeout), ssl=False) as resp:
            if resp.status == 200:
                return await resp.text()
    except Exception:
        return None

# ================= 3. TAHAP 1: DISCOVERY NAV & AJAX LOAD MORE =================
def extract_all_nav_and_sub_links(html_content):
    """
    Ekstraksi link navigasi utama, navbar terminal, dan section 'Lainnya dari Mojok'.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    category_links = set()
    
    # Ambil seluruh tag <a> yang menuju ke halaman kategori/rubrik
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href'].strip()
        if 'mojok.co' in href:
            # Mengambil URL kategori/rubrik baik utama maupun terminal
            if '/topik/' in href or any(rubrik in href for rubrik in ['/esai/', '/tajuk/', '/pojokan/', '/kilas/', '/cuan/', '/otomojok/', '/maljum/', '/liputan/']):
                category_links.add(href)

    return list(category_links)

def get_max_page(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    pagination_nav = soup.find('nav', class_='pagination') or soup.find('div', class_='nav-links')
    if pagination_nav:
        page_numbers = []
        for a in pagination_nav.find_all('a', class_='page-numbers'):
            href = a.get('href', '')
            match = re.search(r'/page/(\d+)/?', href)
            if match:
                page_numbers.append(int(match.group(1)))
        if page_numbers:
            return max(page_numbers)

    return 1

def extract_article_links_from_page(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    links = set()
    
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href'].strip()
        if is_valid_article_url(href):
            links.add(href)
            
    return list(links)

async def fetch_wp_rest_api_posts(session, category_url, proxies, visited_slugs, lock):
    """
    Simulasi aksi 'Muat Lebih Banyak' dengan menembak API WP-JSON bawaan WordPress
    untuk mengambil seluruh postingan yang dimuat via AJAX secara otomatis.
    """
    parsed = urllib.parse.urlparse(category_url)
    is_terminal = 'terminal' in parsed.path
    base_domain = f"{parsed.scheme}://{parsed.netloc}" + ("/terminal" if is_terminal else "")
    api_endpoint = f"{base_domain}/wp-json/wp/v2/posts?per_page=50&page="

    page = 1
    while True:
        target_api = f"{api_endpoint}{page}"
        json_raw = await fetch_html_loop(session, target_api, proxies)
        if not json_raw:
            break
        
        try:
            posts = json.loads(json_raw)
            if not posts or not isinstance(posts, list):
                break
                
            new_found = 0
            async with lock:
                with open(TEMP_SLUGS_FILE, "a", encoding="utf-8") as f:
                    for post in posts:
                        link = post.get('link')
                        if link and is_valid_article_url(link) and link not in visited_slugs:
                            visited_slugs.add(link)
                            f.write(f"{link}\n")
                            new_found += 1
            if new_found == 0 or len(posts) < 50:
                break
            page += 1
        except Exception:
            break

async def process_single_category_page(session, page_url, proxies, semaphore, visited_slugs, lock, pbar):
    async with semaphore:
        html_data = await fetch_html_loop(session, page_url, proxies)
        if html_data:
            links = extract_article_links_from_page(html_data)
            async with lock:
                with open(TEMP_SLUGS_FILE, "a", encoding="utf-8") as f:
                    for link in links:
                        if link not in visited_slugs:
                            visited_slugs.add(link)
                            f.write(f"{link}\n")
        pbar.update(1)

async def fetch_all_sub_slugs(session, category_urls, proxies):
    print("\n[=== TAHAP 1: SCRAPING DAFTAR SLUG/URL ===]")
    visited_slugs = clean_existing_slugs_file()
    print(f"[+] Ditemukan {len(visited_slugs)} slug valid dari file cache lokal. Memulai crawling...")

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT_CATEGORY)
    lock = asyncio.Lock()

    for cat_url in category_urls:
        print(f"\n[*] Mengambil halaman kategori/rubrik: {cat_url}")
        first_html = await fetch_html_loop(session, cat_url, proxies)
        if not first_html:
            continue

        # 1. Scraping lewat standard WP Pagination
        max_page = get_max_page(first_html)
        base_url = cat_url.rstrip('/')

        cat_name = [p for p in cat_url.split('/') if p][-1]
        pbar = tqdm(total=max_page, desc=f"Pagination ({cat_name})", unit="page", leave=True)

        tasks = [
            process_single_category_page(
                session, 
                cat_url if page == 1 else f"{base_url}/page/{page}/", 
                proxies, semaphore, visited_slugs, lock, pbar
            )
            for page in range(1, max_page + 1)
        ]
        await asyncio.gather(*tasks)
        pbar.close()

        # 2. Scraping via WP REST API (Menangani Tombol "Muat Lebih Banyak")
        await fetch_wp_rest_api_posts(session, cat_url, proxies, visited_slugs, lock)

    print(f"\n[✔] Tahap 1 Selesai! Total {len(visited_slugs)} slug unik tersimpan di '{TEMP_SLUGS_FILE}'.")
    return list(visited_slugs)

# ================= 4. TAHAP 2: SCRAPE DETAIL =================
def parse_article_detail(html_content, url):
    soup = BeautifulSoup(html_content, 'html.parser')

    is_headline = False
    headline_span = soup.find('span', string=re.compile(r'Headline', re.I))
    if headline_span:
        is_headline = True

    title_tag = soup.find('h1', class_='mj-hero-slide-title') or soup.find('h1')
    title = title_tag.get_text(strip=True) if title_tag else "Tanpa Judul"

    featured_image_url = ""
    og_img = soup.find('meta', property='og:image')
    if og_img and og_img.get('content'):
        featured_image_url = og_img['content']

    date_published = ""
    date_modified = ""
    pub_match = RE_PUB.search(html_content)
    if pub_match:
        date_published = pub_match.group(1)
    mod_match = RE_MOD.search(html_content)
    if mod_match:
        date_modified = mod_match.group(1)

    slug_fields = extract_slugs_dynamic(url)

    content_blocks = []
    main_container = soup.find('div', class_='archive-main-content') or soup.find('article') or soup

    for elem in main_container.find_all(['h2', 'h3', 'h4', 'p']):
        text = elem.get_text(strip=True)
        if not text:
            continue
        text_upper = text.upper()
        if any(keyword in text_upper for keyword in ["PENULIS:", "EDITOR:", "BACA JUGA", "CEK BERITA"]):
            continue

        if elem.name in ['h2', 'h3', 'h4']:
            content_blocks.append({"type": "heading", "level": int(elem.name[1]), "value": text})
        else:
            content_blocks.append({"type": "paragraph", "value": text})

    doc = {
        'url': url,
        'is_headline': is_headline,
        'date_published': date_published,
        'date_modified': date_modified,
        'title': title,
        'featured_image': featured_image_url,
        'content_blocks': content_blocks,
        'crawled_at': datetime.now().isoformat()
    }
    doc.update(slug_fields)
    return doc

async def process_single_article(session, url, proxies, semaphore, lock, new_articles_data, pbar):
    async with semaphore:
        html_data = await fetch_html_loop(session, url, proxies)
        if html_data:
            article_data = parse_article_detail(html_data, url)
            async with lock:
                new_articles_data.append(article_data)
        pbar.update(1)

# ================= 5. MAIN EXECUTION =================
async def main():
    print("🔄 Connecting to MongoDB Atlas...")
    collection = get_db_collection()
    print("✅ Connected to Database & Indexing Active!\n")

    cleanup_database_pages(collection)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    }
    connector = aiohttp.TCPConnector(limit=200)
    session_timeout = aiohttp.ClientTimeout(total=10, connect=4)

    async with aiohttp.ClientSession(headers=headers, connector=connector, timeout=session_timeout) as session:
        proxies = await fetch_proxy_list(session)

        # --- TAHAP 1: DISCOVERY SLUGS DARI MOJOK & TERMINAL ---
        print("[*] Mengambil link menu navbar, 'Lainnya dari Mojok', & seluruh kategori...")
        terminal_html = await fetch_html_loop(session, TARGET_TERMINAL_URL, proxies)
        main_html = await fetch_html_loop(session, TARGET_MOJOK_URL, proxies)
        
        category_urls = set()
        if terminal_html:
            category_urls.update(extract_all_nav_and_sub_links(terminal_html))
        if main_html:
            category_urls.update(extract_all_nav_and_sub_links(main_html))

        category_urls = list(category_urls)
        print(f"[+] Ditemukan {len(category_urls)} topik/sub-topik/rubrik gabungan!")

        all_slugs = await fetch_all_sub_slugs(session, category_urls, proxies)
        clean_slugs = [s for s in all_slugs if is_valid_article_url(s)]

        # --- BATCH CHECK ANTI-DUPLIKAT MONGODB ($in) ---
        print("\n🔍 [CEK ANTI-DUPLIKAT] Memeriksa keberadaan URL di MongoDB Atlas...")
        existing_docs = collection.find(
            {"url": {"$in": clean_slugs}}, 
            {"url": 1, "_id": 0}
        )
        existing_urls = {doc["url"] for doc in existing_docs}
        
        pending_urls = [url for url in clean_slugs if url not in existing_urls]

        total_new = len(pending_urls)
        print(f"📊 Laporan Hasil Pengecekan:")
        print(f"   ├─ Total Link Ditemukan di Web : {len(clean_slugs)}")
        print(f"   ├─ Sudah Ada di MongoDB Atlas  : {len(existing_urls)}")
        print(f"   └─ Artikel BARU Siap Ingest   : {total_new}\n")

        if total_new == 0:
            print("🎉 Semua artikel sudah tersimpan di Database. Tidak ada artikel baru!")
            send_telegram_raw("ℹ️ *Bot Mojok Check*: Semua artikel di website sudah up-to-date.")
            return

        # --- TAHAP 2 & 3: SCRAPING & BULK UPSERT MONGODB ---
        print(f"🚀 Memulai proses scraping & ingest untuk {total_new} artikel baru...")
        new_articles_data = []
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT_ARTICLE)
        lock = asyncio.Lock()

        pbar = tqdm(total=total_new, desc="Ingesting Articles", unit="artikel")

        tasks = [process_single_article(session, url, proxies, semaphore, lock, new_articles_data, pbar) for url in pending_urls]
        await asyncio.gather(*tasks)
        pbar.close()

        # BULK WRITE TO MONGODB
        operations = []
        added_articles_list = []
        success_count = 0

        for article in new_articles_data:
            operations.append(
                UpdateOne(
                    {"url": article["url"]},
                    {
                        "$set": article,
                        "$setOnInsert": {"created_at": datetime.now().isoformat()}
                    },
                    upsert=True
                )
            )
            success_count += 1
            added_articles_list.append((article["title"], article["url"], article["is_headline"]))

        if operations:
            collection.bulk_write(operations)

        # --- TAHAP 4: FINISHING & NOTIFICATION ---
        print("\n" + "="*60)
        print(f"🎉 SUCCESS! {success_count} Artikel baru berhasil ditambahkan ke Database MongoDB Atlas:\n")
        for idx, (title, url, is_headline) in enumerate(added_articles_list, 1):
            tag_str = "[HEADLINE] " if is_headline else ""
            print(f" [{idx}] {tag_str}{title}")
            print(f"     URL: {url}\n")
        print("="*60)

        send_telegram_notification(added_articles_list, total_new, success_count)

if __name__ == "__main__":
    asyncio.run(main())
