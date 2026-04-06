"""
VOZ Forum Thread Scraper
Scrapes all posts from a VOZ forum thread, cleaning nested quotes.
Outputs a Markdown file with Author, Date, and cleaned text content.
"""

import asyncio
import random
import re
import sys
from pathlib import Path

import httpx
from bs4 import BeautifulSoup, Tag

# ── Config ────────────────────────────────────────────────────────────────────
THREADS = [
    {
        "url": "https://voz.vn/t/event-box-cntt-2023-chia-se-kinh-nghiem-phong-van.694369/",
        "label": "Interview Experiences",
        "start_page": 1,
        "output": "output_interview.md",
    },
    {
        "url": "https://voz.vn/t/review-cong-ty-cntt-boi-het-vao-viet-tat-ten-moi-cty.677450/",
        "label": "Company Reviews",
        "start_page": 350,
        "output": "output_review.md",
    },
]
OUTPUT_COMBINED = Path(__file__).parent / "output.md"
MIN_POST_LENGTH = 20  # Skip short "Ưng", "Hóng", "Chấm" posts
DELAY_MIN = 1.0  # seconds
DELAY_MAX = 3.0
CONCURRENT_LIMIT = 5  # max concurrent page fetches

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}

# Some environments have SSL issues with VOZ's cert chain.
# Set to False to skip verification if needed (scraping a known public site).
VERIFY_SSL = False


def get_total_pages(soup: BeautifulSoup) -> int:
    """Extract total page count from the pagination nav."""
    # XenForo puts page numbers in <ul class="pageNav-main"> > <li> > <a>
    page_nav = soup.select_one("ul.pageNav-main")
    if not page_nav:
        return 1
    links = page_nav.select("li a")
    max_page = 1
    for link in links:
        text = link.get_text(strip=True)
        if text.isdigit():
            max_page = max(max_page, int(text))
    return max_page


def extract_posts(soup: BeautifulSoup) -> list[dict]:
    """Extract all posts from a single page."""
    posts = []
    articles = soup.select("article.message")
    for article in articles:
        try:
            # ── Author ────────────────────────────────────────────────────
            author = article.get("data-author", "").strip()
            if not author:
                author_el = article.select_one(".message-name")
                author = author_el.get_text(strip=True) if author_el else "Unknown"

            # ── Date ──────────────────────────────────────────────────────
            time_el = article.select_one(".message-attribution-main time.u-dt")
            if not time_el:
                time_el = article.select_one("time.u-dt")
            if time_el:
                post_date = time_el.get("title") or time_el.get("datetime", time_el.get_text(strip=True))
            else:
                post_date = "Unknown"

            # ── Content (cleaned) ─────────────────────────────────────────
            body = article.select_one(".message-body .bbWrapper")
            if not body:
                continue

            # Remove nested quotes BEFORE extracting text
            for quote_block in body.select(".bbCodeBlock--expandable"):
                quote_block.decompose()
            # Also remove non-expandable quote blocks (simple quotes)
            for quote_block in body.select(".bbCodeBlock--quote"):
                quote_block.decompose()

            # Get text, normalise whitespace
            text = body.get_text(separator="\n", strip=True)
            text = re.sub(r"\n{3,}", "\n\n", text)  # collapse excessive newlines

            if len(text) < MIN_POST_LENGTH:
                continue

            posts.append({"author": author, "date": post_date, "text": text})
        except Exception as e:
            print(f"  [WARN] Skipping a post due to error: {e}")
            continue

    return posts


async def fetch_page(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    base_url: str,
    page_num: int,
) -> list[dict]:
    """Fetch and parse a single page, respecting concurrency + delay."""
    url = base_url if page_num == 1 else f"{base_url}page-{page_num}"
    async with semaphore:
        # Random delay to be polite
        await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        try:
            resp = await client.get(url, follow_redirects=True)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(f"  [ERROR] Page {page_num}: HTTP {e.response.status_code}")
            return []
        except httpx.RequestError as e:
            print(f"  [ERROR] Page {page_num}: {e}")
            return []

    soup = BeautifulSoup(resp.text, "lxml")
    posts = extract_posts(soup)
    print(f"  Page {page_num:>3d}: {len(posts)} posts extracted")
    return posts


async def scrape_thread(
    client: httpx.AsyncClient,
    base_url: str,
    label: str,
    start_page: int,
) -> list[dict]:
    """Scrape a single thread from start_page to the last page."""
    print(f"\n{'='*60}")
    print(f"Thread: {label}")
    print(f"URL: {base_url}")
    print(f"Start page: {start_page}")

    # Fetch start page to detect total pages
    first_url = base_url if start_page == 1 else f"{base_url}page-{start_page}"
    resp = await client.get(first_url, follow_redirects=True)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    total_pages = get_total_pages(soup)
    print(f"Total pages in thread: {total_pages} (scraping {start_page}→{total_pages})")

    # Parse the first fetched page immediately
    all_posts = extract_posts(soup)
    print(f"  Page {start_page:>3d}: {len(all_posts)} posts extracted")

    # Fetch remaining pages concurrently (bounded)
    remaining = list(range(start_page + 1, total_pages + 1))
    if remaining:
        semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
        tasks = [
            fetch_page(client, semaphore, base_url, p) for p in remaining
        ]
        results = await asyncio.gather(*tasks)
        for page_posts in results:
            all_posts.extend(page_posts)

    print(f"  Subtotal: {len(all_posts)} posts from {label}")
    return all_posts


async def main() -> None:
    all_posts: list[dict] = []
    thread_labels: list[str] = []
    total_pages_all = 0

    async with httpx.AsyncClient(headers=HEADERS, timeout=30, verify=VERIFY_SSL) as client:
        for thread_cfg in THREADS:
            posts = await scrape_thread(
                client,
                base_url=thread_cfg["url"],
                label=thread_cfg["label"],
                start_page=thread_cfg["start_page"],
            )
            # Tag each post with its source thread
            for p in posts:
                p["source"] = thread_cfg["label"]
            all_posts.extend(posts)
            thread_labels.append(thread_cfg["label"])

            # Also write individual thread output
            out_path = Path(__file__).parent / thread_cfg["output"]
            _write_md(out_path, posts, thread_cfg["label"])

    print(f"\n{'='*60}")
    print(f"Grand total: {len(all_posts)} posts")

    # Write combined output
    _write_md(OUTPUT_COMBINED, all_posts, "VOZ Combined")
    print(f"Combined output: {OUTPUT_COMBINED}")


def _write_md(path: Path, posts: list[dict], title: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# {title}\n\n")
        f.write(f"_Scraped {len(posts)} posts._\n\n")
        f.write("---\n\n")
        for i, post in enumerate(posts, 1):
            source_tag = f" [{post.get('source', '')}]" if post.get("source") else ""
            f.write(f"### Post #{i} — {post['author']}{source_tag}\n\n")
            f.write(f"**Date:** {post['date']}\n\n")
            f.write(post["text"])
            f.write("\n\n---\n\n")
    print(f"  Saved: {path} ({len(posts)} posts)")


if __name__ == "__main__":
    asyncio.run(main())
