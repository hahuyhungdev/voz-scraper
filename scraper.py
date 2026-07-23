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
        "start_page": 1,
        "output": "output_review.md",
    },
    {
        "url": "https://voz.vn/t/thread-tong-hop-chia-se-ve-muc-luong-tai-cac-cong-ty-part-2.515355/",
        "label": "Salary Sharing",
        "start_page": 1,
        "output": "output_salary.md",
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
    start_page: int = 1,
    end_page: int | None = None,
) -> list[dict]:
    """Scrape a single thread from start_page to end_page (or total pages)."""
    print(f"\n{'='*60}")
    print(f"Thread: {label}")
    print(f"URL: {base_url}")

    first_url = base_url if start_page == 1 else f"{base_url}page-{start_page}"
    resp = await client.get(first_url, follow_redirects=True)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    detected_max = get_total_pages(soup)
    total_pages = min(detected_max, end_page) if end_page else detected_max
    print(f"Scraping range: {start_page} → {total_pages} (detected total: {detected_max})")

    all_posts = extract_posts(soup)
    print(f"  Page {start_page:>3d}: {len(all_posts)} posts extracted")

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


async def main(
    target_idx: int | None = None,
    start_page: int | None = None,
    end_page: int | None = None,
    output_filename: str | None = None,
) -> None:
    all_posts: list[dict] = []

    configs = [THREADS[target_idx]] if target_idx is not None else THREADS

    async with httpx.AsyncClient(headers=HEADERS, timeout=30, verify=VERIFY_SSL) as client:
        for thread_cfg in configs:
            s_page = start_page if start_page is not None else thread_cfg["start_page"]
            posts = await scrape_thread(
                client,
                base_url=thread_cfg["url"],
                label=thread_cfg["label"],
                start_page=s_page,
                end_page=end_page,
            )
            # Tag each post with its source thread
            for p in posts:
                p["source"] = thread_cfg["label"]
            all_posts.extend(posts)

            # Also write individual thread output
            out_file = output_filename or thread_cfg["output"]
            out_path = Path(__file__).parent / out_file
            _write_md(out_path, posts, thread_cfg["label"])

    if target_idx is None:
        print(f"\n{'='*60}")
        print(f"Grand total: {len(all_posts)} posts")
        _write_md(OUTPUT_COMBINED, all_posts, "VOZ Combined")
        print(f"Combined output: {OUTPUT_COMBINED}")


def combine_all_outputs() -> None:
    """Combine all output_*.md files into output.md."""
    combined_content = ["# VOZ Combined\n\n"]
    output_files = sorted(Path(__file__).parent.glob("output_*.md"))
    for out_path in output_files:
        if out_path.name == "output.md":
            continue
        text = out_path.read_text(encoding="utf-8")
        lines = text.splitlines()
        body_lines = [l for l in lines if not l.startswith("# ")]
        combined_content.append("\n".join(body_lines) + "\n\n")

    OUTPUT_COMBINED.write_text("\n".join(combined_content), encoding="utf-8")
    print(f"Combined {len(output_files)-1 if OUTPUT_COMBINED in output_files else len(output_files)} files into: {OUTPUT_COMBINED}")


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
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg == "--combine":
            combine_all_outputs()
        else:
            t_idx = int(sys.argv[1])
            s_page = int(sys.argv[2]) if len(sys.argv) > 2 else None
            e_page = int(sys.argv[3]) if len(sys.argv) > 3 else None
            o_file = sys.argv[4] if len(sys.argv) > 4 else None
            asyncio.run(main(t_idx, s_page, e_page, o_file))
    else:
        asyncio.run(main())
