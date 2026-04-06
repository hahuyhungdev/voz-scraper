"""
Process output.md → posts.json with company name extraction & deobfuscation.
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

INPUT_FILE = Path(__file__).parent / "output.md"
OUTPUT_JSON = Path(__file__).parent / "posts.json"

# ── Known company mappings: obfuscated → canonical name ───────────────────────
# The key is a lowercase "deobfuscation pattern" and the value is the real name.
# We also auto-detect obfuscated names via * removal + fuzzy matching.
KNOWN_COMPANIES = {
    "nab": "NAB",
    "nab innovation centre vietnam": "NAB",
    "nab innovation centre vietnam (nicv)": "NAB",
    "nicv": "NAB",
    "n*b": "NAB",
    "n*b vietnam": "NAB",
    "naver": "Naver",
    "n*ver": "Naver",
    "epam": "EPAM Anywhere",
    "epam anywhere": "EPAM Anywhere",
    "ep*m": "EPAM Anywhere",
    "grab": "Grab",
    "g***": "Grab",
    "g**b": "Grab",
    "gr*b": "Grab",
    "shopee": "Shopee",
    "sh*pee": "Shopee",
    "sh**ee": "Shopee",
    "s**pee": "Shopee",
    "shopback": "ShopBack",
    "vtex": "VTex",
    "v*tex": "VTex",
    "multiplier": "Multiplier",
    "m*ltiplier": "Multiplier",
    "infinity technology": "Infinity Technology",
    "in*inity t*chnology": "Infinity Technology",
    "idealab": "IdeaLab",
    "id*alab": "IdeaLab",
    "fsoft": "FPT Software",
    "fpt software": "FPT Software",
    "fpt": "FPT Software",
    "go1 platform": "Go1",
    "go1": "Go1",
    "ninjavan": "NinjaVan",
    "ninja van": "NinjaVan",
    "garena": "Garena",
    "rakuten": "Rakuten",
    "manabie": "Manabie",
    "one mount group": "One Mount Group",
    "one mount": "One Mount Group",
    "samsung r&d center vietnam": "Samsung R&D",
    "samsung": "Samsung R&D",
    "employment hero": "Employment Hero",
    "opswat": "OPSWAT",
    "paypay japan": "PayPay Japan",
    "paypay": "PayPay Japan",
    "worldquant": "WorldQuant",
    "thoughtworks": "Thoughtworks",
    "dytechlab": "Dytechlab",
    "gotit.ai": "Got It AI",
    "got it ai": "Got It AI",
    "gotit ai": "Got It AI",
    "zalo": "Zalo",
    "cmc": "CMC",
    "datalogic": "Datalogic",
    "minswap": "Minswap",
    "nexon": "Nexon",
    "nexon dev vina": "Nexon",
    "nexon vina networks": "Nexon",
    "opencommerce": "OpenCommerce",
    "plusteam.io": "Plusteam.io",
    "plusteam": "Plusteam.io",
    "techvify": "Techvify",
    "viettel": "Viettel",
    "viettel telecom": "Viettel",
    "tiki": "Tiki",
    "ti*i": "Tiki",
    "t*ki": "Tiki",
    "momo": "MoMo",
    "m*mo": "MoMo",
    "mo*o": "MoMo",
    "vnpay": "VNPay",
    "vn*ay": "VNPay",
    "being": "Being",
    "axon": "Axon",
    "ax*on": "Axon",
    "a*on": "Axon",
    "lazada": "Lazada",
    "l*zada": "Lazada",
    "la*ada": "Lazada",
    "bosch": "Bosch",
    "b*sch": "Bosch",
    "ến ô pây": "VNPay",
    "rích cây sóp": "RichCashop",
    "da nô pay": "VNPay",
    "vi ti ai": "VTI",
    "vti": "VTI",
    "iapp tech": "iApp Tech",
    "outcubator": "Outcubator",
    "rapiddweller gmbh": "RapidDweller GmbH",
    "next practice": "Next Practice",
    "next practice (ptc b.o.t.)": "Next Practice",
}


def deobfuscate(name: str) -> str:
    """Remove obfuscation characters and normalize a company name."""
    # Strip whitespace
    cleaned = name.strip()
    if not cleaned:
        return ""

    low = cleaned.lower()

    # Direct lookup
    if low in KNOWN_COMPANIES:
        return KNOWN_COMPANIES[low]

    # Try removing * and matching
    no_stars = re.sub(r'[*_]+', '', low)
    no_stars_stripped = no_stars.strip()
    if no_stars_stripped in KNOWN_COMPANIES:
        return KNOWN_COMPANIES[no_stars_stripped]

    # Try matching with * replaced by wildcards
    # Build regex from obfuscated name: each * matches any single char
    if '*' in low:
        pattern = ''
        for ch in low:
            if ch == '*':
                pattern += '.'
            else:
                pattern += re.escape(ch)
        pattern = f'^{pattern}$'
        for key, canonical in KNOWN_COMPANIES.items():
            if '*' not in key and re.match(pattern, key):
                return canonical

    # Check if no_stars matches any known key (without stars)
    for key, canonical in KNOWN_COMPANIES.items():
        key_no_stars = re.sub(r'[*_]+', '', key)
        if no_stars_stripped == key_no_stars:
            return canonical

    # Return the original (title-cased) if no match
    return cleaned


def parse_posts(md_text: str) -> list[dict]:
    """Parse the markdown output into structured post dicts."""
    posts = []
    # Split by the --- separator and ### Post headers
    post_blocks = re.split(r'\n---\n', md_text)

    for block in post_blocks:
        block = block.strip()
        if not block:
            continue

        # Match post header: ### Post #N — Author [Source]
        header_match = re.match(
            r'###\s+Post\s+#(\d+)\s+[—–-]\s+(.+?)(?:\s+\[(.+?)\])?\s*$', block, re.MULTILINE
        )
        if not header_match:
            continue

        post_num = int(header_match.group(1))
        author = header_match.group(2).strip()
        source = header_match.group(3) or ""

        # Extract date
        date_match = re.search(r'\*\*Date:\*\*\s*(.+)', block)
        date_str = date_match.group(1).strip() if date_match else "Unknown"

        # Extract content (everything after the date line)
        content_match = re.search(r'\*\*Date:\*\*[^\n]*\n\n(.*)', block, re.DOTALL)
        content = content_match.group(1).strip() if content_match else ""

        if not content:
            continue

        # Extract company names from content
        companies = extract_companies(content)

        posts.append({
            "id": post_num,
            "author": author,
            "date": date_str,
            "content": content,
            "companies": companies,
            "source": source,
        })

    return posts


def extract_companies(text: str) -> list[str]:
    """Extract and deobfuscate company names from post content."""
    companies = set()

    # Pattern 1: "Công ty: X" or "Cty: X" (structured posts)
    for m in re.finditer(
        r'(?:Công ty|Cty|Company)\s*:\s*([^\n,]{2,60})', text, re.IGNORECASE
    ):
        raw = m.group(1).strip()
        # Clean trailing junk
        raw = re.sub(r'\s*\(.*$', '', raw)  # remove parentheticals at end
        raw = re.sub(r'\s*[,.].*$', '', raw)  # remove after comma/period
        if len(raw) < 2 or raw.lower() == "abc":
            continue
        canonical = deobfuscate(raw)
        if canonical and len(canonical) >= 2:
            companies.add(canonical)

    # Pattern 2: Check for well-known company names mentioned in text
    # (even without "Công ty:" prefix)
    text_lower = text.lower()
    # Only check unambiguous company names (3+ chars, distinct)
    check_names = {
        "shopee", "grab", "tiki", "lazada", "momo", "vnpay",
        "ninjavan", "rakuten", "garena", "zalo", "bosch",
        "samsung", "employment hero", "opswat", "worldquant",
        "thoughtworks", "dytechlab", "shopback", "manabie",
        "one mount", "axon", "nexon", "fsoft", "fpt software",
        "datalogic", "minswap", "techvify", "viettel",
    }
    for name in check_names:
        if name in text_lower:
            canonical = KNOWN_COMPANIES.get(name, name.title())
            companies.add(canonical)

    return sorted(companies)


def build_company_index(posts: list[dict]) -> list[dict]:
    """Build a company → post_ids index with mention counts."""
    company_posts = defaultdict(set)
    for post in posts:
        for company in post["companies"]:
            company_posts[company].add(post["id"])

    result = []
    for company, post_ids in sorted(
        company_posts.items(), key=lambda x: -len(x[1])
    ):
        result.append({
            "name": company,
            "count": len(post_ids),
            "post_ids": sorted(post_ids),
        })

    return result


def main():
    print("Reading output.md...")
    md_text = INPUT_FILE.read_text(encoding="utf-8")

    print("Parsing posts...")
    posts = parse_posts(md_text)
    print(f"  Parsed {len(posts)} posts")

    print("Building company index...")
    companies = build_company_index(posts)
    print(f"  Found {len(companies)} unique companies")

    # Show top companies
    print("\nTop companies by mention count:")
    for c in companies[:20]:
        print(f"  {c['count']:>3d}x  {c['name']}")

    data = {
        "meta": {
            "total_posts": len(posts),
            "total_companies": len(companies),
            "source": "https://voz.vn/t/event-box-cntt-2023-chia-se-kinh-nghiem-phong-van.694369/",
        },
        "companies": companies,
        "posts": posts,
    }

    print(f"\nWriting {OUTPUT_JSON}...")
    OUTPUT_JSON.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print("Done!")


if __name__ == "__main__":
    main()
