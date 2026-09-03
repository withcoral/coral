#!/usr/bin/env python3
"""Scrape web pages and write structured JSONL for the Coral web_scraper source.

Uses requests + BeautifulSoup + lxml for robust scraping.
Optional: --js flag uses Playwright for JavaScript-rendered pages.

Usage:
    python3 scrape.py https://example.com https://example.com/about
    python3 scrape.py --file urls.txt
    python3 scrape.py --js https://spa-site.com        # JS rendering
    python3 scrape.py --file urls.txt --output ~/.coral/web_scraper

Dependencies:
    pip install requests beautifulsoup4 lxml             # required
    pip install playwright && playwright install chromium # optional, for --js

Output:
    pages.jsonl  — one row per URL with title, text, metadata
    links.jsonl  — one row per discovered link on each page
"""

import argparse
import json
import os
import re
import sys
import tempfile
import shutil
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

try:
    import requests
except ImportError:
    sys.exit("Missing dependency: pip install requests")

try:
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("Missing dependency: pip install beautifulsoup4")

try:
    import lxml  # noqa: F401
    BS_PARSER = "lxml"
except ImportError:
    BS_PARSER = "html.parser"
    print(
        "Warning: lxml not installed, falling back to html.parser. "
        "Install for better parsing: pip install lxml",
        file=sys.stderr,
    )

DEFAULT_OUTPUT = os.path.expanduser("~/.coral/web_scraper")
MAX_RESPONSE_BYTES = 10 * 1024 * 1024  # 10 MB
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _is_html(content_type):
    ct = (content_type or "").lower().split(";")[0].strip()
    return ct in ("text/html", "application/xhtml+xml", "")


def fetch_with_requests(url, timeout=30):
    headers = {"User-Agent": USER_AGENT}
    try:
        resp = requests.get(
            url, headers=headers, timeout=timeout,
            allow_redirects=True, stream=True,
        )
    except requests.RequestException as exc:
        print(f"  ✗ {url}: {exc}", file=sys.stderr)
        return None

    try:
        content_type = resp.headers.get("Content-Type", "")
        if not _is_html(content_type):
            return {
                "final_url": resp.url,
                "status_code": resp.status_code,
                "content_type": content_type,
                "html": b"",
            }
        chunks = []
        bytes_read = 0
        for chunk in resp.iter_content(chunk_size=65536):
            chunks.append(chunk)
            bytes_read += len(chunk)
            if bytes_read >= MAX_RESPONSE_BYTES:
                break
        raw = b"".join(chunks)[:MAX_RESPONSE_BYTES]
        return {
            "final_url": resp.url,
            "status_code": resp.status_code,
            "content_type": content_type,
            "html": raw,
        }
    except requests.RequestException as exc:
        print(f"  ✗ {url}: {exc}", file=sys.stderr)
        return None
    finally:
        resp.close()


def create_playwright_fetcher(timeout=30):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        sys.exit(
            "Missing dependency for --js mode:\n"
            "  pip install playwright && playwright install chromium"
        )

    ctx = sync_playwright().start()
    try:
        browser = ctx.chromium.launch(headless=True)
    except Exception:
        ctx.stop()
        raise

    def fetch(url, timeout=timeout):
        page = browser.new_page(user_agent=USER_AGENT)
        try:
            resp = page.goto(url, timeout=timeout * 1000, wait_until="load")
            status_code = resp.status if resp else 0
            content_type = resp.headers.get("content-type", "") if resp else ""
            html = page.content()
            final_url = page.url
            return {
                "final_url": final_url,
                "status_code": status_code,
                "content_type": content_type,
                "html": html,
            }
        except Exception as exc:
            print(f"  ✗ {url}: {exc}", file=sys.stderr)
            return None
        finally:
            page.close()

    def cleanup():
        browser.close()
        ctx.stop()

    return fetch, cleanup


def extract_page(url, result):
    soup = BeautifulSoup(result["html"], BS_PARSER)

    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else None

    meta_desc = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
    description = (
        meta_desc["content"].strip()
        if meta_desc and meta_desc.get("content")
        else None
    )

    html_tag = soup.find("html")
    language = html_tag.get("lang") if html_tag else None

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)

    return {
        "url": url,
        "final_url": result["final_url"],
        "title": title,
        "description": description,
        "text": text,
        "status_code": result["status_code"],
        "content_type": result["content_type"],
        "language": language,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def extract_links(base_url, result):
    soup = BeautifulSoup(result["html"], BS_PARSER)
    final_url = result["final_url"]
    parsed_base = urlparse(final_url)
    links = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href")
        if href is None:
            continue
        href = href.strip()
        if not href or href.lower().startswith(
            ("#", "javascript:", "mailto:", "tel:")
        ):
            continue

        absolute = urljoin(final_url, href)
        parsed = urlparse(absolute)
        is_external = (
            (parsed.hostname or "").lower() != (parsed_base.hostname or "").lower()
        )
        link_text = a_tag.get_text(" ", strip=True) or None

        links.append({
            "source_url": base_url,
            "href": absolute,
            "text": link_text,
            "is_external": is_external,
        })

    return links


def main():
    parser = argparse.ArgumentParser(
        description="Scrape URLs to JSONL for Coral web_scraper source"
    )
    parser.add_argument("urls", nargs="*", help="URLs to scrape")
    parser.add_argument("--file", "-f", help="File with one URL per line")
    parser.add_argument(
        "--output", "-o", default=DEFAULT_OUTPUT,
        help=f"Output directory (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--js", action="store_true",
        help="Use Playwright for JS-rendered pages (requires: pip install playwright)",
    )
    parser.add_argument(
        "--timeout", "-t", type=int, default=30, help="Request timeout in seconds"
    )
    args = parser.parse_args()

    urls = list(args.urls)
    if args.file:
        with open(args.file) as fh:
            for line in fh:
                stripped = line.strip()
                if stripped and not stripped.startswith("#"):
                    urls.append(stripped)

    if not urls:
        parser.error("No URLs provided. Pass URLs as arguments or use --file.")

    pw_cleanup = None
    if args.js:
        fetch, pw_cleanup = create_playwright_fetcher(timeout=args.timeout)
    else:
        fetch = fetch_with_requests

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    pages_path = output_dir / "pages.jsonl"
    links_path = output_dir / "links.jsonl"

    pages_count = 0
    links_count = 0
    fail_count = 0

    tmp_pages = tempfile.NamedTemporaryFile(
        mode="w", dir=output_dir, suffix=".jsonl", delete=False
    )
    tmp_links = tempfile.NamedTemporaryFile(
        mode="w", dir=output_dir, suffix=".jsonl", delete=False
    )

    try:
        for url in urls:
            if not url.startswith(("http://", "https://")):
                url = "https://" + url

            mode = "JS" if args.js else "HTTP"
            print(f"  → [{mode}] {url}")
            result = fetch(url, timeout=args.timeout)
            if result is None:
                fail_count += 1
                continue

            page = extract_page(url, result)
            tmp_pages.write(json.dumps(page) + "\n")
            pages_count += 1

            page_links = extract_links(url, result)
            for link in page_links:
                tmp_links.write(json.dumps(link) + "\n")
                links_count += 1

        tmp_pages.close()
        tmp_links.close()

        if fail_count:
            os.unlink(tmp_pages.name)
            os.unlink(tmp_links.name)
            print(f"\n  ✗ {fail_count} URLs failed — existing files preserved",
                  file=sys.stderr)
            sys.exit(1)

        shutil.move(tmp_pages.name, pages_path)
        shutil.move(tmp_links.name, links_path)

    except BaseException:
        tmp_pages.close()
        tmp_links.close()
        for p in (tmp_pages.name, tmp_links.name):
            if os.path.exists(p):
                os.unlink(p)
        raise
    finally:
        if pw_cleanup:
            pw_cleanup()

    print(f"\n  ✓ {pages_count} pages → {pages_path}")
    print(f"  ✓ {links_count} links → {links_path}")


if __name__ == "__main__":
    main()
