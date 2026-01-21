#!/usr/bin/env python3
"""
Macedonian Wikipedia Scraper for LLM Training Data
===================================================

A production-grade, high-performance web scraper for collecting text data
from the Macedonian Wikipedia (mk.wikipedia.org) using the MediaWiki API.

Features:
- Recursive category crawling with configurable depth
- Async/await concurrency with rate limiting (politeness)
- Incremental JSONL output (crash-resistant)
- Automatic retries with exponential backoff
- URL deduplication across categories
- Comprehensive logging
- Resume capability via visited URLs tracking

Author: Senior Python Data Engineer
License: MIT
"""

import asyncio
import aiohttp
import json
import logging
import time
import re
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional, Set
from urllib.parse import quote
from datetime import datetime


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class ScraperConfig:
    """Configuration for the Wikipedia scraper."""

    # Wikipedia API settings
    base_url: str = "https://mk.wikipedia.org/w/api.php"
    wiki_base: str = "https://mk.wikipedia.org/wiki/"

    # Starting category (without "Category:" prefix)
    root_category: str = "Македонија"

    # Crawling limits
    max_depth: int = 5  # Maximum category recursion depth
    max_articles: int = 100000  # Maximum articles to scrape (safety limit)

    # Concurrency and rate limiting (politeness)
    max_concurrent_requests: int = 5  # Parallel requests (conservative)
    requests_per_second: float = 10.0  # Rate limit (be polite!)

    # Retry settings
    max_retries: int = 3  # Retries per failed request
    retry_base_delay: float = 1.0  # Base delay for exponential backoff
    request_timeout: int = 30  # Request timeout in seconds

    # Output settings
    output_file: str = "mk_wikipedia_dataset.jsonl"
    visited_urls_file: str = "visited_urls.txt"  # For resume capability
    log_interval: int = 100  # Log progress every N articles

    # User agent - CRITICAL:  Must follow Wikimedia guidelines
    # Format: <client name>/<version> (<contact info>) <library/framework>
    # See: https://meta.wikimedia.org/wiki/User-Agent_policy
    user_agent: str = (
        "MacedonianLLMDataCollector/1.0 "
        "(https://github.com/yourusername/project; youremail@example.com) "
        "Python/3.11 aiohttp/3.9"
    )


@dataclass
class Article:
    """Represents a scraped Wikipedia article."""
    title: str
    url: str
    text: str
    categories: list[str]
    page_id: int
    scraped_at: str


# ============================================================================
# RATE LIMITER
# ============================================================================

class RateLimiter:
    """
    Token bucket rate limiter for polite API access.
    Ensures we don't exceed the configured requests per second.
    """

    def __init__(self, rate: float):
        """
        Initialize the rate limiter.

        Args:
            rate: Maximum requests per second
        """
        self.rate = rate
        self.tokens = rate
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Wait until a request token is available."""
        async with self._lock:
            now = time.monotonic()
            # Replenish tokens based on elapsed time
            elapsed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_update = now

            if self.tokens < 1:
                # Wait for token to become available
                wait_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1


# ============================================================================
# WIKIPEDIA SCRAPER
# ============================================================================

class MacedonianWikipediaScraper:
    """
    High-performance async scraper for Macedonian Wikipedia.
    Uses the MediaWiki API for clean, structured data extraction.
    """

    def __init__(self, config: ScraperConfig):
        """Initialize the scraper with the given configuration."""
        self.config = config
        self.rate_limiter = RateLimiter(config.requests_per_second)

        # Deduplication sets
        self.visited_page_ids: Set[int] = set()
        self.visited_categories: Set[str] = set()

        # Statistics
        self.stats = {
            "articles_scraped": 0,
            "categories_processed": 0,
            "errors": 0,
            "retries": 0,
            "start_time": None
        }

        # Setup logging
        self._setup_logging()

        # Load previously visited URLs for resume capability
        self._load_visited_urls()

    def _setup_logging(self):
        """Configure logging with both console and file output."""
        self.logger = logging.getLogger("WikiScraper")
        self.logger.setLevel(logging.INFO)

        # Avoid duplicate handlers on re-init
        if self.logger.handlers:
            return

        # Console handler with formatting
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # File handler for debugging
        file_handler = logging.FileHandler("scraper.log", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _load_visited_urls(self):
        """Load previously visited page IDs for resume capability."""
        visited_file = Path(self.config.visited_urls_file)
        if visited_file.exists():
            with open(visited_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.isdigit():
                        self.visited_page_ids.add(int(line))
            self.logger.info(f"Resumed:  loaded {len(self.visited_page_ids)} previously visited pages")

    def _save_visited_url(self, page_id: int):
        """Append a visited page ID to the tracking file."""
        with open(self.config.visited_urls_file, "a", encoding="utf-8") as f:
            f.write(f"{page_id}\n")

    async def _make_request(
            self,
            session: aiohttp.ClientSession,
            params: dict
    ) -> Optional[dict]:
        """
        Make an API request with rate limiting and retries.

        Args:
            session: aiohttp session
            params: API parameters

        Returns:
            JSON response or None on failure
        """
        params["format"] = "json"
        params["formatversion"] = "2"  # Use modern format

        for attempt in range(self.config.max_retries):
            try:
                # Wait for rate limiter
                await self.rate_limiter.acquire()

                async with session.get(
                        self.config.base_url,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=self.config.request_timeout)
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:  # Too Many Requests
                        wait_time = int(response.headers.get("Retry-After", 60))
                        self.logger.warning(f"Rate limited.  Waiting {wait_time}s...")
                        await asyncio.sleep(wait_time)
                    elif response.status == 403:
                        # Log detailed error for debugging
                        body = await response.text()
                        self.logger.error(
                            f"HTTP 403 Forbidden.  This usually means the User-Agent "
                            f"is being blocked. Response: {body[: 500]}"
                        )
                        # Don't retry 403s - it won't help
                        return None
                    else:
                        self.logger.warning(f"HTTP {response.status} for params: {params}")

            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout (attempt {attempt + 1}/{self.config.max_retries})")
            except aiohttp.ClientError as e:
                self.logger.warning(f"Client error: {e} (attempt {attempt + 1}/{self.config.max_retries})")
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")

            # Exponential backoff
            if attempt < self.config.max_retries - 1:
                delay = self.config.retry_base_delay * (2 ** attempt)
                self.stats["retries"] += 1
                await asyncio.sleep(delay)

        self.stats["errors"] += 1
        return None

    async def get_category_members(
            self,
            session: aiohttp.ClientSession,
            category: str,
            member_type: str = "page|subcat"
    ) -> tuple[list[dict], list[str]]:
        """
        Get all members of a category (articles and subcategories).

        Args:
            session: aiohttp session
            category: Category name (without "Category:" prefix)
            member_type: Type of members to fetch ("page", "subcat", or "page|subcat")

        Returns:
            Tuple of (list of page info dicts, list of subcategory names)
        """
        pages = []
        subcategories = []
        continue_token = None

        while True:
            params = {
                "action": "query",
                "list": "categorymembers",
                "cmtitle": f"Категорија:{category}",
                "cmlimit": 500,  # Maximum allowed
                "cmtype": member_type
            }

            if continue_token:
                params["cmcontinue"] = continue_token

            data = await self._make_request(session, params)

            if not data or "query" not in data:
                break

            for member in data.get("query", {}).get("categorymembers", []):
                ns = member.get("ns")
                if ns == 0:  # Article namespace
                    pages.append({
                        "pageid": member["pageid"],
                        "title": member["title"]
                    })
                elif ns == 14:  # Category namespace
                    # Extract category name without prefix
                    cat_name = member["title"].replace("Категорија:", "")
                    subcategories.append(cat_name)

            # Check for continuation
            if "continue" in data:
                continue_token = data["continue"].get("cmcontinue")
            else:
                break

        return pages, subcategories

    async def get_article_content(
            self,
            session: aiohttp.ClientSession,
            page_id: int,
            title: str
    ) -> Optional[Article]:
        """
        Fetch the full content and categories of an article.

        Args:
            session: aiohttp session
            page_id: Wikipedia page ID
            title: Article title

        Returns:
            Article object or None on failure
        """
        params = {
            "action": "query",
            "pageids": page_id,
            "prop": "extracts|categories",
            "explaintext": 1,  # Plain text, no HTML
            "exsectionformat": "plain",
            "cllimit": 50,  # Get up to 50 categories
        }

        data = await self._make_request(session, params)

        if not data:
            return None

        try:
            # formatversion=2 returns pages as a list
            pages = data.get("query", {}).get("pages", [])
            if not pages:
                return None

            page_data = pages[0] if isinstance(pages, list) else pages.get(str(page_id), {})

            # Extract text content
            text = page_data.get("extract", "")
            if not text or len(text.strip()) < 50:  # Skip very short articles
                return None

            # Clean the text
            text = self._clean_text(text)

            # Extract categories
            categories = [
                cat.get("title", "").replace("Категорија:", "")
                for cat in page_data.get("categories", [])
            ]

            # Build article URL
            url = self.config.wiki_base + quote(title.replace(" ", "_"))

            return Article(
                title=title,
                url=url,
                text=text,
                categories=categories,
                page_id=page_id,
                scraped_at=datetime.utcnow().isoformat()
            )

        except Exception as e:
            self.logger.error(f"Error parsing article {title}: {e}")
            return None

    def _clean_text(self, text: str) -> str:
        """
        Clean extracted text for LLM training.

        Args:
            text: Raw extracted text

        Returns:
            Cleaned text
        """
        # Remove multiple newlines
        text = re.sub(r'\n{3,}', '\n\n', text)

        # Remove reference markers like [1], [2], etc.
        text = re.sub(r'\[\d+\]', '', text)

        # Remove edit links markers
        text = re.sub(r'\[уреди.*?\]', '', text)

        # Normalize whitespace
        text = re.sub(r'[ \t]+', ' ', text)

        # Strip lines
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)

        return text.strip()

    def _save_article(self, article: Article):
        """
        Append an article to the JSONL output file.

        Args:
            article: Article to save
        """
        with open(self.config.output_file, "a", encoding="utf-8") as f:
            json_line = json.dumps(asdict(article), ensure_ascii=False)
            f.write(json_line + "\n")

        # Also save to visited URLs for resume capability
        self._save_visited_url(article.page_id)

    async def _process_articles_batch(
            self,
            session: aiohttp.ClientSession,
            pages: list[dict],
            semaphore: asyncio.Semaphore
    ) -> int:
        """
        Process a batch of articles concurrently.

        Args:
            session: aiohttp session
            pages: List of page info dicts
            semaphore: Concurrency limiter

        Returns:
            Number of articles successfully scraped
        """

        async def fetch_one(page_info: dict) -> Optional[Article]:
            async with semaphore:
                page_id = page_info["pageid"]

                # Skip if already visited (deduplication)
                if page_id in self.visited_page_ids:
                    return None

                article = await self.get_article_content(
                    session,
                    page_id,
                    page_info["title"]
                )

                if article:
                    self.visited_page_ids.add(page_id)
                    self._save_article(article)
                    self.stats["articles_scraped"] += 1

                    # Log progress
                    if self.stats["articles_scraped"] % self.config.log_interval == 0:
                        elapsed = time.time() - self.stats["start_time"]
                        rate = self.stats["articles_scraped"] / elapsed
                        self.logger.info(
                            f"Progress: {self.stats['articles_scraped']: ,} articles | "
                            f"{rate:.1f} articles/sec | "
                            f"Categories: {self.stats['categories_processed']:,} | "
                            f"Errors: {self.stats['errors']:,}"
                        )

                return article

        # Fetch all articles in parallel (limited by semaphore)
        tasks = [fetch_one(page) for page in pages]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        return sum(1 for r in results if isinstance(r, Article))

    async def crawl_category_recursive(
            self,
            session: aiohttp.ClientSession,
            category: str,
            depth: int,
            semaphore: asyncio.Semaphore
    ):
        """
        Recursively crawl a category and all its subcategories.

        Args:
            session:  aiohttp session
            category: Category name to crawl
            depth: Current recursion depth
            semaphore: Concurrency limiter
        """
        # Check limits
        if depth > self.config.max_depth:
            return

        if self.stats["articles_scraped"] >= self.config.max_articles:
            self.logger.info(f"Reached max articles limit:  {self.config.max_articles}")
            return

        # Skip if already visited
        if category in self.visited_categories:
            return

        self.visited_categories.add(category)
        self.stats["categories_processed"] += 1

        self.logger.debug(f"Crawling category: {category} (depth={depth})")

        # Get category members
        pages, subcategories = await self.get_category_members(session, category)

        self.logger.info(
            f"Category '{category}':  {len(pages)} articles, "
            f"{len(subcategories)} subcategories (depth={depth})"
        )

        # Process articles in this category
        if pages:
            # Filter out already visited pages
            new_pages = [p for p in pages if p["pageid"] not in self.visited_page_ids]
            if new_pages:
                await self._process_articles_batch(session, new_pages, semaphore)

        # Recursively process subcategories
        for subcat in subcategories:
            if self.stats["articles_scraped"] >= self.config.max_articles:
                break
            await self.crawl_category_recursive(session, subcat, depth + 1, semaphore)

    async def run(self):
        """
        Main entry point to run the scraper.
        """
        self.logger.info("=" * 60)
        self.logger.info("Macedonian Wikipedia Scraper - Starting")
        self.logger.info("=" * 60)
        self.logger.info(f"Root category: {self.config.root_category}")
        self.logger.info(f"Max depth: {self.config.max_depth}")
        self.logger.info(f"Concurrency:  {self.config.max_concurrent_requests}")
        self.logger.info(f"Rate limit: {self.config.requests_per_second} req/sec")
        self.logger.info(f"Output file: {self.config.output_file}")
        self.logger.info(f"User-Agent: {self.config.user_agent}")
        self.logger.info("=" * 60)

        self.stats["start_time"] = time.time()

        # Create HTTP session with PROPER headers for Wikipedia
        # See: https://meta.wikimedia.org/wiki/User-Agent_policy
        headers = {
            "User-Agent": self.config.user_agent,
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "mk,en;q=0.9",
        }

        connector = aiohttp.TCPConnector(
            limit=self.config.max_concurrent_requests,
            limit_per_host=self.config.max_concurrent_requests,
            ttl_dns_cache=300,  # Cache DNS for 5 minutes
        )

        async with aiohttp.ClientSession(
                headers=headers,
                connector=connector,
                raise_for_status=False  # We handle status codes manually
        ) as session:
            # Semaphore for limiting concurrent requests
            semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)

            try:
                await self.crawl_category_recursive(
                    session,
                    self.config.root_category,
                    depth=0,
                    semaphore=semaphore
                )
            except KeyboardInterrupt:
                self.logger.info("Interrupted by user.  Progress saved.")
            except Exception as e:
                self.logger.error(f"Critical error: {e}")
                raise

        # Final statistics
        elapsed = time.time() - self.stats["start_time"]
        self.logger.info("=" * 60)
        self.logger.info("Scraping Complete!")
        self.logger.info("=" * 60)
        self.logger.info(f"Total articles:  {self.stats['articles_scraped']: ,}")
        self.logger.info(f"Categories processed:  {self.stats['categories_processed']:,}")
        self.logger.info(f"Errors: {self.stats['errors']:,}")
        self.logger.info(f"Retries: {self.stats['retries']: ,}")
        self.logger.info(f"Time elapsed:  {elapsed:.1f} seconds")
        if elapsed > 0:
            self.logger.info(f"Average rate: {self.stats['articles_scraped'] / elapsed:. 2f} articles/sec")
        self.logger.info(f"Output saved to: {self.config.output_file}")
        self.logger.info("=" * 60)


# ============================================================================
# ENTRY POINT
# ============================================================================

async def main():
    """Main async entry point."""
    # Create configuration - CUSTOMIZE THESE VALUES!
    config = ScraperConfig(
        root_category="Култура",
        max_depth=5,
        max_articles=100000,
        max_concurrent_requests=5,  # Conservative to avoid blocks
        requests_per_second=10.0,  # Polite rate limiting
        output_file="mk_wikipedia_dataset.jsonl",

        # IMPORTANT: Update this with YOUR contact info!
        # Wikipedia REQUIRES a valid contact email/URL
        user_agent=(
            "MacedonianLLMDataCollector/1.0 "
            "(dimitar.ahtarov@students.finki.ukim.mk) "
            "Python/3.11 aiohttp/3.9"
        )
    )

    # Create and run scraper
    scraper = MacedonianWikipediaScraper(config)
    await scraper.run()


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())