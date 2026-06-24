#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Playwright-first article crawler.

The public class name remains HybridCrawler for compatibility with the
existing scheduler and API code, but the implementation no longer calls
Firecrawl. The crawler treats the user-provided URL as a column/list page:
it uses Playwright to discover every article candidate it can find, then
delegates detail-page extraction and database writes to ArticleLinkExtractor.
"""

import asyncio
import time
from typing import Dict, Optional
from urllib.parse import urlparse

from crawl_options import normalize_crawl_options
from sqlite_database import SQLiteDatabase
from supplemental_link_discovery import (
    discover_supplemental_article_links,
    merge_link_candidates,
)
from utils import get_china_time


class HybridCrawler:
    """Compatibility wrapper for the Playwright column crawler."""

    def __init__(self, db: Optional[SQLiteDatabase] = None):
        self.db = db or SQLiteDatabase()
        print("Playwright article crawler initialized")
        print("   - Column/list pages are crawled with Playwright")
        print("   - Article details are extracted with the smart multi-parser pipeline")
        print("   - Firecrawl is not used in this flow")

    def crawl_news_site(
        self,
        list_url: str,
        limit: float = float("inf"),
        wait_for: int = 8000,
        keywords: str = "",
        kb_id: str = None,
        days_limit: int = 7,
        start_date: str = None,
        end_date: str = None,
        log_callback=None,
        crawl_options: Dict = None,
        task_id: str = None,
    ) -> Dict:
        """
        Crawl a news/article column page without Firecrawl.

        Args:
            list_url: Column/list page URL.
            limit: Kept for compatibility and display. Actual discovery is
                intentionally exhaustive unless a caller passes a finite value
                to PlaywrightLinkExtractor in the future.
            wait_for: JavaScript render wait time in milliseconds.
            keywords: Optional keyword filter; applied during article save.
            kb_id: Optional RAGFlow dataset id.
            days_limit: Only save articles within the last N days; 0 disables it.
            start_date: Optional explicit lower date bound (YYYY-MM-DD).
            end_date: Optional explicit upper date bound (YYYY-MM-DD).
            log_callback: Optional UI log callback.
        """
        crawl_options = normalize_crawl_options(crawl_options)
        wait_for = int(crawl_options.get("wait_for_ms") or wait_for)
        start_time = time.time()

        def log(message: str):
            print(message)
            if log_callback:
                log_callback(message)

        log("=" * 70)
        log("Playwright column crawler started")
        log("=" * 70)
        log(f"Target URL: {list_url}")
        log(f"Wait time: {wait_for}ms")
        log(
            "Strategy options: "
            f"max_pages={crawl_options.get('max_pages')}, "
            f"max_empty_pages={crawl_options.get('max_empty_pages')}, "
            f"detail_retries={crawl_options.get('detail_max_retries')}, "
            f"supplemental={'on' if crawl_options.get('supplemental_enabled') else 'off'}, "
            f"network_json={'on' if crawl_options.get('network_json_enabled') else 'off'}, "
            f"proxy={'on' if crawl_options.get('proxy_enabled') else 'off'}"
        )
        if keywords:
            log(f"Keyword filter: {keywords}")
        if start_date or end_date:
            log(f"Date limit: {start_date or 'unbounded'} ~ {end_date or 'unbounded'}")
        elif days_limit and days_limit > 0:
            log(f"Date limit: last {days_limit} days")
        else:
            log("Date limit: disabled")
        log("Strategy: Playwright discovers candidates; smart extractor saves article details")
        log("")

        from article_link_extractor import ArticleLinkExtractor
        from playwright_link_extractor import PlaywrightLinkExtractor

        extractor = ArticleLinkExtractor(db=self.db, enable_smart_validation=False)

        playwright_error = None
        playwright_stats = {}

        try:
            link_extractor = PlaywrightLinkExtractor(crawl_options=crawl_options)

            async def extract_links_async():
                return await link_extractor.extract_links_from_url(
                    url=list_url,
                    max_articles=float("inf"),
                    max_pages=crawl_options.get("max_pages", float("inf")),
                    wait_time=max(1, wait_for // 1000),
                )

            log("=" * 70)
            log("Step 1: Discover article candidates with Playwright")
            log("=" * 70)
            pl_result = asyncio.run(extract_links_async())

            if not pl_result.get("success"):
                error = pl_result.get("error", "unknown Playwright extraction error")
                log(f"Playwright link discovery failed: {error}")
                playwright_error = error
                pl_result = {"articles": [], "stats": {}}
            playwright_stats = pl_result.get("stats", {})

            raw_links = []
            for article in pl_result.get("articles", []):
                link_info = {
                    "title": article.get("title", ""),
                    "url": article.get("url", ""),
                    "text": article.get("title", ""),
                    "publish_date": article.get("publish_date", ""),
                    "source_method": article.get("source_method") or "playwright",
                }
                for field in ("content_hint", "content_hint_source", "discovery_source_url", "authors"):
                    if article.get(field):
                        link_info[field] = article.get(field)
                raw_links.append(link_info)

            log(f"Playwright discovered {len(raw_links)} candidate links")
            if playwright_stats.get("network_json_candidates"):
                log(
                    "   - Network JSON candidates: "
                    f"{playwright_stats.get('network_json_candidates', 0)} "
                    f"(inline {playwright_stats.get('network_json_inline_candidates', 0)})"
                )
            log("")

        except Exception as e:
            import traceback

            traceback.print_exc()
            log(f"Playwright link discovery exception: {e}")
            playwright_error = str(e)
            raw_links = []

        playwright_candidate_count = len(raw_links)

        log("=" * 70)
        log("Step 2: Discover supplemental candidates")
        log("=" * 70)

        supplemental_links = []
        supplemental_stats = {}
        try:
            supplemental_result = discover_supplemental_article_links(
                list_url,
                crawl_options=crawl_options,
            )
            supplemental_links = supplemental_result.get("links", [])
            supplemental_stats = supplemental_result.get("stats", {})
            log(f"Supplemental candidates discovered: {len(supplemental_links)}")
            log(f"   - Static HTML: {supplemental_stats.get('html_static_candidates', 0)}")
            log(f"   - Attributes/buttons: {supplemental_stats.get('attribute_candidates', 0)}")
            log(f"   - Structured data: {supplemental_stats.get('structured_candidates', 0)}")
            log(f"   - Embedded scripts: {supplemental_stats.get('embedded_script_candidates', 0)}")
            log(f"   - Static pagination: {supplemental_stats.get('static_pagination_candidates', 0)}")
            log(f"   - Feeds: {supplemental_stats.get('feed_candidates', 0)}")
            if supplemental_stats.get("feed_inline_candidates"):
                log(f"   - Feed inline bodies: {supplemental_stats.get('feed_inline_candidates', 0)}")
            log(f"   - Sitemaps: {supplemental_stats.get('sitemap_candidates', 0)}")
            if supplemental_stats.get("errors"):
                log(f"   - Supplemental warnings: {len(supplemental_stats.get('errors', []))}")
        except Exception as e:
            supplemental_stats = {"errors": [str(e)]}
            log(f"Supplemental discovery skipped after error: {e}")

        raw_links = merge_link_candidates(raw_links, supplemental_links)

        log("")
        log("=" * 70)
        log("Step 3: Merge, filter, and deduplicate candidates")
        log("=" * 70)
        log(f"Candidate links before filtering: {len(raw_links)}")

        valid_links = []
        discovery_audit_items = []
        filtered_count = 0
        seen_urls = set()
        invalid_patterns = (
            ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bmp", ".tiff",
            ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar",
            ".mp3", ".mp4", ".avi", ".mov", ".wmv",
            "/images/", "/img/", "/static/", "/assets/", "/media/", "/uploads/",
            "/photo/", "/picture/", "/gallery/", "/thumbnail/", "/thumb/",
            "data:image", "javascript:", "mailto:",
        )

        def add_discovery_audit(link: Dict, status: str, reason: str):
            discovery_audit_items.append(
                {
                    "url": link.get("url", ""),
                    "title": link.get("title") or link.get("text") or "",
                    "source_method": link.get("source_method") or "unknown",
                    "discovery_source_url": link.get("discovery_source_url"),
                    "content_hint_source": link.get("content_hint_source"),
                    "candidate_publish_date": link.get("publish_date"),
                    "status": status,
                    "reason": reason,
                    "updated_at": get_china_time().isoformat(),
                }
            )

        for link in raw_links:
            original_url = (link.get("url") or "").strip()
            url_key = original_url.lower()
            if not original_url:
                filtered_count += 1
                add_discovery_audit(link, "discovery_filtered", "empty_url")
                continue
            if url_key in seen_urls:
                filtered_count += 1
                add_discovery_audit(link, "duplicate_candidate", "duplicate_after_merge")
                continue
            seen_urls.add(url_key)
            if any(pattern in url_key for pattern in invalid_patterns):
                filtered_count += 1
                log(f"Filtered non-article/static link: {(link.get('title') or original_url)[:60]}")
                add_discovery_audit(link, "discovery_filtered", "static_or_asset_url")
                continue
            valid_links.append(link)

        log(f"Valid candidate links: {len(valid_links)}")
        log(f"Filtered links: {filtered_count}")
        log("")

        if not valid_links:
            error_message = "No valid article candidates found"
            if playwright_error:
                error_message = f"{error_message}; Playwright failed: {playwright_error}"
            return {
                "success": False,
                "error": error_message,
                "articles": [],
                "audit": {
                    "source_url": list_url,
                    "started_at": get_china_time().isoformat(),
                    "completed_at": get_china_time().isoformat(),
                    "total_candidates_before_db_dedupe": len(raw_links),
                    "total_candidates_after_db_dedupe": 0,
                    "status_counts": _count_audit_statuses(discovery_audit_items),
                    "items": discovery_audit_items,
                },
                "stats": {
                    "playwright_links": playwright_candidate_count,
                    "playwright_error": playwright_error,
                    "network_json_candidates": playwright_stats.get("network_json_candidates", 0),
                    "network_json_inline_candidates": playwright_stats.get("network_json_inline_candidates", 0),
                    "network_json_responses_checked": playwright_stats.get("network_json_responses_checked", 0),
                    "network_json_responses_used": playwright_stats.get("network_json_responses_used", 0),
                    "supplemental_links": len(supplemental_links),
                    "merged_links": len(raw_links),
                    "html_static_candidates": supplemental_stats.get("html_static_candidates", 0),
                    "attribute_candidates": supplemental_stats.get("attribute_candidates", 0),
                    "structured_candidates": supplemental_stats.get("structured_candidates", 0),
                    "embedded_script_candidates": supplemental_stats.get("embedded_script_candidates", 0),
                    "static_pagination_candidates": supplemental_stats.get("static_pagination_candidates", 0),
                    "static_pages_checked": supplemental_stats.get("static_pages_checked", 0),
                    "feed_candidates": supplemental_stats.get("feed_candidates", 0),
                    "feed_inline_candidates": supplemental_stats.get("feed_inline_candidates", 0),
                    "sitemap_candidates": supplemental_stats.get("sitemap_candidates", 0),
                    "filtered_count": filtered_count,
                    "valid_links": 0,
                    "final_count": 0,
                    "elapsed_time": time.time() - start_time,
                    "crawl_options": crawl_options,
                },
            }

        log("=" * 70)
        log("Step 4: Extract article details and save")
        log("=" * 70)
        result = extractor._crawl_article_details(
            links=valid_links,
            limit=limit,
            source_url=list_url,
            keywords=keywords,
            kb_id=kb_id,
            days_limit=days_limit,
            start_date=start_date,
            end_date=end_date,
            crawl_options=crawl_options,
            task_id=task_id,
        )

        articles = result.get("articles", []) if result.get("success") else []
        detail_stats = result.get("stats", {}) if isinstance(result, dict) else {}
        audit = result.get("audit", {}) if isinstance(result, dict) else {}
        if discovery_audit_items:
            audit_items = list(audit.get("items", [])) + discovery_audit_items
            audit = {
                **audit,
                "source_url": audit.get("source_url", list_url),
                "started_at": audit.get("started_at", get_china_time().isoformat()),
                "completed_at": get_china_time().isoformat(),
                "total_candidates_before_db_dedupe": audit.get("total_candidates_before_db_dedupe", len(raw_links)),
                "total_candidates_after_db_dedupe": audit.get("total_candidates_after_db_dedupe", len(valid_links)),
                "items": audit_items,
                "status_counts": _count_audit_statuses(audit_items),
                "discovery_filtered_count": len(discovery_audit_items),
            }
        domain = urlparse(list_url).netloc
        db_stats = self.db.get_statistics(domain=domain)
        final_count = db_stats.get("total_articles", len(articles))
        elapsed_time = time.time() - start_time

        log("")
        log("=" * 70)
        log(f"Playwright column crawl completed in {elapsed_time:.1f}s")
        log("=" * 70)
        log("Stats:")
        log(f"   - Playwright candidates: {playwright_candidate_count}")
        if playwright_stats.get("network_json_candidates"):
            log(
                "   - Network JSON candidates: "
                f"{playwright_stats.get('network_json_candidates', 0)} "
                f"(inline {playwright_stats.get('network_json_inline_candidates', 0)})"
            )
        log(f"   - Supplemental candidates: {len(supplemental_links)}")
        log(f"   - Merged candidates: {len(raw_links)}")
        log(f"   - Valid candidates: {len(valid_links)}")
        log(f"   - Saved this run: {len(articles)}")
        if detail_stats:
            log(f"   - Duplicate in DB: {detail_stats.get('duplicates', 0)}")
            log(f"   - Date unknown but processed: {detail_stats.get('date_unknown', 0)}")
            log(f"   - Date out of range: {detail_stats.get('date_skipped', 0)}")
            log(f"   - Quality skipped: {detail_stats.get('quality_skipped', 0)}")
            log(f"   - Keyword skipped: {detail_stats.get('keyword_skipped', 0)}")
            log(f"   - Failed extraction: {detail_stats.get('failed', 0)}")
        log(f"   - Existing articles in DB for {domain}: {final_count}")
        if kb_id:
            log(f"   - RAGFlow dataset: {kb_id}")
        log("=" * 70)

        return {
            "success": True,
            "articles": articles,
            "audit": audit,
            "stats": {
                "playwright_links": playwright_candidate_count,
                "playwright_error": playwright_error,
                "network_json_candidates": playwright_stats.get("network_json_candidates", 0),
                "network_json_inline_candidates": playwright_stats.get("network_json_inline_candidates", 0),
                "network_json_responses_checked": playwright_stats.get("network_json_responses_checked", 0),
                "network_json_responses_used": playwright_stats.get("network_json_responses_used", 0),
                "network_json_errors": playwright_stats.get("network_json_errors", 0),
                "supplemental_links": len(supplemental_links),
                "merged_links": len(raw_links),
                "html_static_candidates": supplemental_stats.get("html_static_candidates", 0),
                "attribute_candidates": supplemental_stats.get("attribute_candidates", 0),
                "structured_candidates": supplemental_stats.get("structured_candidates", 0),
                "embedded_script_candidates": supplemental_stats.get("embedded_script_candidates", 0),
                "static_pagination_candidates": supplemental_stats.get("static_pagination_candidates", 0),
                "static_pages_checked": supplemental_stats.get("static_pages_checked", 0),
                "feed_candidates": supplemental_stats.get("feed_candidates", 0),
                "feed_inline_candidates": supplemental_stats.get("feed_inline_candidates", 0),
                "sitemap_candidates": supplemental_stats.get("sitemap_candidates", 0),
                "sitemaps_checked": supplemental_stats.get("sitemaps_checked", 0),
                "feeds_checked": supplemental_stats.get("feeds_checked", 0),
                "filtered_count": filtered_count,
                "valid_links": len(valid_links),
                "final_count": len(articles),
                **detail_stats,
                "elapsed_time": elapsed_time,
                "crawl_options": crawl_options,
            },
        }


_hybrid_crawler = None


def _count_audit_statuses(items):
    counts = {}
    for item in items or []:
        status = item.get("status", "unknown")
        counts[status] = counts.get(status, 0) + 1
    return counts


def get_hybrid_crawler(db: Optional[SQLiteDatabase] = None) -> HybridCrawler:
    """Return the compatibility crawler singleton."""
    global _hybrid_crawler
    if _hybrid_crawler is None:
        _hybrid_crawler = HybridCrawler(db=db)
    return _hybrid_crawler


if __name__ == "__main__":
    test_url = "https://www.fangdalaw.com/news/"
    crawler = HybridCrawler()
    result = crawler.crawl_news_site(
        list_url=test_url,
        limit=10,
        wait_for=8000,
    )
    print(result)
