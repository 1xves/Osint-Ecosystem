"""
osint/utils/document_fetcher.py

Reusable document retrieval utility for the OSINT pipeline.

Handles PDF, HTML, and XML documents fetched over HTTP.
All fetch methods return decoded text — callers receive strings, not bytes.

Design:
    - httpx.AsyncClient with a per-process semaphore to cap concurrent requests.
    - User-Agent spoofing to avoid trivial bot detection.
    - Consistent error handling: returns empty string on failure (never raises),
      unless strict=True is passed (for cases where empty = must retry).
    - Logs at WARNING for fetch failures so they surface without noise.

Dependencies:
    httpx         — async HTTP (in requirements.txt)
    pdfplumber    — PDF text extraction (in requirements.txt)
    beautifulsoup4 — HTML parsing (in requirements.txt)
    lxml          — XML parsing (in requirements.txt)

Usage:
    from osint.utils.document_fetcher import DocumentFetcher

    fetcher = DocumentFetcher()

    # Fetch PDF and extract plain text
    text = await fetcher.fetch_pdf_text("https://www.sec.gov/Archives/.../proxy.pdf")

    # Fetch HTML page and extract visible text
    text = await fetcher.fetch_html_text("https://www.example.com/about")

    # Fetch XML document and return parsed ElementTree
    tree = await fetcher.fetch_xml("https://www.sec.gov/Archives/.../filing.xml")

    # Fetch raw bytes (advanced — caller handles parsing)
    raw = await fetcher.fetch_bytes("https://example.com/doc.pdf")
"""

from __future__ import annotations

import asyncio
import io
import logging
from typing import Any
from xml.etree import ElementTree

import httpx
import pdfplumber
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# Global semaphore — caps concurrent document fetches across all agents.
# Documents are large; too many concurrent fetches = memory pressure.
_DOCUMENT_FETCH_SEMAPHORE = asyncio.Semaphore(6)

# Default request timeout in seconds.
_DEFAULT_TIMEOUT = 45

# User-Agent string that looks like a real browser (not "Python-httpx/x.y.z").
_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

_BASE_HEADERS = {
    "User-Agent": _USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}


class DocumentFetchError(Exception):
    """Raised only when strict=True and the fetch fails."""
    pass


class DocumentFetcher:
    """
    Async document fetcher for PDF, HTML, and XML documents.

    Thread-safe and concurrency-limited via a module-level semaphore.
    Safe to instantiate once per agent and reuse across calls.
    """

    def __init__(
        self,
        timeout: int = _DEFAULT_TIMEOUT,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self._timeout = timeout
        self._extra_headers = extra_headers or {}

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {**_BASE_HEADERS, **self._extra_headers}
        if extra:
            headers.update(extra)
        return headers

    # ─────────────────────────────────────────────────────────────────────────
    # PDF
    # ─────────────────────────────────────────────────────────────────────────

    async def fetch_pdf_text(
        self,
        url: str,
        *,
        max_pages: int | None = None,
        strict: bool = False,
    ) -> str:
        """
        Fetch a PDF from URL and extract all text using pdfplumber.

        Args:
            url:        URL of the PDF to fetch.
            max_pages:  If set, only extract text from the first N pages.
                        Useful for large documents where only the header sections matter.
            strict:     If True, raise DocumentFetchError on failure.
                        If False (default), return empty string.

        Returns:
            Extracted plain text, with page breaks preserved as double newlines.
            Returns empty string if the fetch or extraction fails.
        """
        raw = await self._fetch_raw(url, strict=strict)
        if not raw:
            return ""

        try:
            with pdfplumber.open(io.BytesIO(raw)) as pdf:
                pages = pdf.pages
                if max_pages is not None:
                    pages = pages[:max_pages]

                page_texts = []
                for page in pages:
                    text = page.extract_text()
                    if text:
                        page_texts.append(text.strip())

                return "\n\n".join(page_texts)
        except Exception as exc:
            log.warning("document_fetcher: PDF extraction failed for '%s': %s", url[:80], exc)
            if strict:
                raise DocumentFetchError(f"PDF extraction failed: {url}") from exc
            return ""

    # ─────────────────────────────────────────────────────────────────────────
    # HTML
    # ─────────────────────────────────────────────────────────────────────────

    async def fetch_html_text(
        self,
        url: str,
        *,
        selector: str | None = None,
        strict: bool = False,
    ) -> str:
        """
        Fetch an HTML page and extract visible text using BeautifulSoup.

        Args:
            url:        URL of the HTML page.
            selector:   CSS selector to limit extraction to a specific section.
                        E.g., "main", ".content", "#about". None = full page.
            strict:     If True, raise DocumentFetchError on failure.

        Returns:
            Extracted visible text with boilerplate removed (nav, footer, scripts).
            Returns empty string on failure.
        """
        raw = await self._fetch_raw(url, strict=strict)
        if not raw:
            return ""

        try:
            soup = BeautifulSoup(raw, "lxml")

            # Remove boilerplate elements
            for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
                tag.decompose()

            if selector:
                target = soup.select_one(selector)
                if target is None:
                    log.debug(
                        "document_fetcher: selector '%s' not found in '%s', using full page",
                        selector, url[:80],
                    )
                    target = soup
            else:
                target = soup

            text = target.get_text(separator="\n", strip=True)
            # Collapse excessive blank lines
            lines = [line for line in text.splitlines() if line.strip()]
            return "\n".join(lines)

        except Exception as exc:
            log.warning("document_fetcher: HTML extraction failed for '%s': %s", url[:80], exc)
            if strict:
                raise DocumentFetchError(f"HTML extraction failed: {url}") from exc
            return ""

    # ─────────────────────────────────────────────────────────────────────────
    # XML
    # ─────────────────────────────────────────────────────────────────────────

    async def fetch_xml(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        strict: bool = False,
    ) -> ElementTree.Element | None:
        """
        Fetch an XML document and return the parsed root Element.

        Args:
            url:        URL of the XML document.
            headers:    Extra headers to include (e.g., for EDGAR namespace quirks).
            strict:     If True, raise DocumentFetchError on failure.

        Returns:
            Parsed XML root Element, or None on failure.
        """
        raw = await self._fetch_raw(url, extra_headers=headers, strict=strict)
        if not raw:
            return None

        try:
            return ElementTree.fromstring(raw)
        except ElementTree.ParseError as exc:
            log.warning("document_fetcher: XML parse error for '%s': %s", url[:80], exc)
            if strict:
                raise DocumentFetchError(f"XML parse failed: {url}") from exc
            return None

    # ─────────────────────────────────────────────────────────────────────────
    # Raw bytes (advanced — caller parses)
    # ─────────────────────────────────────────────────────────────────────────

    async def fetch_bytes(
        self,
        url: str,
        *,
        extra_headers: dict[str, str] | None = None,
        strict: bool = False,
    ) -> bytes:
        """
        Fetch raw bytes from a URL with no parsing.
        Use when the caller needs to handle parsing itself (e.g., custom XML namespaces).

        Returns:
            Raw response bytes, or b"" on failure.
        """
        return await self._fetch_raw(url, extra_headers=extra_headers, strict=strict) or b""

    # ─────────────────────────────────────────────────────────────────────────
    # Internal: raw fetch with semaphore + retry
    # ─────────────────────────────────────────────────────────────────────────

    async def _fetch_raw(
        self,
        url: str,
        *,
        extra_headers: dict[str, str] | None = None,
        strict: bool = False,
        _attempt: int = 0,
    ) -> bytes | None:
        """
        Core fetch: HTTP GET under semaphore, up to 2 retries on transient errors.

        Returns raw bytes on success, None on failure.
        """
        async with _DOCUMENT_FETCH_SEMAPHORE:
            try:
                async with httpx.AsyncClient(
                    timeout=self._timeout,
                    follow_redirects=True,
                ) as client:
                    response = await client.get(
                        url,
                        headers=self._headers(extra_headers),
                    )
                    response.raise_for_status()
                    return response.content

            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code

                # Retry once on 429 / 503
                if status in (429, 503) and _attempt < 2:
                    wait = 5 * (_attempt + 1)
                    log.debug(
                        "document_fetcher: HTTP %d for '%s', retry in %ds",
                        status, url[:80], wait,
                    )
                    await asyncio.sleep(wait)
                    return await self._fetch_raw(
                        url, extra_headers=extra_headers, strict=strict, _attempt=_attempt + 1
                    )

                log.warning(
                    "document_fetcher: HTTP %d for '%s'", status, url[:80]
                )
                if strict:
                    raise DocumentFetchError(f"HTTP {status}: {url}")
                return None

            except httpx.TimeoutException:
                log.warning("document_fetcher: timeout for '%s'", url[:80])
                if strict:
                    raise DocumentFetchError(f"Timeout: {url}")
                return None

            except Exception as exc:
                log.warning("document_fetcher: fetch failed for '%s': %s", url[:80], exc)
                if strict:
                    raise DocumentFetchError(f"Fetch failed: {url}") from exc
                return None
