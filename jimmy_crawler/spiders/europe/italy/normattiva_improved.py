import datetime
import math
import re
from urllib.parse import urlparse, parse_qs, urlencode
from typing import Optional, Tuple

import scrapy

from jimmy_crawler.spiders.base import BaseJimmySpider
from jimmy_crawler.utils import clean_text, convert_to_markdown, format_date


class ItalyNormattivaHttpSpider(BaseJimmySpider):
    name = "normattiva_improved"

    EXPORT_URL = "https://www.normattiva.it/esporta/attoCompleto"

    custom_settings = {
        'DOWNLOAD_DELAY': 1.5,
        'CONCURRENT_REQUESTS': 16,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
        'COOKIES_ENABLED': True,
        'COOKIES_DEBUG': False,
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;'\
                      'q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
        },
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
    }

    def start_requests(self):
        current_year = datetime.datetime.now().year
        # For full historical coverage you can set start_year = 1861
        start_year = 2025  # This spider defaults to the current year for testing

        for year in range(start_year, current_year + 1):
            year_str = str(year)
            url = f"https://www.normattiva.it/ricerca/elencoPerData/anno/{year_str}"
            yield scrapy.Request(
                url,
                callback=self.parse_listing,
                meta={'cookiejar': year, 'year': year_str, 'is_start_page': True},
                dont_filter=True,
            )

    def parse_listing(self, response: scrapy.http.Response):
        """
        Parse the year listing page and schedule requests for the detail pages.

        Each act listed in the result set contains a link to a dynamic
        endpoint (``caricaDettaglioAtto``).  From that URL we derive the
        parameters required for the hidden export endpoint.  We also try
        to capture the raw title from the listing page as a fallback.
        """
        # Extract all anchors that link to the detail view.
        detail_links = response.xpath("//a[contains(@href, 'caricaDettaglioAtto')]")
        for link_node in detail_links:
            # Join all text nodes to form the full title.  Using .//text() allows
            # extraction across nested spans without dropping words.
            raw_title_parts = link_node.xpath(".//text()").getall()
            raw_title = clean_text(raw_title_parts) if raw_title_parts else "Untitled Act"

            relative_url = link_node.xpath("./@href").get()
            if not relative_url:
                continue
            parsed_url = urlparse(relative_url)
            query_params = parse_qs(parsed_url.query)

            pub_date = query_params.get('atto.dataPubblicazioneGazzetta', [None])[0]
            code_red = query_params.get('atto.codiceRedazionale', [None])[0]

            if pub_date and code_red:
                export_params = {
                    'atto.dataPubblicazioneGazzetta': pub_date,
                    'atto.codiceRedazionale': code_red,
                }
                full_export_url = f"{self.EXPORT_URL}?{urlencode(export_params)}"
                yield scrapy.Request(
                    full_export_url,
                    callback=self.parse_full_text,
                    meta={
                        'title_from_listing': raw_title,
                        'pub_date': pub_date,
                        'doc_code': code_red,
                        'cookiejar': response.meta['cookiejar'],
                    }
                )

        # Pagination: determine the next page index and follow it if present.
        current_page_index = 0
        match = re.search(r'/elencoPerData/(\d+)', response.url)
        if match:
            current_page_index = int(match.group(1))

        next_page_index = current_page_index + 1
        expected_next_href = f"/ricerca/elencoPerData/{next_page_index}"
        next_link_exists = response.xpath(
            f"//a[contains(@href, '{expected_next_href}')]").get()
        if next_link_exists:
            next_page_url = f"https://www.normattiva.it{expected_next_href}"
            yield scrapy.Request(
                next_page_url,
                callback=self.parse_listing,
                meta={
                    'cookiejar': response.meta['cookiejar'],
                    'year': response.meta['year'],
                },
                dont_filter=True,
            )

    # --------------------------------------------------------------------------
    # Helper functions
    # --------------------------------------------------------------------------

    @staticmethod
    def _parse_effective_date(date_str: str) -> Optional[datetime.date]:
        """
        Given a raw date string scraped from the Normattiva detail page,
        heuristically determine the day/month/year ordering and return a
        normalised ``datetime.date``.

        The portal displays dates in at least three forms:

        * ``DD‑MM‑YYYY`` (e.g. ``26-10-1930``)
        * ``D‑M‑YYYY``   (e.g. ``1-7-1931``)
        * ``DD/MM/YYYY`` or ``MM/DD/YYYY`` (e.g. ``04/26/2011`` or ``09/24/2025``)

        Where the delimiter is ``-`` we always assume day‑month order.  For
        ``/`` delimiters we attempt to infer the order by looking at the
        magnitude of the first two components; if the first number is greater
        than 12 it cannot be a month and is thus treated as the day.  In
        ambiguous cases (e.g. ``1/7/1931``) we default to day‑month order,
        reflecting the typical Italian format.
        """
        if not date_str:
            return None
        cleaned = date_str.strip().replace(' ', '')
        # Normalise unicode dashes to ASCII
        cleaned = cleaned.replace('\u2011', '-').replace('\u2012', '-').replace('\u2013', '-')
        # Determine delimiter
        delimiter = '/' if '/' in cleaned else '-'
        parts = cleaned.split(delimiter)
        if len(parts) != 3:
            return None
        try:
            first = int(parts[0])
            second = int(parts[1])
        except ValueError:
            return None
        # Choose date_order based on delimiter and component sizes
        if delimiter == '-':
            date_order = 'DMY'
        else:
            # Slash separated: detect whether it looks like US style
            if first > 12:
                date_order = 'DMY'
            elif second > 12:
                date_order = 'MDY'
            else:
                # Ambiguous – default to DMY to match Italian usage
                date_order = 'DMY'
        return format_date(cleaned, date_order=date_order)

    @staticmethod
    def _extract_journal_info(response: scrapy.http.Response) -> Tuple[Optional[str], Optional[datetime.date]]:
        """
        Extract the Official Journal number and publication date from the detail
        page.  The relevant text appears either in Italian as ``GU n.XXX del
        DD‑MM‑YYYY`` or in English as ``Official Journal No. XXX of DD‑MM‑YYYY``.

        Returns a tuple ``(journal_number, journal_date)`` where
        ``journal_number`` is a string (e.g. ``251``) and ``journal_date`` is a
        ``datetime.date`` instance.  If parsing fails, ``None`` values are
        returned.
        """
        journal_text = response.xpath(
            "//a[contains(@href, 'gazzetta') or contains(@href, 'Gazzetta')]"
            "/text()"
        ).get()
        if not journal_text:
            return None, None
        journal_text = journal_text.strip()
        # Match number and date in either Italian or English phrasing
        m = re.search(r"(?:n\.|No\.?)\s*(\d+)\D+(\d{1,2}[/-]\d{1,2}[/-]\d{4})", journal_text)
        if not m:
            return None, None
        number = m.group(1)
        date_str = m.group(2)
        journal_date = ItalyNormattivaHttpSpider._parse_effective_date(date_str)
        return number, journal_date

    def parse_full_text(self, response: scrapy.http.Response):
        """
        Parse the exported detail page to construct an item.

        We rely on the export endpoint because it provides a static HTML view
        without additional navigation or JavaScript.  From this page we
        extract the human‑readable title, the effective date and any journal
        metadata.  The page body is converted to both cleaned text and
        markdown using shared utilities.
        """
        raw_html = response.css('body').get()
        content_lines = response.css('body ::text').getall()
        clean_content = clean_text(content_lines)
        markdown_content = convert_to_markdown(raw_html, include_tables=True)

        # ------------------------------------------------------------------
        # Title extraction
        # ------------------------------------------------------------------
        # Try several selectors to find the full act title.  On the
        # exported page the heading can appear as a <h2> inside a div
        # with id ``titoloAtto``, as a span/div with ``titolo`` in the
        # class list, or as part of the document <title>.  These
        # selectors are ordered from most specific to most general.
        title_selectors = [
            "//div[@id='titoloAtto']//text()",
            "//h2//text()",
            "//div[contains(@class, 'titolo')]//text()",
            "//span[contains(@class, 'titolo')]//text()",
            "//h1//text()",
            "//title/text()",
        ]
        title_text = None
        for sel in title_selectors:
            texts = response.xpath(sel).getall()
            if texts:
                candidate = clean_text(texts)
                if candidate:
                    title_text = candidate
                    break
        if not title_text:
            # Fallback to the title captured from the listing page
            title_text = response.meta.get('title_from_listing', 'Untitled Act')

        # ------------------------------------------------------------------
        # Effective date (entry into force / text in force)
        # ------------------------------------------------------------------
        effective_date_obj: Optional[datetime.date] = None
        effective_date_str: Optional[str] = None
        # 1. Look for the note area that highlights entry into force in any
        # language.  The ``NoteEvidenza`` class wraps the entire text
        # (e.g. "Entry into force of the provision: 12/12/2025" or
        # "Entrata in vigore del provvedimento: 1/7/1931").  We search
        # inside and pick the last date‑like token from the text.
        note_texts = response.xpath(
            "//span[contains(@class, 'NoteEvidenza')]//text()"
        ).getall()
        if note_texts:
            for note in note_texts:
                if ':' in note:
                    # Take substring after the last colon
                    parts = note.split(':')
                    candidate_date = parts[-1].strip()
                    if re.search(r"\d{1,2}[/-]\d{1,2}[/-]\d{4}", candidate_date):
                        effective_date_str = candidate_date
                        break
        # 2. Fallback to the "artInizio" element which contains the
        # translated "Text in force from" date (Italian: Testo in vigore).
        if not effective_date_str:
            candidate = response.xpath(
                "//span[@id='artInizio']//text()"
            ).get()
            if candidate:
                effective_date_str = candidate.strip()
        # 3. Fallback to the "vigenti" panel (English: effective as of)
        if not effective_date_str:
            candidate = response.xpath(
                "//div[@id='vigenti']//span[contains(@class,'rosso')]/text()"
            ).get()
            if candidate:
                effective_date_str = candidate.strip()
        # Parse the extracted string if we found one
        if effective_date_str:
            effective_date_obj = self._parse_effective_date(effective_date_str)

        # Convert the publication date from the URL to a date object.  The
        # ``dataPubblicazioneGazzetta`` param is already in ISO format.
        pub_date_obj = format_date(response.meta['pub_date'], date_order="YMD")

        # Extract Official Journal info
        journal_number, journal_date = self._extract_journal_info(response)

        extra_meta = {
            'source_url': response.url,
            'is_export_version': True,
        }
        if journal_number:
            extra_meta['official_journal_number'] = journal_number
        if journal_date:
            extra_meta['official_journal_date'] = journal_date

        yield self.build_item(
            response=response,
            title=title_text,
            jurisdiction="Italy",
            source_identifier=response.meta['doc_code'],
            date_of_publication=pub_date_obj,
            date_of_effective=effective_date_obj,
            content=clean_content,
            content_markdown=markdown_content,
            extra_metadata=extra_meta,
        )