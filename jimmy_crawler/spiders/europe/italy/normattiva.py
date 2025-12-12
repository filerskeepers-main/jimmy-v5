import scrapy
import datetime
import re
from typing import Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlencode
from jimmy_crawler.spiders.base import BaseJimmySpider
from jimmy_crawler.utils import clean_text, convert_to_markdown, format_date


class ItalyNormattivaHttpSpider(BaseJimmySpider):
    name = "normattiva"

    EXPORT_URL = "https://www.normattiva.it/esporta/attoCompleto"

    custom_settings = {
        'DOWNLOAD_DELAY': 1.0,
        'CONCURRENT_REQUESTS': 16,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
        'COOKIES_ENABLED': True,
        'COOKIES_DEBUG': False,
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
        },
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
    }

    def start_requests(self):
        current_year = datetime.datetime.now().year
        start_year = 1861

        for year in range(start_year, current_year + 1):
            year_str = str(year)
            url = f"https://www.normattiva.it/ricerca/elencoPerData/anno/{year_str}"
            yield scrapy.Request(
                url,
                callback=self.parse_listing,
                meta={'cookiejar': year, 'year': year_str},
                dont_filter=True
            )

    def parse_listing(self, response):
        """Step 1: Listing -> Detail Page"""
        # --- Session Management ---
        current_qs = parse_qs(urlparse(response.url).query)
        tab_id = current_qs.get('tabID', [None])[0] or response.meta.get('tabID')
        if not tab_id:
            tab_id = response.css('input[name="tabID"]::attr(value)').get()

        # --- Links ---
        detail_links = response.xpath("//a[contains(@href, 'caricaDettaglioAtto')]")
        self.logger.info(f"[YEAR {response.meta['year']}] Found {len(detail_links)} acts")

        for link_node in detail_links:
            # Fallback title from listing
            raw_title = link_node.xpath("./text() | ./parent::*/text()").get()
            title_fallback = self._clean_string(raw_title) if raw_title else "Untitled Act"

            relative_url = link_node.xpath("./@href").get()
            if relative_url:
                detail_url = f"https://www.normattiva.it{relative_url}"
                # Request Detail Page (Step 2)
                yield scrapy.Request(
                    detail_url,
                    callback=self.parse_summary_and_trigger_export,
                    meta={
                        'title_fallback': title_fallback,
                        'cookiejar': response.meta['cookiejar'],
                        'tabID': tab_id
                    },
                    headers={'Referer': response.url}
                )

        # --- Pagination (Incremental) ---
        if len(detail_links) == 0: return

        current_page_index = 0
        if "/anno/" not in response.url:
            match = re.search(r'/elencoPerData/(\d+)', response.url)
            if match: current_page_index = int(match.group(1))

        next_page_index = current_page_index + 1
        expected_next_href = f"/ricerca/elencoPerData/{next_page_index}"

        if response.xpath(f"//a[contains(@href, '{expected_next_href}')]"):
            base_next_url = f"https://www.normattiva.it/ricerca/elencoPerData/{next_page_index}"
            params = {'title': 'Dettaglio', 'bloccoAggiornamentoBreadCrumb': 'true'}
            if tab_id: params['tabID'] = tab_id

            yield scrapy.Request(
                f"{base_next_url}?{urlencode(params)}",
                callback=self.parse_listing,
                meta={
                    'cookiejar': response.meta['cookiejar'],
                    'year': response.meta['year'],
                    'tabID': tab_id
                },
                dont_filter=True,
                headers={'Referer': response.url}
            )

    def parse_summary_and_trigger_export(self, response):
        """
        Step 2: Detail Page. Extract high-quality metadata.
        """
        # --- 1. FULL TITLE  ---
        header_parts = response.css('#titoloAtto h2::text').getall()
        header_clean = self._clean_string(" ".join(header_parts))

        desc_parts = response.css('#titoloAtto ~ h3::text, .titolo_provvedimento::text').getall()
        desc_clean = self._clean_string(" ".join(desc_parts))

        full_title = header_clean
        if desc_clean:
            full_title += f"\n{desc_clean}"

        if not full_title.strip():
            full_title = response.meta.get('title_fallback', "Untitled Act")

        # --- 2. EFFECTIVE DATE (Multiple Selectors) ---
        # Priority 1: The green "NoteEvidenza" box
        eff_date_text = " ".join(response.css('.NoteEvidenza *::text').getall())

        # Priority 2: The "vigenti" sidebar box
        if not eff_date_text.strip():
            eff_date_text = " ".join(response.css('#vigenti .rosso::text').getall())

        # Priority 3: Any text saying "Vigente al:"
        if not eff_date_text.strip():
            eff_date_text = response.xpath("//*[contains(text(), 'Vigente al')]/text()").get() or ""

        effective_date_obj = self._parse_date_string(eff_date_text)

        # --- 3. JOURNAL INFO / SOURCE ID ---
        # Join all text in the link to avoid splitting "n." and "287"
        journal_text = " ".join(response.css('.link_gazzetta a::text').getall())
        journal_num, journal_date_obj = self._parse_journal_info(journal_text)

        # --- 4. TRIGGER EXPORT ---
        parsed = urlparse(response.url)
        qs = parse_qs(parsed.query)
        pub_date_param = qs.get('atto.dataPubblicazioneGazzetta', [None])[0]
        code_red_param = qs.get('atto.codiceRedazionale', [None])[0]

        final_pub_date = journal_date_obj if journal_date_obj else format_date(pub_date_param, date_order="YMD")

        if pub_date_param and code_red_param:
            export_params = {
                'atto.dataPubblicazioneGazzetta': pub_date_param,
                'atto.codiceRedazionale': code_red_param
            }
            full_export_url = f"{self.EXPORT_URL}?{urlencode(export_params)}"

            yield scrapy.Request(
                full_export_url,
                callback=self.parse_full_text,
                meta={
                    'final_title': full_title,
                    'final_pub_date': final_pub_date,
                    'final_eff_date': effective_date_obj,
                    'journal_number': journal_num,
                    'doc_code': code_red_param,
                    'cookiejar': response.meta['cookiejar']
                },
                headers={'Referer': response.url}
            )

    def parse_full_text(self, response):
        """Step 3: Combine Metadata + Content"""
        # Content
        raw_html = response.css('body').get()
        content_lines = response.css('body ::text').getall()
        clean_content = clean_text(content_lines)
        markdown_content = convert_to_markdown(raw_html, include_tables=True)

        # Metadata
        extra_meta = {
            'source_url': response.url,
            'is_export_version': True,
            'year_group': str(response.meta['cookiejar']),
            'normattiva_id': response.meta['doc_code']
        }

        # Source ID Logic: "n. 287"
        src_id = response.meta.get('journal_number')

        yield self.build_item(
            response=response,
            title=response.meta['final_title'],
            jurisdiction="Italy",
            source_identifier=src_id,
            date_of_publication=response.meta['final_pub_date'],
            date_of_effective=response.meta['final_eff_date'],
            content=clean_content,
            content_markdown=markdown_content,
            extra_metadata=extra_meta,
        )

    # --- Helpers ---

    def _clean_string(self, text: str) -> str:
        if not text: return ""
        text = re.sub(r'[\r\n\t]+', ' ', text)
        return re.sub(r'\s+', ' ', text).strip()

    def _parse_date_string(self, date_str: str) -> Optional[datetime.date]:
        """Extracts date from strings like 'Entrata in vigore...: 12/12/2025' or '12-12-2025'"""
        if not date_str: return None
        clean_str = re.sub(r'[-.]', '/', date_str)

        # Find dd/mm/yyyy pattern
        match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', clean_str)
        if match:
            d, m, y = map(int, match.groups())
            try:
                return datetime.date(y, m, d)
            except:
                pass
        return None

    def _parse_journal_info(self, text: str) -> Tuple[Optional[str], Optional[datetime.date]]:
        """Parses '(GU n.287 del 11-12-2025)'"""
        if not text: return None, None

        # Extract Number (e.g. "n.287" or "n. 287")
        num_match = re.search(r'(n\.\s*\d+)', text, re.IGNORECASE)
        number = num_match.group(1).replace(" ", "") if num_match else None  # Normalize to "n.287"

        # Extract Date
        date_obj = self._parse_date_string(text)

        return number, date_obj