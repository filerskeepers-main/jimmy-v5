import scrapy


class JimmyCrawlerItem(scrapy.Item):
    url = scrapy.Field()
    portal_id = scrapy.Field()
    run_id = scrapy.Field()
    jurisdiction = scrapy.Field()

    # --- Status & Versioning ---
    status = scrapy.Field()
    version = scrapy.Field()
    not_ro = scrapy.Field()
    weekly_gap_analyzed = scrapy.Field()
    is_repealed = scrapy.Field()

    # --- Content ---
    title = scrapy.Field()
    section_title = scrapy.Field()
    content = scrapy.Field()
    content_markdown = scrapy.Field()
    summary = scrapy.Field()

    # --- Analytics / Keywords ---
    appeared_keywords = scrapy.Field()
    total_occurrences = scrapy.Field()
    keywords_proximity = scrapy.Field()
    source_keywords = scrapy.Field()
    tags = scrapy.Field()

    # --- Identifiers ---
    document_number = scrapy.Field()
    source_identifier = scrapy.Field()
    source_secondary_identifier = scrapy.Field()
    legal_reference = scrapy.Field()

    # --- Classification ---
    type_of_law = scrapy.Field()
    legal_classification = scrapy.Field()
    issuing_authority = scrapy.Field()

    # --- Dates ---
    date_of_enactment = scrapy.Field()
    date_of_last_amendment = scrapy.Field()
    date_of_publication = scrapy.Field()
    date_of_effective = scrapy.Field()
    date_of_expiration = scrapy.Field()
    date_of_repealed = scrapy.Field()
    date_of_decision = scrapy.Field()
    last_updated_at = scrapy.Field()

    # --- Metadata & Files ---
    extra_metadata = scrapy.Field()
    file_data = scrapy.Field()

    # --- Scrapy Internal (Not in DB) ---
    file_urls = scrapy.Field()
    files = scrapy.Field()
