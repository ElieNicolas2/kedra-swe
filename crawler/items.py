# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlerItem(scrapy.Item):
    # Core identity
    identifier = scrapy.Field()        # e.g., ADJ-00000000; fallback to URL-hash
    title = scrapy.Field()
    description = scrapy.Field()       # optional snippet/summary
    decision_date_raw = scrapy.Field() # raw text as seen
    decision_date = scrapy.Field()     # normalized ISO (set later if you wish)
    body = scrapy.Field()              # e.g., 'WRC' or numeric id

    # Source & links
    source_url = scrapy.Field()        # search-card detail link
    detail_url = scrapy.Field()        # same as source_url (kept explicit)
    file_urls = scrapy.Field()         # URLs to download (pdf/doc/docx or html)
    # FilesPipeline will populate this:
    files = scrapy.Field()             # [{'path', 'checksum', 'url'}] (md5 checksum)

    # Post-download metadata
    stored_files = scrapy.Field()      # our structured records per saved file
    content_types = scrapy.Field()     # list like ['pdf','html',...]
    partition_date = scrapy.Field()    # YYYY-MM
    scraped_at = scrapy.Field()
    extra = scrapy.Field()             # any site-specific bits
