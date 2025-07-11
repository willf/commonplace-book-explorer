"""
Folger first line scraper.

This script will take a URL of that looks like this:

https://firstlines.folger.edu/advancedSearch.php?val1=rawl.+d.+1092&col1=shelfmark1&sort=lib1#results

and then scrape the results from the page.

The page will have zero or more DIV elements of class "resultRow". This div will look like this:
<div class="resultsRow row0" style="width:800px;">
      <a class="_blank" href="detail.php?id=80836" target="_blank">
      <span class="resultsCell" style="width:150px">The bees of Hybla have besides sweet honey smarting stings,&nbsp;</span>
      <span class="resultsCell" style="width:112px">&nbsp;</span>
      <span class="resultsCell" style="width:113px">&nbsp;</span>
      <span class="resultsCell" style="width:150px">[Auxilium meum in domino(?)]&nbsp;</span>
      <span class="resultsCell" style="width:90px">Bodley&nbsp;</span>
      <span class="resultsCell" style="width:75px">Rawl. D. 1092&nbsp;</span>
      <span class="resultsCell" style="width:60px">f.   3&nbsp;</span>
      </a>
    </div>

It contains a link at the start of the div that points to a detail page; for example:

<a class="_blank" href="detail.php?id=80836" target="_blank">

The script will follow this link and scrape the detail page.

In this detail page, there are a set of divs of class "detailRow" that look like this:
    <div class="detailRow row1">
      <span class="detailLabel">First Line:</span>
      <span class="detailCell">If eighty-eight be past then thrive&nbsp;</span>
    </div>
    <div class="detailRow row0">
      <span class="detailLabel">Author (Last name, First):</span>
      <span class="detailCell">&nbsp;</span>
    </div>

    Notice that each div has two spans, the first one is the label and the second one is the value.

    For each detail page, the script should return a dictionary with the keys being the labels and the values being the text in the second span. Convert
    any entities in the text to their unicode equivalents (e.g., "&nbsp;" to " ", "&amp;" to "&"), and strip any leading or trailing whitespace from the values.

The script should have two main functions:
- `scrape_folger(url: str) -> List[Dict[str, str]]`:
    This function takes a URL as input, scrapes the first lines from the page,
    follows the links to the detail pages, and returns a list of dictionaries containing the scraped data
- `scrape_detail_page(detail_url: str) -> Dict[str, str]`:
    This function takes a detail page URL as input, scrapes the details from the page,
    and returns a dictionary with the details.

Insert a pause between requests to avoid overwhelming the server. Make the pause duration configurable via a parameter, defaulting to 1 second.
"""

# ...existing code...
import logging
import math
import sqlite3
import time

import requests
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = "folger_results.db"
TABLE_NAME_DETAILS = "details"
TABLE_NAME = "all_details"


def fetch_with_retries(url, max_retries=5, base_delay=1.0, timeout=10):
    """Fetch a URL with exponential backoff on failure."""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except Exception as e:
            wait = base_delay * (2**attempt)
            logger.warning(
                f"Request to {url} failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait:.1f} seconds..."
            )
            time.sleep(wait)
    logger.error(f"Failed to fetch {url} after {max_retries} attempts.")
    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts.")


# Patch scrape_folger and scrape_detail_page to use fetch_with_retries
# List all possible fields you want as columns
FIELDS = [
    "id",
    "First Line",
    "Author (Last name, First)",
    "Title",
    "First Line (Transcribed)",
    "Second Line",
    "Last Line",
    "Library",
    "Shelfmark",
    "Folio",
    "Ref Nbr",
    "Number of Lines",
    "Verse/Stanza Form",
    "Publication Author/Editor",
    "Publication Title",
    "Publication Date",
    "Gender",
    "Musical Settings",
    "Names Mentioned",
    "Other Names",
    "Translations/Imitations",
    "Notes",
]


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Build CREATE TABLE statement with all fields as TEXT
    columns = ", ".join([f'"{field}" TEXT' for field in FIELDS])
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            {columns},
            PRIMARY KEY (id)
        )
    """)
    conn.commit()
    return conn


def detail_exists(conn, detail_id):
    c = conn.cursor()
    c.execute(f"SELECT 1 FROM {TABLE_NAME} WHERE id = ?", (detail_id,))
    return c.fetchone() is not None


def insert_detail(conn, data):
    c = conn.cursor()
    # Ensure all fields are present
    logger.info(f"Inserting detail with ID: {data.get('id', 'unknown')}")
    logger.info(f"Data to insert: {data}")
    row = [data.get(field, "") for field in FIELDS]
    placeholders = ", ".join(["?"] * len(FIELDS))
    quoted_fields = ", ".join([f'"{f}"' for f in FIELDS])
    c.execute(f"INSERT OR REPLACE INTO {TABLE_NAME} ({quoted_fields}) VALUES ({placeholders})", row)
    conn.commit()


def scrape_folger(url: str, pause_duration: float = 1.0, max_details=math.inf, conn=None) -> list[dict[str, str]]:
    logger.info(f"Scraping Folger first lines from URL: {url}")
    response = fetch_with_retries(url, max_retries=10, base_delay=1.0, timeout=10)
    soup = BeautifulSoup(response.text, "html.parser")

    results = []

    for i, result_row in enumerate(soup.find_all("div", class_="resultsRow")):
        if i >= max_details:
            logger.info(f"Reached maximum details limit: {max_details}. Stopping further scraping.")
            break
        detail_link = result_row.find("a", href=True)
        if detail_link:
            detail_url = detail_link["href"]
            if not detail_url.startswith("http"):
                detail_url = f"https://firstlines.folger.edu/{detail_url}"
            detail_id = detail_url.split("id=")[-1].split("&")[0]
            if conn and detail_exists(conn, detail_id):
                logger.info(f"Detail {detail_id} already exists in DB, skipping.")
                continue
            detail_data = scrape_detail_page(detail_url)
            if conn:
                insert_detail(conn, detail_data)
            results.append(detail_data)
        time.sleep(pause_duration)
    return results


def scrape_detail_page(detail_url: str) -> dict[str, str]:
    logger.info(f"Scraping detail page: {detail_url}")
    response = fetch_with_retries(detail_url, max_retries=10, base_delay=1.0, timeout=10)
    soup = BeautifulSoup(response.text, "html.parser")

    detail_data = {}
    detail_data["id"] = detail_url.split("id=")[-1].split("&")[0]  # Extract ID from URL

    for row in soup.find_all("div", class_="detailRow"):
        label_span = row.find("span", class_="detailLabel")
        value_span = row.find("span", class_="detailCell")
        if label_span and value_span:
            label = label_span.get_text(strip=True)  # remove leading/trailing whitespace and colons
            label = label.rstrip(":")  # Remove trailing colon if present
            value = value_span.get_text(strip=True).replace("&nbsp;", " ").replace("&amp;", "&")
            detail_data[label] = value

    return detail_data


def scrape_detail_page_by_id(detail_id: str) -> dict[str, str]:
    """Scrape detail page by ID."""
    detail_url = f"https://firstlines.folger.edu/detail.php?id={detail_id}"
    return scrape_detail_page(detail_url)


URLS = [
    "https://firstlines.folger.edu/advancedSearch.php?val1=add.+44963&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=add.+11811&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=add.+15227&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=add.+19268&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=add.+22118&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=add.+22582&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=Add.+30982&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=Add.+44963&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=Add.+62134&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=Ashmole+47&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=Corpus+Christi+328&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=misc.+e.+13&col1=shelfmark1&lib_bod=Y&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=poet.+e.+30&col1=shelfmark1&lib_bod=Y&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=poet.+f.+16&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=v.a.97&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=v.a.148&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=v.a.170&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=v.a.319&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=v.a.322&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=v.b.43&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=harley+3511&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=harley+6931&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=lansdowne+777&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=malone+21&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=rawl.+d.+1092&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=rawl.+poet.+84&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=rawl.+poet.+116&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=rawl.+poet.+199&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=sloane+542&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=sloane+1446&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=sloane+1792&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=v.b.110&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=g1401b&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=w3686&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=K0501&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=L0643&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=MS+Eng+703&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=hm116&col1=shelfmark1&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=b.200&col1=shelfmark1&lib_yo=Y&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=b.205&col1=shelfmark1&lib_yo=Y&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=b.62&col1=shelfmark1&lib_yo=Y&sort=lib1#results",
    "https://firstlines.folger.edu/advancedSearch.php?val1=MS+240%2F7&col1=shelfmark1&sort=lib1#results",
]

# ok, this is the main function that will be called when the script is run
# if __name__ == "__main__":
#     conn = init_db()
#     for url in URLS:
#         logger.info(f"Scraping URL: {url}")
#         results = scrape_folger(url, pause_duration=1.0, max_details=math.inf, conn=conn)
#         print(f"Scraped {len(results)} details from {url}")
#         logger.info(f"Finished scraping URL: {url}")
#         logger.info("Sleeping for 1 second before next request...")
#         time.sleep(1)
#     logger.info("All URLs have been processed.")
#     conn.close()

if __name__ == "__main__":
    conn = init_db()
    start_id = 0
    end_id = 1000
    pause_duration = 2.5
    # get start_id and end_id from command line arguments
    import sys

    if len(sys.argv) > 1:
        start_id = int(sys.argv[1])
    if len(sys.argv) > 2:
        end_id = int(sys.argv[2])
    logger.info(f"Scraping details with IDs from {start_id} to {end_id}")
    for detail_id in range(start_id, end_id + 1):
        if conn and detail_exists(conn, detail_id):
            logger.info(f"Detail {detail_id} already exists in DB, skipping.")
            continue
        try:
            detail_data = scrape_detail_page_by_id(str(detail_id))
            if conn and detail_data:
                insert_detail(conn, detail_data)
            else:
                logger.warning(f"No data found for detail ID: {detail_id}")
        except Exception as e:
            logger.error(f"Error scraping detail ID {detail_id}: {e}")
        time.sleep(pause_duration)
    logger.info("Finished scraping all details.")
    conn.close()
