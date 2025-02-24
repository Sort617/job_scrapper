import asyncio
import random
from collections import deque
from playwright.async_api import async_playwright
import pandas as pd
import logging
from datetime import datetime
import time
from tenacity import retry, stop_after_attempt, wait_exponential

# Constants
ROOT_URL = "https://justjoin.it/job-offers/warszawa/testing"  # Starting URL for scraping
BASE_URL = "https://justjoin.it"  # Base URL for resolving relative links
CSV_OUTPUT = "JobResults.csv"  # Output file for job data
LOG_FILE = f"debug_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"  # Log file with timestamp
MAX_RECORDS = 150  # Limit of unique job postings to collect
GLOBAL_TIMEOUT = 30000  # Timeout for loading pages in milliseconds
MAX_RETRY_FAILS = 3  # Maximum retries for failed tasks
TIMEOUT_TOTAL_SECONDS = 60  # Overall timeout for entire list
TIMEOUT_DEEPER_SECONDS = 45  # Timeout for going deeper from a single link

# Configure logging to file and console
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.getLogger().addHandler(logging.StreamHandler())  # Add console logging

# Helper function to write job data to a CSV file
def write_to_csv(data, filename):
    """Writes a list of job dictionaries to a CSV file."""
    try:
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
    except Exception as e:
        logging.error(f"Error writing to CSV: {e}")

# Function to wait until a page is fully loaded
async def wait_for_page_load(page):
    """Ensures the page is completely loaded before proceeding."""
    try:
        await page.wait_for_function("document.readyState === 'complete'", timeout=GLOBAL_TIMEOUT)
        await page.wait_for_timeout(3000)  # Additional delay to ensure completeness
    except Exception as e:
        logging.warning(f"Page load timeout: {e}")

# Function to extract job details from the current page
@retry(stop=stop_after_attempt(MAX_RETRY_FAILS), wait=wait_exponential(multiplier=1, min=2, max=10))
async def extract_job_data(page):
    """Extracts job title and URL from the current page."""
    try:
        await wait_for_page_load(page)
        title = await page.locator("h1").inner_text(timeout=GLOBAL_TIMEOUT)  # Extract job title
        url = page.url  # Get current page URL
        return {"title": title, "url": url}
    except Exception as e:
        logging.error(f"Failed to extract job data: {e}")
        return None

# Function to find all job links on a page
@retry(stop=stop_after_attempt(MAX_RETRY_FAILS), wait=wait_exponential(multiplier=1, min=2, max=10))
async def find_job_links(page):
    """Finds all job links on the current page."""
    try:
        await wait_for_page_load(page)
        links = await page.eval_on_selector_all(
            "a[href*='/job-offer/']", "elements => elements.map(e => e.href)"
        )  # Extract job links
        return links
    except Exception as e:
        logging.error(f"Error finding job links: {e}")
        return []

# Recursive function to process links at different depths
async def process_links_recursively(link, visited_links, page, job_results, unique_urls, last_save_time, depth):
    """Processes a link and recursively explores deeper levels."""
    if time.time() - last_save_time[0] > TIMEOUT_TOTAL_SECONDS:
        logging.warning("Overall timeout reached. Ending scrape.")
        return

    try:
        logging.info(f"[Phase: Exploration at Depth {depth}] Visiting: {link}")
        await page.goto(link, wait_until="load", timeout=GLOBAL_TIMEOUT)  # Navigate to job link

        job_data = await extract_job_data(page)  # Extract job details
        if job_data and job_data["url"] not in unique_urls:
            job_results.append(job_data)
            unique_urls.add(job_data["url"])  # Add to unique URLs set
            last_save_time[0] = time.time()  # Update last save time
            logging.info(f"[Phase: Exploration at Depth {depth}] Saved: {link}")
        else:
            logging.info(f"[Phase: Exploration at Depth {depth}] Visited but not saved (duplicate or invalid): {link}")

        # Find deeper links and explore recursively
        deeper_start_time = time.time()
        deeper_links = await find_job_links(page)
        random.shuffle(deeper_links)  # Randomize deeper links for variation

        for deeper_link in deeper_links:
            if time.time() - deeper_start_time > TIMEOUT_DEEPER_SECONDS:
                logging.warning(f"[Phase: Exploration at Depth {depth}] Timeout for deeper exploration.")
                break

            if deeper_link not in visited_links:
                visited_links.add(deeper_link)
                await process_links_recursively(deeper_link, visited_links, page, job_results, unique_urls, last_save_time, depth + 1)

    except Exception as e:
        logging.error(f"[Phase: Exploration at Depth {depth}] Error visiting: {link}")

# Main scraping function
async def scrape_jobs():
    """Orchestrates the job scraping process."""
    visited_links = set()  # Keep track of visited links
    unique_urls = set()  # Keep track of unique URLs
    job_results = []  # Store extracted job data
    last_save_time = [time.time()]  # Initialize last save time (mutable for inner functions)
    root_queue = deque()  # Queue for root links

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-dev-shm-usage", "--no-sandbox"])
        context = await browser.new_context()
        page = await context.new_page()

        try:
            logging.info("[Phase: Root Exploration] Starting root exploration")
            await page.goto(ROOT_URL, wait_until="load", timeout=GLOBAL_TIMEOUT)  # Navigate to root URL
            root_links = await find_job_links(page)  # Collect all links from the root page
            root_queue.extend(root_links)  # Add root links to queue

            while root_queue and len(job_results) < MAX_RECORDS:
                root_link = root_queue.popleft()  # Get the next root link

                if root_link not in visited_links:
                    visited_links.add(root_link)
                    await process_links_recursively(root_link, visited_links, page, job_results, unique_urls, last_save_time, depth=0)

        except Exception as e:
            logging.error(f"Critical error during scraping: {e}")

        finally:
            await context.close()  # Close browser context
            await browser.close()  # Close browser

    if job_results:
        logging.info(f"Scraped {len(job_results)} jobs. Saving to CSV...")
        write_to_csv(job_results, CSV_OUTPUT)  # Save job data to CSV
    else:
        logging.warning("No jobs scraped. Creating empty CSV file.")
        write_to_csv([], CSV_OUTPUT)  # Create an empty CSV file if no jobs scraped

# Entry point of the script
if __name__ == "__main__":
    asyncio.run(scrape_jobs())
