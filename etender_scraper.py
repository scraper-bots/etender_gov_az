import asyncio
import aiohttp
import csv
import pandas as pd
from datetime import datetime
import json
import logging
from typing import List, Dict, Any
import time

class ETenderScraper:
    def __init__(self):
        self.base_url = "https://etender.gov.az/api/events"
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6",
            "dnt": "1",
            "origin": "https://www.etender.gov.az",
            "referer": "https://www.etender.gov.az/",
            "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
            "x-origin-token": "0gQftPlv+3Gv+ItMwph9UDGszzZHmyEgqsyS/tBJp3O+icNkxLLe5tFZ1APwxUg8sMunRe/v9CGnHP+oBQWB07lQtS7ic78UGZbzZStGSXIB/+dHZhO7acBUn+df+9uoSWHv5+VRIcfAxLaipsNX6w==",
            "x-recaptcha-token": "",
            "x-xsrf-token": ""
        }
        self.params = {
            "EventType": "2",
            "PageSize": "15",
            "EventStatus": "1",
            "Keyword": "",
            "buyerOrganizationName": "",
            "PrivateRfxId": "",
            "publishDateFrom": "",
            "publishDateTo": "",
            "AwardedparticipantName": "",
            "AwardedparticipantVoen": "",
            "DocumentViewType": ""
        }
        self.all_items = []
        self.semaphore = asyncio.Semaphore(5)  # Limit concurrent requests

        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    async def fetch_page(self, session: aiohttp.ClientSession, page_number: int) -> Dict[str, Any]:
        """Fetch a single page of data from the API"""
        async with self.semaphore:
            params = self.params.copy()
            params["PageNumber"] = str(page_number)

            try:
                await asyncio.sleep(0.1)  # Rate limiting
                async with session.get(self.base_url, params=params, headers=self.headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.logger.info(f"Successfully fetched page {page_number}")
                        return data
                    else:
                        self.logger.error(f"Failed to fetch page {page_number}: HTTP {response.status}")
                        return None
            except Exception as e:
                self.logger.error(f"Error fetching page {page_number}: {str(e)}")
                return None

    async def get_total_pages(self, session: aiohttp.ClientSession) -> int:
        """Get the total number of pages available"""
        first_page = await self.fetch_page(session, 1)
        if first_page:
            return first_page.get("totalPages", 0)
        return 0

    async def scrape_all_pages(self):
        """Scrape all pages from the API"""
        connector = aiohttp.TCPConnector(limit=10)
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Get total pages
            total_pages = await self.get_total_pages(session)
            if total_pages == 0:
                self.logger.error("Could not determine total pages")
                return

            self.logger.info(f"Total pages to scrape: {total_pages}")

            # Create tasks for all pages
            tasks = []
            for page_num in range(1, total_pages + 1):
                task = asyncio.create_task(self.fetch_page(session, page_num))
                tasks.append(task)

            # Execute tasks in batches to avoid overwhelming the server
            batch_size = 10
            for i in range(0, len(tasks), batch_size):
                batch = tasks[i:i + batch_size]
                results = await asyncio.gather(*batch, return_exceptions=True)

                for result in results:
                    if isinstance(result, dict) and result is not None:
                        items = result.get("items", [])
                        self.all_items.extend(items)

                self.logger.info(f"Completed batch {i//batch_size + 1}/{(len(tasks) + batch_size - 1)//batch_size}")
                await asyncio.sleep(0.5)  # Pause between batches

            self.logger.info(f"Scraped {len(self.all_items)} total items")

    def save_to_csv(self, filename: str = None):
        """Save scraped data to CSV file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"etender_data_{timestamp}.csv"

        if not self.all_items:
            self.logger.warning("No data to save")
            return

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            if self.all_items:
                fieldnames = self.all_items[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.all_items)

        self.logger.info(f"Data saved to {filename}")

    def save_to_xlsx(self, filename: str = None):
        """Save scraped data to XLSX file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"etender_data_{timestamp}.xlsx"

        if not self.all_items:
            self.logger.warning("No data to save")
            return

        df = pd.DataFrame(self.all_items)

        # Convert date columns to datetime
        date_columns = ['publishDate', 'endDate']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Save to Excel with formatting
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='ETender Data', index=False)

            # Auto-adjust column widths
            worksheet = writer.sheets['ETender Data']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width

        self.logger.info(f"Data saved to {filename}")

    async def run(self, csv_filename: str = None, xlsx_filename: str = None):
        """Main method to run the scraper"""
        start_time = time.time()
        self.logger.info("Starting ETender scraping...")

        await self.scrape_all_pages()

        if self.all_items:
            self.save_to_csv(csv_filename)
            self.save_to_xlsx(xlsx_filename)

            end_time = time.time()
            self.logger.info(f"Scraping completed in {end_time - start_time:.2f} seconds")
            self.logger.info(f"Total items scraped: {len(self.all_items)}")
        else:
            self.logger.error("No data was scraped")

async def main():
    scraper = ETenderScraper()
    await scraper.run()

if __name__ == "__main__":
    asyncio.run(main())