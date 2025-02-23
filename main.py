import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from dateutil import parser
import os
import re
import logging
import time
import sys
import csv

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Set pandas display options for Thai language
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

class WebScraper:
    def __init__(self, base_url="https://www.innwhy.com/"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "th,en-US;q=0.7,en;q=0.3",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        })

    def clean_url(self, url):
        """Clean and validate URL format with improved handling"""
        if not url:
            return None
        
        # Remove unwanted characters and normalize
        url = url.strip().replace("\\", "/").strip(',')
        url = re.sub(r',+\d{4}-\d{2}-\d{2}$', '', url)
        
        # Handle relative URLs
        if not url.startswith(('http://', 'https://')):
            url = urljoin(self.base_url, url.lstrip('/'))
        
        try:
            parsed = urlparse(url)
            # Ensure URL is from the correct domain
            if parsed.netloc and "innwhy.com" in parsed.netloc:
                return url
            return None
        except Exception as e:
            logging.error(f"URL parsing error: {e}")
            return None

    def fetch_html(self, url, retries=3, delay=1):
        """Fetch HTML content with improved error handling and rate limiting"""
        url = self.clean_url(url)
        if not url:
            return None

        for attempt in range(retries):
            try:
                time.sleep(delay)  # Rate limiting
                response = self.session.get(url, timeout=30)
                response.encoding = 'utf-8'
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt == retries - 1:
                    logging.error(f"Failed to fetch HTML after {retries} attempts")
                    return None
                time.sleep(delay * (attempt + 1))  # Exponential backoff
                continue

    def extract_date(self, element):
        """Extract and parse date from various HTML elements"""
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'\d{1,2}\s+[มกราคมกุมภาพันธ์มีนาคมเมษายนพฤษภาคมมิถุนายนกรกฎาคมสิงหาคมกันยายนตุลาคมพฤศจิกายนธันวาคม]\s+\d{4}'
        ]
        
        for pattern in date_patterns:
            if element:
                # Check attributes
                for attr in ['datetime', 'data-date', 'content']:
                    date_str = element.get(attr, '')
                    match = re.search(pattern, date_str)
                    if match:
                        try:
                            return parser.parse(match.group(0)).strftime("%Y-%m-%d")
                        except:
                            continue

                # Check text content
                date_str = element.get_text(strip=True)
                match = re.search(pattern, date_str)
                if match:
                    try:
                        return parser.parse(match.group(0)).strftime("%Y-%m-%d")
                    except:
                        continue
        return None

    def clean_content(self, text):
        """Clean and normalize content text"""
        if not text:
            return ""
        # Remove extra whitespace and normalize Thai characters
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        return text

    def fetch_article_content(self, url):
        """Fetch and extract article content with improved accuracy"""
        html = self.fetch_html(url)
        if not html:
            return "ไม่สามารถดึงเนื้อหาบทความได้"

        soup = BeautifulSoup(html, "html.parser")
        
        # Remove unwanted elements
        for element in soup.select("script, style, iframe, .related-posts, .comments-area, .advertisement, .social-share, .author-bio"):
            element.decompose()

        # Try multiple selectors for content
        content_selectors = [
            "div.entry-content",
            "article.post",
            ".content-area",
            ".post-content",
            "main#main"
        ]

        content = ""
        for selector in content_selectors:
            content_element = soup.select_one(selector)
            if content_element:
                # Extract text from paragraphs and headers
                content_parts = []
                for elem in content_element.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                    text = elem.get_text(strip=True)
                    if text:
                        content_parts.append(text)
                
                content = "\n\n".join(content_parts)
                if content:
                    break

        return self.clean_content(content) or "ไม่พบเนื้อหา"

    def extract_main_page_content(self, soup):
        """Extract content from the main page"""
        main_content = []
        
        # Try different selectors for main page content
        main_selectors = [
            ".site-main",
            "#primary",
            ".main-content",
            ".content-area"
        ]
        
        for selector in main_selectors:
            main_section = soup.select_one(selector)
            if main_section:
                text = main_section.get_text(strip=True)
                if text:
                    main_content.append({"type": "Main Page Content", "content": self.clean_content(text)})
                break
        
        return main_content

    def parse_single_page(self, page_number):
        """Parse all content from a specific page number"""
        page_url = f"{self.base_url}page/{page_number}" if page_number > 1 else self.base_url
        logging.info(f"Scraping page {page_number}: {page_url}")
        
        html = self.fetch_html(page_url)
        if not html:
            logging.error(f"Failed to fetch page {page_number}")
            return []
        
        soup = BeautifulSoup(html, "html.parser")
        all_content = []
        
        # First, get the main page content
        main_content = self.extract_main_page_content(soup)
        all_content.extend(main_content)
        
        # Then get all article links on the page
        article_links = []
        link_selectors = [
            "article a",
            ".post-title a",
            ".entry-title a",
            ".read-more-link"
        ]
        
        for selector in link_selectors:
            links = soup.select(selector)
            for link in links:
                url = self.clean_url(link.get('href'))
                if url and url not in [item.get('Link') for item in article_links]:
                    article_links.append({
                        'Link': url,
                        'Title': link.get_text(strip=True)
                    })
        
        # Process each article
        for article in article_links:
            try:
                logging.info(f"Processing article: {article['Title']}")
                
                content = self.fetch_article_content(article['Link'])
                date = None
                
                # Try to get the date from the article page
                article_html = self.fetch_html(article['Link'])
                if article_html:
                    article_soup = BeautifulSoup(article_html, 'html.parser')
                    date_element = article_soup.select_one("time, .published, .post-date")
                    if date_element:
                        date = self.extract_date(date_element)
                
                all_content.append({
                    "type": "Article",
                    "Headline": article['Title'],
                    "Link": article['Link'],
                    "Date": date or datetime.now().strftime("%Y-%m-%d"),
                    "Content": content
                })
                
                # Add a small delay between articles
                time.sleep(1)
                
            except Exception as e:
                logging.error(f"Error processing article {article['Link']}: {e}")
                continue
        
        return all_content

def save_to_csv(data, page_number, filename_prefix="scraped_content"):
    """Save data to CSV with proper Thai language encoding"""
    if not data:
        logging.warning("No data to save")
        return

    try:
        # Create a proper DataFrame structure
        processed_data = []
        for item in data:
            if item['type'] == 'Main Page Content':
                processed_data.append({
                    'Type': 'Main Page',
                    'Headline': f'Page {page_number} Main Content',
                    'Link': f"{item.get('Link', 'N/A')}",
                    'Date': datetime.now().strftime("%Y-%m-%d"),
                    'Content': item['content']
                })
            else:
                processed_data.append({
                    'Type': 'Article',
                    'Headline': item['Headline'],
                    'Link': item['Link'],
                    'Date': item['Date'],
                    'Content': item['Content']
                })

        df = pd.DataFrame(processed_data)
        
        # Ensure output directory exists
        os.makedirs('output', exist_ok=True)
        
        # Create filename with page number
        filename = f"{filename_prefix}_page_{page_number}.csv"
        output_path = os.path.join('output', filename)
        
        # Save with Thai language support using the correct CSV quoting
        df.to_csv(
            output_path,
            index=False,
            encoding='utf-8-sig',
            quoting=csv.QUOTE_ALL,
            escapechar='\\',
            errors='replace'
        )
        
        logging.info(f"Successfully saved {len(processed_data)} items to {output_path}")
    except Exception as e:
        logging.error(f"Error saving to CSV: {e}")

def main():
    try:
        # Get page number from user input
        page_number = int(input("Enter the page number to scrape (1 for main page): "))
        
        news_url = urljoin("https://www.innwhy.com/", "pr-news/")
        scraper = WebScraper(news_url)
        
        logging.info(f"Starting web scraping process for page {page_number}...")
        scraped_data = scraper.parse_single_page(page_number)
        
        if scraped_data:
            save_to_csv(scraped_data, page_number)
            logging.info(f"Scraping process completed successfully for page {page_number}")
        else:
            logging.warning(f"No content was scraped from page {page_number}")
            
    except ValueError:
        logging.error("Please enter a valid page number")
    except Exception as e:
        logging.error(f"Main process error: {e}")

if __name__ == "__main__":
    main()