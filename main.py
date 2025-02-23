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
        
        # Try to find date in various attributes and text
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

    def parse_page(self, html):
        """Parse page content with improved accuracy"""
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        articles = []

        # Multiple selectors for article containers
        article_selectors = [
            "article",
            ".post",
            ".blog-post",
            ".news-item"
        ]

        for selector in article_selectors:
            for article in soup.select(selector):
                try:
                    # Extract title
                    title_selectors = [
                        "h2.entry-title a",
                        ".post-title a",
                        "h2 a",
                        ".entry-header h2 a"
                    ]
                    
                    title_element = None
                    for title_selector in title_selectors:
                        title_element = article.select_one(title_selector)
                        if title_element:
                            break

                    if not title_element:
                        continue

                    headline = title_element.get_text(strip=True)
                    link = self.clean_url(title_element.get('href', ''))
                    
                    if not link:
                        continue

                    # Extract date
                    date_selectors = [
                        "time",
                        ".published",
                        ".post-date",
                        ".entry-date"
                    ]
                    
                    date = None
                    for date_selector in date_selectors:
                        date_element = article.select_one(date_selector)
                        if date_element:
                            date = self.extract_date(date_element)
                            if date:
                                break

                    if not date:
                        continue

                    # Fetch full article content
                    content = self.fetch_article_content(link)
                    
                    articles.append({
                        "Headline": headline,
                        "Link": link,
                        "Date": date,
                        "Content": content
                    })

                except Exception as e:
                    logging.error(f"Error parsing article: {e}")
                    continue

        return articles

    def scrape_website(self, num_pages=5):
        """Scrape website with improved pagination handling"""
        all_articles = []
        one_month_ago = datetime.now() - timedelta(days=30)
        
        for page in range(5, num_pages + 1):
            try:
                # Handle different pagination URL formats
                if page > 1:
                    page_url = f"{self.base_url}page/{page}"
                else:
                    page_url = self.base_url

                logging.info(f"Scraping page {page}: {page_url}")
                
                html = self.fetch_html(page_url)
                if not html:
                    logging.warning(f"No HTML content found for page {page}")
                    break

                articles = self.parse_page(html)
                
                # Filter articles within last month
                filtered_articles = [
                    article for article in articles
                    if article["Date"] and datetime.strptime(article["Date"], "%Y-%m-%d") > one_month_ago
                ]

                if not filtered_articles:
                    logging.info(f"No recent articles found on page {page}")
                    break

                all_articles.extend(filtered_articles)
                logging.info(f"Found {len(filtered_articles)} articles on page {page}")

            except Exception as e:
                logging.error(f"Error processing page {page}: {e}")
                break

        return all_articles

def save_to_csv(data, filename="scraped_articles.csv"):
    """Save data to CSV with proper Thai language encoding"""
    if not data:
        logging.warning("No data to save")
        return

    try:
        df = pd.DataFrame(data)
        
        # Ensure output directory exists
        os.makedirs('output', exist_ok=True)
        output_path = os.path.join('output', filename)
        
        # Save with Thai language support
        df.to_csv(
            output_path,
            index=False,
            encoding='utf-8-sig',  # Use UTF-8 with BOM for Thai support
            quoting=pd.io.common.csv.QUOTE_ALL,
            escapechar='\\',
            errors='replace'
        )
        
        logging.info(f"Successfully saved {len(data)} articles to {output_path}")
    except Exception as e:
        logging.error(f"Error saving to CSV: {e}")

def main():
    try:
        scraper = WebScraper()
        news_url = urljoin(scraper.base_url, "pr-news/")
        scraper = WebScraper(news_url)
        
        logging.info("Starting web scraping process...")
        scraped_data = scraper.scrape_website()
        
        if scraped_data:
            save_to_csv(scraped_data)
            logging.info("Scraping process completed successfully")
        else:
            logging.warning("No articles were scraped")
            
    except Exception as e:
        logging.error(f"Main process error: {e}")

if __name__ == "__main__":
    main()