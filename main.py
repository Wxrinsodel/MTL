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
# Purpose: Sets up robust logging to track scraper activities, errors, and progress
# Benefits: Enables debugging, monitors performance, and provides audit trail
# Implementation: Uses both file and console handlers with timestamp formatting
logging.basicConfig(
    level=logging.INFO,  # Sets verbosity level to capture informational messages
    format='%(asctime)s - %(levelname)s - %(message)s',  #Includes timestamp for chronological tracking
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),  # Persists logs to file with UTF-8 support for Thai characters
        logging.StreamHandler(sys.stdout)  # Outputs logs to console for real-time monitoring
    ]
)

# Set pandas display options for Thai language and proper data visualization
# Purpose: Configures pandas to correctly display Thai text and large datasets
pd.set_option('display.unicode.east_asian_width', True)  # Ensures proper display width for Thai characters
pd.set_option('display.max_colwidth', 100)  # Prevents content truncation in wide columns
pd.set_option('display.max_columns', None)  # Shows all columns without truncation
pd.set_option('display.max_rows', None)  # Shows all rows without truncation
pd.set_option('display.width', 1000)  # Provides sufficient width for console output

class WebScraper:
    def __init__(self, base_url="https://www.innwhy.com/"):
        """
        Initialize the web scraper with configurable base URL and browser-like session
        
        Parameters:
            base_url (str): Target website URL, defaults to innwhy.com
            
        How it works:
            - Creates a persistent session to maintain cookies and connection efficiency
            - Sets realistic browser headers to avoid being blocked by anti-scraping measures
        """
        self.base_url = base_url  # Stores base URL for relative URL resolution
        self.session = requests.Session()  # Creates persistent HTTP session for connection pooling and cookie retention
        
        # Configure request headers to mimic a real browser
        # Why needed: Websites often block requests without proper browser identification
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",  # Mimics Chrome browser
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",  # Accepts various content types
            "Accept-Language": "th,en-US;q=0.7,en;q=0.3",  # Prioritizes Thai language for content
            "Accept-Encoding": "gzip, deflate, br",  # Supports compressed responses for efficiency
            "Connection": "keep-alive",  # Maintains persistent connection for better performance
        })

    def clean_url(self, url):
        """
        Sanitizes and validates URL format with robust error handling
        
        Parameters:
            url (str): Raw URL that might contain formatting issues
            
        Returns:
            str or None: Properly formatted URL or None if invalid
            
        How it works:
            - Removes extraneous characters and normalizes format
            - Handles relative URLs by joining with base URL
            - Validates against target domain to prevent off-site scraping
        """
        if not url:
            return None
        
        # Normalize URL format and remove problematic characters
        # Why needed: URLs from HTML often contain escape sequences or formatting artifacts
        url = url.strip().replace("\\", "/").strip(',')  # Standardizes slashes and removes trailing commas
        url = re.sub(r',+\d{4}-\d{2}-\d{2}$', '', url)  # Removes date suffixes that shouldn't be part of the URL
        
        # Convert relative URLs to absolute using base URL
        # Why needed: Many href attributes contain relative paths that need base URL context
        if not url.startswith(('http://', 'https://')):
            url = urljoin(self.base_url, url.lstrip('/'))  # Joins with base URL, handling leading slashes
        
        try:
            parsed = urlparse(url)  # Parses URL into components for validation
            
            # Domain validation to ensure we only scrape intended website
            # Why needed: Prevents scraping unintended domains if links point elsewhere
            if parsed.netloc and "innwhy.com" in parsed.netloc:  # Verifies domain matches target site
                return url
            return None
        except Exception as e:
            logging.error(f"URL parsing error: {e}")  # Logs parsing failures for debugging
            return None

    def fetch_html(self, url, retries=3, delay=1):
        """
        Retrieves HTML content with retry logic, rate limiting, and error handling
        
        Parameters:
            url (str): Target URL to fetch
            retries (int): Number of retry attempts if request fails
            delay (int): Seconds to wait between requests (rate limiting)
            
        Returns:
            str or None: HTML content as string or None if retrieval fails
            
        How it works:
            - Implements exponential backoff for retries to handle temporary failures
            - Uses rate limiting to avoid overloading the server
            - Enforces UTF-8 encoding for proper Thai text handling
        """
        url = self.clean_url(url)  # Sanitizes URL before fetching
        if not url:
            return None

        for attempt in range(retries):  # Implements retry logic
            try:
                time.sleep(delay)  # Rate limiting to prevent server overload
                
                # Execute request with timeout to prevent hanging
                # Why needed: Prevents scraper from freezing on slow or non-responsive pages
                response = self.session.get(url, timeout=30)  # 30-second timeout prevents hanging on slow responses
                
                response.encoding = 'utf-8'  
                response.raise_for_status()  # Raises exception for HTTP error status codes (4xx, 5xx)
                
                return response.text  # Returns HTML content as string
                
            except requests.exceptions.RequestException as e:  # Catches all request-related exceptions
                logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")  # Logs failure details
                
                if attempt == retries - 1:  # Checks if this is the final retry attempt
                    logging.error(f"Failed to fetch HTML after {retries} attempts") 
                    return None
                    
                # Exponential backoff: gradually increases delay between retries
                # Why needed: Helps handle temporary server issues and avoid being rate-limited
                time.sleep(delay * (attempt + 1))  # Increases delay with each retry
                continue

    def extract_date(self, element):
        """
        Extracts and normalizes publication dates from various HTML elements and formats
        
        Parameters:
            element (BeautifulSoup object): HTML element potentially containing date information
            
        Returns:
            str: Standardized date in YYYY-MM-DD format
            
        How it works:
            - Uses regex patterns to identify dates in multiple formats (including Thai)
            - Searches both HTML attributes and text content
            - Falls back to current date if no date is found
        """
        # Define regex patterns for different date formats (ISO, Thai, etc.)
        # Why multiple patterns: Websites use different date formats across elements
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # ISO format: YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # Common format: DD/MM/YYYY
            r'\d{1,2}\s+[มกราคมกุมภาพันธ์มีนาคมเมษายนพฤษภาคมมิถุนายนกรกฎาคมสิงหาคมกันยายนตุลาคมพฤศจิกายนธันวาคม]\s+\d{4}'  # Thai format: DD Month YYYY
        ]
        
        # Check for dates in various locations and formats
        for pattern in date_patterns:
            if element:
                # Check common HTML attributes that might contain dates
                # Why check attributes: Dates are often stored in metadata attributes rather than visible text
                for attr in ['datetime', 'data-date', 'content']:  # Common date-containing attributes
                    date_str = element.get(attr, '')  # Gets attribute value or empty string if not present
                    match = re.search(pattern, date_str)  # Searches for date pattern in attribute value
                    
                    if match:  
                        try:
                            return parser.parse(match.group(0)).strftime("%Y-%m-%d")  # Normalizes to YYYY-MM-DD
                        except:
                            continue  # Continue if parsing fails

                # Check visible text content if attributes didn't yield a date
                date_str = element.get_text(strip=True)  # Gets visible text with whitespace removed
                match = re.search(pattern, date_str)  # Searches for date pattern in text
                
                if match:
                    try:
                        return parser.parse(match.group(0)).strftime("%Y-%m-%d")  # Normalizes to YYYY-MM-DD
                    except:
                        continue  # Continue if parsing fails
        
        # Fallback: use current date if no date found
        # Why fallback: Articles without dates should still be included with some timestamp
        return datetime.now().strftime("%Y-%m-%d")  # Returns current date in YYYY-MM-DD format

    def clean_content(self, text):
        """
        Sanitizes and normalizes extracted text content
        
        Parameters:
            text (str): Raw text content that might contain formatting issues
            
        Returns:
            str: Cleaned and normalized text
            
        How it works:
            - Removes excessive whitespace and normalizes line breaks
            - Handles empty content gracefully
        """
        if not text:
            return ""  # Returns empty string for None or empty input
            
        # Normalize whitespace while preserving content
        # Why needed: HTML often contains inconsistent spacing and line breaks
        text = re.sub(r'\s+', ' ', text)  # Replaces multiple whitespace characters with single space
        text = text.strip()  # Removes leading and trailing whitespace
        
        return text

    def fetch_article_content(self, url):
        """
        Retrieves and extracts article body content with robust fallback strategies
        
        Parameters:
            url (str): URL of the article to scrape
            
        Returns:
            str: Extracted article content or error message
            
        How it works:
            - Removes non-content elements like scripts, ads, and comments
            - Tries multiple CSS selectors to find content in different page layouts
            - Extracts text from semantic elements (paragraphs, headings)
            - Provides meaningful fallback when content can't be found
        """
        html = self.fetch_html(url)  # Fetches page HTML
        if not html:
            return "ไม่สามารถดึงเนื้อหาบทความได้"  # Error message in Thai if fetching fails

        soup = BeautifulSoup(html, "html.parser")  # Parses HTML into navigable DOM
        
        # Remove non-content elements to improve extraction quality
        # Why needed: These elements contain text that isn't part of the article body
        for element in soup.select("script, style, iframe, .related-posts, .comments-area, .advertisement, .social-share, .author-bio"):
            element.decompose()  # Removes element from DOM completely

        # Multiple selectors for content to handle different site layouts
        # Why multiple selectors: Different websites and even different sections use various content containers
        content_selectors = [
            "div.entry-content",  # Common WordPress content container
            "article.post",  # Standard article element
            ".content-area",  # Common content area class
            ".post-content",  # Another common content class
            "main#main",  # Main content element
            "#main-content",  # Alternative main content ID
            ".article-content"  # Generic article content class
        ]

        content = ""
        for selector in content_selectors:  # Try each selector until content is found
            content_element = soup.select_one(selector)  # Gets first matching element
            
            if content_element:  # If selector matched an element
                # Extract text from semantic elements only
                # Why semantic elements: Focuses on actual content and ignores navigation, widgets, etc.
                content_parts = []
                for elem in content_element.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):  # Paragraphs and headings
                    text = elem.get_text(strip=True)  # Gets text with whitespace removed
                    
                    if text:  # Skip empty elements
                        content_parts.append(text)  # Adds text to content collection
                
                # Join content parts with double newlines for readability
                content = "\n\n".join(content_parts)
                
                if content:  # If content was found, stop trying selectors
                    break

        # Return cleaned content or error message if no content found
        return self.clean_content(content) or "ไม่พบเนื้อหา"  # Returns cleaned content or "No content found" in Thai

    
    def parse_page(self, html):
        """
        Extracts article information from a list/index page
        
        Parameters:
            html (str): HTML content of the page
            
        Returns:
            list: List of dictionaries containing article information
            
        How it works:
            - Uses multiple selectors to identify article containers in different layouts
            - Extracts headline, link, date, and fetches full content for each article
            - Implements robust error handling for individual article parsing failures
        """
        if not html:
            return []  # Returns empty list if no HTML provided

        soup = BeautifulSoup(html, "html.parser")  # Parses HTML into navigable DOM
        articles = []  # Initializes empty articles list

        # Multiple selectors for article containers to handle different page layouts
        # Why multiple selectors: Different websites use various container elements for articles
        article_selectors = [
            "article",  # Standard HTML5 article element
            ".post",  # Common post class
            ".blog-post",  # Blog-specific post class
            ".news-item",  # News-specific item class
            ".item",  # Generic item class
            ".entry",  # Common entry class
            ".card"  # Bootstrap/modern card component
        ]

        for selector in article_selectors:  # Try each selector to find article containers
            for article in soup.select(selector):  # Process each article found with current selector
                try:
                    # Multiple title selectors to handle different article layouts
                    # Why multiple selectors: Titles often have different HTML structures across sites
                    title_selectors = [
                        "h2.entry-title a",  # WordPress standard
                        ".post-title a",  # Common blog format
                        "h2 a",  # Generic heading with link
                        ".entry-header h2 a",  # Entry header pattern
                        "h3 a",  # Smaller heading with link
                        ".title a",  # Generic title class
                        ".heading a"  # Generic heading class
                    ]
                    
                    title_element = None  # Initialize title element variable
                    for title_selector in title_selectors:  # Try each title selector
                        title_element = article.select_one(title_selector)  # Get first matching element
                        if title_element:  # If selector matched an element
                            break  # Stop trying selectors

                    if not title_element:  # If no title found after all selectors
                        logging.warning("No title element found, skipping article")  # Log warning
                        continue  # Skip to next article

                    # Extract headline text and link URL
                    headline = title_element.get_text(strip=True)  # Gets title text
                    link = self.clean_url(title_element.get('href', ''))  # Gets and cleans link URL
                    
                    if not link:  # Skip article if no valid link
                        continue

                    # Multiple date selectors to handle different article layouts
                    # Why multiple selectors: Publication dates appear in various elements across sites
                    date_selectors = [
                        "time",  # HTML5 time element
                        ".published",  # Common published date class
                        ".post-date",  # Post date class
                        ".entry-date",  # Entry date class
                        ".date",  # Generic date class
                        ".meta-date"  # Meta information date class
                    ]
                    
                    # Extract publication date
                    date = None
                    for date_selector in date_selectors:  # Try each date selector
                        date_element = article.select_one(date_selector)  # Get first matching element
                        if date_element:  # If selector matched an element
                            date = self.extract_date(date_element)  # Extract and parse date
                            if date:  # If valid date extracted
                                break  # Stop trying selectors

                    # Use current date as fallback if no date found
                    # Why fallback: Articles without dates should still be included with some timestamp
                    if not date:
                        date = datetime.now().strftime("%Y-%m-%d")  # Current date in YYYY-MM-DD format
                        logging.warning(f"No date found for article: {headline}, using current date")  # Log warning

                    # Fetch full article content from link
                    content = self.fetch_article_content(link)  # Gets detailed content from article page
                    
                    # Add complete article information to results
                    articles.append({
                        "Headline": headline,
                        "Link": link,
                        "Date": date,
                        "Content": content
                    })
                    logging.info(f"Successfully parsed article: {headline}") 

                except Exception as e:  # Catch any errors during individual article parsing
                    logging.error(f"Error parsing article: {e}")  # Log error details
                    continue  # Skip to next article rather than failing entire page

        return articles  # Return all successfully parsed articles

    def scrape_website(self, num_pages=2):
        """
        Orchestrates multi-page scraping with filtering for recent articles
        
        Parameters:
            num_pages (int): Number of pages to scrape
            
        Returns:
            list: Combined list of article information from all pages
            
        How it works:
            - Handles different pagination formats (page/N vs. query parameters)
            - Filters articles to include only those published within the last month
            - Implements robust error handling for individual page failures
        """
        all_articles = []
        one_month_ago = datetime.now() - timedelta(days=30)  # Calculate cutoff date for filtering
        
        for page in range(1, num_pages + 1):  # Iterate through pages, starting from 1
            try:
                # Handle pagination URL format based on page number
                # Why conditional: First page often has different URL format than subsequent pages
                if page > 1:
                    page_url = f"{self.base_url}page/{page}"  # Common WordPress pagination format
                else:
                    page_url = self.base_url  # Base URL for first page
                
                logging.info(f"Scraping page {page}: {page_url}")  # Log current page being processed
                
                # Fetch and verify HTML content
                html = self.fetch_html(page_url)  # Get page HTML
                if not html:
                    logging.warning(f"No HTML content found for page {page}")  # Log warning
                    continue  # Skip to next page rather than breaking entire loop

                # Parse articles from current page
                articles = self.parse_page(html)  # Extract articles from page HTML
                logging.info(f"Found {len(articles)} potential articles on page {page}")  # Log article count
                
                if not articles:
                    logging.warning(f"No articles found on page {page}")  # Log warning
                    continue  # Skip to next page rather than breaking entire loop
                
                # Filter articles to include only those from the last month
                # Why filter: Focuses on recent content and limits dataset size
                filtered_articles = []
                for article in articles:
                    try:
                        # Parse and compare article date with cutoff date
                        article_date = datetime.strptime(article["Date"], "%Y-%m-%d")  # Parse date string to datetime
                        if article_date > one_month_ago:  # Check if article is newer than cutoff
                            filtered_articles.append(article)  # Add to filtered results
                    except Exception as e:
                        logging.error(f"Error filtering article by date: {e}")
                        # Include article despite date parsing failure
                        # Why include: Better to have too much data than miss valuable content
                        filtered_articles.append(article)
                
                logging.info(f"Kept {len(filtered_articles)} articles within date range on page {page}")  # Log filtered count
                all_articles.extend(filtered_articles)  # Add filtered articles to overall results

            except Exception as e:  # Catch any errors during page processing
                logging.error(f"Error processing page {page}: {e}")  # Log error details
                continue  # Skip to next page rather than breaking entire loop

        logging.info(f"Total articles scraped: {len(all_articles)}")  # Log final article count
        return all_articles  # Return combined articles from all pages

def save_to_csv(data, filename="scraped_articles.csv"):

    import csv  # Import csv module directly for explicit control

    if not data:
        logging.warning("No data to save") 
        return False

    try:
        # Convert list of dictionaries to DataFrame for easier manipulation
        df = pd.DataFrame(data)
        
        # Create output directory if it doesn't exist to ensures consistent output location
        os.makedirs('output', exist_ok=True)  # Creates directory if missing, otherwise does nothing
        output_path = os.path.join('output', filename)  # Builds full path with directory
        
        
        # Why: Thai requires specific encoding and escaping for proper handling
        df.to_csv(
            output_path,
            index=False,  # Omits row numbers
            encoding='utf-8-sig',  # Uses UTF-8 with BOM for Thai support in Excel
            quoting=csv.QUOTE_ALL,  # Quotes all fields for maximum compatibility
            escapechar='\\',  # Escapes special characters within quoted fields
            errors='replace'  # Replaces any characters that can't be encoded
        )
        
        logging.info(f"Successfully saved {len(data)} articles to {output_path}")
        return True
    except Exception as e:
        logging.error(f"Error saving to CSV: {e}")
        return False

def main():
    """
    Main execution function with fallback strategy
    
    How it works:
        - First attempts to scrape PR news section
        - Falls back to main page if PR section yields no results
        - Implements comprehensive error handling
    """
    try:
        # First try dedicated PR news section
        # Why PR section first: Contains more targeted content
        logging.info("Starting scrape of PR news section: https://www.innwhy.com/pr-news/")
        news_url = urljoin("https://www.innwhy.com/", "pr-news/")  # Builds PR section URL
        scraper = WebScraper(news_url)  # Creates scraper for PR section
        
        scraped_data = scraper.scrape_website(num_pages=3)  # Scrapes first 3 pages of PR section
        
        # Fallback strategy if PR section has no articles
        # Why fallback: Ensures some data is collected even if primary source fails
        if not scraped_data:
            logging.info("No articles found in PR news section, trying main page") 
            scraper = WebScraper("https://www.innwhy.com/")  # Creates scraper for main page
            scraped_data = scraper.scrape_website(num_pages=3)  # choose the page
        
       
        if scraped_data:
            success = save_to_csv(scraped_data)  
            if success:
                logging.info("Scraping process completed successfully")  
            else:
                logging.error("Failed to save scraped data")  
        else:
            logging.warning("No articles were scraped") 
            
    except Exception as e:  
        logging.error(f"Main process error: {e}")

if __name__ == "__main__":
    main()  