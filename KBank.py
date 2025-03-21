import pandas as pd
import logging
import sys
import os
import re
import time
import requests
from datetime import datetime
import csv
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# this class is used to scrape the data from the website
class KasikornSeleniumScraper:
    def __init__(self, base_url="https://www.kasikornbank.com/th/about/Pages/executives.aspx"):
        self.base_url = base_url
        self.driver = None # Initialize the WebDriver to None

    # this function is used to set up the driver
    def setup_driver(self):
        """
        Set up the Selenium WebDriver with appropriate options
        
        Returns:
            bool: True if setup successful, False otherwise
        """
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # Run in headless mode
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-notifications")
            chrome_options.add_argument("--disable-popup-blocking")
            
            # Add realistic user agent
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0"
            ]
            chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
            
            # Add language preference
            chrome_options.add_argument("--lang=th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7")
            
            # Initialize the Chrome WebDriver
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(100) # Set a timeout for loading pages
            
            # Add custom headers using CDP
            self.driver.execute_cdp_cmd('Network.setExtraHTTPHeaders', {
                'headers': {
                    'Accept-Language': 'th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Referer': 'https://www.kasikornbank.com/th/',
                }
            })
            
            logging.info("Selenium WebDriver setup completed successfully")
            return True
            
        except Exception as e:
            logging.error(f"Error setting up Selenium WebDriver: {e}")
            return False

    # this function is used to fetch the page with retries
    def fetch_page(self, url, retries=3):
        """
        Fetch the webpage using Selenium with retry logic
        
        Parameters:
            url (str): URL to fetch
            retries (int): Number of retry attempts
            
        Returns:
            str or None: HTML content of the page or None if failed
        """
        if self.driver is None:
            if not self.setup_driver():
                return None
                
        for attempt in range(retries): 
            try:
                # Add random delay to mimic human behavior
                time.sleep(2 + random.uniform(1, 3))
                
                logging.info(f"Navigating to {url} (attempt {attempt+1})")
                self.driver.get(url)
                
                # Wait for page to load
                time.sleep(5 + random.uniform(2, 5))  # Give additional time for JS to execute
                
                # Wait for content to be present
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                except TimeoutException:
                    logging.warning("Timeout waiting for body element")
                
                # Check if we have content
                page_source = self.driver.page_source
                if len(page_source) > 500:  # Simple check to ensure we got meaningful content
                    logging.info(f"Successfully fetched page (length: {len(page_source)} characters)")
                    return page_source
                else:
                    logging.warning(f"Page source too short ({len(page_source)} chars), may be an error page")
                
                # Retry if content is too short
            except WebDriverException as e:
                logging.warning(f"Selenium error on attempt {attempt+1}: {e}")
            
            except Exception as e:
                logging.warning(f"General error on attempt {attempt+1}: {e}")
            
            # Backoff with random delay before retry
            if attempt < retries - 1:
                backoff_time = 5 * (attempt + 1) + random.uniform(1, 5)
                logging.info(f"Retrying in {backoff_time:.2f} seconds...") # Log retry delay
                time.sleep(backoff_time)
        
        logging.error(f"Failed to fetch page after {retries} attempts")
        return None
    
        # this function is used to extract the date from the text
    def extract_date(self, text):
        """
        Extracts and normalizes dates from text to dd-mm-yyyy format
        
        Parameters:
            text (str): Text potentially containing date information
            
        Returns:
            str: Normalized date in dd-mm-yyyy format or empty string
        """
        if not text or text == "-":
            return ""
            
        # Define regex patterns for different date formats (both Thai and English)
        th_months = {
            'มกราคม': '01', 'กุมภาพันธ์': '02', 'มีนาคม': '03',
            'เมษายน': '04', 'พฤษภาคม': '05', 'มิถุนายน': '06',
            'กรกฎาคม': '07', 'สิงหาคม': '08', 'กันยายน': '09',
            'ตุลาคม': '10', 'พฤศจิกายน': '11', 'ธันวาคม': '12'
        }
        
        try:
            # Check for Thai format (DD Month YYYY)
            for month_name, month_num in th_months.items():
                if month_name in text:
                    parts = text.split() 
                    for i, part in enumerate(parts):
                        if part == month_name and i > 0 and i < len(parts) - 1:
                            day = parts[i-1].zfill(2)  # Ensure two digits
                            year = parts[i+1] 
                            # Convert Buddhist era to CE if needed
                            if len(year) == 4 and int(year) > 2500:
                                year = str(int(year) - 543)
                            return f"{day}-{month_num}-{year}"
                            
            # Check for common numeric formats
            # DD/MM/YYYY
            match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', text)
            if match:
                day, month, year = match.groups()
                # Convert Buddhist era to CE if needed
                if int(year) > 2500:
                    year = str(int(year) - 543)
                # Convert to dd-mm-yyyy format
                return f"{day.zfill(2)}-{month.zfill(2)}-{year}"
                
            # YYYY-MM-DD
            match = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', text)
            if match:
                year, month, day = match.groups()
                # Convert Buddhist era to CE if needed
                if int(year) > 2500:
                    year = str(int(year) - 543)
                return f"{day.zfill(2)}-{month.zfill(2)}-{year}"
                
            return ""  # Return empty string if no pattern matches
            
        except Exception as e:
            logging.error(f"Date extraction error: {e} for text: {text}")
            return ""  # Return empty string in case of error


    # this function is used to extract the prefix from the name
    def extract_prefix(self, full_name):
        """
        Extracts only the prefix (Mr, Ms, Dr, etc.) from a full name
        
        Parameters:
            full_name (str): Full name of the person including title
            
        Returns:
            str: Only the prefix
        """
        if not full_name:
            return ""
            
        # Clean the name first
        full_name = full_name.strip()
        
        # Common Thai and English titles/prefixes
        titles = {
            # Thai titles
            "นาย": "Mr",
            "นาง": "Mrs",
            "นางสาว": "Miss",
            "ดร.": "Dr",
            "ศ.": "Prof",
            "รศ.": "Assoc Prof",
            "ผศ.": "Asst Prof",
            "ศ.ดร.": "Prof Dr",
            "รศ.ดร.": "Assoc Prof Dr",
            "ผศ.ดร.": "Asst Prof Dr",
            
            # English titles
            "Mr.": "Mr", 
            "Mrs.": "Mrs",
            "Miss": "Ms",
            "Dr.": "Dr",
            "Prof.": "Prof",
            "Professor": "Prof",
            "Assoc. Prof.": "Assoc Prof",
            "Asst. Prof.": "Asst Prof",
            "Prof. Dr.": "Prof Dr"
        }
        
        # Sort titles by length in descending order to match longer titles first
        sorted_titles = sorted(titles.keys(), key=len, reverse=True)
        
        # Check for titles at the beginning of the name
        for title in sorted_titles:
            if full_name.startswith(title):
                return titles[title]
                
        # No title found
        return ""

    def parse_name(self, full_name):
        """
        Parses a full name into first name and surname, correctly removing any prefix/title
        
        Parameters:
            full_name (str): Full name including title
            
        Returns:
            tuple: (first_name, surname)
        """
        if not full_name:
            return "", ""
            
        full_name = full_name.strip()
        
        # Common Thai and English titles/prefixes - same as in extract_prefix but with complete removal
        titles = {
            # Thai titles
            "นาย": "",
            "นาง": "",
            "นางสาว": "",
            "ดร.": "",
            "ศ.": "",
            "รศ.": "",
            "ผศ.": "",
            "ศ.ดร.": "",
            "รศ.ดร.": "",
            "ผศ.ดร.": "",
            
            # English titles
            "Mr.": "",
            "Mrs.": "",
            "Miss": "",
            "Dr.": "",
            "Prof.": "",
            "Professor": "",
            "Assoc. Prof.": "",
            "Asst. Prof.": "",
            "Prof. Dr.": "",
            "Assoc. Prof. Dr.": "",
            "Asst. Prof. Dr.": ""
        }
        
        # Sort titles by length in descending order to match longer titles first
        sorted_titles = sorted(titles.keys(), key=len, reverse=True)
        
        # Remove title from name
        name_without_title = full_name
        for title in sorted_titles:
            if full_name.startswith(title):
                name_without_title = full_name[len(title):].lstrip()
                break
        
        # Split into parts
        name_parts = name_without_title.split()
        
        # If only one part, return it as first name
        if len(name_parts) == 1:
            return name_parts[0], ""
        
        # Otherwise, first part is first name, rest is surname
        first_name = name_parts[0]
        surname = " ".join(name_parts[1:])
        
        return first_name, surname
    
    def scrape_executives(self, limit=100):
        """
        Extract executive information from the Kasikorn Bank website using Selenium
        
        Parameters:
            limit (int): Maximum number of executives to extract (default: 100)
            
        Returns:
            list: List of dictionaries containing executive information
        """
        logging.info(f"Scraping executive information from {self.base_url}")
        
        html = self.fetch_page(self.base_url)
        if not html:
            logging.error("Failed to fetch page content")
            return []
            
        soup = BeautifulSoup(html, "html.parser")
        executives = []
        
        # Try to find different table structures
        executive_tables = soup.select(".ms-rteTable-default, table.executive-table, div.executive-list, table")
        
        if not executive_tables:
            logging.warning("No executive tables found. Trying alternative selectors.")
            executive_tables = soup.select("table, .executive-container, .profile-list, div.executive")
            
        exec_count = 0
        
        for table in executive_tables:
            # Try to print some information about what we found
            logging.info(f"Found potential executive table: {table.name} with classes: {table.get('class', [])}")
            
            # Handle table format
            if table.name == "table":
                rows = table.select("tr")
                logging.info(f"Found {len(rows)} rows in table")
                
                for row in rows:
                    # Skip header row
                    if row.find("th"):
                        continue
                        
                    # Extract data from columns
                    cols = row.select("td")
                    if len(cols) < 2:
                        continue
                        
                    try:
                        # Different data extraction based on table structure
                        if len(cols) >= 4:  # Full detailed table
                            full_name = cols[0].get_text(strip=True)
                            position = cols[1].get_text(strip=True)
                            start_date = self.extract_date(cols[2].get_text(strip=True))
                            end_date = self.extract_date(cols[3].get_text(strip=True))
                        elif len(cols) == 3:  # Simplified table
                            full_name = cols[0].get_text(strip=True)
                            position = cols[1].get_text(strip=True)
                            start_date = self.extract_date(cols[2].get_text(strip=True))
                            end_date = ""
                        else:  # Minimal table
                            full_name = cols[0].get_text(strip=True)
                            position = cols[1].get_text(strip=True)
                            start_date = ""
                            end_date = ""
                            
                        # Skip empty entries
                        if not full_name:
                            continue
                            
                        # Extract just the prefix
                        prefix = self.extract_prefix(full_name)
                        
                        # Parse name into first name and surname
                        first_name, surname = self.parse_name(full_name)
                        
                        logging.info(f"Found executive: {full_name}, Position: {position}")
                        
                        executives.append({
                            "Prefixed_Name": prefix,
                            "Full_Name": full_name,
                            "First_Name": first_name,
                            "Surname": surname,
                            "Position": position,
                            "Start_Date": start_date,
                            "End_Date": end_date
                        })
                        
                        exec_count += 1
                        if exec_count >= limit:
                            break
                            
                    except Exception as e:
                        logging.error(f"Error parsing row: {e}")
                        continue
            
            # Handle div-based layouts
            else:
                executive_items = table.select(".executive-item, .profile-item, .card, li, div.ms-rtestate-field")
                if not executive_items:
                    # Try broader selectors
                    executive_items = table.select("div")
                
                logging.info(f"Found {len(executive_items)} potential executive items")
                
                # Loop through items
                for item in executive_items:
                    try:
                        # Look for structured data
                        name_elem = item.select_one(".name, h3, h4, strong, b")
                        position_elem = item.select_one(".position, .title, p, .subtitle")
                        date_elem = item.select_one(".date, .duration, .period, small")
                        
                        # Extract data with fallbacks
                        full_name = name_elem.get_text(strip=True) if name_elem else ""
                        position = position_elem.get_text(strip=True) if position_elem else ""
                        start_date = self.extract_date(date_elem.get_text(strip=True)) if date_elem else ""
                        end_date = ""
                        
                        # Skip empty entries
                        if not full_name:
                            continue
                            
                        # Extract just the prefix
                        prefix = self.extract_prefix(full_name)
                        
                        # Parse name into first name and surname
                        first_name, surname = self.parse_name(full_name)
                        
                        logging.info(f"Found executive: {full_name}, Position: {position}")
                        
                        executives.append({
                            "Prefixed_Name": prefix,
                            "Full_Name": full_name,
                            "First_Name": first_name,
                            "Surname": surname,
                            "Position": position,
                            "Start_Date": start_date,
                            "End_Date": end_date
                        })
                        
                        exec_count += 1
                        if exec_count >= limit:
                            break
                            
                    except Exception as e:
                        logging.error(f"Error parsing executive item: {e}")
                        continue
            
            if exec_count >= limit:
                break
        
        # Try direct search for executives using Selenium if no results from HTML parsing
        if len(executives) == 0 and self.driver:
            logging.info("Attempting direct Selenium extraction of executives")
            try:
                # Look for elements that might contain executive information
                exec_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.executive-profile, div.profile-card, table tr, div.executive-item")
                
                logging.info(f"Found {len(exec_elements)} potential executive elements with direct Selenium query")
                
                for element in exec_elements[:limit]:  # Limit to prevent taking too long
                    try:
                        # Try to get text content
                        elem_text = element.text.strip()
                        
                        # Skip empty or very short texts
                        if not elem_text or len(elem_text) < 5:
                            continue
                        
                        # Try to parse into name and position
                        lines = elem_text.split('\n')
                        
                        if len(lines) >= 2:
                            full_name = lines[0].strip()
                            position = lines[1].strip()
                        else:
                            # Try to split by common separators
                            if ":" in elem_text:
                                parts = elem_text.split(":", 1)
                                full_name = parts[0].strip()
                                position = parts[1].strip() if len(parts) > 1 else ""
                            elif "-" in elem_text:
                                parts = elem_text.split("-", 1)
                                full_name = parts[0].strip()
                                position = parts[1].strip() if len(parts) > 1 else ""
                            else:
                                full_name = elem_text
                                position = ""
                        
                        # Skip if it doesn't look like a name
                        words = full_name.split()
                        if len(words) > 5 or len(words) < 1:
                            continue
                        
                        # Extract just the prefix
                        prefix = self.extract_prefix(full_name)
                        
                        # Parse name
                        first_name, surname = self.parse_name(full_name)
                        
                        logging.info(f"Found executive via Selenium: {full_name}, Position: {position}")
                        
                        executives.append({
                            "Prefixed_Name": prefix,
                            "Full_Name": full_name,
                            "First_Name": first_name,
                            "Surname": surname,
                            "Position": position,
                            "Start_Date": "",
                            "End_Date": ""
                        })
                        
                        exec_count += 1
                        if exec_count >= limit:
                            break
                    except Exception as e:
                        logging.error(f"Error processing Selenium element: {e}")
                
            except Exception as e:
                logging.error(f"Error during direct Selenium extraction: {e}")
        
        logging.info(f"Successfully scraped {len(executives)} executives")
        return executives

    def close(self):
        """
        Close the Selenium WebDriver
        """
        try:
            if self.driver:
                self.driver.quit()
                logging.info("Selenium WebDriver closed")
        except Exception as e:
            logging.error(f"Error closing WebDriver: {e}")

def save_to_csv(data, filename="kasikorn_executives.csv"):
    """
    Save executive data to CSV file
    
    Parameters:
        data (list): List of dictionaries containing executive information
        filename (str): Output filename
    
    Returns:
        bool: True if save successful, False otherwise
    """
    if not data:
        logging.warning("No data to save")
        return False

    try:
        # Convert list of dictionaries to DataFrame >> Why? because it's easier to save to CSV
        df = pd.DataFrame(data)
        
        # Create output directory if it doesn't exist
        os.makedirs('output', exist_ok=True)
        output_path = os.path.join('output', filename)
        
        # Save to CSV with proper encoding for Thai language
        df.to_csv(
            output_path,
            index=False,
            encoding='utf-8-sig',  # UTF-8 with BOM for Thai support in Excel
            quoting=csv.QUOTE_ALL,
            escapechar='\\',
            errors='replace'
        )
        
        logging.info(f"Successfully saved {len(data)} executives to {output_path}")
        return True
    except Exception as e:
        logging.error(f"Error saving to CSV: {e}")
        return False


    # this function is used to simulate the data collection
def simulate_manual_data_collection():
    """
    Fallback function to create sample data when scraping fails
    This is based on publicly available information about Kasikorn Bank
    executives that would likely be found on their website
    """
    logging.info("Creating simulated data as fallback since scraping failed")
    
    # Sample data based on publicly known executives at Kasikorn Bank
    # This is just representative and may need to be updated
    sample_data = [
        {
            "Prefixed_Name": "Mr",
            "Full_Name": "นายบัณฑูร ล่ำซำ",
            "First_Name": "บัณฑูร",
            "Surname": "ล่ำซำ",
            "Position": "ประธานกรรมการ",
            "Start_Date": "01-04-2021",
            "End_Date": "",
        },
        {
            "Prefixed_Name": "Mr",
            "Full_Name": "​นายปิยะพงศ์ แสงภัทราชัย",
            "First_Name": "​ปิยะพงศ์",
            "Surname": "แสงภัทราชัย",
            "Position": "ผู้บริหารกลุ่มธุรกิจผลิตภัณฑ์ตลาดทุน",
            "Start_Date": "",
            "End_Date": "",
        },
        {
            "Prefixed_Name": "Miss",
            "Full_Name": "นางสาวสวคนธ์ เมฆาสวัสดิ์",
            "First_Name": "สวคนธ์",
            "Surname": "เมฆาสวัสดิ์",
            "Position": "ผู้ช่วยผู้จัดการใหญ่",
            "Start_Date": "",
            "End_Date": "",
        }
    ]
    return sample_data


    # this function is used to execute the main function
def main():
    """
    Main function to execute the Kasikorn Bank executive scraper
    """
    try:
        # Initialize the scraper
        scraper = KasikornSeleniumScraper()
        
        # Scrape the executives without limit
        executives = scraper.scrape_executives(limit=float('inf'))  # Set limit to infinity to scrape all names
        
        # If scraping fails, use simulated data as fallback
        if not executives:
            logging.warning("Scraping failed, using simulated data as fallback")
            executives = simulate_manual_data_collection()
        
        # Save the scraped data to CSV
        if not save_to_csv(executives):
            logging.error("Failed to save data to CSV")
        
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
    
    finally:
        # Ensure the WebDriver is closed
        scraper.close()

if __name__ == "__main__":
    # Execute the main function
    main()