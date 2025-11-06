from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from urllib.parse import urljoin, urlparse
import re
import time

# Configure automatic driver management
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
base_url = 'https://www.rameshkumarsah.com.np/'
visited_urls = set()
all_emails = set()

# Email validation configuration
LD_WHITELIST = {
    'com', 'org', 'net', 'edu', 'gov', 'mil', 'co', 'io', 'ai', 'biz',
    'info', 'name', 'mobi', 'pro', 'travel', 'xxx', 'aero', 'coop', 'int',
    'jobs', 'museum', 'asia', 'tel', 'cat', 'post', 'uk', 'us', 'eu', 'ca',
    'de', 'fr', 'it', 'es', 'nl', 'cn', 'jp', 'in', 'ru', 'ch', 'se', 'br',
    'au', 'nz', 'mx', 'ar', 'za', 'gr', 'kr', 'sg', 'hk', 'ae', 'il', 'pl',
    'at', 'be', 'dk', 'fi', 'ie', 'no', 'pt', 'ro', 'sa', 'tr', 'tw', 'vn',
    'cl', 'id', 'my', 'ph', 'th', 've', 'xyz', 'app', 'blog', 'cloud', 'dev',
    'online', 'shop', 'site', 'store', 'tech', 'website', 'icu', 'art', 'bar',
    'bio', 'eco', 'law', 'med', 'now', 'tv', 'video', 'wiki', 'zone'
}
FORBIDDEN_PREFIXES = ('http://www.', 'https://www.', 'http://', 'https://')

def is_valid_url(url):
    """Check if URL belongs to target domain and is not already visited"""
    parsed = urlparse(url)
    return parsed.netloc == 'gojilabs.com' and url not in visited_urls

def extract_valid_emails(text):
    """Extract emails with TLD validation and protocol filtering"""
    tld_pattern = '|'.join(map(re.escape, LD_WHITELIST))
    email_regex = rf'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.(?:{tld_pattern})\b'
    potential_emails = re.findall(email_regex, text, flags=re.IGNORECASE)
    
    valid_emails = []
    for email in potential_emails:
        email_lower = email.lower()
        if not any(email_lower.startswith(prefix) for prefix in FORBIDDEN_PREFIXES):
            valid_emails.append(email_lower)
    
    return valid_emails

def process_page(url):
    """Process a single page and extract emails"""
    try:
        driver.get(url)
        time.sleep(2)  # Allow page to load
        visited_urls.add(url)
        
        # Extract emails from page content
        page_text = driver.find_element(By.TAG_NAME, 'body').text
        emails = extract_valid_emails(page_text)
        
        if emails:
            print(f"Found {len(emails)} email(s) on {url}")
            all_emails.update(emails)
            
    except Exception as e:
        print(f"Error processing {url}: {str(e)}")

try:
    # Start with base URL
    driver.get(base_url)
    time.sleep(3)
    
    # Collect all initial links
    links = driver.find_elements(By.TAG_NAME, 'a')
    href_list = [link.get_attribute('href') for link in links if link.get_attribute('href')]
    
    # Process mailto links first
    for href in href_list:
        if href.startswith('mailto:'):
            email = href[7:].split('?')[0].strip().lower()
            if not any(email.startswith(prefix) for prefix in FORBIDDEN_PREFIXES):
                all_emails.add(email)
    
    # Normalize and filter URLs for page crawling
    unique_urls = set()
    for href in href_list:
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        clean_url = parsed.scheme + "://" + parsed.netloc + parsed.path
        clean_url = clean_url.rstrip('/')
        if is_valid_url(clean_url):
            unique_urls.add(clean_url)
    
    # Process all unique URLs
    print(f"Found {len(unique_urls)} unique pages to scan")
    for url in unique_urls:
        process_page(url)
    
    print("\nFinal email results:")
    for email in sorted(all_emails):
        print(email)

finally:
    driver.quit()