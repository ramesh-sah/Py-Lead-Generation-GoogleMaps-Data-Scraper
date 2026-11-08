"""
Enterprise Lead Generation Suite v8.1.1
- Phone Number Extraction With Country Code - But Must Pass the country name in search Query
- Single email extraction per domain
- Mobile and WhatsApp number extraction
- Visit only contact-related pages (about, contact, reach us, etc.)
- Domain-specific crawling optimization
"""
import os  # Add this with other imports
import csv
import re
import sys
import json
import time
import asyncio
import phonenumbers
from datetime import datetime
from urllib.parse import urlparse, urlunparse, urljoin
from typing import Dict, List, Set, Optional, Tuple

from playwright.async_api import async_playwright, Page
from bs4 import BeautifulSoup
from py_lead_generation import GoogleMapsEngine

# System Configuration
OUTPUT_FILENAME = "property_brokers_valley.csv"  # User-defined filename
MAX_CONCURRENT_BROWSERS = 1
REQUEST_TIMEOUT = 180
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
DEFAULT_CONFIG_FILE = "search_configs.json"
VALID_ZOOM_RANGE = (10, 22)

LD_WHITELIST = {
    # Generic TLDs (gTLDs)
    'com', 'org', 'net', 'edu', 'gov', 'mil', 'co', 'io', 'ai', 'biz',
    'info', 'name', 'mobi', 'pro', 'travel', 'xxx', 'aero', 'coop', 'int',
    'jobs', 'museum', 'asia', 'tel', 'cat', 'post', 'xyz', 'app', 'blog',
    'cloud', 'dev', 'online', 'shop', 'site', 'store', 'tech', 'website',
    'icu', 'art', 'bar', 'bio', 'eco', 'law', 'med', 'now', 'tv', 'video',
    'wiki', 'zone', 'me', 'fm', 'am', 'gg', 'to', 'cc', 'ly', 'sh', 'ac',
    'live', 'studio', 'design', 'space', 'news', 'money', 'bank', 'cash',
    'club', 'social', 'email', 'events', 'games', 'group', 'network', 'services',
    'guru', 'expert', 'agency', 'company', 'global', 'world', 'city', 'tools',
    'center', 'digital', 'express', 'plus', 'team', 'community', 'foundation',
    'realtor', 'properties', 'marketing', 'media', 'press', 'reviews', 'directory',
    'systems', 'solutions', 'computer', 'software', 'host', 'security', 'data',
    'careers', 'recruiting', 'school', 'academy', 'university', 'college',
    'church', 'charity', 'ngo', 'green', 'organic', 'farm', 'health', 'clinic',
    'dental', 'pharmacy', 'hospital', 'vet', 'care', 'beauty', 'fitness', 'yoga',
    
    # Country-Code TLDs (ccTLDs)
    'uk', 'us', 'eu', 'ca', 'de', 'fr', 'it', 'es', 'nl', 'cn', 'jp', 'in',
    'ru', 'ch', 'se', 'br', 'au', 'nz', 'mx', 'ar', 'za', 'gr', 'kr', 'sg',
    'hk', 'ae', 'il', 'pl', 'at', 'be', 'dk', 'fi', 'ie', 'no', 'pt', 'ro',
    'sa', 'tr', 'tw', 'vn', 'cl', 'id', 'my', 'ph', 'th', 've', 'ng', 'eg',
    'ma', 'pk', 'bd', 'lk', 'ke', 'tz', 'gh', 'ug', 'zw', 'dz', 'tn', 'jo',
    'qa', 'kw', 'om', 'lb', 'cy', 'mt', 'is', 'lu', 'li', 'ad', 'mc', 'sm',
    'va', 'by', 'ua', 'kz', 'uz', 'az', 'ge', 'am', 'kg', 'tj', 'tm', 'md',
    'mk', 'al', 'ba', 'hr', 'me', 'rs', 'si', 'sk', 'cz', 'hu', 'bg', 'ee',
    'lv', 'lt', 'ie', 'im', 'je', 'gg', 'fo', 'gl', 'es', 'pt', 'it', 'cv',
    'mu', 'mv', 'mw', 'ne', 'np', 'pg', 'rw', 'sb', 'sc', 'sd', 'sl', 'sn',
    'so', 'sr', 'st', 'sy', 'td', 'tg', 'tl', 'to', 'tt', 'vu', 'ws', 'ye',
    'zm', 'bt', 'bn', 'kh', 'la', 'mn', 'mm', 'np', 'lk', 'mv', 'pk', 'tj',
    
    # Common Second-Level Domains
    'com.np', 'com.my', 'com.au', 'co.uk', 'co.jp', 'co.in', 'co.za', 'co.kr',
    'com.br', 'com.mx', 'com.es', 'com.pe', 'com.ve', 'com.co', 'com.ar', 'com.uy',
    'org.uk', 'net.au', 'edu.au', 'gov.uk', 'ac.uk', 'gov.au', 'edu.ph', 'gov.in',
    'org.au', 'net.nz', 'edu.sg', 'gov.sg', 'co.nz', 'co.th', 'co.id', 'co.il',
    'co.ke', 'co.tz', 'co.ug', 'co.zw', 'org.nz', 'org.ca', 'org.in', 'org.jp',
    'net.ph', 'net.th', 'edu.my', 'edu.pk', 'gov.za', 'gov.tr', 'gov.pl', 'gov.ro'
}
FORBIDDEN_PREFIXES = ('http://www.', 'https://www.', 'http://', 'https://')

class EnterpriseLeadGenerator(GoogleMapsEngine):
    """Enterprise lead processor with single-email extraction"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.leads = []
        self.country_code = self._detect_country()
        self.output_filename = kwargs.get('output_filename', OUTPUT_FILENAME)
        self.field_map = {
            'Title': ['title', 'Title'],
            'Address': ['address', 'Address'],
            'Phone': ['phone', 'PhoneNumber'],
            'Website': ['website', 'WebsiteURL']
        }
        self._lead_counter = self._get_max_lead_number()

    def _get_max_lead_number(self) -> int:
        """Find the highest lead number in the existing CSV"""
        csv_path = os.path.join('Leads_Generated', self.output_filename)
        last_number = 0
        if os.path.exists(csv_path):
            try:
                with open(csv_path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        lead_id = str(row.get('lead_id', '')).strip()
                        match = re.search(r'LEAD-(\d+)', lead_id)
                        if match:
                            number = int(match.group(1))
                            if number > last_number:
                                last_number = number
            except Exception:
                pass
        return last_number

    async def run(self) -> None:
        """Async execution workflow reusing py_lead_generation browser"""
        async with async_playwright() as playwright:
            self.playwright = playwright
            await self._setup_browser()
            await self._open_url_and_wait(self.url)
            urls = await self._get_search_results_urls()
            self._entries = await self._get_search_results_entries(urls)

            self.leads = []
            for lead in (self._entries or []):
                processed = await self._process_lead(lead)
                self.leads.append(processed)

            await self.browser.close()

    def _detect_country(self) -> str:
        """Country code detection"""
        location_parts = [part.strip().lower() for part in self.location.split(',')]
        country_mapping = {
            # initial custom entries (override defaults)
            'nepal': 'NP',
            'us': 'US',
            'usa': 'US',
            'u.s.': 'US',
            'u.s.a.': 'US',
            'united states': 'US',
            'united states of america': 'US',
            'uk': 'GB',
            'germany': 'DE',
            'india': 'IN',
            'france': 'FR',
            'spain': 'ES',

            # common alternative names and aliases
            'czechia': 'CZ',
            'drc': 'CD',
            'prc': 'CN',
            'republic of korea': 'KR',
            'north macedonia': 'MK',
            'eswatini': 'SZ',
            'cabo verde': 'CV',
            'burma': 'MM',
            'timor-leste': 'TL',
            'vatican city': 'VA',

            # full list of countries and territories
            'afghanistan': 'AF',
            'albania': 'AL',
            'algeria': 'DZ',
            'american samoa': 'AS',
            'andorra': 'AD',
            'angola': 'AO',
            'anguilla': 'AI',
            'antarctica': 'AQ',
            'antigua and barbuda': 'AG',
            'argentina': 'AR',
            'armenia': 'AM',
            'aruba': 'AW',
            'australia': 'AU',
            'austria': 'AT',
            'azerbaijan': 'AZ',
            'bahamas': 'BS',
            'bahrain': 'BH',
            'bangladesh': 'BD',
            'barbados': 'BB',
            'belarus': 'BY',
            'belgium': 'BE',
            'belize': 'BZ',
            'benin': 'BJ',
            'bermuda': 'BM',
            'bhutan': 'BT',
            'bolivia': 'BO',
            'bosnia and herzegovina': 'BA',
            'botswana': 'BW',
            'brazil': 'BR',
            'british indian ocean territory': 'IO',
            'british virgin islands': 'VG',
            'brunei': 'BN',
            'bulgaria': 'BG',
            'burkina faso': 'BF',
            'burundi': 'BI',
            'cambodia': 'KH',
            'cameroon': 'CM',
            'canada': 'CA',
            'cape verde': 'CV',
            'cayman islands': 'KY',
            'central african republic': 'CF',
            'chad': 'TD',
            'chile': 'CL',
            'china': 'CN',
            'christmas island': 'CX',
            'cocos islands': 'CC',
            'colombia': 'CO',
            'comoros': 'KM',
            'cook islands': 'CK',
            'costa rica': 'CR',
            'croatia': 'HR',
            'cuba': 'CU',
            'curacao': 'CW',
            'cyprus': 'CY',
            'czech republic': 'CZ',
            'democratic republic of the congo': 'CD',
            'denmark': 'DK',
            'djibouti': 'DJ',
            'dominica': 'DM',
            'dominican republic': 'DO',
            'east timor': 'TL',
            'ecuador': 'EC',
            'egypt': 'EG',
            'el salvador': 'SV',
            'equatorial guinea': 'GQ',
            'eritrea': 'ER',
            'estonia': 'EE',
            'ethiopia': 'ET',
            'falkland islands': 'FK',
            'faroe islands': 'FO',
            'fiji': 'FJ',
            'finland': 'FI',
            'french polynesia': 'PF',
            'gabon': 'GA',
            'gambia': 'GM',
            'georgia': 'GE',
            'ghana': 'GH',
            'gibraltar': 'GI',
            'greece': 'GR',
            'greenland': 'GL',
    'grenada': 'GD',
            'guam': 'GU',
            'guatemala': 'GT',
            'guernsey': 'GG',
            'guinea': 'GN',
            'guinea-bissau': 'GW',
            'guyana': 'GY',
            'haiti': 'HT',
            'honduras': 'HN',
            'hong kong': 'HK',
            'hungary': 'HU',
            'iceland': 'IS',
            'indonesia': 'ID',
            'iran': 'IR',
            'iraq': 'IQ',
            'ireland': 'IE',
            'isle of man': 'IM',
            'israel': 'IL',
            'italy': 'IT',
            'ivory coast': 'CI',
            'jamaica': 'JM',
            'japan': 'JP',
            'jersey': 'JE',
            'jordan': 'JO',
            'kazakhstan': 'KZ',
            'kenya': 'KE',
            'kiribati': 'KI',
            'kosovo': 'XK',
            'kuwait': 'KW',
            'kyrgyzstan': 'KG',
            'laos': 'LA',
            'latvia': 'LV',
            'lebanon': 'LB',
            'lesotho': 'LS',
            'liberia': 'LR',
            'libya': 'LY',
            'liechtenstein': 'LI',
            'lithuania': 'LT',
            'luxembourg': 'LU',
            'macau': 'MO',
            'macedonia': 'MK',
            'madagascar': 'MG',
            'malawi': 'MW',
            'malaysia': 'MY',
            'maldives': 'MV',
            'mali': 'ML',
    'malta': 'MT',
            'marshall islands': 'MH',
            'mauritania': 'MR',
            'mauritius': 'MU',
            'mayotte': 'YT',
            'mexico': 'MX',
            'micronesia': 'FM',
            'moldova': 'MD',
            'monaco': 'MC',
            'mongolia': 'MN',
            'montenegro': 'ME',
            'montserrat': 'MS',
            'morocco': 'MA',
            'mozambique': 'MZ',
            'myanmar': 'MM',
            'namibia': 'NA',
            'nauru': 'NR',
            'netherlands': 'NL',
            'netherlands antilles': 'AN',
            'new caledonia': 'NC',
            'new zealand': 'NZ',
            'nicaragua': 'NI',
            'niger': 'NE',
            'nigeria': 'NG',
            'niue': 'NU',
            'north korea': 'KP',
            'northern mariana islands': 'MP',
            'norway': 'NO',
            'oman': 'OM',
            'pakistan': 'PK',
            'palau': 'PW',
            'palestine': 'PS',
            'panama': 'PA',
            'papua new guinea': 'PG',
            'paraguay': 'PY',
            'peru': 'PE',
            'philippines': 'PH',
            'pitcairn': 'PN',
            'poland': 'PL',
            'portugal': 'PT',
            'puerto rico': 'PR',
            'qatar': 'QA',
            'republic of the congo': 'CG',
            'reunion': 'RE',
            'romania': 'RO',
            'russia': 'RU',
            'rwanda': 'RW',
            'saint barthelemy': 'BL',
            'saint helena': 'SH',
            'saint kitts and nevis': 'KN',
            'saint lucia': 'LC',
            'saint martin': 'MF',
            'saint pierre and miquelon': 'PM',
            'saint vincent and the grenadines': 'VC',
            'samoa': 'WS',
            'san marino': 'SM',
            'sao tome and principe': 'ST',
            'saudi arabia': 'SA',
            'senegal': 'SN',
            'serbia': 'RS',
            'seychelles': 'SC',
            'sierra leone': 'SL',
            'singapore': 'SG',
            'sint maarten': 'SX',
            'slovakia': 'SK',
            'slovenia': 'SI',
            'solomon islands': 'SB',
            'somalia': 'SO',
            'south africa': 'ZA',
            'south korea': 'KR',
            'south sudan': 'SS',
            'sri lanka': 'LK',
            'sudan': 'SD',
            'suriname': 'SR',
            'svalbard and jan mayen': 'SJ',
            'swaziland': 'SZ',
            'sweden': 'SE',
            'switzerland': 'CH',
            'syria': 'SY',
            'taiwan': 'TW',
            'tajikistan': 'TJ',
            'tanzania': 'TZ',
            'thailand': 'TH',
            'togo': 'TG',
            'tokelau': 'TK',
            'tonga': 'TO',
            'trinidad and tobago': 'TT',
            'tunisia': 'TN',
            'turkey': 'TR',
            'turkmenistan': 'TM',
            'turks and caicos islands': 'TC',
            'tuvalu': 'TV',
            'u.s. virgin islands': 'VI',
            'uganda': 'UG',
            'ukraine': 'UA',
            'united arab emirates': 'AE',
            'united kingdom': 'GB',
            'uruguay': 'UY',
    'uzbekistan': 'UZ',
            'vanuatu': 'VU',
            'vatican': 'VA',
            'venezuela': 'VE',
            'vietnam': 'VN',
            'wallis and futuna': 'WF',
            'western sahara': 'EH',
            'yemen': 'YE',
            'zambia': 'ZM',
            'zimbabwe': 'ZW',
}

        for part in reversed(location_parts):
            if part in country_mapping:
                return country_mapping[part]
        return ''

    def _process_phone_number(self, raw_phone: str) -> Tuple[str, str]:
        """Enhanced phone number validation with better length checking"""
        try:
            # Clean the input first
            clean_number = re.sub(r'[^\d+]', '', raw_phone)
            
            # Skip if too short after cleaning (minimum 7 digits excluding country code)
            if len(clean_number.replace('+', '')) < 7:
                return '', self.country_code
                
            # Try to parse with phonenumbers library
            parsed = phonenumbers.parse(raw_phone, self.country_code)
            
            # Validate the number
            if not phonenumbers.is_valid_number(parsed):
                return '', self.country_code
                
            # Format to E164
            formatted = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
            return formatted, str(parsed.country_code)
        except phonenumbers.NumberParseException:
            # Fallback processing
            clean_number = re.sub(r'[^\d+]', '', raw_phone)
            
            # Skip if too short
            if len(clean_number.replace('+', '')) < 7:
                return '', self.country_code
                
            # Add country code if missing
            if not clean_number.startswith('+') and self.country_code:
                clean_number = f'+{self.country_code}{clean_number}'
                
            return clean_number, self.country_code

    
    def _normalize_url(self, url: str) -> str:
        """URL normalization with social media filtering"""
        try:
            # Handle empty or None input
            if not url or url.strip() == '':
                return ''
                
            url = url.strip()
            
            # Add https:// if no protocol is present
            if not re.match(r'^https?://', url, re.IGNORECASE):
                url = f'https://{url}'
            
            # Parse the URL
            parsed = urlparse(url)
            
            # Extract domain and normalize
            netloc = parsed.netloc.split(':', 1)[0].lower().strip()
            if '@' in netloc or not netloc:
                return ''
                
            # Remove 'www.' prefix if present
            if netloc.startswith('www.'):
                netloc = netloc[4:]
                
            # List of social media domains to filter out
            social_media_domains = {
                'facebook.com', 'instagram.com', 'twitter.com', 'x.com', 'linkedin.com', 
                'youtube.com', 'tiktok.com', 'pinterest.com', 'snapchat.com', 'reddit.com',
                'whatsapp.com', 'telegram.org', 'discord.com', 'tumblr.com', 'flickr.com',
                'vimeo.com', 'dribbble.com', 'behance.net', 'medium.com', 'quora.com'
            }
            
            # Check if the domain belongs to or is a subdomain of a social media domain
            for social_domain in social_media_domains:
                if netloc == social_domain or netloc.endswith(f".{social_domain}"):
                    return ''  # Filtered out
            
            # Normalize the URL
            normalized_url = urlunparse((
                'https',
                netloc,
                parsed.path.rstrip('/'),
                '',
                '',
                ''
            ))
            
            return normalized_url

        except Exception:
            return ''
        


    async def _scroll_page(self, page: Page) -> None:
        """Dynamic content loader"""
        try:
            last_height = await page.evaluate("document.body.scrollHeight")
            for _ in range(2):  # Limited scroll attempts
                await asyncio.sleep(2)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                await asyncio.sleep(2)
                new_height = await page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
        except Exception:
            pass

    async def _extract_emails(self, page: Page, base_url: str) -> dict:
        """Extract data from contact-related pages only with 60s crawl timeout."""
        start_time = time.time()
        CRAWL_TIMEOUT = 60

        parsed_base = urlparse(base_url)
        base_domain = parsed_base.netloc
        visited = set()
        email_found = None
        mobile_numbers = []
        whatsapp_numbers = []

        # ... (rest of function unchanged, just need to add timeout check in the contact_urls loop)

        # Patterns to identify contact-related pages
        contact_pattern = re.compile(
            r'(about|contact|reach|connect|connect[-_]?us|'
            r'contact[-_]?us|about[-_]?us|get[-_]?in[-_]?touch|'
            r'support|help|team|staff|location|find[-_]?us|visit)',
            re.IGNORECASE
        )

        # Patterns to exclude from crawling
        exclude_patterns = [
            re.compile(r'\.(pdf|jpg|jpeg|png|gif|svg|ico|css|js)$', re.IGNORECASE),
            re.compile(r'#', re.IGNORECASE),
            re.compile(r'mailto:', re.IGNORECASE),
            re.compile(r'tel:', re.IGNORECASE),
            re.compile(r'javascript:', re.IGNORECASE)
        ]

        # First, visit the main page to extract contact-related links
        try:
            await page.goto(base_url, timeout=REQUEST_TIMEOUT * 1000, wait_until='domcontentloaded')
            await self._scroll_page(page)

            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            page_text = soup.get_text(separator=' ')

            # Extract email from main page
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.lower().startswith('mailto:'):
                    email = href.split('mailto:')[1].split('?')[0].strip().lower()
                    if self._is_valid_email(email):
                        email_found = email

            # If email not found in mailto links, check page text
            if not email_found:
                potential_emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', page_text)
                for email in potential_emails:
                    if self._is_valid_email(email):
                        email_found = email.lower()
                        break

            # Extract mobile and WhatsApp from main page
            if not mobile_numbers:
                mobile = self._extract_mobile_from_tel_links(soup)
                if mobile:
                    mobile_numbers.append(mobile)
                else:
                    mobile = self._extract_mobile_from_text(page_text)
                    if mobile:
                        mobile_numbers.append(mobile)

            if not whatsapp_numbers:
                whatsapp = self._extract_whatsapp_from_page(soup, page_text)
                if whatsapp:
                    whatsapp_numbers.append(whatsapp)

            # Discover all contact-related links on the main page
            contact_urls = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if any(pattern.search(href) for pattern in exclude_patterns):
                    continue

                full_url = urljoin(base_url, href)
                parsed = urlparse(full_url)
                if parsed.netloc == base_domain:
                    link_text = link.get_text().strip().lower()
                    path_match = contact_pattern.search(parsed.path)
                    query_match = contact_pattern.search(parsed.query)
                    text_match = contact_pattern.search(link_text)

                    clean_url = urlunparse(parsed._replace(
                        query='',
                        fragment=''
                    )).rstrip('/')

                    if clean_url not in visited and (path_match or query_match or text_match):
                        contact_urls.append(clean_url)

            # Visit each contact-related page
            for url in contact_urls:
                if time.time() - start_time > CRAWL_TIMEOUT:
                    print(f"⌛ Crawl timeout reached for {base_url}")
                    break
                if url in visited:
                    continue
                visited.add(url)

                try:
                    await page.goto(url, timeout=REQUEST_TIMEOUT * 1000, wait_until='domcontentloaded')
                    await self._scroll_page(page)

                    sub_html = await page.content()
                    sub_soup = BeautifulSoup(sub_html, 'html.parser')
                    sub_page_text = sub_soup.get_text(separator=' ')

                    # Extract email from this page
                    if not email_found:
                        for link in sub_soup.find_all('a', href=True):
                            href = link['href']
                            if href.lower().startswith('mailto:'):
                                email = href.split('mailto:')[1].split('?')[0].strip().lower()
                                if self._is_valid_email(email):
                                    email_found = email

                    if not email_found:
                        potential_emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', sub_page_text)
                        for email in potential_emails:
                            if self._is_valid_email(email):
                                email_found = email.lower()
                                break

                    # Extract mobile from this page
                    if not mobile_numbers:
                        mobile = self._extract_mobile_from_tel_links(sub_soup)
                        if mobile:
                            mobile_numbers.append(mobile)
                        else:
                            mobile = self._extract_mobile_from_text(sub_page_text)
                            if mobile:
                                mobile_numbers.append(mobile)

                    # Extract WhatsApp from this page
                    if not whatsapp_numbers:
                        whatsapp = self._extract_whatsapp_from_page(sub_soup, sub_page_text)
                        if whatsapp:
                            whatsapp_numbers.append(whatsapp)

                    # If we've found all data, we can stop early
                    if email_found and mobile_numbers and whatsapp_numbers:
                        break

                except Exception as e:
                    print(f"Error processing {url}: {str(e)[:80]}")
                    continue

        except Exception as e:
            print(f"Error processing {base_url}: {str(e)[:80]}")

        # Return whatever we found
        return {
            'emails': ({email_found} if email_found else set()),
            'mobile': mobile_numbers[0] if mobile_numbers else '',
            'whatsapp': whatsapp_numbers[0] if whatsapp_numbers else ''
        }

    def _extract_mobile_from_tel_links(self, soup: BeautifulSoup) -> str:
        """Extract mobile numbers from tel: links with enhanced logic"""
        fallback_mobile = ''
        mobile_numbers = []

        try:
            tel_links = soup.find_all('a', href=re.compile(r'^tel:', re.IGNORECASE))

            for tlink in tel_links:
                thref = tlink.get('href', '')
                if not thref:
                    continue

                # Extract the raw number from tel: link
                raw_num = thref.split('tel:')[1].split('?')[0]
                formatted, _ = self._process_phone_number(raw_num)

                if not formatted:
                    continue

                # Try to determine number type; prefer mobile/fixed_line_or_mobile
                try:
                    parsed_num = phonenumbers.parse(formatted, None)
                    ntype = phonenumbers.number_type(parsed_num)

                    # If it's a mobile number, add to mobile numbers list
                    if ntype in (phonenumbers.NumberType.MOBILE, phonenumbers.NumberType.FIXED_LINE_OR_MOBILE):
                        mobile_numbers.append(formatted)
                    # Otherwise, keep as fallback
                    elif not fallback_mobile:
                        fallback_mobile = formatted
                except Exception:
                    # If we can't determine the type, use it as fallback
                    if not fallback_mobile:
                        fallback_mobile = formatted

            # Return the first mobile number if found, otherwise fallback
            return mobile_numbers[0] if mobile_numbers else fallback_mobile
        except Exception:
            pass

        return ''

    def _extract_mobile_from_text(self, page_text: str) -> str:
        """Extract mobile numbers from page text with enhanced regex patterns"""
        fallback_mobile = ''
        mobile_numbers = []
        
        # First try with phonenumbers library
        try:
            for match in phonenumbers.PhoneNumberMatcher(page_text, self.country_code or None):
                num_obj = match.number
                formatted = phonenumbers.format_number(num_obj, phonenumbers.PhoneNumberFormat.E164)
                num_type = phonenumbers.number_type(num_obj)
                
                # If it's a mobile number, add to mobile numbers list
                if num_type in (phonenumbers.NumberType.MOBILE, phonenumbers.NumberType.FIXED_LINE_OR_MOBILE):
                    mobile_numbers.append(formatted)
                # Otherwise, keep as fallback
                elif not fallback_mobile:
                    fallback_mobile = formatted
        except Exception:
            pass
            
        # If no mobile was found with phonenumbers, try regex patterns
        if not mobile_numbers:
            # Enhanced phone number patterns
            phone_patterns = [
                # International format with country code
                r'\+?\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}',
                # Local format with area code
                r'\(?\d{2,5}\)?[-.\s]?\d{2,5}[-.\s]?\d{3,8}',
                # Simple digits pattern
                r'\b\d{7,15}\b'
            ]
            
            for pattern in phone_patterns:
                matches = re.findall(pattern, page_text)
                for match in matches:
                    formatted, _ = self._process_phone_number(match)
                    if formatted and len(formatted.replace('+', '').replace('-', '').replace(' ', '')) >= 7:
                        # Try to determine if it's a mobile number
                        try:
                            parsed_num = phonenumbers.parse(formatted, None)
                            num_type = phonenumbers.number_type(parsed_num)
                            
                            if num_type in (phonenumbers.NumberType.MOBILE, phonenumbers.NumberType.FIXED_LINE_OR_MOBILE):
                                mobile_numbers.append(formatted)
                            elif not fallback_mobile:
                                fallback_mobile = formatted
                        except Exception:
                            if not fallback_mobile:
                                fallback_mobile = formatted
                            
        # Return the first mobile number if found, otherwise fallback
        return mobile_numbers[0] if mobile_numbers else fallback_mobile

    def _extract_whatsapp_from_page(self, soup: BeautifulSoup, page_text: str) -> str:
        """Extract WhatsApp numbers with enhanced detection"""
        whatsapp_numbers = []

        try:
            # Find all links on the page
            links = soup.find_all('a', href=True)

            for link in links:
                href = link['href']
                if not href:
                    continue

                lower = href.lower()
                
                # Check for various WhatsApp link patterns
                if any(pattern in lower for pattern in ['wa.me/', 'api.whatsapp.com', 'whatsapp.com/send', 'whatsapp']):
                    raw_wp = ''
                    
                    # Extract phone number from different WhatsApp URL formats
                    try:
                        if 'wa.me/' in lower:
                            # Format: https://wa.me/1234567890
                            raw_wp = href.split('wa.me/')[1].split('?')[0].split('/')[0]
                        elif 'api.whatsapp.com' in lower and 'phone=' in lower:
                            # Format: https://api.whatsapp.com/send?phone=1234567890
                            m = re.search(r'phone=(\+?\d+)', lower)
                            raw_wp = m.group(1) if m else ''
                        elif 'whatsapp.com/send' in lower and 'phone=' in lower:
                            # Format: https://web.whatsapp.com/send?phone=1234567890
                            m = re.search(r'phone=(\+?\d+)', lower)
                            raw_wp = m.group(1) if m else ''
                        else:
                            # Fallback: find digits in href
                            digits = re.findall(r'\+?\d[\d\s\-\(\)]{5,}\d', href)
                            raw_wp = digits[0] if digits else ''
                    except Exception:
                        raw_wp = ''
                        
                    if raw_wp:
                        formatted_wp, _ = self._process_phone_number(raw_wp)
                        if formatted_wp:
                            whatsapp_numbers.append(formatted_wp)
                            
            # If no WhatsApp link found, check for phone numbers in text with WhatsApp context
            if not whatsapp_numbers:
                # Enhanced WhatsApp context patterns
                whatsapp_context_patterns = [
                    r'whatsapp[:\s]*([+0-9\-\s\(\)]{7,20})',
                    r'chat on whatsapp[:\s]*([+0-9\-\s\(\)]{7,20})',
                    r'contact on whatsapp[:\s]*([+0-9\-\s\(\)]{7,20})',
                    r'([+0-9\-\s\(\)]{7,20})\s*whatsapp',
                    r'([+0-9\-\s\(\)]{7,20})\s*(for|on)\s*whatsapp',
                    r'whatsapp\s*([+0-9\-\s\(\)]{7,20})',
                    r'([+0-9\-\s\(\)]{7,20})\s*(via|through)\s*whatsapp'
                ]
                
                for pattern in whatsapp_context_patterns:
                    matches = re.findall(pattern, page_text.lower())
                    for match in matches:
                        formatted_wp, _ = self._process_phone_number(match)
                        if formatted_wp:
                            whatsapp_numbers.append(formatted_wp)
                            
        except Exception:
            pass
            
        # Return the first WhatsApp number if found
        return whatsapp_numbers[0] if whatsapp_numbers else ''

    def _is_valid_email(self, email: str) -> bool:
        """Email validation"""
        email = email.lower()
        if any(email.startswith(prefix) for prefix in FORBIDDEN_PREFIXES):
            return False
        # Check TLD (handle com.np etc by checking last 2 parts)
        parts = email.split('.')
        if len(parts) < 2: return False
        tld = '.'.join(parts[-2:])
        # Check against whitelist (also support simple tlds)
        return tld in LD_WHITELIST or parts[-1] in LD_WHITELIST

   
    def parse_address(self,s):
        s = s.strip()
        if not s:
            return {'street': '', 'city': '', 'postal': None}

        parts = [x.strip() for x in s.split(',') if x.strip()]
        if not parts:
            return {'street': '', 'city': '', 'postal': None}

        last = parts[-1]

        # Find rightmost digit
        i = len(last) - 1
        while i >= 0 and not last[i].isdigit():
            i -= 1

        if i <= 0:
            # No postal code
            if len(parts) == 1:
                return {'street': last, 'city': '', 'postal': None}
            return {
                'street': ', '.join(parts[:-1]),
                'city': last,
                'postal': None
            }

        j = i
        while j >= 0 and last[j].isdigit():
            j -= 1

        postal = last[j+1:i+1]
        city = last[:j+1].rstrip('-,; ').strip()
        street = ', '.join(parts[:-1]) if len(parts) > 1 else ''

        return {
            'street': street,
            'city': city,
            'postal': postal
        }
        
    def _is_valid_lead(self, lead: Dict) -> bool:
        """
        Change validation rules here only
        """
        phone = str(lead.get('Phone', '')).strip().lower()
        

        # ❌ Reject if phone is empty or null
        if not phone or phone == 'null':
            return False
       

        return True

    def _generate_lead_id(self) -> str:
        """Generate sequential unique lead ID from in-memory counter"""
        self._lead_counter += 1
        return f"LEAD-{self._lead_counter:04d}"
    
    async def _process_lead(self, lead: Dict) -> Dict:
        """Lead processing pipeline"""
        standardized = {
            key: next((lead[field] for field in fields if field in lead), '')
            for key, fields in self.field_map.items()
        }

        # 🔹 Parse address
        raw_address = standardized.get('Address')
        address_parsed = self.parse_address(raw_address)

        phone_number, country_code = self._process_phone_number(standardized['Phone'])
        raw_url = standardized['Website']
        url = self._normalize_url(raw_url)

        emails = set()
        mobile = ''
        whatsapp = ''

        if url:
            try:
                page = await self.browser.new_page(**self.PAGE_PARAMS)
                result = await self._extract_emails(page, url)
                await page.close()

                if isinstance(result, dict):
                    emails = result.get('emails', set()) or set()
                    mobile = result.get('mobile', '') or ''
                    whatsapp = result.get('whatsapp', '') or ''
                else:
                    emails = set(result) if result else set()
            except Exception as e:
                print(f"Browser error: {str(e)[:80]}")

        return {
            'lead_id': self._generate_lead_id(),   # 🔥 ADD THIS
            **standardized,

            # 🔹 Structured address fields
            'street': address_parsed['street'],
            'city': address_parsed['city'],
            'postal_code': address_parsed['postal'] or 'null',

            'Phone': phone_number if phone_number else 'null',
            'Country': self.country_code,
            'Website': url if url else 'null',
            'Email': next(iter(emails), 'null'),
            'mobile_number': mobile if mobile else 'null',
            'whatsapp_number': whatsapp if whatsapp else 'null',

            'email_contacted': 'false',
            'phone_mobile_contacted': 'false',
            'whatsapp_contacted': 'false',
            'potentials_already_have': "false",
            
            'meeting_fixed': "false",
            'meeting_date': "",
            'meeting_time': "",
            'meeting_mode': "",            # online/offline
            'meeting_status': "",          # scheduled/completed/cancelled
            
            'quotation_send': "false",
            'quotation_date': "",
            'after_quotation_follow_up': "false",
            'after_quotation_follow_up_date': "",
            
            
            'leads_converted': "false",
            'conversion_date': "",
            
            
            'next_follow_up_date': "",
            'comment': ""
        }
    
    def export_csv(self, filename: str) -> None:
        """Generate consolidated CSV with deduplication (keyed by Title, Phone, Website)"""
        if not self.leads:
            print("⛔ No results - verify search parameters")
            return

        try:
            os.makedirs('Leads_Generated', exist_ok=True)
            full_path = os.path.join('Leads_Generated', filename)
            fieldnames = [
                                'lead_id',
                                'Title',
                                'Address',
                                'street',
                                'city',
                                'postal_code',
                                'Phone',
                                'Country',
                                'Website',
                                'Email',
                                'mobile_number',
                                'whatsapp_number',
                                'email_contacted',
                                'phone_mobile_contacted',
                                'whatsapp_contacted',

                                'potentials_already_have',
                                'meeting_fixed',
                                'meeting_date',
                                'meeting_time',
                                'meeting_mode',
                                'meeting_status',


                                'quotation_send',
                                'quotation_date',
                                'after_quotation_follow_up',
                                'after_quotation_follow_up_date',

                                'leads_converted',
                                'conversion_date',
                                'next_follow_up_date',
                                'comment'
                            ]


            # Read existing data for deduplication based on Title, Phone, Website
            existing_entries = set()
            if os.path.exists(full_path):
                with open(full_path, 'r', newline='', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Dedup tuple: Title, Phone, Website (normalized/stripped)
                        key = (
                            str(row.get('Title', '')).strip().lower(),
                            str(row.get('Phone', '')).strip().lower(),
                            str(row.get('Website', '')).strip().lower()
                        )
                        existing_entries.add(key)

            new_leads = []
            for lead in self.leads:
                if not self._is_valid_lead(lead):
                    continue

                key = (
                    str(lead.get('Title', '')).strip().lower(),
                    str(lead.get('Phone', '')).strip().lower(),
                    str(lead.get('Website', '')).strip().lower()
                )

                if key not in existing_entries:
                    new_leads.append(lead)
                    existing_entries.add(key)

            if not new_leads:
                print(f"✅ No new leads to add to {filename}")
                return


            # Write to file
            write_header = not os.path.exists(full_path)
            with open(full_path, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if write_header:
                    writer.writeheader()
                writer.writerows(new_leads)

            print(f"✅ Added {len(new_leads)} new leads to {full_path}")
        except Exception as e:
            print(f"⛔ Export failed: {str(e)}")




def sanitize_filename(text: str) -> str:
    """Filename sanitization"""
    return re.sub(r'[\\/*?:"<>|]', "", text.replace(",", "_")).strip()[:100]

async def execute_search(query: str, location: str, zoom: int, output_filename: str = None) -> None:
    """Async search orchestration"""
    print(f"\n🚀 Enterprise Lead Generator v8.1.1 - {query}")
    print("★★★★★★★★★★★★★★★★★★★★★★★★★★★★")

    try:
        os.makedirs('Leads_Generated', exist_ok=True)
        # Use provided output filename or fallback
        filename = output_filename if output_filename else f"{sanitize_filename(query)}_{sanitize_filename(location)}.csv"

        engine = EnterpriseLeadGenerator(
            query=query,
            location=location,
            zoom=max(min(zoom, VALID_ZOOM_RANGE[1]), VALID_ZOOM_RANGE[0]),
            output_filename=filename
        )

        print(f"\n🔍 Initiating intelligence gathering... Output: {filename}")
        await engine.run()

        if engine.leads:
            engine.export_csv(filename)

    except KeyboardInterrupt:
        print("\n🛑 Operation terminated by user")
    except Exception as e:
        print(f"\n⛔ System failure: {str(e)}")

def load_configurations(file_path: str) -> List[Dict]:
    """Config loader supporting new config structure"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Handle new format (dict with 'searches') or old format (list)
        if isinstance(data, dict) and 'searches' in data:
            configs = data['searches']
            output_filename = data.get('output_filename')
        else:
            configs = data
            output_filename = None

        valid_configs = []
        for config in configs:
            try:
                valid_configs.append({
                    'query': config['query'].strip(),
                    'location': config['location'].strip(),
                    'zoom': max(min(int(config.get('zoom', 15)), VALID_ZOOM_RANGE[1]), VALID_ZOOM_RANGE[0]),
                    'output_filename': output_filename
                })
            except Exception:
                continue

        return valid_configs
    except Exception as e:
        print(f"Config error: {str(e)}")
        raise

async def main():
    """Main executor"""
    try:
        config_file = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CONFIG_FILE
        configs = load_configurations(config_file)
        
        if not configs:
            print("No valid configs")
            return
            
        for config in configs:
            print(f"\nProcessing: {config['query']}")
            await execute_search(**config)
            
    except Exception as e:
        print(f"Critical error: {str(e)}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        sys.exit(1)
