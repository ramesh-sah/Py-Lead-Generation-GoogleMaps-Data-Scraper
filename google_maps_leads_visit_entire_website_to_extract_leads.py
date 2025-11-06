"""
Enterprise Lead Generation Suite v8.1.1
- Phone Number Extraction With Country Code - But Must Pass the country name in search Query
- Single email extraction per domain
- Mobile and WhatsApp number extraction
- Visit all pages until all data is found
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
from urllib.parse import urlparse, urlunparse
from typing import Dict, List, Set, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from py_lead_generation import GoogleMapsEngine

# System Configuration
OUTPUT_FILENAME = "rename_this_file_after_completed.csv"  # User-defined filename
MAX_CONCURRENT_BROWSERS = 2
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
        self.field_map = {
            'Title': ['title', 'Title'],
            'Address': ['address', 'Address'],
            'Phone': ['phone', 'PhoneNumber'],
            'Website': ['website', 'WebsiteURL']
        }

    async def run(self) -> None:
        """Async execution workflow"""
        await super().run()
        
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_BROWSERS) as executor:
            futures = [
                loop.run_in_executor(executor, self._process_lead, lead)
                for lead in self.entries
            ]
            self.leads = await asyncio.gather(*futures)

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

    def _scroll_page(self, driver: webdriver.Chrome) -> None:
        """Dynamic content loader"""
        last_height = driver.execute_script("return document.body.scrollHeight")
        for _ in range(2):  # Limited scroll attempts
            time.sleep(3)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
           
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def _extract_emails(self, driver: webdriver.Chrome, base_url: str) -> dict:
        """Comprehensive data extraction visiting all pages until all data is found.

        Returns a dict with keys: 'emails' (set), 'mobile' (str), 'whatsapp' (str).
        """
        parsed_base = urlparse(base_url)
        base_domain = parsed_base.netloc
        visited = set()
        queue = [base_url]
        email_found = None
        mobile_numbers = []
        whatsapp_numbers = []
        
        # Patterns to identify important pages
        # contact_pattern = re.compile(
        #     r'(about|contact|reach|connect|connect[-_]?us|'
        #     r'contact[-_]?us|about[-_]?us|get[-_]?in[-_]?touch)',
        #     re.IGNORECASE
        # )

        # Patterns to identify contact-related pages (Expanded)
        contact_pattern = re.compile(
            r'(about|contact|reach|connect|connect[-_]?us|'
            r'contact[-_]?us|about[-_]?us|get[-_]?in[-_]?touch|'
            r'support|help|team|staff|location|find[-_]?us|visit|'
            
            # --- Direct Contact & Communication ---
            r'call|email|phone|message|enquir|inquir|feedback|ask|talk|speak|office|branch|'
            
            # --- Company & Team Information ---
            r'who[-_]?we[-_]?are|our[-_]?story|profile|information|overview|company|'
            
            # --- Location & Directions ---
            r'directions|map|address|stores?|offices?|branches?|'
            
            # --- Support & Service ---
            r'faq|customer[-_]?service|service|assist|ticket|'
            
            # --- General Information ---
            r'info|details)',
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

        # Continue processing all pages until all data is found or no more pages to visit
        while queue and (not email_found or not mobile_numbers or not whatsapp_numbers):
            current_url = queue.pop(0)
            if current_url in visited:
                continue
            visited.add(current_url)

            try:
                driver.get(current_url)
                self._scroll_page(driver)

                # Immediate mailto check
                if not email_found:
                    mailto_links = driver.find_elements(By.XPATH, '//a[starts-with(@href, "mailto:")]')
                    for link in mailto_links:
                        href = link.get_attribute('href')
                        if href:
                            email = href.split('mailto:')[1].split('?')[0].strip().lower()
                            if self._is_valid_email(email):
                                email_found = email

                # Page text email extraction
                if not email_found:
                    page_text = driver.find_element(By.TAG_NAME, 'body').text
                    potential_emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', page_text)
                    for email in potential_emails:
                        if self._is_valid_email(email):
                            email_found = email.lower()
                            break
                else:
                    # Get page text for phone extraction even if email is found
                    page_text = driver.find_element(By.TAG_NAME, 'body').text

                # Enhanced phone extraction from tel: links
                if not mobile_numbers:
                    mobile = self._extract_mobile_from_tel_links(driver)
                    if mobile:
                        mobile_numbers.append(mobile)
                    
                    # If still not found, try to extract from page text
                    if not mobile_numbers:
                        mobile = self._extract_mobile_from_text(page_text)
                        if mobile:
                            mobile_numbers.append(mobile)

                # Enhanced WhatsApp links detection
                if not whatsapp_numbers:
                    whatsapp = self._extract_whatsapp_from_page(driver, page_text)
                    if whatsapp:
                        whatsapp_numbers.append(whatsapp)
                
                # If we've found all data, we can stop early
                if email_found and mobile_numbers and whatsapp_numbers:
                    return {
                        'emails': {email_found}, 
                        'mobile': mobile_numbers[0], 
                        'whatsapp': whatsapp_numbers[0]
                    }

                # Discover all internal links on the page
                links = driver.find_elements(By.TAG_NAME, 'a')
                for link in links:
                    href = link.get_attribute('href')
                    if href:
                        # Skip if it matches any exclude pattern
                        if any(pattern.search(href) for pattern in exclude_patterns):
                            continue
                            
                        parsed = urlparse(href)
                        if parsed.netloc == base_domain:
                            # Prioritize contact/about pages
                            path_match = contact_pattern.search(parsed.path)
                            query_match = contact_pattern.search(parsed.query)
                            
                            clean_url = urlunparse(parsed._replace(
                                query='', 
                                fragment=''
                            )).rstrip('/')
                            
                            if clean_url not in visited:
                                # Prioritize contact/about pages by adding them to the front of the queue
                                if path_match or query_match:
                                    queue.insert(0, clean_url)
                                else:
                                    queue.append(clean_url)

            except Exception as e:
                print(f"Error processing {current_url}: {str(e)[:80]}")
                continue

        # Return whatever we found. If an email was captured earlier, include it.
        return {
            'emails': ({email_found} if email_found else set()), 
            'mobile': mobile_numbers[0] if mobile_numbers else '', 
            'whatsapp': whatsapp_numbers[0] if whatsapp_numbers else ''
        }

    def _extract_mobile_from_tel_links(self, driver: webdriver.Chrome) -> str:
        """Extract mobile numbers from tel: links with enhanced logic"""
        fallback_mobile = ''
        mobile_numbers = []
        
        try:
            # Find all tel: links
            tel_links = driver.find_elements(By.XPATH, '//a[starts-with(@href, "tel:")]')
            
            for tlink in tel_links:
                thref = tlink.get_attribute('href')
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

    def _extract_whatsapp_from_page(self, driver: webdriver.Chrome, page_text: str) -> str:
        """Extract WhatsApp numbers with enhanced detection"""
        whatsapp_numbers = []
        
        try:
            # Find all links on the page
            links = driver.find_elements(By.TAG_NAME, 'a')
            
            for link in links:
                href = link.get_attribute('href')
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
        tld = email.split('.')[-1]
        return tld in LD_WHITELIST

    def _process_lead(self, lead: Dict) -> Dict:
        """Lead processing pipeline"""
        standardized = {
            key: next((lead[field] for field in fields if field in lead), '')
            for key, fields in self.field_map.items()
        }
        
        phone_number, country_code = self._process_phone_number(standardized['Phone'])
        raw_url = standardized['Website']
        url = self._normalize_url(raw_url)


        emails = set()
        mobile = ''
        whatsapp = ''
        if url:
            try:
                driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
                driver.set_page_load_timeout(REQUEST_TIMEOUT)
                result = self._extract_emails(driver, url)
                driver.quit()

                # _extract_emails now returns dict-like info
                if isinstance(result, dict):
                    emails = result.get('emails', set()) or set()
                    mobile = result.get('mobile', '') or ''
                    whatsapp = result.get('whatsapp', '') or ''
                else:
                    # backward-compatible fallback
                    emails = set(result) if result else set()
            except Exception as e:
                print(f"Browser error: {str(e)[:80]}")

        return {
            **standardized,
            'Phone': phone_number,
            'Country': self.country_code,
            'Website': url if url else '',
            'Email': next(iter(emails), 'null'),
            'mobile_number': mobile,
            'whatsapp_number': whatsapp
        }

    def export_csv(self, filename: str) -> None:
        """Generate consolidated CSV with deduplication"""
        if not self.leads:
            print("⛔ No results - verify search parameters")
            return

        try:
            os.makedirs('Leads_Generated', exist_ok=True)
            full_path = os.path.join('Leads_Generated', filename)
            fieldnames = ['Title', 'Address', 'Phone', 'Country', 'Website', 'Email', 'mobile_number', 'whatsapp_number']

            # Read existing data
            existing_entries = set()
            if os.path.exists(full_path):
                with open(full_path, 'r', newline='', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Create values for expected fields; missing columns default to ''
                        entry_tuple = tuple(row.get(field, '') for field in fieldnames)
                        existing_entries.add(entry_tuple)

            # Prepare new entries
            new_leads = []
            for lead in self.leads:
                lead_tuple = tuple(str(lead.get(field, '')) for field in fieldnames)
                if lead_tuple not in existing_entries:
                    new_leads.append(lead)
                    existing_entries.add(lead_tuple)

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

            print(f"✅ Added {len(new_leads)} new leads to {full_path} (Total: {len(existing_entries)})")
        except Exception as e:
            print(f"⛔ Export failed: {str(e)}")

def sanitize_filename(text: str) -> str:
    """Filename sanitization"""
    return re.sub(r'[\\/*?:"<>|]', "", text.replace(",", "_")).strip()[:100]

async def execute_search(query: str, location: str, zoom: int) -> None:
    """Async search orchestration"""
    print("\n🚀 Enterprise Lead Generator v8.1.1")
    print("★★★★★★★★★★★★★★★★★★★★★★★★★★★★")
    
    try:
        os.makedirs('Leads_Generated', exist_ok=True)
        engine = EnterpriseLeadGenerator(
            query=query,
            location=location,
            zoom=max(min(zoom, VALID_ZOOM_RANGE[1]), VALID_ZOOM_RANGE[0])
        )
        
        print("\n🔍 Initiating intelligence gathering...")
        await engine.run()
        
        if engine.leads:
            engine.export_csv(OUTPUT_FILENAME)  # Use predefined filename

    except KeyboardInterrupt:
        print("\n🛑 Operation terminated by user")
    except Exception as e:
        print(f"\n⛔ System failure: {str(e)}")
        
def load_configurations(file_path: str) -> List[Dict]:
    """Config loader"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            configs = json.load(f)
            
        valid_configs = []
        for config in configs:
            try:
                valid_configs.append({
                    'query': config['query'].strip(),
                    'location': config['location'].strip(),
                    'zoom': max(min(int(config.get('zoom', 15)), VALID_ZOOM_RANGE[1]), VALID_ZOOM_RANGE[0])
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