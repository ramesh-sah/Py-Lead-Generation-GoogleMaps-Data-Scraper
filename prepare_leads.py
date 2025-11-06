import pandas as pd
import hashlib
import re
import os
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

class LeadDataProcessor:
    """Professional lead data processor for multiple marketing platforms"""
    
    def __init__(self, input_file: str, output_dir: str = "Prepared_Data_Platform_Specific"):
        self.input_file = input_file
        self.base_name = os.path.splitext(os.path.basename(input_file))[0]
        
        # Create main directory if it doesn't exist
        main_dir = output_dir
        if not os.path.exists(main_dir):
            os.makedirs(main_dir, exist_ok=True)
        
        # Create subdirectory with filename and timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir = os.path.join(main_dir, f"{self.base_name}_completed_{timestamp}")
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Load the data
        try:
            dtype_dict = {
                'Phone': str,
                'mobile_number': str,
                'whatsapp_number': str
            }
            self.df = pd.read_csv(input_file, dtype=dtype_dict)
            logging.info(f"📖 Loaded {len(self.df)} leads from {input_file}")
            # Remove rows without Email AND Phone
            self.df = self.df[(self.df['Email'].notna() & (self.df['Email'] != '') & (self.df['Email'] != 'null')) |
                            (self.df['Phone'].notna() & (self.df['Phone'] != '') & (self.df['Phone'] != 'null'))]
            logging.info(f"📌 Filtered leads with at least Email or Phone: {len(self.df)} leads remaining")
        except Exception as e:
            logging.error(f"Error loading input file: {e}")
            raise
        
        # Define segment thresholds
        self.MAX_ROWS_PER_FILE = 40000
        
        # Log file structure
        logging.info(f"Input file columns: {list(self.df.columns)}")
    
  
    
    def parse_name(self, title: str) -> Tuple[str, str]:
        """
        Parse business title into first name and last name.
        
        This is a simple split on the first space, with no additional cleaning.
        
        Args:
            title: The business title to parse
            
        Returns:
            A tuple of (first_name, last_name)
        """
        if not title or title == 'null' or pd.isna(title):
            return '', ''
        
        # Convert to string and strip whitespace
        title = str(title).strip()
        
        # Split by the first space only
        parts = title.split(' ', 1)
        
        if len(parts) >= 2:
            return parts[0].strip(), parts[1].strip()
        elif len(parts) == 1:
            # If only one part, use it as first name
            return parts[0].strip(), ''
        else:
            return '', ''
    
    def extract_postal_code(self, address: str) -> str:
        """Extract postal code from address using universal patterns optimized for Google Maps data."""
        if not address or address == 'null' or pd.isna(address):
            return ''
        
        # Universal postal code patterns by country (ordered from most specific to most general)
        postal_patterns = [
            # US ZIP code (5 digits or 5+4)
            r'\b(\d{5}(?:-\d{4})?)\b',
            # Canadian postal code (A1A 1A1)
            r'\b([A-Z]\d[A-Z] ?\d[A-Z]\d)\b',
            # UK postal code (various formats)
            r'\b([A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2})\b',
            # Irish postal code (D01 W123 or A12 B345)
            r'\b([A-Z]\d{2} ?[A-Z]\d{3})\b',
            # Dutch postal code (1234 AB)
            r'\b(\d{4} ?[A-Z]{2})\b',
            # Argentine postal code (A1234ABC or old 4-digit)
            r'\b([A-Z]\d{4}[A-Z]{3})\b',
            # Jamaican postal code (JMABC 12)
            r'\b(JM[A-Z]{3}\s?\d{2})\b',
            # Barbados postal code (BB12345)
            r'\b(BB\d{5})\b',
            # Haitian postal code (HT1234)
            r'\b(HT\d{4})\b',
            # Andorran postal code (AD123)
            r'\b(AD\d{3})\b',
            # Maltese postal code (MTA 1234 or MTB 1234)
            r'\b(M[TA][A-Z]\s?\d{4})\b',
            # Azerbaijani postal code (AZ 1234)
            r'\b(AZ\s?\d{4})\b',
            # Moldovan postal code (MD-1234)
            r'\b(MD-\d{4})\b',
            # Portuguese postal code (1234-567)
            r'\b(\d{4}-\d{3})\b',
            # Brazilian postal code (12345-678)
            r'\b(\d{5}-\d{3})\b',
            # Japanese postal code (123-4567)
            r'\b(\d{3}-\d{4})\b',
            # Polish postal code (12-345)
            r'\b(\d{2}-\d{3})\b',
            # Czech postal code (123 45)
            r'\b(\d{3}\s?\d{2})\b',
            # Iranian postal code (12345-6789)
            r'\b(\d{5}-?\d{5})\b',
            # Slovakian postal code (123 45)
            r'\b(\d{3}\s?\d{2})\b',
            # Swedish, Greek, Taiwanese postal code (123 45)
            r'\b(\d{3}\s?\d{2})\b',
            # South Korean postal code (12345)
            r'\b(\d{5})\b',
            # 5-digit postal codes (Germany, France, Italy, Spain, Mexico, etc.)
            r'\b(\d{5})\b',
            # Indian, Chinese, Russian, Vietnamese, Romanian, Singaporean, Colombian, Nigerian postal codes (6 digits)
            r'\b(\d{6})\b',
            # Chilean postal code (7 digits)
            r'\b(\d{7})\b',
            # 4-digit postal codes (Australia, South Africa, Switzerland, etc.)
            r'\b(\d{4})\b',
            # Icelandic postal code (3 digits)
            r'\b(\d{3})\b',
            # Generic pattern for other countries (3-10 digits, optionally with dash or space)
            r'\b(\d{3,10}(?:[-\s]\d{3,10})*)\b'
        ]
        # Try each pattern
        for pattern in postal_patterns:
            match = re.search(pattern, address)
            if match:
                return match.group(1)
        
        return ''
    
    def parse_address(self, address: str) -> Tuple[str, str, str]:
        """Parse address into street address, city, and zip code."""
        if not address or address == 'null' or pd.isna(address):
            return '', '', ''
        
        # Extract postal code using universal patterns
        zip_code = self.extract_postal_code(address)
        
        # The rest is the street address
        street_address = address
        if zip_code:
            street_address = street_address.replace(zip_code, '')
            # Also handle cases where postal code might be preceded by a comma
            street_address = re.sub(r',?\s*' + re.escape(zip_code), '', street_address)
        
        return street_address.strip().strip(','), '', zip_code
    
    def get_best_phone(self, row) -> str:
        """Get the best phone number (WhatsApp > Mobile > General) - keeping original format."""
        # Check WhatsApp first
        if pd.notna(row['whatsapp_number']) and row['whatsapp_number'] != '' and row['whatsapp_number'] != 'null':
            return row['whatsapp_number']
        # Then mobile
        elif pd.notna(row['mobile_number']) and row['mobile_number'] != '' and row['mobile_number'] != 'null':
            return row['mobile_number']
        # Then general phone
        elif pd.notna(row['Phone']) and row['Phone'] != '' and row['Phone'] != 'null':
            return row['Phone']
        else:
            return ''
    
    def segment_data(self) -> Dict[str, pd.DataFrame]:
        """Segment data into three categories with mutually exclusive segments."""
        # Create a copy to avoid SettingWithCopyWarning
        df = self.df.copy()
        
        # Parse names and addresses
        df['first_name'], df['last_name'] = zip(*df['Title'].apply(self.parse_name))
        df['street_address'], df['city'], df['zip'] = zip(*df['Address'].apply(self.parse_address))
        
        # Get best phone (keeping original format)
        df['best_phone'] = df.apply(self.get_best_phone, axis=1)
        
        # Check if email is valid (not null or empty)
        df['has_email'] = ~((df['Email'] == '') | (df['Email'] == 'null') | pd.isna(df['Email']))
        
        # Check if phone is valid (not null or empty)
        df['has_phone'] = (df['best_phone'] != '')
        
        # Check if website is valid
        df['has_website'] = ~((df['Website'] == '') | (df['Website'] == 'null') | pd.isna(df['Website']))
        
        # Create a copy of indices to track which leads have been assigned
        unassigned_indices = set(df.index)
        
        # Segment 1: Leads with both email and phone
        both_email_phone_mask = (
            (df['has_email']) & 
            (df['has_phone'])
        )
        segment1_indices = set(df[both_email_phone_mask].index)
        segment1 = df.loc[both_email_phone_mask]
        unassigned_indices -= segment1_indices
        
        # Segment 2: Leads without website (from remaining unassigned leads)
        no_website_mask = (
            (df.index.isin(unassigned_indices)) &
            (~df['has_website'])
        )
        segment2_indices = set(df[no_website_mask].index)
        segment2 = df.loc[no_website_mask]
        unassigned_indices -= segment2_indices
        
        # Segment 3: Leads with only email or only phone (from remaining unassigned leads)
        single_contact_mask = (
            (df.index.isin(unassigned_indices)) &
            ((df['has_email']) | (df['has_phone']))
        )
        segment3 = df.loc[single_contact_mask]
        
        segments = {
            'both_email_phone': segment1,
            'no_website': segment2,
            'single_contact': segment3
        }
        
        # Verify that segments are mutually exclusive
        total_segmented = len(segment1) + len(segment2) + len(segment3)
        logging.info(f"Total leads: {len(df)}, Total segmented: {total_segmented}")
        
        for name, segment in segments.items():
            if len(segment) > 0:  # Only log non-empty segments
                logging.info(f"📊 Segment '{name}': {len(segment)} leads")
        
        return segments
    
    def split_large_dataframe(self, df: pd.DataFrame, segment_name: str) -> List[pd.DataFrame]:
        """Split a large dataframe into smaller chunks."""
        if len(df) <= self.MAX_ROWS_PER_FILE:
            return [df]
        
        chunks = []
        for i in range(0, len(df), self.MAX_ROWS_PER_FILE):
            chunk = df.iloc[i:i + self.MAX_ROWS_PER_FILE].copy()
            chunks.append(chunk)
        
        logging.info(f"📂 Split segment '{segment_name}' into {len(chunks)} files")
        return chunks
    
    def prepare_for_meta_ads(self, segments: Dict[str, pd.DataFrame]):
        """Prepare data for Meta Ads Custom Audience."""
        # Create platform-specific directory
        meta_dir = os.path.join(self.output_dir, 'meta_ads')
        os.makedirs(meta_dir, exist_ok=True)
        
        all_meta_dfs = []
        
        for segment_name, segment_df in segments.items():
            # Skip empty segments
            if len(segment_df) == 0:
                continue
                
            # Split into chunks if needed
            chunks = self.split_large_dataframe(segment_df, segment_name)
            
            for i, chunk in enumerate(chunks):
                meta_df = pd.DataFrame()
                
                # Hash Email (using original email)
                meta_df['Email'] = chunk['Email'] 
                
                # Hash phones (using original best phone)
                meta_df['Phone'] = chunk['best_phone'] 
                
                # Hash names
                meta_df['First Name'] = chunk['first_name'] 
                meta_df['Last Name'] = chunk['last_name'] 
                
                # Hash country (ISO format)
                meta_df['Country'] = chunk['Country'] 
                
               
                
                # Hash zip
                meta_df['Zip'] = chunk['zip'] 
                
                # Remove rows with no primary identifiers
                meta_df = meta_df[(meta_df['Email'] != '') | (meta_df['Phone'] != '')]
                
                # Save without headers
                part_suffix = f"_part_{i+1}" if len(chunks) > 1 else ""
                output_file = os.path.join(meta_dir, f'{self.base_name}_{segment_name}_meta{part_suffix}.csv')
                meta_df.to_csv(output_file, index=False, header=True)
                all_meta_dfs.append(meta_df)
                logging.info(f"✅ Created Meta Ads file: {output_file} ({len(meta_df)} contacts)")
        
        return all_meta_dfs
    
    def prepare_for_google_ads(self, segments: Dict[str, pd.DataFrame]):
        """Prepare data for Google Ads Customer Match."""
        # Create platform-specific directory
        google_dir = os.path.join(self.output_dir, 'google_ads')
        os.makedirs(google_dir, exist_ok=True)
        
        all_google_dfs = []
        
        for segment_name, segment_df in segments.items():
            # Skip empty segments
            if len(segment_df) == 0:
                continue
                
            # Split into chunks if needed
            chunks = self.split_large_dataframe(segment_df, segment_name)
            
            for i, chunk in enumerate(chunks):
                google_df = pd.DataFrame()
                
                # Hash Email (using original email)
                google_df['Email'] = chunk['Email'] 
                
                # Hash phones (using original best phone)
                google_df['Phone'] = chunk['best_phone'] 
                
                # Hash names
                google_df['First Name'] = chunk['first_name'] 
                google_df['Last Name'] = chunk['last_name'] 
                
                # Hash country (ISO format)
                google_df['Country'] = chunk['Country'] 
                
                # Hash zip
                google_df['Zip'] = chunk['zip'] 
                
                # Remove rows with no primary identifiers
                google_df = google_df[(google_df['Email'] != '') | (google_df['Phone'] != '')]
                
                # Save without headers
                part_suffix = f"_part_{i+1}" if len(chunks) > 1 else ""
                output_file = os.path.join(google_dir, f'{self.base_name}_{segment_name}_google{part_suffix}.csv')
                google_df.to_csv(output_file, index=False, header=True)
                all_google_dfs.append(google_df)
                logging.info(f"✅ Created Google Ads file: {output_file} ({len(google_df)} contacts)")
        
        return all_google_dfs
    
    def prepare_for_mautic(self, segments: Dict[str, pd.DataFrame]):
        """Prepare data for Mautic contact import with phone numbers."""
        # Create Mautic output directory
        mautic_dir = os.path.join(self.output_dir, 'mautic')
        os.makedirs(mautic_dir, exist_ok=True)
        
        all_mautic_dfs = []
        
        for segment_name, segment_df in segments.items():
            if len(segment_df) == 0:
                continue
            
            # Split large segments into chunks
            chunks = self.split_large_dataframe(segment_df, segment_name)
            
            for i, chunk in enumerate(chunks):
                mautic_df = pd.DataFrame()
                
                # Map fields to Mautic format
                mautic_df['firstname'] = chunk['first_name']
                mautic_df['lastname'] = chunk['last_name']
                mautic_df['email'] = chunk['Email']  # Keep original email
                
                # Add phone numbers safely
                mautic_df['phone1'] = chunk.get('Phone', '')
                mautic_df['phone2'] = chunk.get('mobile_number', '')  # Will be empty if not present
                mautic_df['phone3'] = chunk.get('whatsapp_number', '')  # Will be empty if not present
                
                # Address components
                mautic_df['address1'] = chunk.get('street_address', '')
                mautic_df['city'] = chunk.get('city', '')
                mautic_df['zipcode'] = chunk.get('zip', '')
                mautic_df['country'] = chunk.get('Country', '')
                
                # Additional fields
                mautic_df['website'] = chunk.get('Website', '')
                mautic_df['company'] = chunk.apply(
                    lambda row:  row.get('Title', ''), axis=1
                )
                mautic_df['tags'] = segment_name
                
                # Keep rows with at least one identifier (email or any phone)
                mautic_df = mautic_df[
                    (mautic_df['email'] != '') |
                    (mautic_df['phone1'] != '') |
                    (mautic_df['phone2'] != '') |
                    (mautic_df['phone3'] != '')
                ]
                
                # Save CSV with headers
                part_suffix = f"_part_{i+1}" if len(chunks) > 1 else ""
                output_file = os.path.join(mautic_dir, f'{self.base_name}_{segment_name}_mautic{part_suffix}.csv')
                mautic_df.to_csv(output_file, index=False, header=True)
                all_mautic_dfs.append(mautic_df)
                logging.info(f"✅ Created Mautic file: {output_file} ({len(mautic_df)} contacts)")
        
        return all_mautic_dfs

    
    def prepare_for_whatsapp(self, segments: Dict[str, pd.DataFrame]):
        """Prepare WhatsApp marketing Excel with a single column for all numbers (deduplicated)."""
        import pandas as pd
        import os
        import logging

        whatsapp_dir = os.path.join(self.output_dir, 'whatsapp')
        os.makedirs(whatsapp_dir, exist_ok=True)

        all_numbers = []

        for segment_name, segment_df in segments.items():
            if len(segment_df) == 0:
                continue

            chunks = self.split_large_dataframe(segment_df, segment_name)

            for chunk in chunks:
                for col in ['Phone', 'mobile_number', 'whatsapp_number']:
                    if col in chunk.columns:
                        # Convert all to string, drop NaN/empty
                        numbers = chunk[col].dropna().astype(str).str.strip()
                        numbers = numbers[numbers != '']
                        all_numbers.extend(numbers.tolist())

        # Remove duplicates
        all_numbers = list(dict.fromkeys(all_numbers))

        # Create DataFrame
        whatsapp_df = pd.DataFrame({'WhatsApp Number (with country code)': all_numbers})

        # Save Excel
        output_file = os.path.join(whatsapp_dir, f'{self.base_name}_whatsapp.xlsx')
        whatsapp_df.to_excel(output_file, index=False, header=True)
        logging.info(f"✅ Created WhatsApp Excel: {output_file} ({len(whatsapp_df)} contacts)")

        return whatsapp_df

    
    def prepare_summary_report(self):
        """Generate a summary report of the data preparation."""
        report_file = os.path.join(self.output_dir, f'{self.base_name}_summary_report.txt')
        
        with open(report_file, 'w') as f:
            f.write(f"Lead Data Preparation Summary Report\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Input File: {self.input_file}\n")
            f.write(f"Total Leads: {len(self.df)}\n\n")
            
            segments = self.segment_data()
            for name, segment in segments.items():
                if len(segment) > 0:  # Only include non-empty segments
                    f.write(f"Segment '{name}': {len(segment)} leads\n")
                    
                    # Count contacts with email
                    email_count = len(segment[segment['has_email']])
                    f.write(f"  - With Email: {email_count}\n")
                    
                    # Count contacts with phone
                    phone_count = len(segment[segment['has_phone']])
                    f.write(f"  - With Phone: {phone_count}\n")
                    
                    # Count contacts with website
                    website_count = len(segment[segment['has_website']])
                    f.write(f"  - With Website: {website_count}\n\n")
        
        logging.info(f"📋 Generated summary report: {report_file}")
    
    def process_all(self):
        """Process all data for all platforms."""
        logging.info(f"\n🚀 Starting lead data processing...")
        
        # Segment the data
        segments = self.segment_data()
        
        # Prepare data for each platform
        self.prepare_for_meta_ads(segments)
        self.prepare_for_google_ads(segments)
        self.prepare_for_mautic(segments)
        self.prepare_for_whatsapp(segments)
        
        # Generate summary report
        self.prepare_summary_report()
        
        logging.info(f"\n✅ All files prepared successfully in the '{self.output_dir}' directory")
        logging.info(f"📁 Files are ready for import into Meta Ads, Google Ads, Mautic, and WhatsApp")

# HOW TO USE
if __name__ == "__main__":
    # Replace with the path to your CSV file
    input_file = 'Leads_Generated/salon_data.csv'
    
    # Create processor and run
    processor = LeadDataProcessor(input_file)
    processor.process_all()