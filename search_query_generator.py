import json
import os

# -----------------------------
# INDUSTRIES - B2B ERP & Frappe Software Sales Targets (Global)
# Products: ERPNext, Frappe HR, Frappe CRM, Frappe POS, Frappe LMS, Frappe Helpdesk
# -----------------------------
industries = [

    # --- Manufacturing ---
    {"key": "manufacturing_company",          "query": "Manufacturing company"},
    {"key": "garment_manufacturer",           "query": "Garment manufacturer"},
    {"key": "food_processing",                "query": "Food processing company"},
    {"key": "pharmaceutical_manufacturer",    "query": "Pharmaceutical manufacturer"},
    {"key": "chemical_company",               "query": "Chemical company"},
    {"key": "packaging_manufacturer",         "query": "Packaging manufacturer"},
    {"key": "steel_metal_fabrication",        "query": "Metal fabrication company"},
    {"key": "furniture_manufacturer",         "query": "Furniture manufacturer"},
    {"key": "electronics_manufacturer",       "query": "Electronics manufacturer"},
    {"key": "plastic_manufacturer",           "query": "Plastic manufacturer"},
    {"key": "paint_manufacturer",             "query": "Paint manufacturer"},
    {"key": "cement_manufacturer",            "query": "Cement manufacturer"},
    {"key": "beverage_company",               "query": "Beverage company"},
    {"key": "cosmetics_manufacturer",         "query": "Cosmetics manufacturer"},
    {"key": "printing_company",               "query": "Printing company"},

    # --- Wholesale & Distribution ---
    {"key": "trading_company",                "query": "Trading company"},
    {"key": "wholesale_distributor",          "query": "Wholesale distributor"},
    {"key": "import_export_company",          "query": "Import export company"},
    {"key": "fmcg_distributor",               "query": "FMCG distributor"},
    {"key": "pharmaceutical_distributor",     "query": "Pharmaceutical distributor"},
    {"key": "building_materials_supplier",    "query": "Building materials supplier"},
    {"key": "auto_parts_distributor",         "query": "Auto parts distributor"},
    {"key": "electrical_supplier",            "query": "Electrical supplier"},
    {"key": "food_distributor",               "query": "Food distributor"},

    # --- Retail ---
    {"key": "supermarket",                    "query": "Supermarket"},
    {"key": "electronics_store",              "query": "Electronics store"},
    {"key": "pharmacy_retail",                "query": "Pharmacy"},
    {"key": "clothing_store",                 "query": "Clothing store"},
    {"key": "hardware_store",                 "query": "Hardware store"},
    {"key": "furniture_store",                "query": "Furniture store"},
    {"key": "jewelry_store",                  "query": "Jewelry store"},
    {"key": "auto_showroom",                  "query": "Car dealership"},
    {"key": "medical_store",                  "query": "Medical store"},
    {"key": "sports_goods_store",             "query": "Sporting goods store"},

    # --- Agriculture & Agribusiness ---
    {"key": "agribusiness",                   "query": "Agribusiness company"},
    {"key": "poultry_company",                "query": "Poultry company"},
    {"key": "dairy_farm",                     "query": "Dairy farm"},
    {"key": "rice_mill",                      "query": "Rice mill"},
    {"key": "flour_mill",                     "query": "Flour mill"},
    {"key": "agro_processing",                "query": "Agro processing company"},
    {"key": "farm_supply_company",            "query": "Farm supply company"},
    {"key": "tea_estate",                     "query": "Tea estate"},
    {"key": "sugar_mill",                     "query": "Sugar mill"},

    # --- Construction & Real Estate ---
    {"key": "construction_company",           "query": "Construction company"},
    {"key": "real_estate_developer",          "query": "Real estate developer"},
    {"key": "property_management",            "query": "Property management company"},
    {"key": "architecture_firm",              "query": "Architecture firm"},
    {"key": "civil_engineering_firm",         "query": "Civil engineering firm"},
    {"key": "interior_design_firm",           "query": "Interior design firm"},
    {"key": "infrastructure_company",         "query": "Infrastructure company"},
    {"key": "mep_contractor",                 "query": "MEP contractor"},

    # --- Healthcare ---
    {"key": "hospital",                       "query": "Hospital"},
    {"key": "medical_clinic",                 "query": "Medical clinic"},
    {"key": "diagnostic_center",              "query": "Diagnostic center"},
    {"key": "dental_clinic",                  "query": "Dental clinic"},
    {"key": "pharmaceutical_company",         "query": "Pharmaceutical company"},
    {"key": "medical_equipment_supplier",     "query": "Medical equipment supplier"},
    {"key": "pathology_lab",                  "query": "Pathology lab"},
    {"key": "nursing_home",                   "query": "Nursing home"},
    {"key": "veterinary_clinic",              "query": "Veterinary clinic"},

    # --- Education ---
    {"key": "school",                         "query": "School"},
    {"key": "college",                        "query": "College"},
    {"key": "university",                     "query": "University"},
    {"key": "training_institute",             "query": "Training institute"},
    {"key": "vocational_school",              "query": "Vocational school"},
    {"key": "coaching_center",                "query": "Coaching center"},
    {"key": "education_consultancy",          "query": "Education consultancy"},
    {"key": "corporate_training",             "query": "Corporate training company"},

    # --- Logistics & Supply Chain ---
    {"key": "logistics_company",              "query": "Logistics company"},
    {"key": "freight_forwarder",              "query": "Freight forwarder"},
    {"key": "trucking_company",               "query": "Trucking company"},
    {"key": "courier_company",                "query": "Courier company"},
    {"key": "warehouse_company",              "query": "Warehouse company"},
    {"key": "customs_brokerage",              "query": "Customs brokerage company"},
    {"key": "shipping_company",               "query": "Shipping company"},
    {"key": "3pl_company",                    "query": "3PL logistics company"},
    {"key": "cold_storage",                   "query": "Cold storage company"},

    # --- Finance & Insurance ---
    {"key": "bank",                           "query": "Bank"},
    {"key": "microfinance",                   "query": "Microfinance institution"},
    {"key": "cooperative_bank",               "query": "Cooperative bank"},
    {"key": "insurance_company",              "query": "Insurance company"},
    {"key": "accounting_firm",                "query": "Accounting firm"},
    {"key": "tax_consulting",                 "query": "Tax consulting firm"},
    {"key": "investment_company",             "query": "Investment company"},
    {"key": "leasing_company",                "query": "Leasing company"},
    {"key": "fintech_company",                "query": "Fintech company"},

    # --- IT & Technology ---
    {"key": "software_company",               "query": "Software company"},
    {"key": "it_services_company",            "query": "IT services company"},
    {"key": "system_integrator",              "query": "System integrator"},
    {"key": "telecom_company",                "query": "Telecommunications company"},
    {"key": "isp",                            "query": "Internet service provider"},
    {"key": "digital_agency",                 "query": "Digital agency"},
    {"key": "bpo_company",                    "query": "BPO company"},
    {"key": "data_center",                    "query": "Data center company"},

    # --- Professional Services ---
    {"key": "law_firm",                       "query": "Law firm"},
    {"key": "management_consulting",          "query": "Management consulting firm"},
    {"key": "recruitment_agency",             "query": "Recruitment agency"},
    {"key": "marketing_agency",               "query": "Marketing agency"},
    {"key": "event_management",               "query": "Event management company"},
    {"key": "facility_management",            "query": "Facility management company"},
    {"key": "security_company",               "query": "Security company"},
    {"key": "engineering_consultancy",        "query": "Engineering consultancy"},
    {"key": "audit_firm",                     "query": "Audit firm"},

    # --- Hospitality & Travel ---
    {"key": "hotel",                          "query": "Hotel"},
    {"key": "resort",                         "query": "Resort"},
    {"key": "travel_agency",                  "query": "Travel agency"},
    {"key": "tour_operator",                  "query": "Tour operator"},
    {"key": "restaurant_chain",               "query": "Restaurant chain"},
    {"key": "catering_company",               "query": "Catering company"},
    {"key": "event_venue",                    "query": "Event venue"},
    {"key": "trekking_agency",                "query": "Trekking agency"},

    # --- Energy & Utilities ---
    {"key": "energy_company",                 "query": "Energy company"},
    {"key": "solar_company",                  "query": "Solar energy company"},
    {"key": "oil_and_gas",                    "query": "Oil and gas company"},
    {"key": "mining_company",                 "query": "Mining company"},
    {"key": "water_treatment",                "query": "Water treatment company"},
    {"key": "waste_management",               "query": "Waste management company"},

    # --- Automotive ---
    {"key": "auto_parts_store",               "query": "Auto parts store"},
    {"key": "auto_service_center",            "query": "Auto service center"},
    {"key": "fleet_operator",                 "query": "Fleet operator"},
    {"key": "truck_dealer",                   "query": "Truck dealer"},

    # --- NGO & Associations ---
    {"key": "ngo",                            "query": "Non-governmental organization"},
    {"key": "chamber_of_commerce",            "query": "Chamber of commerce"},
    {"key": "cooperative_society",            "query": "Cooperative society"},
    {"key": "trade_association",              "query": "Trade association"},
    {"key": "social_enterprise",              "query": "Social enterprise"},
]

# -----------------------------
# DISTRICTS - Major Nepal Cities / Districts
# -----------------------------
districts = [
    "Kathmandu",
    "Lalitpur",
    "Bhaktapur",
    "Biratnagar",
    "Dhangadhi",
    "Pokhara"
]

# Create output folder
output_folder = "nepal_search_config"
os.makedirs(output_folder, exist_ok=True)

for district in districts:
    district_slug = district.lower().replace(" ", "_")
    district_targets = []

    for industry in industries:
        district_targets.append({
            "room_id": f"b2b_target_nepal_{district_slug}",
            "query": f"{industry['query']} in",
            "location": f"{district}, Nepal",
            "zoom": 13
        })

    # ✅ Updated filename format
    filename = f"nepal_b2b_{district_slug}.json"

    with open(f"{output_folder}/{filename}", "w", encoding="utf-8") as f:
        json.dump(district_targets, f, indent=2, ensure_ascii=False)

print("✅ Nepal B2B ERP/Frappe district files generated inside 'nepal_search_config' folder.")
print(f"📊 Total industries targeted: {len(industries)}")
print(f"📍 Total districts: {len(districts)}")
print(f"🔍 Total search queries per run: {len(industries) * len(districts)}")