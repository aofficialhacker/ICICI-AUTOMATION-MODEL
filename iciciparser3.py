import pandas as pd
import re
import os
from collections import defaultdict

# --- Configuration ---
OUTPUT_COLUMNS = [
    "cluster_code", "bike_make", "model", "plan_type", "engine_type", "fuel_type",
    "plan_subtype", "add_on", "plan_term", "business_slab", "age", "po_percent",
    "slab_month", "remark", "product_type", "ncb", "vehicle", "veh_type",
    "seating_cap", "gvw"
]

BIKE_MAKES_RAW = [
    "TATA", "AL", "ASHOK LEYLAND", "M&M", "MAHINDRA", "EICHER", "MARUTI", "MARUTI SUZUKI",
    "PIAGGIO", "BAJAJ", "ATUL", "TVS", "TOYOTA", "FORCE MOTORS", "SML ISUZU",
    "SWARAJ MAZDA", "HINDUSTAN MOTORS", "MAHINDRA NAVISTAR", "BHARATBENZ",
    "SCANIA", "VOLVO", "MARUTI SUPER CARRY" # Added from original request example
]
BIKE_MAKES = sorted(list(set(bm.upper() for bm in BIKE_MAKES_RAW)), key=len, reverse=True)
BIKE_MAKE_REGEX_STR = r"\b(" + "|".join(re.escape(bm) for bm in BIKE_MAKES) + r")\b"
BIKE_MAKE_REGEX = re.compile(BIKE_MAKE_REGEX_STR, re.IGNORECASE)


SPECIAL_CLUSTER_CODES_RAW = [
    "WB1", "DL", "NON DL RTO", "JK1 RTO", "GJ1 RTO", "UP1 EAST", "UK1 RTO", "UP EAST 1", "UP EAST1",
    "KA1 RTOS", "KA1 RTO", "TN10", "TN12", "TN02", "TN22", "TN04", "TN06", "TN09",
    "TN18", "TN19", "TN20", "TN11", "TN14", # Added from original request example
    "KA01-05", "OD1", "PIMPRI", "PIMPRICHINCHWAD", "PIMPRI CHINCHWAD",
    "DELHI SURROUNDING RTO", "GJ1", "JK1" # Added from original request example
]
SPECIAL_CLUSTER_CODES = sorted(list(set(scc.upper() for scc in SPECIAL_CLUSTER_CODES_RAW)), key=len, reverse=True)
SPECIAL_CLUSTER_REGEX_STR = r"\b(" + "|".join(re.escape(scc) for scc in SPECIAL_CLUSTER_CODES) + r")\b"
SPECIAL_CLUSTER_REGEX = re.compile(SPECIAL_CLUSTER_REGEX_STR, re.IGNORECASE)

VEHICLE_CATEGORIES_MAP = {
    "GCV": "GCV", "SCV": "GCV", "LCV": "GCV", "MHCV": "GCV",
    "PCV": "PCV", "PCVTAXI": "PCV", # From original request example
    "MISC D CE": "MISC", "MIsc D CE": "MISC", "MIS D CE": "MISC", "MISC": "MISC"
}
VEHICLE_CATEGORY_REGEX_STR = r"\b(" + "|".join(re.escape(k) for k in VEHICLE_CATEGORIES_MAP.keys()) + r")\b"
VEHICLE_CATEGORY_REGEX = re.compile(VEHICLE_CATEGORY_REGEX_STR, re.IGNORECASE)


SPECIFIC_VEHICLES_RAW = [
    "TANKER", "TIPPER", "TRUCK", "TRAILER", "DUMPER", "CRANES", "TRACTOR", "TRACTER",
    "SCHOOL BUS", "STAFF BUS", "BUS", "TAXI", "CE", "BACKHOELOADER"
]
SPECIFIC_VEHICLES = sorted(list(set(sv.upper() for sv in SPECIFIC_VEHICLES_RAW)), key=len, reverse=True)
SPECIFIC_VEHICLE_REGEX_STR = r"\b(" + "|".join(re.escape(sv) for sv in SPECIFIC_VEHICLES) + r")\b"
SPECIFIC_VEHICLE_REGEX = re.compile(SPECIFIC_VEHICLE_REGEX_STR, re.IGNORECASE)


FUEL_TYPES_RAW = ["ELECTRIC", "PETROL", "CNG", "BIFUEL", "DIESEL"]
FUEL_TYPES = sorted(list(set(ft.upper() for ft in FUEL_TYPES_RAW)), key=len, reverse=True)
FUEL_TYPE_REGEX_STR = r"\b(" + "|".join(re.escape(ft) for ft in FUEL_TYPES) + r")\b"
FUEL_TYPE_REGEX = re.compile(FUEL_TYPE_REGEX_STR, re.IGNORECASE)

AGE_KEYWORD_PATTERNS = [
    (re.compile(r"\bNEW\b", re.IGNORECASE), "NEW"),
    (re.compile(r"\bOLD\b", re.IGNORECASE), "OLD"),
    (re.compile(r"\b(\d+\s*-\s*\d+\s*(?:YRS?|YEARS?|AGE))\b", re.IGNORECASE), lambda m: m.group(1).upper().replace("AGE","YRS").replace("YEARS","YRS").replace("YEAR","YRS").replace(" ","")),
    (re.compile(r"\b([<>]=?\s*\d+\s*(?:YRS?|YEARS?|AGE))\b", re.IGNORECASE), lambda m: m.group(1).upper().replace("AGE","YRS").replace("YEARS","YRS").replace("YEAR","YRS").replace(" ","")),
    (re.compile(r"\b(ABOVE\s*\d+\s*(?:YRS?|YEARS?|AGE))\b", re.IGNORECASE), lambda m: m.group(1).upper().replace("AGE","YRS").replace("YEARS","YRS").replace("YEAR","YRS").replace(" ","").replace("ABOVE",">")),
    (re.compile(r"\b(UPTO\s*\d+\s*(?:YRS?|YEARS?|AGE))\b", re.IGNORECASE), lambda m: m.group(0).upper().replace("AGE","YRS").replace("YEARS","YRS").replace("YEAR","YRS").replace(" ","")),
    (re.compile(r"\b(\d+\s*\+\s*(?:YRS?|YEARS?|AGE))\b", re.IGNORECASE), lambda m: m.group(0).upper().replace("AGE","YRS").replace("YEARS","YRS").replace("YEAR","YRS").replace(" ","")),
    (re.compile(r"\b(\d+(?:ST|ND|RD|TH)\s*YEAR)\b", re.IGNORECASE), lambda m: m.group(1).upper()),
]

GVW_REGEX_PATTERNS = [
    re.compile(r"([<>]=?)\s*(\d+(?:\.\d+)?)\s*GVW", re.IGNORECASE), # e.g. <2450 GVW
    re.compile(r"(\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?)\s*T\b", re.IGNORECASE), # e.g. 3.5-7.5T
    re.compile(r"([<>]=?)\s*(\d+(?:\.\d+)?)\s*T\b", re.IGNORECASE), # e.g. >40T
]

SEATING_CAP_REGEX_PATTERNS_CONTEXTUAL = [
    (re.compile(r"(>\s*18\s*UPTO\s*36\s*SEATER)", re.IGNORECASE), lambda m: ">18 UPTO 36"),
    (re.compile(r"([<>]=?)\s*(\d+)\s*SEATER", re.IGNORECASE), lambda m: f"{m.group(1)}{m.group(2)}".replace(" ","")),
    (re.compile(r"([<>]=?)\s*(\d+)\b", re.IGNORECASE), lambda m: f"{m.group(1)}{m.group(2)}".replace(" ","")), # <18, >36
    (re.compile(r"(\d+\s*-\s*\d+)\b", re.IGNORECASE), lambda m: m.group(1).replace(" ","")), # 18-36
]


ENGINE_TYPE_REGEX_PATTERNS = [
    re.compile(r"([<>]=?)\s*(\d+)\s*HP\b", re.IGNORECASE),
    re.compile(r"\bABOVE\s*(\d+)\s*HP\b", re.IGNORECASE),
    re.compile(r"([<>]=?)\s*(\d+)\s*CC\b", re.IGNORECASE),
]

PLAN_TYPE_KEYWORDS = { # Storing the target value directly
    "AOTP": "SATP", "SATP": "SATP", "TP": "SATP",
    "ON OD": "SAOD", "OD": "SAOD", # OD alone might be too broad, "ON OD" is safer.
    "COMP": "COMP"
}
# Regex to find ANY of these keywords, then map
PLAN_TYPE_REGEX_STR = r"\b(" + "|".join(re.escape(k) for k in PLAN_TYPE_KEYWORDS.keys()) + r")\b"
PLAN_TYPE_REGEX = re.compile(PLAN_TYPE_REGEX_STR, re.IGNORECASE)


# --- Helper Functions ---
def clean_text_general(text):
    if pd.isna(text) or text is None:
        return ""
    text = str(text)
    text = text.replace('\n', ' ').replace('\r', ' ')
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def find_header_row(df, keyword="RTO CLUSTER"):
    print(f"DEBUG: find_header_row: Searching for header row with keyword '{keyword.upper()}'")
    for i, row in df.iterrows():
        for cell_value in row:
            if keyword.upper() in clean_text_general(cell_value).upper():
                print(f"DEBUG: find_header_row: Found header keyword '{keyword.upper()}' in row {i}")
                return i
    print(f"DEBUG: find_header_row: Header keyword '{keyword.upper()}' not found.")
    return None

def extract_slab_month_from_df(df):
    print("DEBUG: extract_slab_month_from_df: Extracting slab month")
    for i in range(min(15, len(df))): # Search a bit deeper if needed
        for j in range(min(10, len(df.columns))): # Search a few columns
            cell_text_cleaned = clean_text_general(df.iloc[i, j]).upper()
            # Regex to find "CV AGENCY GRID" followed by month and year (e.g., MAR'25, JAN'25, APRIL 25)
            match = re.search(r"CV\s*AGENCY\s*GRID\s*([A-Z]+(?:UARY|BRUARY|RCH|RIL|MAY|JUNE|JULY|GUST|TEMBER|TOBER|VEMBER|CEMBER)?\'?\s*\d{2,4})", cell_text_cleaned)
            if match:
                slab_month_raw = match.group(1)
                # Normalize: remove apostrophe, ensure space if month name + year
                slab_month_normalized = slab_month_raw.replace("'", "").replace(" ", "") 
                # Attempt to reformat to MonYY (e.g. Apr25)
                month_year_match = re.match(r"([A-Z]+)(\d+)", slab_month_normalized)
                if month_year_match:
                    month_part = month_year_match.group(1)[:3] # Take first 3 chars of month
                    year_part = month_year_match.group(2)
                    if len(year_part) == 4: year_part = year_part[2:] # Convert YYYY to YY
                    slab_month = f"{month_part.capitalize()}{year_part}"
                    print(f"DEBUG: extract_slab_month_from_df: Extracted slab_month: {slab_month}")
                    return slab_month
                print(f"DEBUG: extract_slab_month_from_df: Extracted slab_month (raw but normalized): {slab_month_normalized}")
                return slab_month_normalized # Fallback to normalized raw
    print("DEBUG: extract_slab_month_from_df: Slab month not found.")
    return None


def extract_pure_percentage_val(text_segment):
    text_segment_upper = clean_text_general(text_segment).upper()
    # Prioritize percentages explicitly with %
    match_with_symbol = re.search(r"(\d+(?:\.\d+)?%)\b", text_segment_upper)
    if match_with_symbol:
        return match_with_symbol.group(1)
    # Fallback for numbers that might be percentages without the symbol
    # Ensure it's somewhat isolated or at the start to avoid grabbing random numbers
    match_without_symbol = re.search(r"^\s*(\d+(?:\.\d+)?)\b", text_segment_upper)
    if match_without_symbol:
        # Check if this number is followed by text that is NOT typical for other data types
        # e.g., not followed by "HP", "CC", "YRS", "GVW", "T" immediately
        # This is heuristic
        num_val = match_without_symbol.group(1)
        # Check if what follows is mostly non-alphanumeric or typical percentage context words
        remaining_text = text_segment_upper[match_without_symbol.end():].strip()
        if not remaining_text or not re.match(r"^(HP|CC|YRS|GVW|T)", remaining_text, re.IGNORECASE):
             return num_val + "%" # Assume it's a percentage
    return None


def parse_main_table_header(header_text_full):
    context = {"bike_makes_main": [], "remarks_main": [], "age_main": None, "plan_type_main": None, "veh_type_main": None}
    text_upper = clean_text_general(header_text_full).upper()
    print(f"DEBUG: parse_main_table_header: Parsing main table header: '{header_text_full}' -> '{text_upper}'")

    veh_type_match = VEHICLE_CATEGORY_REGEX.search(text_upper)
    if veh_type_match:
        context["veh_type_main"] = VEHICLE_CATEGORIES_MAP.get(veh_type_match.group(1).upper())

    plan_type_match = PLAN_TYPE_REGEX.search(text_upper)
    if plan_type_match:
        context["plan_type_main"] = PLAN_TYPE_KEYWORDS.get(plan_type_match.group(1).upper())

    # More specific age search for main header
    age_search_main = re.search(r"([<>]=?\s*\d+\s*(?:YRS?|YEARS?|AGE))", text_upper, re.IGNORECASE)
    if age_search_main:
        context["age_main"] = age_search_main.group(1).upper().replace("AGE","YRS").replace("YEARS","YRS").replace("YEAR","YRS").replace(" ","")
    
    bike_makes_found = BIKE_MAKE_REGEX.findall(text_upper)
    if bike_makes_found:
         context["bike_makes_main"] = list(set(bm.upper() for bm in bike_makes_found)) # Ensure unique and upper

    # Capture all remarks, especially those in parentheses
    all_remarks_main_header = []
    paren_remarks = re.findall(r"\((.*?)\)", header_text_full) # Use original case for remarks
    for pr in paren_remarks:
        all_remarks_main_header.append(f"({pr})")
    
    # Add the whole text if it's not just keyword phrases, or if it adds context
    # This is heuristic: if the text is long and not fully parsed by keywords, add it.
    if header_text_full and len(header_text_full) > 50 and not paren_remarks: # Arbitrary length
        all_remarks_main_header.append(header_text_full)

    context["remarks_main"] = list(set(all_remarks_main_header)) # Unique remarks
        
    print(f"DEBUG: parse_main_table_header: Parsed main table header context: {context}")
    return context

def parse_column_header_text(header_cell_text_original):
    base_details = defaultdict(lambda: None)
    text_original_cleaned = clean_text_general(header_cell_text_original)
    text_upper = text_original_cleaned.upper()
    
    base_details["remarks_col_header_list"] = [] # Store as list first
    print(f"DEBUG: parse_column_header_text: Parsing column header: '{header_cell_text_original}' -> '{text_upper}'")

    base_details["product_type"] = "COMMERCIAL VEHICLE" # Default

    # Exclusion logic - must run before make/vehicle extraction
    exclusion_triggered = False
    excluded_makes_in_header = []
    excluded_vehicles_in_header = []

    exclusion_keywords = ["EXCLUDING", "EXCEPT"]
    for ex_kw in exclusion_keywords:
        if ex_kw in text_upper:
            exclusion_triggered = True
            # Add the part with "excluding/except" to remarks
            ex_match_remark = re.search(rf"\b({ex_kw}[^\(\)]*)\b", text_original_cleaned, re.IGNORECASE)
            if ex_match_remark:
                 base_details["remarks_col_header_list"].append(ex_match_remark.group(1).strip())

            # Find makes/vehicles mentioned *after* the exclusion keyword
            parts_after_exclusion = text_upper.split(ex_kw, 1)
            if len(parts_after_exclusion) > 1:
                text_after_exclusion = parts_after_exclusion[1]
                excluded_makes_in_header.extend(bm.upper() for bm in BIKE_MAKE_REGEX.findall(text_after_exclusion))
                excluded_vehicles_in_header.extend(sv.upper() for sv in SPECIFIC_VEHICLE_REGEX.findall(text_after_exclusion))
    
    base_details["excluded_makes_col_header"] = list(set(excluded_makes_in_header))
    base_details["excluded_vehicles_col_header"] = list(set(excluded_vehicles_in_header))


    veh_type_match = VEHICLE_CATEGORY_REGEX.search(text_upper)
    if veh_type_match:
        cat_key = veh_type_match.group(1).upper() # Ensure key is upper for map lookup
        base_details["veh_type"] = VEHICLE_CATEGORIES_MAP.get(cat_key) # Use .get for safety
        if base_details["veh_type"] == "PCV": base_details["product_type"] = "PASSENGER CARRYING VEHICLE"
        elif base_details["veh_type"] == "MISC":
            base_details["product_type"] = "MISCELLANEOUS VEHICLE"
            if "CE" in cat_key: base_details["vehicle"] = "CE" # Specific for "MISC D CE"
    
    if not base_details.get("vehicle"): # Check if vehicle is not already set (e.g. by "MISC D CE")
        vehicle_match = SPECIFIC_VEHICLE_REGEX.search(text_upper)
        if vehicle_match:
            matched_vehicle = vehicle_match.group(1).upper()
            if not (exclusion_triggered and matched_vehicle in base_details["excluded_vehicles_col_header"]):
                base_details["vehicle"] = matched_vehicle
                if "BUS" in matched_vehicle: base_details["veh_type"] = "PCV"; base_details["product_type"] = "PASSENGER CARRYING VEHICLE"
                elif matched_vehicle in ["CRANES", "TRACTOR", "TRACTER", "BACKHOELOADER", "CE"]:
                     base_details["veh_type"] = "MISC"; base_details["product_type"] = "MISCELLANEOUS VEHICLE"
                elif matched_vehicle == "TAXI": base_details["veh_type"] = "PCV"; base_details["product_type"] = "PASSENGER CARRYING VEHICLE"


    if not base_details.get("vehicle"): # If still no specific vehicle
        if re.search(r"\b3W\b", text_upper, re.IGNORECASE): base_details["vehicle"] = "3W"
        elif re.search(r"\b2W\b", text_upper, re.IGNORECASE): base_details["vehicle"] = "2W"

    # Age: Only "NEW" or "OLD" from header. More specific ages come from cell content.
    for age_pattern, age_val_fixed in AGE_KEYWORD_PATTERNS:
        if isinstance(age_val_fixed, str) and age_val_fixed in ["NEW", "OLD"]: # Only these fixed strings from header
            if age_pattern.search(text_upper):
                base_details["age"] = age_val_fixed
                break # Found primary age indicator
    
    # Fuel Type: Collect all found, handle multiple fuel types for row splitting later
    base_details["found_fuel_types_col_header"] = list(set(ft.upper() for ft in FUEL_TYPE_REGEX.findall(text_upper)))


    for pattern in GVW_REGEX_PATTERNS:
        gvw_match = pattern.search(text_upper)
        if gvw_match:
            # For patterns like ([<>]=?)(\d+...), groups() will give ('<', '2450')
            # For patterns like (\d+-\d+)T, groups() will give ('3.5-7.5')
            base_details["gvw"] = "".join(g.strip() for g in gvw_match.groups() if g).upper()
            break # Take first GVW match
    
    # Seating Capacity for Buses/PCVs
    # Check if it's a bus type to apply seating capacity logic
    is_bus_type = False
    if base_details.get("vehicle") and "BUS" in base_details.get("vehicle", "").upper(): is_bus_type = True
    if base_details.get("veh_type") == "PCV": is_bus_type = True
    if "BUS" in text_upper or "SEATER" in text_upper : is_bus_type = True

    if is_bus_type:
        text_to_search_seating = text_upper
        # Try to narrow down search area if "BUS" or "SEATER" is present
        bus_seater_keyword_match = re.search(r"\b(BUS|SEATER)\b", text_upper, re.IGNORECASE)
        if bus_seater_keyword_match: # If keyword found, search after it for numbers
            text_to_search_seating = text_upper[bus_seater_keyword_match.end():]
        
        for pattern, resolver in SEATING_CAP_REGEX_PATTERNS_CONTEXTUAL:
            sc_match = pattern.search(text_to_search_seating)
            if sc_match:
                base_details["seating_cap"] = resolver(sc_match)
                print(f"DEBUG: parse_column_header_text: Matched seating_cap: {base_details['seating_cap']} using pattern {pattern.pattern} on text '{text_to_search_seating}'")
                break


    for pattern in ENGINE_TYPE_REGEX_PATTERNS:
        et_match = pattern.search(text_upper)
        if et_match:
            # .group(0) gives the whole match, e.g. "<=1000CC" or ">50HP"
            base_details["engine_type"] = et_match.group(0).upper().replace(" ","")
            break # Take first engine type match
            
    # Parenthetical remarks from column header (original case)
    bracket_remarks = re.findall(r"\((.*?)\)", text_original_cleaned) # Use original cleaned text
    for br in bracket_remarks:
         # Avoid adding if it's just a bike make (some files have "(TATA only)")
        if not BIKE_MAKE_REGEX.fullmatch(br.strip().upper()):
            base_details["remarks_col_header_list"].append(f"({br})")

    # Extract bike makes mentioned in the header (excluding those after "EXCLUDING")
    header_bike_makes_found = BIKE_MAKE_REGEX.findall(text_upper)
    valid_header_bike_makes = []
    if header_bike_makes_found:
        for hbm_found_upper in set(bm.upper() for bm in header_bike_makes_found):
            if not (exclusion_triggered and hbm_found_upper in base_details["excluded_makes_col_header"]):
                valid_header_bike_makes.append(hbm_found_upper)
        if valid_header_bike_makes:
             base_details["header_bike_makes_list"] = valid_header_bike_makes # Store as list


    final_details = {k: v for k, v in base_details.items() if v is not None and v != []}
    if "remarks_col_header_list" in final_details: # Consolidate list into string
        final_details["remarks_col_header"] = " ".join(list(dict.fromkeys(final_details["remarks_col_header_list"]))).strip()
        del final_details["remarks_col_header_list"]
    else:
        final_details["remarks_col_header"] = None # Ensure it's None if no remarks
    
    print(f"DEBUG: parse_column_header_text: Parsed column header details: {dict(final_details)}")
    return dict(final_details)


def parse_percentage_cell_text(cell_text_original, base_header_details, rto_cluster_from_row, main_table_context_global):
    results = []
    cell_text_cleaned_orig_case = clean_text_general(cell_text_original) # Keep original case for remarks
    cell_text_cleaned_upper = cell_text_cleaned_orig_case.upper()
    
    print(f"DEBUG: parse_percentage_cell_text: Parsing cell text: '{cell_text_original}' (Cleaned Upper: '{cell_text_cleaned_upper}') for RTO: {rto_cluster_from_row}")
    print(f"DEBUG: parse_percentage_cell_text: Base header details: {base_header_details}")
    print(f"DEBUG: parse_percentage_cell_text: Main table context (if any): {main_table_context_global}")

    if cell_text_cleaned_upper in ["DECLINE", "NO BUSINESS", "CC", "NO BIZ", "#REF!", "TBD", ""] or not cell_text_cleaned_upper:
        # Create a basic entry even for these, with the value in po_percent and other details from header
        entry = base_header_details.copy()
        entry["cluster_code"] = rto_cluster_from_row
        entry["po_percent"] = cell_text_cleaned_upper if cell_text_cleaned_upper else cell_text_original # Keep original if just "CC" etc.
        entry["remark"] = base_header_details.get("remarks_col_header") # Only column header remark

        # Apply main table context if present
        if main_table_context_global:
            if main_table_context_global.get("veh_type_main") and not entry.get("veh_type"): entry["veh_type"] = main_table_context_global["veh_type_main"]
            if main_table_context_global.get("age_main") and not entry.get("age"): entry["age"] = main_table_context_global["age_main"]
            if main_table_context_global.get("plan_type_main") and not entry.get("plan_type"): entry["plan_type"] = main_table_context_global["plan_type_main"]
            # Main context bike makes are handled below in a loop for actual data rows
            current_remarks = [entry.get("remark")] if entry.get("remark") else []
            current_remarks.extend(main_table_context_global.get("remarks_main", []))
            entry["remark"] = " | ".join(list(dict.fromkeys(filter(None, current_remarks)))).strip() or None
        
        # If main table context has bike makes, create variants for these non-data cells too
        main_context_bike_makes = main_table_context_global.get("bike_makes_main", [None]) if main_table_context_global else [None]
        if not main_context_bike_makes: main_context_bike_makes = [None] # Ensure at least one iteration

        for mcbm in main_context_bike_makes:
            final_entry_for_non_data = entry.copy()
            if mcbm and not final_entry_for_non_data.get("bike_make"): # Only apply if not already set and mcbm exists
                final_entry_for_non_data["bike_make"] = mcbm
            results.append(final_entry_for_non_data)
        
        print(f"DEBUG: parse_percentage_cell_text: Cell is non-data value ('{cell_text_original}'). Created {len(results)} basic entries.")
        return results

    # --- Main parsing logic for data cells ---
    
    # Each item in segments_data will be a dict: {"po_percent": "X%", "associated_text": "text", "sub_segments": []}
    # Sub_segments are for "MAKE % OTHERS %" pattern.
    segments_data = []

    # Pattern for "MAKE X% OTHERS Y%" or "X% on MAKE OTHERS Y%"
    # This needs to capture the make, its percent, and the "others" percent
    # Example: "45% on TATA others 16%"
    # Example: "TATA 45% others 16%" (less common but possible)
    others_pattern_match = re.search(
        r"(?P<primary_text_before_others>.*?)\bOTHERS\s*(?P<others_percent>\d+(?:\.\d+)?%?)",
        cell_text_cleaned_upper, re.IGNORECASE
    )

    if others_pattern_match:
        print(f"DEBUG: parse_percentage_cell_text: Found 'OTHERS' pattern.")
        primary_text = others_pattern_match.group("primary_text_before_others").strip()
        others_po = others_pattern_match.group("others_percent")
        if '%' not in others_po: others_po += "%"

        # Process the primary part (before "OTHERS")
        primary_make = None
        primary_po = extract_pure_percentage_val(primary_text) # Extracts first % found
        
        potential_makes_in_primary = BIKE_MAKE_REGEX.findall(primary_text)
        if potential_makes_in_primary:
            primary_make = potential_makes_in_primary[0].upper() # Take first make for simplicity with "others"

        if primary_po: # If primary part has a percentage
            segments_data.append({
                "po_percent": primary_po,
                "associated_text": primary_text, # Full text for further parsing
                "specific_make_for_segment": primary_make # Explicit make for this segment
            })
        
        # Add the "OTHERS" segment
        segments_data.append({
            "po_percent": others_po,
            "associated_text": "OTHERS", # Associated text is just "OTHERS"
            "is_others_segment": True
        })
    else:
        # Standard segmentation by percentages if no "OTHERS" pattern detected at cell level
        # Find all percentages, then split the string by them to get associated text
        percent_indices = [(m.start(), m.end()) for m in re.finditer(r"\d+(?:\.\d+)?%?", cell_text_cleaned_upper)]
        
        if not percent_indices: # No percentages found at all
            # Treat the whole cell as a single "value" to be put in po_percent
            # This handles cases where the cell is just "Some Condition" without a clear %
            # Or if it's a malformed percentage like "TATA only"
            segments_data.append({"po_percent": cell_text_cleaned_orig_case, "associated_text": cell_text_cleaned_orig_case})
        else:
            for i in range(len(percent_indices)):
                start_char_idx_percent = percent_indices[i][0]
                end_char_idx_percent = percent_indices[i][1]
                
                current_po = cell_text_cleaned_upper[start_char_idx_percent:end_char_idx_percent]
                if '%' not in current_po: current_po += "%"

                # Associated text is from end of *previous* percentage (or start of cell) to start of *current* percentage
                # OR, from end of current percentage to start of *next* percentage.
                # Let's try: text from end of current percentage to start of next or end of cell
                
                start_assoc_text = end_char_idx_percent
                end_assoc_text = percent_indices[i+1][0] if i + 1 < len(percent_indices) else len(cell_text_cleaned_upper)
                
                assoc_text_segment = cell_text_cleaned_orig_case[start_assoc_text:end_assoc_text].strip(" ,()").strip()

                # Also consider text *before* the percentage if it's the first one and there's leading text
                if i == 0 and start_char_idx_percent > 0:
                    leading_text = cell_text_cleaned_orig_case[0:start_char_idx_percent].strip(" ,()").strip()
                    assoc_text_segment = (leading_text + " " + assoc_text_segment).strip()
                
                segments_data.append({"po_percent": current_po, "associated_text": assoc_text_segment if assoc_text_segment else ""})


    # Iterate through main context bike makes (e.g., from Table 2 header)
    # If no main context, this loop runs once with mcbm = None
    main_context_bike_makes = main_table_context_global.get("bike_makes_main", [None]) if main_table_context_global else [None]
    if not main_context_bike_makes: main_context_bike_makes = [None]

    for mcbm in main_context_bike_makes:
        print(f"DEBUG: parse_percentage_cell_text: Iterating with main_context_bike_make: {mcbm}")
        
        for seg_data in segments_data:
            current_details = base_header_details.copy() # Start fresh from column header for each segment
            current_details["cluster_code"] = rto_cluster_from_row # Set current RTO cluster
            
            # Apply main table context (will be overridden by more specific if found)
            if main_table_context_global:
                if main_table_context_global.get("veh_type_main") and not current_details.get("veh_type"): current_details["veh_type"] = main_table_context_global["veh_type_main"]
                if main_table_context_global.get("age_main") and not current_details.get("age"): current_details["age"] = main_table_context_global["age_main"]
                if main_table_context_global.get("plan_type_main") and not current_details.get("plan_type"): current_details["plan_type"] = main_table_context_global["plan_type_main"]
            
            current_details["po_percent"] = seg_data["po_percent"]
            associated_text_upper = seg_data["associated_text"].upper()
            associated_text_orig_case = seg_data["associated_text"] # For remarks

            # Initialize remarks list for this specific entry
            current_remarks_list = []
            if base_header_details.get("remarks_col_header"): current_remarks_list.append(base_header_details["remarks_col_header"])
            if main_table_context_global and main_table_context_global.get("remarks_main"):
                current_remarks_list.extend(main_table_context_global.get("remarks_main"))
            
            # Add the original cell text for this segment as a remark if it's not just keywords
            # Or if the segment associated text itself is the remark.
            # Heuristic: if associated_text is not empty and doesn't ONLY contain keywords.
            paren_remarks_in_assoc = re.findall(r"\((.*?)\)", associated_text_orig_case)
            for pr_assoc in paren_remarks_in_assoc:
                current_remarks_list.append(f"({pr_assoc})")

            # If the entire cell_text_original was not parsed into specific keywords, add it as a general remark.
            # This ensures complex unparsed conditions are captured.
            # Check if `associated_text_orig_case` (which is part of `cell_text_original`) is not fully represented
            # by keywords. This is complex. Simpler: add it if it's meaningful.
            # For now, the full original cell text will be added later if this path doesn't consume it.
            
            # --- Apply details from associated_text of the segment ---
            
            # Age from segment's associated text (overrides header/main context age)
            for age_pattern_re, age_val_resolver in AGE_KEYWORD_PATTERNS:
                age_match_cell = age_pattern_re.search(associated_text_upper)
                if age_match_cell:
                    current_details["age"] = age_val_resolver(age_match_cell) if callable(age_val_resolver) else age_val_resolver
                    break 
            
            # Plan Type from segment
            plan_type_match_cell = PLAN_TYPE_REGEX.search(associated_text_upper)
            if plan_type_match_cell:
                current_details["plan_type"] = PLAN_TYPE_KEYWORDS.get(plan_type_match_cell.group(1).upper())

            # Engine Type from segment
            for pattern_et_cell in ENGINE_TYPE_REGEX_PATTERNS:
                et_match_cell = pattern_et_cell.search(associated_text_upper)
                if et_match_cell:
                    current_details["engine_type"] = et_match_cell.group(0).upper().replace(" ","")
                    break

            # Fuel Type from segment
            fuel_match_cell = FUEL_TYPE_REGEX.search(associated_text_upper)
            if fuel_match_cell:
                current_details["fuel_type"] = fuel_match_cell.group(1).upper()

            # Special Cluster Code from segment (if "ONLY" is present)
            scc_match_cell = SPECIAL_CLUSTER_REGEX.search(associated_text_upper)
            if scc_match_cell and "ONLY" in associated_text_upper: # Check for "ONLY" with the SCC
                 current_details["cluster_code"] = scc_match_cell.group(1).upper() # Override RTO

            # --- Bike Make Logic: Cell Segment -> Column Header -> Main Context ---
            segment_bike_makes = []
            if seg_data.get("specific_make_for_segment"): # From "MAKE % OTHERS %"
                segment_bike_makes = [seg_data["specific_make_for_segment"]]
            elif not seg_data.get("is_others_segment", False): # Don't look for makes if it's an "others" segment
                 segment_bike_makes = list(set(bm.upper() for bm in BIKE_MAKE_REGEX.findall(associated_text_upper)))

            
            # Filter makes based on column header exclusions
            final_segment_bike_makes = [
                bm for bm in segment_bike_makes 
                if bm not in base_header_details.get("excluded_makes_col_header", [])
            ]
            
            # Bike make selection strategy:
            # 1. Use makes from the current cell segment if any.
            # 2. Else, use makes from the column header if any.
            # 3. Else, use the current main_context_bike_make (mcbm) if any (this is for Table 2 type headers).
            # 4. If none of the above, bike_make remains None (or its initial value from base_header_details if any).

            bike_makes_for_this_entry_iteration = [None] # Default for one iteration if no specific makes

            if final_segment_bike_makes:
                bike_makes_for_this_entry_iteration = final_segment_bike_makes
            elif base_header_details.get("header_bike_makes_list"): # From column header
                bike_makes_for_this_entry_iteration = base_header_details.get("header_bike_makes_list")
            elif mcbm: # From main table context (e.g. TATA or AL for second table)
                bike_makes_for_this_entry_iteration = [mcbm]
            
            # If it's an "others" segment, bike_make must be blank
            if seg_data.get("is_others_segment", False):
                current_details["bike_make"] = None 
                bike_makes_for_this_entry_iteration = [None] # Force one entry with no make for "others"
            
            # Exclusion for vehicles from column header
            if current_details.get("vehicle") and current_details.get("vehicle").upper() in base_header_details.get("excluded_vehicles_col_header", []):
                current_details["vehicle"] = None # Blank out excluded vehicle

            for bm_iteration in bike_makes_for_this_entry_iteration:
                final_entry = current_details.copy()
                if bm_iteration: # If a specific bike make is determined for this iteration
                    final_entry["bike_make"] = bm_iteration
                
                # Consolidate remarks just before appending
                # Add associated_text_orig_case if it's not already covered or just keywords
                # Heuristic: if it's short and made of keywords, it might be redundant.
                # For now, add it and rely on deduplication.
                if associated_text_orig_case:
                    current_remarks_list.append(associated_text_orig_case)

                # Add the original full cell text as a general remark if not too noisy
                # (this ensures unparsed complex conditions are captured)
                if cell_text_cleaned_orig_case:
                    current_remarks_list.append(f"(Cell: {cell_text_cleaned_orig_case})")


                final_entry["remark"] = " | ".join(list(dict.fromkeys(filter(None, current_remarks_list)))).strip() or None
                results.append(final_entry)
                print(f"DEBUG: parse_percentage_cell_text: Appended entry: {final_entry}")

    print(f"DEBUG: parse_percentage_cell_text: Total results for cell '{cell_text_original}' = {len(results)}")
    return results


def process_sheet(df_sheet, sheet_name):
    all_rows_for_sheet = []
    slab_month = extract_slab_month_from_df(df_sheet)
    print(f"INFO: process_sheet: Processing sheet: {sheet_name}, Slab Month: {slab_month}")

    header_row_idx_t1 = find_header_row(df_sheet, "RTO CLUSTER")
    if header_row_idx_t1 is None:
        print(f"WARN: process_sheet: RTO CLUSTER header not found for Table 1 in sheet {sheet_name}. Skipping.")
        return []

    try:
        rto_cluster_col_idx_t1 = df_sheet.iloc[header_row_idx_t1][df_sheet.iloc[header_row_idx_t1].astype(str).str.upper().str.strip().eq("RTO CLUSTER")].index[0]
    except IndexError:
        print(f"CRITICAL: Could not find 'RTO CLUSTER' column index in identified header row {header_row_idx_t1} for sheet {sheet_name}. Skipping sheet.")
        return []

    
    header_row_idx_t2 = None
    main_table2_context = None
    rto_cluster_col_idx_t2 = None
    
    # Scan for a second "RTO CLUSTER" header *after* the first one
    for i in range(header_row_idx_t1 + 1, len(df_sheet)):
        row_series = df_sheet.iloc[i]
        is_second_header = row_series.astype(str).str.upper().str.strip().eq("RTO CLUSTER").any()
        if is_second_header:
            header_row_idx_t2 = i
            try:
                rto_cluster_col_idx_t2 = row_series[row_series.astype(str).str.upper().str.strip().eq("RTO CLUSTER")].index[0]
            except IndexError:
                print(f"WARN: Found second 'RTO CLUSTER' text but couldn't get col index at row {i}. Skipping second table.")
                header_row_idx_t2 = None # Invalidate
                break

            # Try to find the main title for this second table (e.g., "MHCV-AOTP GRID...")
            # Search in rows above the second header, across a few columns usually near the start or where table 2 headers align
            title_search_start_row = max(0, header_row_idx_t2 - 5)
            for j_scan_main_header in range(title_search_start_row, header_row_idx_t2): 
                # Check a few columns, including where the second RTO cluster col is, and to its left
                scan_cols_for_title = list(range(max(0, rto_cluster_col_idx_t2 - 3), min(len(df_sheet.columns), rto_cluster_col_idx_t2 + 2)))
                if 1 not in scan_cols_for_title: scan_cols_for_title.append(1) # Ensure column B (index 1) is checked as per example

                for k_col_scan_main_header in scan_cols_for_title:
                    if j_scan_main_header < len(df_sheet) and k_col_scan_main_header < len(df_sheet.columns): # Bounds check
                        cell_val_scan = df_sheet.iloc[j_scan_main_header, k_col_scan_main_header]
                        cell_text_upper_scan = clean_text_general(cell_val_scan).upper()
                        if "GRID" in cell_text_upper_scan and ("MHCV" in cell_text_upper_scan or "LCV" in cell_text_upper_scan or "AOTP" in cell_text_upper_scan) :
                            main_table2_context = parse_main_table_header(str(cell_val_scan))
                            print(f"DEBUG: process_sheet: Found Main Table 2 Context Title: '{str(cell_val_scan)}' at ({j_scan_main_header},{k_col_scan_main_header})")
                            break 
                if main_table2_context: break
            break # Found second header, stop scanning for it

    print(f"INFO: process_sheet: Processing Table 1 (Header row: {header_row_idx_t1}, RTO Col: {rto_cluster_col_idx_t1})")
    end_row_t1 = header_row_idx_t2 if header_row_idx_t2 is not None else len(df_sheet)
    
    # Determine the end column for Table 1: it's before the RTO cluster of Table 2, or end of sheet
    end_col_t1 = rto_cluster_col_idx_t2 if header_row_idx_t2 is not None else len(df_sheet.columns)


    for i_row_t1 in range(header_row_idx_t1 + 1, end_row_t1):
        if i_row_t1 >= len(df_sheet): break # Safety break
        rto_cluster_val_t1_orig = df_sheet.iloc[i_row_t1, rto_cluster_col_idx_t1]
        rto_cluster_val_t1 = clean_text_general(rto_cluster_val_t1_orig).upper()
        
        # Stop processing rows for table 1 if we hit an empty RTO or what looks like another header
        if not rto_cluster_val_t1 or "RTO CLUSTER" in rto_cluster_val_t1: 
            print(f"DEBUG: process_sheet: Stopping Table 1 row processing at row {i_row_t1} due to empty/header-like RTO value: '{rto_cluster_val_t1_orig}'")
            break 

        for j_col_idx_t1, col_header_text_t1_orig in df_sheet.iloc[header_row_idx_t1].items():
            if j_col_idx_t1 <= rto_cluster_col_idx_t1 or j_col_idx_t1 >= end_col_t1: continue # Process only relevant columns for Table 1
            if pd.isna(col_header_text_t1_orig) or clean_text_general(str(col_header_text_t1_orig)) == "": continue

            base_details_t1 = parse_column_header_text(str(col_header_text_t1_orig))
            base_details_t1["slab_month"] = slab_month
            
            cell_value_t1_orig = df_sheet.iloc[i_row_t1, j_col_idx_t1]

            # Handle multiple fuel types from column header for Table 1
            fuel_types_from_header_t1 = base_details_t1.pop("found_fuel_types_col_header", []) # Get and remove
            if not fuel_types_from_header_t1: fuel_types_from_header_t1 = [base_details_t1.get("fuel_type")] # Use existing or None

            for ft_t1 in fuel_types_from_header_t1:
                current_base_details_t1_for_fuel = base_details_t1.copy()
                if ft_t1: current_base_details_t1_for_fuel["fuel_type"] = ft_t1 # Set specific fuel type

                parsed_rows_t1 = parse_percentage_cell_text(str(cell_value_t1_orig), current_base_details_t1_for_fuel, str(rto_cluster_val_t1_orig), None) # No main context for table 1
                all_rows_for_sheet.extend(parsed_rows_t1)

    if header_row_idx_t2 is not None and rto_cluster_col_idx_t2 is not None:
        print(f"INFO: process_sheet: Processing Table 2 (Header row: {header_row_idx_t2}, RTO Col: {rto_cluster_col_idx_t2}) with Main Context: {main_table2_context}")
        for i_row_t2 in range(header_row_idx_t2 + 1, len(df_sheet)):
            if i_row_t2 >= len(df_sheet): break # Safety break
            rto_cluster_val_t2_orig = df_sheet.iloc[i_row_t2, rto_cluster_col_idx_t2]
            rto_cluster_val_t2 = clean_text_general(rto_cluster_val_t2_orig).upper()
            
            if not rto_cluster_val_t2 or "RTO CLUSTER" in rto_cluster_val_t2: 
                print(f"DEBUG: process_sheet: Stopping Table 2 row processing at row {i_row_t2} due to empty/header-like RTO value: '{rto_cluster_val_t2_orig}'")
                break

            for j_col_idx_t2, col_header_text_t2_orig in df_sheet.iloc[header_row_idx_t2].items():
                if j_col_idx_t2 <= rto_cluster_col_idx_t2 : continue # Only process columns after Table 2's RTO Cluster
                if pd.isna(col_header_text_t2_orig) or clean_text_general(str(col_header_text_t2_orig)) == "": continue

                base_details_t2 = parse_column_header_text(str(col_header_text_t2_orig))
                base_details_t2["slab_month"] = slab_month
                
                cell_value_t2_orig = df_sheet.iloc[i_row_t2, j_col_idx_t2]
                
                fuel_types_from_header_t2 = base_details_t2.pop("found_fuel_types_col_header", [])
                if not fuel_types_from_header_t2: fuel_types_from_header_t2 = [base_details_t2.get("fuel_type")]

                for ft_t2 in fuel_types_from_header_t2:
                    current_base_details_t2_for_fuel = base_details_t2.copy()
                    if ft_t2: current_base_details_t2_for_fuel["fuel_type"] = ft_t2
                    
                    # Pass main_table2_context here
                    parsed_rows_t2 = parse_percentage_cell_text(str(cell_value_t2_orig), current_base_details_t2_for_fuel, str(rto_cluster_val_t2_orig), main_table2_context)
                    all_rows_for_sheet.extend(parsed_rows_t2)
                
    return all_rows_for_sheet

# --- Main Execution ---
if __name__ == "__main__":
    excel_file_path = input("Please provide the path to the ICICI CV grid Excel file: ")

    if not os.path.exists(excel_file_path):
        print(f"Error: File not found at {excel_file_path}")
    else:
        try:
            xls = pd.ExcelFile(excel_file_path)
            all_processed_data = []
            
            # Process only the first sheet as per user's script structure
            # If multiple sheets need processing, this list should be xls.sheet_names
            sheet_names_to_process = [xls.sheet_names[0]] 

            for sheet_name in sheet_names_to_process:
                print(f"INFO: Main: Reading sheet: {sheet_name}")
                # keep_default_na=False, na_filter=False helps treat empty strings as empty, not NaN by default for string columns
                df_sheet_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, keep_default_na=False, na_filter=False) 
                
                if df_sheet_raw.empty or len(df_sheet_raw) < 3: # Basic check
                    print(f"WARN: Main: Sheet '{sheet_name}' is empty or too small (less than 3 rows). Skipping.")
                    continue

                sheet_data_rows = process_sheet(df_sheet_raw, sheet_name)
                all_processed_data.extend(sheet_data_rows)

            if all_processed_data:
                output_df = pd.DataFrame(all_processed_data)
                
                # Ensure all OUTPUT_COLUMNS are present, add if missing
                for col in OUTPUT_COLUMNS:
                    if col not in output_df.columns:
                        output_df[col] = None # Initialize missing columns with None (or pd.NA)
                output_df = output_df[OUTPUT_COLUMNS] # Reorder/select columns

                output_filename = f"processed_{os.path.splitext(os.path.basename(excel_file_path))[0]}.xlsx"
                output_df.to_excel(output_filename, index=False)
                print(f"\nSuccessfully processed. Output saved to: {output_filename}")
            else:
                print("\nNo data processed or no valid entries generated. The output file was not created.")

        except Exception as e:
            print(f"An error occurred during processing: {e}")
            import traceback
            traceback.print_exc()