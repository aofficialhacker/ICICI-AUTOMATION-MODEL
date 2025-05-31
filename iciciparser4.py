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
    "TATA", "AL", "ASHOK LEYLAND", "M&M", "MAHINDRA", "EICHER", "MARUTI", "MARUTI SUZUKI", "MARUTI SUPER CARRY",
    "PIAGGIO", "BAJAJ", "ATUL", "TVS", "TOYOTA", "FORCE MOTORS", "SML ISUZU",
    "SWARAJ MAZDA", "HINDUSTAN MOTORS", "MAHINDRA NAVISTAR", "BHARATBENZ",
    "SCANIA", "VOLVO"
]
BIKE_MAKES = sorted(list(set(bm.upper() for bm in BIKE_MAKES_RAW)), key=len, reverse=True)
BIKE_MAKE_REGEX_STR = r"\b(" + "|".join(re.escape(bm) for bm in BIKE_MAKES) + r")\b"
BIKE_MAKE_REGEX = re.compile(BIKE_MAKE_REGEX_STR, re.IGNORECASE)


SPECIAL_CLUSTER_CODES_RAW = [ # From files and Bug 1 context
    "WB1", "DL", "NON DL RTO", "JK1 RTO", "GJ1 RTO", "UP1 EAST", "UK1 RTO", "UP EAST 1", "UP EAST1",
    "KA1 RTOS", "KA1 RTO", "TN10", "TN12", "TN02", "TN22", "TN04", "TN06", "TN09",
    "TN18", "TN19", "TN20", "TN11", "TN14",
    "KA01-05", "OD1", "PIMPRI", "PIMPRICHINCHWAD", "PIMPRI CHINCHWAD",
    "DELHI SURROUNDING RTO", "GJ1", "JK1"
]
SPECIAL_CLUSTER_CODES = sorted(list(set(scc.upper() for scc in SPECIAL_CLUSTER_CODES_RAW)), key=len, reverse=True)
SPECIAL_CLUSTER_REGEX_STR = r"\b(" + "|".join(re.escape(scc) for scc in SPECIAL_CLUSTER_CODES) + r")\b"
SPECIAL_CLUSTER_REGEX = re.compile(SPECIAL_CLUSTER_REGEX_STR, re.IGNORECASE)


VEHICLE_CATEGORIES_MAP = {
    "GCV": "GCV", "SCV": "GCV", "LCV": "GCV", "MHCV": "GCV",
    "PCV": "PCV", "PCVTAXI": "PCV", # Ensure PCVTAXI maps to PCV veh_type
    "MISC D CE": "MISC", "MIsc D CE": "MISC", "MIS D CE": "MISC", "MISC": "MISC"
}
VEHICLE_CATEGORY_REGEX_STR = r"\b(" + "|".join(re.escape(k) for k in VEHICLE_CATEGORIES_MAP.keys()) + r")\b"
VEHICLE_CATEGORY_REGEX = re.compile(VEHICLE_CATEGORY_REGEX_STR, re.IGNORECASE)
TAXI_MATCH_REGEX = re.compile(r"\bTAXI\b", re.IGNORECASE)


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
    re.compile(r"([<>]=?)\s*(\d+(?:\.\d+)?)\s*GVW", re.IGNORECASE),
    re.compile(r"(\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?)\s*T\b", re.IGNORECASE),
    re.compile(r"([<>]=?)\s*(\d+(?:\.\d+)?)\s*T\b", re.IGNORECASE),
]

SEATING_CAP_REGEX_PATTERNS_CONTEXTUAL = [
    (re.compile(r"(>\s*18\s*UPTO\s*36\s*SEATER)", re.IGNORECASE), lambda m: ">18 UPTO 36"),
    (re.compile(r"([<>]=?)\s*(\d+)\s*SEATER", re.IGNORECASE), lambda m: f"{m.group(1)}{m.group(2)}".replace(" ","")),
    (re.compile(r"([<>]=?)\s*(\d+)\b", re.IGNORECASE), lambda m: f"{m.group(1)}{m.group(2)}".replace(" ","")),
    (re.compile(r"(\d+\s*-\s*\d+)\b", re.IGNORECASE), lambda m: m.group(1).replace(" ","")),
]


ENGINE_TYPE_REGEX_PATTERNS = [
    re.compile(r"([<>]=?)\s*(\d+)\s*HP\b", re.IGNORECASE),
    re.compile(r"\bABOVE\s*(\d+)\s*HP\b", re.IGNORECASE),
    re.compile(r"([<>]=?)\s*(\d+)\s*CC\b", re.IGNORECASE),
]

PLAN_TYPE_KEYWORDS = {
    "AOTP": "SATP", "SATP": "SATP", "TP": "SATP",
    "ON OD": "SAOD", "OD": "SAOD",
    "COMP": "COMP"
}
PLAN_TYPE_REGEX_STR = r"\b(" + "|".join(re.escape(k) for k in PLAN_TYPE_KEYWORDS.keys()) + r")\b"
PLAN_TYPE_REGEX = re.compile(PLAN_TYPE_REGEX_STR, re.IGNORECASE)


# --- Helper Functions ---
def clean_text_general(text):
    if pd.isna(text) or text is None:
        return ""
    text = str(text)
    # First, replace \n and \r with a single space to handle multi-line cells
    text = text.replace('\n', ' ').replace('\r', ' ')
    # Then, collapse multiple spaces (resulting from normalization or original) into one
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
    for i in range(min(15, len(df))):
        for j in range(min(10, len(df.columns))):
            cell_text_cleaned = clean_text_general(df.iloc[i, j]).upper()
            match = re.search(r"CV\s*AGENCY\s*GRID\s*([A-Z]+(?:UARY|BRUARY|RCH|RIL|MAY|JUNE|JULY|GUST|TEMBER|TOBER|VEMBER|CEMBER)?\'?\s*\d{2,4})", cell_text_cleaned)
            if match:
                slab_month_raw = match.group(1)
                slab_month_normalized = slab_month_raw.replace("'", "").replace(" ", "")
                month_year_match = re.match(r"([A-Z]+)(\d+)", slab_month_normalized)
                if month_year_match:
                    month_part = month_year_match.group(1)[:3]
                    year_part = month_year_match.group(2)
                    if len(year_part) == 4: year_part = year_part[2:]
                    slab_month = f"{month_part.capitalize()}{year_part}"
                    print(f"DEBUG: extract_slab_month_from_df: Extracted slab_month: {slab_month}")
                    return slab_month
                print(f"DEBUG: extract_slab_month_from_df: Extracted slab_month (raw but normalized): {slab_month_normalized}")
                return slab_month_normalized
    print("DEBUG: extract_slab_month_from_df: Slab month not found.")
    return None


def extract_pure_percentage_val(text_segment): # Critical for Bug 3
    text_segment_cleaned = clean_text_general(text_segment)
    # Find a number (int or float) explicitly followed by %
    match_with_symbol = re.search(r"(\d+(?:\.\d+)?%)\b", text_segment_cleaned) # \b to ensure it's a standalone percentage
    if match_with_symbol:
        return match_with_symbol.group(1)
    
    # If no explicit %, try to find a number at the start of the segment that could be a percentage
    # This should be more restrictive
    match_start_num = re.match(r"^\s*(\d+(?:\.\d+)?)\b", text_segment_cleaned)
    if match_start_num:
        num_part = match_start_num.group(1)
        # Check that what follows isn't a unit that makes it NOT a percentage
        remaining_text = text_segment_cleaned[match_start_num.end():].strip().upper()
        if not remaining_text or not re.match(r"^(HP|CC|YRS|T\b|GVW|KM)", remaining_text): # Added KM, ensure T is whole word
            return f"{num_part}%" # Assume it's a percentage if no other unit indicator
            
    return None


def parse_main_table_header(header_text_full):
    context = {"bike_makes_main": [], "remarks_main": [], "age_main": None, "plan_type_main": None, "veh_type_main": None}
    text_cleaned_orig = clean_text_general(header_text_full)
    text_upper = text_cleaned_orig.upper()
    print(f"DEBUG: parse_main_table_header: Parsing main table header: '{header_text_full}' -> '{text_upper}'")

    veh_type_match = VEHICLE_CATEGORY_REGEX.search(text_upper)
    if veh_type_match:
        context["veh_type_main"] = VEHICLE_CATEGORIES_MAP.get(veh_type_match.group(1).upper())

    plan_type_match = PLAN_TYPE_REGEX.search(text_upper)
    if plan_type_match:
        context["plan_type_main"] = PLAN_TYPE_KEYWORDS.get(plan_type_match.group(1).upper())

    age_search_main = re.search(r"([<>]=?\s*\d+\s*(?:YRS?|YEARS?|AGE))", text_upper, re.IGNORECASE)
    if age_search_main:
        context["age_main"] = age_search_main.group(1).upper().replace("AGE","YRS").replace("YEARS","YRS").replace("YEAR","YRS").replace(" ","")
    
    bike_makes_found = BIKE_MAKE_REGEX.findall(text_upper)
    if bike_makes_found:
         context["bike_makes_main"] = list(set(bm.upper() for bm in bike_makes_found))

    all_remarks_main_header = []
    paren_remarks = re.findall(r"\((.*?)\)", text_cleaned_orig)
    for pr in paren_remarks:
        all_remarks_main_header.append(f"({pr})")
    
    temp_remark_check = text_cleaned_orig
    # Try to remove already parsed parts to see if significant text remains for remark
    if context.get("veh_type_main"): temp_remark_check = re.sub(re.escape(context["veh_type_main"]),"", temp_remark_check, flags=re.IGNORECASE)
    if context.get("plan_type_main"): temp_remark_check = re.sub(re.escape(context["plan_type_main"]),"", temp_remark_check, flags=re.IGNORECASE) #This might be too broad
    if context.get("age_main"): temp_remark_check = re.sub(re.escape(context["age_main"].replace(">","").replace("<","").replace("=","")),"", temp_remark_check, flags=re.IGNORECASE) #remove symbols for replace
    for bm in context.get("bike_makes_main",[]): temp_remark_check = re.sub(r'\b'+re.escape(bm)+r'\b',"", temp_remark_check, flags=re.IGNORECASE)
    temp_remark_check = temp_remark_check.replace("GRID","").replace("ONLY","").replace("AND","").replace("&","").replace("(","").replace(")","").strip(" ,")

    if len(temp_remark_check) > 3 : # If meaningful text remains
        all_remarks_main_header.append(text_cleaned_orig) # Add the original full text as it has more info

    context["remarks_main"] = list(set(all_remarks_main_header))
        
    print(f"DEBUG: parse_main_table_header: Parsed main table header context: {context}")
    return context

def parse_column_header_text(header_cell_text_original):
    base_details = defaultdict(lambda: None)
    text_original_cleaned = clean_text_general(header_cell_text_original)
    text_upper = text_original_cleaned.upper()
    
    base_details["remarks_col_header_list"] = []
    print(f"DEBUG: parse_column_header_text: Parsing column header: '{header_cell_text_original}' -> '{text_upper}'")

    base_details["product_type"] = "COMMERCIAL VEHICLE"

    exclusion_triggered = False
    excluded_makes_in_header = []
    excluded_vehicles_in_header = []
    exclusion_keywords = ["EXCLUDING", "EXCEPT"]
    for ex_kw in exclusion_keywords:
        if ex_kw in text_upper:
            exclusion_triggered = True
            ex_match_remark = re.search(rf"(\b{ex_kw}[^\(\)]*\b)", text_original_cleaned, re.IGNORECASE)
            if ex_match_remark:
                 base_details["remarks_col_header_list"].append(ex_match_remark.group(1).strip())
            parts_after_exclusion = text_upper.split(ex_kw, 1)
            if len(parts_after_exclusion) > 1:
                text_after_exclusion = parts_after_exclusion[1]
                excluded_makes_in_header.extend(bm.upper() for bm in BIKE_MAKE_REGEX.findall(text_after_exclusion))
                excluded_vehicles_in_header.extend(sv.upper() for sv in SPECIFIC_VEHICLE_REGEX.findall(text_after_exclusion))
    base_details["excluded_makes_col_header"] = list(set(excluded_makes_in_header))
    base_details["excluded_vehicles_col_header"] = list(set(excluded_vehicles_in_header))

    veh_type_match = VEHICLE_CATEGORY_REGEX.search(text_upper)
    if veh_type_match:
        cat_key = veh_type_match.group(1).upper()
        base_details["veh_type"] = VEHICLE_CATEGORIES_MAP.get(cat_key)
        if base_details["veh_type"] == "PCV": base_details["product_type"] = "PASSENGER CARRYING VEHICLE"
        elif base_details["veh_type"] == "MISC":
            base_details["product_type"] = "MISCELLANEOUS VEHICLE"
            if "CE" in cat_key: base_details["vehicle"] = "CE"
    
    # Specific Vehicle (can refine veh_type and product_type)
    # And handle Tipper/Dumper by creating multiple entries at process_sheet level
    specific_vehicle_matches_raw = SPECIFIC_VEHICLE_REGEX.findall(text_upper)
    header_vehicles_list = []
    if specific_vehicle_matches_raw:
        for sv_match_raw in specific_vehicle_matches_raw:
            vehicle_parts_from_match = [p.strip().upper() for p in sv_match_raw.upper().split('/') if p.strip()]
            for vp_upper in vehicle_parts_from_match:
                if vp_upper in SPECIFIC_VEHICLES and not (exclusion_triggered and vp_upper in base_details["excluded_vehicles_col_header"]):
                    header_vehicles_list.append(vp_upper)
    
    base_details["header_specific_vehicles_list"] = list(set(header_vehicles_list))

    if base_details["header_specific_vehicles_list"]:
        primary_vehicle_from_header = base_details["header_specific_vehicles_list"][0] # Use the first one as default for base_details
        base_details["vehicle"] = primary_vehicle_from_header
        if "BUS" in primary_vehicle_from_header: base_details["veh_type"] = "PCV"; base_details["product_type"] = "PASSENGER CARRYING VEHICLE"
        elif primary_vehicle_from_header in ["CRANES", "TRACTOR", "TRACTER", "BACKHOELOADER", "CE"]:
             base_details["veh_type"] = "MISC"; base_details["product_type"] = "MISCELLANEOUS VEHICLE"
        elif primary_vehicle_from_header == "TAXI": base_details["veh_type"] = "PCV"; base_details["product_type"] = "PASSENGER CARRYING VEHICLE"

    # Specifically check for TAXI if not caught, especially for PCVTAXI_ELECTRIC
    if TAXI_MATCH_REGEX.search(text_upper): # Using the new TAXI_MATCH_REGEX
        if not (exclusion_triggered and "TAXI" in base_details["excluded_vehicles_col_header"]):
            base_details["vehicle"] = "TAXI" # Override or set
            base_details["veh_type"] = "PCV"  # Ensure veh_type is PCV for TAXI
            base_details["product_type"] = "PASSENGER CARRYING VEHICLE"


    if not base_details.get("vehicle"):
        if re.search(r"\b3W\b", text_upper): base_details["vehicle"] = "3W"
        elif re.search(r"\b2W\b", text_upper): base_details["vehicle"] = "2W"

    for age_pattern, age_val_fixed in AGE_KEYWORD_PATTERNS:
        if isinstance(age_val_fixed, str) and age_val_fixed in ["NEW", "OLD"]:
            if age_pattern.search(text_upper):
                base_details["age"] = age_val_fixed
                break
    
    base_details["found_fuel_types_col_header"] = list(set(ft.upper() for ft in FUEL_TYPE_REGEX.findall(text_upper)))

    for pattern in GVW_REGEX_PATTERNS:
        gvw_match = pattern.search(text_upper)
        if gvw_match:
            base_details["gvw"] = "".join(g.strip() for g in gvw_match.groups() if g).upper()
            break
    
    is_bus_type_header = base_details.get("veh_type") == "PCV" or \
                         (base_details.get("vehicle") and "BUS" in base_details.get("vehicle","").upper()) or \
                         ("BUS" in text_upper or "SEATER" in text_upper)
    if is_bus_type_header:
        text_to_search_seating_h = text_upper
        bus_seater_keyword_match_h = re.search(r"\b(BUS|SEATER)\b", text_upper, re.IGNORECASE)
        if bus_seater_keyword_match_h:
            text_to_search_seating_h = text_upper[bus_seater_keyword_match_h.end():]
        for pattern, resolver in SEATING_CAP_REGEX_PATTERNS_CONTEXTUAL:
            sc_match_h = pattern.search(text_to_search_seating_h)
            if sc_match_h:
                base_details["seating_cap"] = resolver(sc_match_h)
                break
    for pattern_et in ENGINE_TYPE_REGEX_PATTERNS:
        et_match = pattern_et.search(text_upper)
        if et_match:
            base_details["engine_type"] = et_match.group(0).upper().replace(" ","")
            break
            
    bracket_remarks = re.findall(r"\((.*?)\)", text_original_cleaned)
    for br in bracket_remarks:
        if not BIKE_MAKE_REGEX.fullmatch(br.strip().upper()):
             base_details["remarks_col_header_list"].append(f"({br})")

    header_bike_makes_found = BIKE_MAKE_REGEX.findall(text_upper)
    valid_header_bike_makes = []
    if header_bike_makes_found:
        for hbm_found_upper in set(bm.upper() for bm in header_bike_makes_found):
            if not (exclusion_triggered and hbm_found_upper in base_details["excluded_makes_col_header"]):
                valid_header_bike_makes.append(hbm_found_upper)
        if valid_header_bike_makes:
             base_details["header_bike_makes_list"] = valid_header_bike_makes

    final_details = {k: v for k, v in base_details.items() if v is not None and v != []}
    if "remarks_col_header_list" in final_details:
        final_details["remarks_col_header"] = " | ".join(list(dict.fromkeys(final_details["remarks_col_header_list"]))).strip() or None
        del final_details["remarks_col_header_list"]
    else:
        final_details["remarks_col_header"] = None
    
    print(f"DEBUG: parse_column_header_text: Parsed column header details: {dict(final_details)}")
    return dict(final_details)


def parse_percentage_cell_text(cell_text_original, base_header_details, rto_cluster_from_row, main_table_context_global):
    results = []
    cell_text_cleaned_orig_case = clean_text_general(cell_text_original)
    cell_text_cleaned_upper = cell_text_cleaned_orig_case.upper()
    
    print(f"DEBUG: parse_percentage_cell_text: Parsing cell text: '{cell_text_original}' (Cleaned Upper: '{cell_text_cleaned_upper}') for RTO: {rto_cluster_from_row}")
    #print(f"DEBUG: parse_percentage_cell_text: Base header details: {base_header_details}")
    #print(f"DEBUG: parse_percentage_cell_text: Main table context (if any): {main_table_context_global}")

    non_data_values = ["DECLINE", "NO BUSINESS", "CC", "NO BIZ", "#REF!", "TBD", "IRDA"]
    if cell_text_cleaned_upper in non_data_values or not cell_text_cleaned_upper :
        entry = base_header_details.copy()
        entry["cluster_code"] = rto_cluster_from_row
        entry["po_percent"] = cell_text_cleaned_upper if cell_text_cleaned_upper else cell_text_original
        
        current_remarks_list = [base_header_details.get("remarks_col_header")]
        if main_table_context_global and main_table_context_global.get("remarks_main"):
            current_remarks_list.extend(main_table_context_global.get("remarks_main"))
        if cell_text_cleaned_orig_case and cell_text_cleaned_orig_case not in non_data_values: # Bug 2 refinement
             current_remarks_list.append(f"(Cell Condition: {cell_text_cleaned_orig_case})")
        entry["remark"] = " | ".join(list(dict.fromkeys(filter(None, current_remarks_list)))).strip() or None

        main_context_bike_makes = main_table_context_global.get("bike_makes_main", [None]) if main_table_context_global else [None]
        if not main_context_bike_makes: main_context_bike_makes = [None]

        for mcbm in main_context_bike_makes:
            final_entry_for_non_data = entry.copy()
            if mcbm and not final_entry_for_non_data.get("bike_make"):
                final_entry_for_non_data["bike_make"] = mcbm
            if final_entry_for_non_data.get("bike_make") and final_entry_for_non_data.get("bike_make").upper() in base_header_details.get("excluded_makes_col_header",[]):
                final_entry_for_non_data["bike_make"] = None
            if final_entry_for_non_data.get("vehicle") and final_entry_for_non_data.get("vehicle").upper() in base_header_details.get("excluded_vehicles_col_header",[]):
                final_entry_for_non_data["vehicle"] = None
            results.append(final_entry_for_non_data)
        print(f"DEBUG: parse_percentage_cell_text: Cell is non-data value ('{cell_text_original}'). Created {len(results)} basic entries.")
        return results

    all_segments_from_cell = []
    lines_in_cell_cleaned = [line.strip() for line in cell_text_cleaned_orig_case.splitlines() if line.strip()]
    if not lines_in_cell_cleaned: # If cell was e.g. just newlines
        lines_in_cell_cleaned = [cell_text_cleaned_orig_case] # Use the original cleaned cell text as one line

    for line_idx, line_text_orig_case in enumerate(lines_in_cell_cleaned):
        line_text_upper = line_text_orig_case.upper()
        if not line_text_upper: continue
        print(f"DEBUG: parse_percentage_cell_text: Processing Line {line_idx+1}: '{line_text_orig_case}'")

        # Revised segmentation: find all percentages, then determine text between them
        percent_match_objects = list(re.finditer(r"(\d+(?:\.\d+)?%?)\b", line_text_upper)) # Capture with optional % but add later if missing

        current_pos_in_line = 0
        for i, p_match_obj in enumerate(percent_match_objects):
            percent_val_raw = p_match_obj.group(1)
            percent_val = percent_val_raw if "%" in percent_val_raw else percent_val_raw + "%"
            
            # Text before this percentage, from end of last segment on this line
            text_before_this_percent = line_text_orig_case[current_pos_in_line : p_match_obj.start()].strip(" ,()")
            
            # Text after this percentage, until start of next or end of line
            text_after_this_percent_end_idx = percent_match_objects[i+1].start() if i + 1 < len(percent_match_objects) else len(line_text_orig_case)
            text_after_this_percent = line_text_orig_case[p_match_obj.end() : text_after_this_percent_end_idx].strip(" ,()")
            
            segment_associated_text = (text_before_this_percent + " " + text_after_this_percent).strip()
            all_segments_from_cell.append({"po_percent": percent_val, "associated_text": segment_associated_text})
            
            current_pos_in_line = text_after_this_percent_end_idx # Next segment on this line starts after this one's text
        
        # If line had no percentages, but has text, it's a condition line
        if not percent_match_objects and line_text_orig_case:
            all_segments_from_cell.append({"po_percent": None, "associated_text": line_text_orig_case, "is_condition_line": True})


    general_conditions_from_cell = {}
    final_percent_segments_to_process = []

    for seg in all_segments_from_cell:
        if seg.get("is_condition_line"):
            cond_text_upper = seg["associated_text"].upper()
            # Age from condition line (Bug 4 - Hierarchy)
            if not general_conditions_from_cell.get("age_cond"): # Store separately to allow override decisions
                for age_p, age_r in AGE_KEYWORD_PATTERNS:
                    age_m = age_p.search(cond_text_upper)
                    if age_m: general_conditions_from_cell["age_cond"] = age_r(age_m) if callable(age_r) else age_r; break
            # Make from condition line
            if not general_conditions_from_cell.get("bike_make_cond"):
                bm_cond = BIKE_MAKE_REGEX.findall(cond_text_upper)
                if bm_cond: general_conditions_from_cell["bike_make_cond"] = bm_cond[0].upper()
            # Cluster from condition line
            if not general_conditions_from_cell.get("cluster_code_cond"):
                 scc_cond = SPECIAL_CLUSTER_REGEX.search(cond_text_upper)
                 # Bug 1: Check for ONLY or IN for cluster override from cell
                 if scc_cond and ("ONLY" in cond_text_upper or re.search(r"\bIN\s+" + re.escape(scc_cond.group(1).upper()) + r"\b", cond_text_upper)):
                     general_conditions_from_cell["cluster_code_cond"] = scc_cond.group(1).upper()
            if seg["associated_text"] and not general_conditions_from_cell.get("cond_line_remark_list"):
                general_conditions_from_cell["cond_line_remark_list"] = [seg["associated_text"]]
            elif seg["associated_text"] :
                general_conditions_from_cell["cond_line_remark_list"].append(seg["associated_text"])

        elif seg.get("po_percent"):
            # Bug 6: If po_percent is actually an RTO name, skip
            if seg.get("po_percent", "").upper().replace("%", "") in [r.upper() for r in SPECIAL_CLUSTER_CODES + [rto_cluster_from_row]]: # Check against known RTOs
                print(f"DEBUG: Skipping segment with RTO name as po_percent: {seg.get('po_percent')}")
                continue
            final_percent_segments_to_process.append(seg)
    
    if not final_percent_segments_to_process and not general_conditions_from_cell: # Cell like "TATA" or "0.15" without context parsed
        if extract_pure_percentage_val(cell_text_cleaned_orig_case):
             final_percent_segments_to_process.append({"po_percent": extract_pure_percentage_val(cell_text_cleaned_orig_case), "associated_text": ""})
        elif BIKE_MAKE_REGEX.fullmatch(cell_text_cleaned_upper.strip()): # If cell is JUST a bike make
            general_conditions_from_cell["bike_make_cond"] = cell_text_cleaned_upper.strip()
        else: # Treat as unparsed remark / value for po_percent
             final_percent_segments_to_process.append({"po_percent": cell_text_cleaned_orig_case, "associated_text": ""})


    main_context_bike_makes_list = main_table_context_global.get("bike_makes_main", [None]) if main_table_context_global else [None]
    if not main_context_bike_makes_list: main_context_bike_makes_list = [None]

    for mcbm in main_context_bike_makes_list:
        print(f"DEBUG: parse_percentage_cell_text: Iterating with main_context_bike_make: {mcbm}")
        
        for seg_data_final in final_percent_segments_to_process:
            current_details = base_header_details.copy()
            current_details["cluster_code"] = rto_cluster_from_row
            
            # Hierarchy: 1. Main Table Context (lowest prio for overridable fields)
            if main_table_context_global: # Bug 7
                if main_table_context_global.get("veh_type_main"): current_details["veh_type"] = main_table_context_global.get("veh_type_main")
                if main_table_context_global.get("age_main"): current_details["age"] = main_table_context_global.get("age_main")
                if main_table_context_global.get("plan_type_main"): current_details["plan_type"] = main_table_context_global.get("plan_type_main")
                # Main context bike make (mcbm) applied later if no other make found

            # Hierarchy: 2. General conditions from cell lines
            if general_conditions_from_cell.get("age_cond"): current_details["age"] = general_conditions_from_cell.get("age_cond")
            if general_conditions_from_cell.get("bike_make_cond"): current_details["bike_make"] = general_conditions_from_cell.get("bike_make_cond") # This is a general make for cell
            if general_conditions_from_cell.get("cluster_code_cond"): current_details["cluster_code"] = general_conditions_from_cell.get("cluster_code_cond")


            current_details["po_percent"] = seg_data_final["po_percent"]
            associated_text_segment_orig = seg_data_final["associated_text"]
            associated_text_segment_upper = associated_text_segment_orig.upper()

            current_remarks_list = []
            if base_header_details.get("remarks_col_header"): current_remarks_list.append(base_header_details.get("remarks_col_header"))
            if main_table_context_global and main_table_context_global.get("remarks_main"):
                current_remarks_list.extend(main_table_context_global.get("remarks_main"))
            if general_conditions_from_cell.get("cond_line_remark_list"):
                current_remarks_list.extend(general_conditions_from_cell.get("cond_line_remark_list"))
            
            if associated_text_segment_orig: # Add segment specific text
                current_remarks_list.append(associated_text_segment_orig)
            
            # --- Hierarchy 3: Details from segment's associated_text (OVERRIDE) --- (Bug 4)
            age_override_found_in_segment = False
            for age_pattern_re, age_val_resolver in AGE_KEYWORD_PATTERNS:
                age_match_seg = age_pattern_re.search(associated_text_segment_upper)
                if age_match_seg:
                    current_details["age"] = age_val_resolver(age_match_seg) if callable(age_val_resolver) else age_val_resolver
                    age_override_found_in_segment = True; break
            
            plan_type_match_seg = PLAN_TYPE_REGEX.search(associated_text_segment_upper)
            if plan_type_match_seg: current_details["plan_type"] = PLAN_TYPE_KEYWORDS.get(plan_type_match_seg.group(1).upper())

            for pattern_et_seg in ENGINE_TYPE_REGEX_PATTERNS:
                et_match_seg = pattern_et_seg.search(associated_text_segment_upper)
                if et_match_seg: current_details["engine_type"] = et_match_seg.group(0).upper().replace(" ",""); break

            fuel_match_seg = FUEL_TYPE_REGEX.search(associated_text_segment_upper)
            if fuel_match_seg: current_details["fuel_type"] = fuel_match_seg.group(1).upper()

            scc_match_seg = SPECIAL_CLUSTER_REGEX.search(associated_text_segment_upper)
            if scc_match_seg: # Bug 1 refinement
                if "ONLY" in associated_text_segment_upper or re.search(r"\bIN\s+" + re.escape(scc_match_seg.group(1).upper()) + r"\b", associated_text_segment_upper):
                    current_details["cluster_code"] = scc_match_seg.group(1).upper()

            # --- Bike Make Logic for this segment ---
            segment_bike_makes_list_final = [bm.upper() for bm in BIKE_MAKE_REGEX.findall(associated_text_segment_upper)]
            valid_segment_bike_makes_final = [bm for bm in segment_bike_makes_list_final if bm not in base_header_details.get("excluded_makes_col_header", [])]
            
            bike_makes_to_iterate_for_final_entry = [None]

            if valid_segment_bike_makes_final: # Priority: Segment Specific Makes
                bike_makes_to_iterate_for_final_entry = valid_segment_bike_makes_final
            elif current_details.get("bike_make"): # From general cell condition or header
                 bike_makes_to_iterate_for_final_entry = [current_details.get("bike_make")]
            elif mcbm: # From main table context
                bike_makes_to_iterate_for_final_entry = [mcbm]
            
            # Vehicle: Apply column header exclusions
            if current_details.get("vehicle") and current_details.get("vehicle").upper() in base_header_details.get("excluded_vehicles_col_header", []):
                current_details["vehicle"] = None

            for bm_final_iter_val in bike_makes_to_iterate_for_final_entry:
                final_entry = current_details.copy() # Create a new copy for each bike make iteration
                if bm_final_iter_val:
                    final_entry["bike_make"] = bm_final_iter_val
                
                # Add full original cell text as a remark for context if not too simple (Bug 2 refinement)
                final_remarks_for_entry = current_remarks_list[:] # copy
                if cell_text_cleaned_orig_case:
                    is_simple_val = (cell_text_cleaned_orig_case == final_entry["po_percent"]) or \
                                    (cell_text_cleaned_orig_case.upper() in non_data_values)
                    if not is_simple_val and cell_text_cleaned_orig_case not in final_remarks_for_entry :
                         final_remarks_for_entry.append(f"(Full Cell: {cell_text_cleaned_orig_case})")

                final_entry["remark"] = " | ".join(list(dict.fromkeys(filter(None, final_remarks_for_entry)))).strip() or None
                results.append(final_entry)
                print(f"DEBUG: parse_percentage_cell_text: Appended final entry: {final_entry}")

    print(f"DEBUG: parse_percentage_cell_text: Total results for cell '{cell_text_original}' = {len(results)}")
    return results

# ... (process_sheet and main remain largely the same as your provided "works good" version, but with calls to the updated parsing functions)
# Make sure process_sheet correctly uses the new return structure from parse_column_header_text regarding lists of vehicles/fuels.

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
    
    for i_scan_t2_header in range(header_row_idx_t1 + 1, len(df_sheet)):
        row_series_scan_t2 = df_sheet.iloc[i_scan_t2_header]
        if row_series_scan_t2.astype(str).str.upper().str.strip().eq("RTO CLUSTER").any():
            header_row_idx_t2 = i_scan_t2_header
            try:
                rto_cluster_col_idx_t2 = row_series_scan_t2[row_series_scan_t2.astype(str).str.upper().str.strip().eq("RTO CLUSTER")].index[0]
            except IndexError: header_row_idx_t2 = None; break

            title_search_start_row_t2 = max(0, header_row_idx_t2 - 5) # Search up to 5 rows above
            for j_scan_title_t2 in range(title_search_start_row_t2, header_row_idx_t2): 
                # Scan a few columns around the expected area of the title
                scan_cols_for_title_t2 = list(range(max(0, rto_cluster_col_idx_t2 - 5), min(len(df_sheet.columns), rto_cluster_col_idx_t2 + 5)))
                if 1 not in scan_cols_for_title_t2 and 1 < len(df_sheet.columns): scan_cols_for_title_t2.insert(0,1) # Check column B (index 1)

                for k_col_title_t2 in scan_cols_for_title_t2:
                    if j_scan_title_t2 < len(df_sheet) and k_col_title_t2 < df_sheet.shape[1]: # Bounds check
                        cell_val_title_t2 = df_sheet.iloc[j_scan_title_t2, k_col_title_t2]
                        cell_text_title_t2_upper = clean_text_general(cell_val_title_t2).upper()
                        # More specific title match for table 2
                        if "GRID" in cell_text_title_t2_upper and ("MHCV" in cell_text_title_t2_upper or "LCV" in cell_text_title_t2_upper or "AOTP" in cell_text_title_t2_upper or "TATA & AL ONLY" in cell_text_title_t2_upper) :
                            main_table2_context = parse_main_table_header(str(cell_val_title_t2))
                            print(f"DEBUG: process_sheet: Found Main Table 2 Context Title: '{str(cell_val_title_t2)}' at ({j_scan_title_t2},{k_col_title_t2})")
                            break 
                if main_table2_context: break
            break

    print(f"INFO: process_sheet: Processing Table 1 (Header row: {header_row_idx_t1}, RTO Col: {rto_cluster_col_idx_t1})")
    end_row_t1 = header_row_idx_t2 if header_row_idx_t2 is not None else len(df_sheet)
    end_col_t1 = rto_cluster_col_idx_t2 if header_row_idx_t2 is not None and rto_cluster_col_idx_t2 is not None else len(df_sheet.columns)

    for i_row_t1 in range(header_row_idx_t1 + 1, end_row_t1):
        if i_row_t1 >= len(df_sheet): break
        rto_cluster_val_t1_orig = df_sheet.iloc[i_row_t1, rto_cluster_col_idx_t1]
        rto_cluster_val_t1_cleaned = clean_text_general(rto_cluster_val_t1_orig)
        if not rto_cluster_val_t1_cleaned or "RTO CLUSTER" in rto_cluster_val_t1_cleaned.upper(): break 

        for j_col_idx_t1 in range(len(df_sheet.columns)):
            if j_col_idx_t1 <= rto_cluster_col_idx_t1 or j_col_idx_t1 >= end_col_t1: continue
            col_header_text_t1_orig = df_sheet.iloc[header_row_idx_t1, j_col_idx_t1]
            if pd.isna(col_header_text_t1_orig) or clean_text_general(str(col_header_text_t1_orig)) == "": continue

            base_details_t1 = parse_column_header_text(str(col_header_text_t1_orig))
            base_details_t1["slab_month"] = slab_month
            
            cell_value_t1_orig = df_sheet.iloc[i_row_t1, j_col_idx_t1]

            fuel_types_from_header_t1 = base_details_t1.pop("found_fuel_types_col_header", [])
            if not fuel_types_from_header_t1: fuel_types_from_header_t1 = [base_details_t1.get("fuel_type")]

            specific_vehicles_from_header_t1 = base_details_t1.pop("header_specific_vehicles_list", [])
            if not specific_vehicles_from_header_t1: specific_vehicles_from_header_t1 = [base_details_t1.get("vehicle")]


            for spec_veh_t1 in specific_vehicles_from_header_t1:
                for ft_t1 in fuel_types_from_header_t1:
                    current_base_details_t1_for_iter = base_details_t1.copy()
                    if spec_veh_t1: current_base_details_t1_for_iter["vehicle"] = spec_veh_t1
                    if ft_t1: current_base_details_t1_for_iter["fuel_type"] = ft_t1
                    
                    parsed_rows_t1 = parse_percentage_cell_text(str(cell_value_t1_orig), current_base_details_t1_for_iter, str(rto_cluster_val_t1_orig), None)
                    all_rows_for_sheet.extend(parsed_rows_t1)

    if header_row_idx_t2 is not None and rto_cluster_col_idx_t2 is not None:
        print(f"INFO: process_sheet: Processing Table 2 (Header row: {header_row_idx_t2}, RTO Col: {rto_cluster_col_idx_t2}) with Main Context: {main_table2_context}")
        for i_row_t2 in range(header_row_idx_t2 + 1, len(df_sheet)):
            if i_row_t2 >= len(df_sheet): break
            rto_cluster_val_t2_orig = df_sheet.iloc[i_row_t2, rto_cluster_col_idx_t2]
            rto_cluster_val_t2_cleaned = clean_text_general(rto_cluster_val_t2_orig)
            if not rto_cluster_val_t2_cleaned or "RTO CLUSTER" in rto_cluster_val_t2_cleaned.upper(): break

            for j_col_idx_t2 in range(len(df_sheet.columns)):
                if j_col_idx_t2 <= rto_cluster_col_idx_t2 : continue
                col_header_text_t2_orig = df_sheet.iloc[header_row_idx_t2, j_col_idx_t2]
                if pd.isna(col_header_text_t2_orig) or clean_text_general(str(col_header_text_t2_orig)) == "": continue

                base_details_t2 = parse_column_header_text(str(col_header_text_t2_orig))
                base_details_t2["slab_month"] = slab_month
                
                cell_value_t2_orig = df_sheet.iloc[i_row_t2, j_col_idx_t2]
                
                fuel_types_from_header_t2 = base_details_t2.pop("found_fuel_types_col_header", [])
                if not fuel_types_from_header_t2: fuel_types_from_header_t2 = [base_details_t2.get("fuel_type")]

                specific_vehicles_from_header_t2 = base_details_t2.pop("header_specific_vehicles_list", [])
                if not specific_vehicles_from_header_t2: specific_vehicles_from_header_t2 = [base_details_t2.get("vehicle")]

                for spec_veh_t2 in specific_vehicles_from_header_t2:
                    for ft_t2 in fuel_types_from_header_t2:
                        current_base_details_t2_for_iter = base_details_t2.copy()
                        if spec_veh_t2: current_base_details_t2_for_iter["vehicle"] = spec_veh_t2
                        if ft_t2: current_base_details_t2_for_iter["fuel_type"] = ft_t2
                        
                        parsed_rows_t2 = parse_percentage_cell_text(str(cell_value_t2_orig), current_base_details_t2_for_iter, str(rto_cluster_val_t2_orig), main_table2_context)
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
            
            sheet_names_to_process = [xls.sheet_names[0]] 

            for sheet_name in sheet_names_to_process:
                print(f"INFO: Main: Reading sheet: {sheet_name}")
                df_sheet_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, keep_default_na=False, na_filter=False) 
                
                if df_sheet_raw.empty or len(df_sheet_raw) < 3:
                    print(f"WARN: Main: Sheet '{sheet_name}' is empty or too small. Skipping.")
                    continue

                sheet_data_rows = process_sheet(df_sheet_raw, sheet_name)
                all_processed_data.extend(sheet_data_rows)

            if all_processed_data:
                output_df = pd.DataFrame(all_processed_data)
                
                for col in OUTPUT_COLUMNS:
                    if col not in output_df.columns:
                        output_df[col] = None 
                output_df = output_df[OUTPUT_COLUMNS] 

                output_filename = f"processed_{os.path.splitext(os.path.basename(excel_file_path))[0]}.xlsx"
                output_df.to_excel(output_filename, index=False)
                print(f"\nSuccessfully processed. Output saved to: {output_filename}")
            else:
                print("\nNo data processed. The output file was not created.")

        except Exception as e:
            print(f"An error occurred: {e}")
            import traceback
            traceback.print_exc()