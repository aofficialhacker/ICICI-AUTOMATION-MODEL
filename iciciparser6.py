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


SPECIAL_CLUSTER_CODES_RAW = [
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
    "PCV": "PCV", "PCVTAXI": "PCV",
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

def extract_explicit_percentage(text_segment):
    match = re.search(r"(\d+(?:\.\d+)?%)", text_segment)
    if match:
        return match.group(1)
    num_match = re.match(r"^\s*(\d+(?:\.\d+)?)\s*$", text_segment)
    if num_match:
        return num_match.group(1) + "%"
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
    
    bike_makes_found_tuples = BIKE_MAKE_REGEX.findall(text_upper) # findall returns list of tuples if regex has groups
    if bike_makes_found_tuples:
         context["bike_makes_main"] = list(set((bm[0] if isinstance(bm, tuple) else bm).upper() for bm in bike_makes_found_tuples))


    all_remarks_main_header = []
    paren_remarks = re.findall(r"\((.*?)\)", text_cleaned_orig)
    for pr in paren_remarks:
        all_remarks_main_header.append(f"({pr})")
    
    temp_remark_check = text_cleaned_orig
    if context.get("veh_type_main"): temp_remark_check = re.sub(re.escape(context["veh_type_main"]),"", temp_remark_check, flags=re.IGNORECASE, count=1)
    if context.get("age_main"):
        age_keyword_for_re = context["age_main"].replace(">","").replace("<","").replace("=","")
        age_keyword_for_re = re.sub(r"(YRS?|YEARS?|AGE)","", age_keyword_for_re, flags=re.IGNORECASE).strip()
        if age_keyword_for_re:
             temp_remark_check = re.sub(r'\b'+re.escape(age_keyword_for_re)+r'\b',"", temp_remark_check, flags=re.IGNORECASE, count=1)
    for bm in context.get("bike_makes_main",[]): temp_remark_check = re.sub(r'\b'+re.escape(bm)+r'\b',"", temp_remark_check, flags=re.IGNORECASE, count=1)
    
    for common_word in ["GRID", "ONLY", "AND", "&", "AOTP", "MHCV", "LCV"]:
        temp_remark_check = re.sub(r'\b'+re.escape(common_word)+r'\b',"", temp_remark_check, flags=re.IGNORECASE)
    temp_remark_check = temp_remark_check.replace("(","").replace(")","").strip(" ,")

    if len(temp_remark_check) > 3 and temp_remark_check.lower() not in ["aotp", "mhcv", "lcv", "tata", "al"]: # Avoid adding if only keywords remain
        all_remarks_main_header.append(text_cleaned_orig)

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
                excluded_makes_in_header.extend( (bm[0] if isinstance(bm, tuple) else bm).upper() for bm in BIKE_MAKE_REGEX.findall(text_after_exclusion))
                excluded_vehicles_in_header.extend( (sv[0] if isinstance(sv, tuple) else sv).upper() for sv in SPECIFIC_VEHICLE_REGEX.findall(text_after_exclusion))
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
    
    # Bug 5: Ensure TAXI is picked up correctly for vehicle type.
    if TAXI_MATCH_REGEX.search(text_upper):
        if not (exclusion_triggered and "TAXI" in base_details["excluded_vehicles_col_header"]):
            base_details["vehicle"] = "TAXI"
            # If veh_type was set by PCVTAXI in VEHICLE_CATEGORY_REGEX, it's already PCV.
            # If not, or if it was something else, TAXI implies PCV.
            if base_details.get("veh_type") != "PCV":
                base_details["veh_type"] = "PCV"
            base_details["product_type"] = "PASSENGER CARRYING VEHICLE"


    specific_vehicle_matches_raw_tuples = SPECIFIC_VEHICLE_REGEX.findall(text_upper)
    header_vehicles_list = []
    if specific_vehicle_matches_raw_tuples:
        for sv_match_raw_tuple in specific_vehicle_matches_raw_tuples:
            sv_match_raw = sv_match_raw_tuple if isinstance(sv_match_raw_tuple, str) else sv_match_raw_tuple[0]
            vehicle_parts_from_match = [p.strip().upper() for p in sv_match_raw.upper().split('/') if p.strip()]
            for vp_upper in vehicle_parts_from_match:
                if vp_upper in SPECIFIC_VEHICLES and not (exclusion_triggered and vp_upper in base_details["excluded_vehicles_col_header"]):
                    header_vehicles_list.append(vp_upper)
    
    base_details["header_specific_vehicles_list"] = list(set(header_vehicles_list))

    if base_details["header_specific_vehicles_list"] and not base_details.get("vehicle"): # Only if not already set by TAXI
        primary_vehicle_from_header = base_details["header_specific_vehicles_list"][0]
        base_details["vehicle"] = primary_vehicle_from_header
        if "BUS" in primary_vehicle_from_header: base_details["veh_type"] = "PCV"; base_details["product_type"] = "PASSENGER CARRYING VEHICLE"
        elif primary_vehicle_from_header in ["CRANES", "TRACTOR", "TRACTER", "BACKHOELOADER", "CE"]:
             base_details["veh_type"] = "MISC"; base_details["product_type"] = "MISCELLANEOUS VEHICLE"

    if not base_details.get("vehicle"):
        if re.search(r"\b3W\b", text_upper): base_details["vehicle"] = "3W"
        elif re.search(r"\b2W\b", text_upper): base_details["vehicle"] = "2W"

    for age_pattern, age_val_fixed in AGE_KEYWORD_PATTERNS:
        if isinstance(age_val_fixed, str) and age_val_fixed in ["NEW", "OLD"]:
            if age_pattern.search(text_upper):
                base_details["age"] = age_val_fixed
                break
    
    base_details["found_fuel_types_col_header"] = list(set( (ft[0] if isinstance(ft, tuple) else ft).upper() for ft in FUEL_TYPE_REGEX.findall(text_upper)))


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

    header_bike_makes_found_tuples = BIKE_MAKE_REGEX.findall(text_upper)
    valid_header_bike_makes = []
    if header_bike_makes_found_tuples:
        for hbm_found_upper_tuple in header_bike_makes_found_tuples:
            hbm_found_upper = (hbm_found_upper_tuple[0] if isinstance(hbm_found_upper_tuple, tuple) else hbm_found_upper_tuple).upper()
            if not (exclusion_triggered and hbm_found_upper in base_details["excluded_makes_col_header"]):
                valid_header_bike_makes.append(hbm_found_upper)
        if valid_header_bike_makes:
             base_details["header_bike_makes_list"] = list(set(valid_header_bike_makes))


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

    non_data_values = ["DECLINE", "NO BUSINESS", "CC", "NO BIZ", "#REF!", "TBD", "IRDA"]
    if cell_text_cleaned_upper in non_data_values or not cell_text_cleaned_upper :
        # ... (non-data handling as before) ...
        entry = base_header_details.copy()
        entry["cluster_code"] = rto_cluster_from_row
        entry["po_percent"] = cell_text_cleaned_upper if cell_text_cleaned_upper else cell_text_original
        
        current_remarks_list = [base_header_details.get("remarks_col_header")]
        if main_table_context_global and main_table_context_global.get("remarks_main"):
            current_remarks_list.extend(main_table_context_global.get("remarks_main"))
        if cell_text_cleaned_orig_case and cell_text_cleaned_orig_case not in non_data_values:
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

    all_segments_from_cell_lines = []
    cell_lines = cell_text_cleaned_orig_case.splitlines() if '\n' in cell_text_cleaned_orig_case else [cell_text_cleaned_orig_case]

    for line_idx, line_text_orig in enumerate(cell_lines):
        line_text_orig_stripped = line_text_orig.strip()
        if not line_text_orig_stripped: continue
        line_text_upper = line_text_orig_stripped.upper()
        print(f"DEBUG: parse_percentage_cell_text: Processing Line {line_idx+1}: '{line_text_orig_stripped}'")

        # Bug 3 Fix: Segmentation based on explicit percentages "XX%"
        # This regex specifically looks for digits followed by a % sign.
        explicit_percent_matches = list(re.finditer(r"(\d+(?:\.\d+)?%)", line_text_upper))

        if not explicit_percent_matches:
            all_segments_from_cell_lines.append({"po_percent": None, 
                                                 "associated_text": line_text_orig_stripped, 
                                                 "is_condition_line": True})
            continue

        last_text_end_idx = 0
        for i, match in enumerate(explicit_percent_matches):
            percent_val = match.group(1) # This is "XX%"
            percent_start_idx = match.start()
            percent_end_idx = match.end()
            
            # Text *before* this percentage in this segment (from original case line)
            text_before_percent = line_text_orig_stripped[last_text_end_idx:percent_start_idx].strip()
            
            # Text *after* this percentage, until start of next explicit percent or end of line
            next_percent_actual_start_idx = len(line_text_orig_stripped) # Default to EOL
            if i + 1 < len(explicit_percent_matches):
                next_percent_actual_start_idx = explicit_percent_matches[i+1].start()
            
            text_after_percent = line_text_orig_stripped[percent_end_idx:next_percent_actual_start_idx].strip()
            
            current_associated_text = (text_before_percent + " " + text_after_percent).strip()
            
            all_segments_from_cell_lines.append({
                "po_percent": percent_val,
                "associated_text": current_associated_text
            })
            last_text_end_idx = next_percent_actual_start_idx


    if not any(s.get("po_percent") for s in all_segments_from_cell_lines) and cell_text_cleaned_orig_case:
        single_po = extract_explicit_percentage(cell_text_cleaned_orig_case)
        if single_po:
             all_segments_from_cell_lines.append({"po_percent": single_po, "associated_text": cell_text_cleaned_orig_case.replace(single_po.strip('%'),"",1).strip(" %")})
        elif cell_text_cleaned_orig_case:
             all_segments_from_cell_lines.append({"po_percent": None, "associated_text": cell_text_cleaned_orig_case, "is_condition_line": True})


    general_conditions_from_cell = {}
    final_percent_segments_to_process = []

    # New: Cell-level "TATA only" detection
    cell_is_tata_only_special_case = "TATA" in cell_text_cleaned_upper and "ONLY" in cell_text_cleaned_upper

    for seg in all_segments_from_cell_lines:
        if seg.get("is_condition_line"):
            cond_text_upper = seg["associated_text"].upper()
            if not general_conditions_from_cell.get("age_cond"):
                for age_p, age_r in AGE_KEYWORD_PATTERNS:
                    age_m = age_p.search(cond_text_upper)
                    if age_m: general_conditions_from_cell["age_cond"] = age_r(age_m) if callable(age_r) else age_r; break
            
            # Check for "TATA" and "ONLY" in condition line for bike_make_cond
            if "TATA" in cond_text_upper and "ONLY" in cond_text_upper:
                 general_conditions_from_cell["bike_make_cond"] = "TATA" # Specific TATA ONLY case
            elif not general_conditions_from_cell.get("bike_make_cond"): # General make if not TATA ONLY
                bm_cond_tuple = BIKE_MAKE_REGEX.findall(cond_text_upper)
                if bm_cond_tuple: general_conditions_from_cell["bike_make_cond"] = (bm_cond_tuple[0] if isinstance(bm_cond_tuple[0], str) else bm_cond_tuple[0][0]).upper()

            if not general_conditions_from_cell.get("cluster_code_cond"):
                 scc_cond = SPECIAL_CLUSTER_REGEX.search(cond_text_upper)
                 if scc_cond and ("ONLY" in cond_text_upper or re.search(r"\bIN\s+" + re.escape(scc_cond.group(1).upper()) + r"\b", cond_text_upper)):
                     general_conditions_from_cell["cluster_code_cond"] = scc_cond.group(1).upper()
            if seg["associated_text"]:
                 current_cond_remarks = general_conditions_from_cell.get("cond_line_remark_list", [])
                 current_cond_remarks.append(seg["associated_text"])
                 general_conditions_from_cell["cond_line_remark_list"] = current_cond_remarks

        elif seg.get("po_percent"):
            if seg.get("po_percent", "").upper().replace("%", "") in [r.upper().replace("RTO","").strip().replace("RTOS","").strip() for r in SPECIAL_CLUSTER_CODES + [rto_cluster_from_row]]:
                print(f"DEBUG: Skipping segment with RTO name as po_percent: {seg.get('po_percent')}")
                continue
            final_percent_segments_to_process.append(seg)
    
    if not final_percent_segments_to_process and general_conditions_from_cell:
         final_percent_segments_to_process.append({"po_percent": "", "associated_text": ""})


    main_context_bike_makes_list = main_table_context_global.get("bike_makes_main", [None]) if main_table_context_global else [None]
    if not main_context_bike_makes_list: main_context_bike_makes_list = [None]

    for mcbm in main_context_bike_makes_list:
        print(f"DEBUG: parse_percentage_cell_text: Iterating with main_context_bike_make: {mcbm}")
        
        for seg_data_final in final_percent_segments_to_process:
            current_details = base_header_details.copy()
            current_details["cluster_code"] = rto_cluster_from_row # Default RTO from row
            
            # Hierarchy of applying attributes
            # 1. Main Table Context (lowest priority for overridable fields)
            if main_table_context_global:
                if main_table_context_global.get("veh_type_main") : current_details["veh_type"] = main_table_context_global.get("veh_type_main")
                if main_table_context_global.get("age_main") : current_details["age"] = main_table_context_global.get("age_main")
                if main_table_context_global.get("plan_type_main") : current_details["plan_type"] = main_table_context_global.get("plan_type_main")
            
            # 2. General conditions from cell lines (can override main table context)
            if general_conditions_from_cell.get("age_cond"): current_details["age"] = general_conditions_from_cell.get("age_cond")
            # General bike make from cell (lower priority than segment or TATA ONLY case)
            if general_conditions_from_cell.get("bike_make_cond") and not current_details.get("bike_make"):
                 current_details["bike_make"] = general_conditions_from_cell.get("bike_make_cond")
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
            
            if associated_text_segment_orig: current_remarks_list.append(associated_text_segment_orig)
            
            paren_remarks_in_segment_assoc = re.findall(r"\((.*?)\)", associated_text_segment_orig)
            for pr_seg_assoc in paren_remarks_in_segment_assoc:
                current_remarks_list.append(f"({pr_seg_assoc})")

            # 3. Details from segment's associated_text (highest priority for override)
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
            if fuel_match_seg: current_details["fuel_type"] = (fuel_match_seg.group(1) if isinstance(fuel_match_seg.group(1), str) else fuel_match_seg.group(1)[0]).upper()


            # Cluster code from segment (Bug 1 refined)
            scc_match_seg = SPECIAL_CLUSTER_REGEX.search(associated_text_segment_upper)
            if scc_match_seg:
                # Check for "ONLY" or if the SCC is part of a phrase like "IN WB1" (case insensitive)
                if "ONLY" in associated_text_segment_upper or \
                   re.search(r"\bIN\s+" + re.escape(scc_match_seg.group(1).upper()) + r"\b", associated_text_segment_upper):
                    current_details["cluster_code"] = scc_match_seg.group(1).upper()


            # --- Bike Make Logic with "TATA only" rule ---
            bike_makes_to_iterate_for_final_entry = [None] # Default is one entry, bike_make might be set by hierarchy

            if cell_is_tata_only_special_case: # Highest priority for bike make
                current_details["bike_make"] = "TATA"
                bike_makes_to_iterate_for_final_entry = ["TATA"] # Force iteration only for TATA
            else:
                segment_bike_makes_tuples = BIKE_MAKE_REGEX.findall(associated_text_segment_upper)
                segment_bike_makes_list_final = [(bm[0] if isinstance(bm, tuple) else bm).upper() for bm in segment_bike_makes_tuples]
                valid_segment_bike_makes_final = [bm for bm in segment_bike_makes_list_final if bm not in base_header_details.get("excluded_makes_col_header", [])]

                if valid_segment_bike_makes_final:
                    bike_makes_to_iterate_for_final_entry = valid_segment_bike_makes_final
                elif current_details.get("bike_make"): # Already set by general cell cond or header
                     bike_makes_to_iterate_for_final_entry = [current_details.get("bike_make")]
                # general_conditions_from_cell bike_make_cond already applied to current_details["bike_make"]
                elif base_header_details.get("header_bike_makes_list"):
                    bike_makes_to_iterate_for_final_entry = base_header_details.get("header_bike_makes_list")
                elif mcbm:
                    bike_makes_to_iterate_for_final_entry = [mcbm]
            
            if current_details.get("vehicle") and current_details.get("vehicle").upper() in base_header_details.get("excluded_vehicles_col_header", []):
                current_details["vehicle"] = None

            for bm_final_iter_val in bike_makes_to_iterate_for_final_entry:
                final_entry = current_details.copy()
                if bm_final_iter_val: final_entry["bike_make"] = bm_final_iter_val
                
                # If cell_is_tata_only_special_case, ensure bike_make is TATA
                if cell_is_tata_only_special_case:
                    final_entry["bike_make"] = "TATA"

                final_remarks_for_entry = current_remarks_list[:]
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


# --- process_sheet and main function (largely as provided, ensure they call updated parsers) ---
def process_sheet(df_sheet, sheet_name):
    all_rows_for_sheet = []
    slab_month = extract_slab_month_from_df(df_sheet)
    print(f"INFO: process_sheet: Processing sheet: {sheet_name}, Slab Month: {slab_month}")

    header_row_idx_t1 = find_header_row(df_sheet, "RTO CLUSTER")
    if header_row_idx_t1 is None:
        print(f"WARN: process_sheet: RTO CLUSTER header not found for Table 1 in sheet {sheet_name}. Skipping.")
        return []

    try:
        rto_cluster_col_idx_t1 = \
            df_sheet.iloc[header_row_idx_t1][df_sheet.iloc[header_row_idx_t1].astype(str).str.upper().str.strip().str.contains("RTO CLUSTER", na=False)].index[0]
    except IndexError:
        print(f"CRITICAL: Could not find 'RTO CLUSTER' column index in identified header row {header_row_idx_t1} for sheet {sheet_name}. Skipping sheet.")
        return []
    
    header_row_idx_t2 = None
    main_table2_context = None
    rto_cluster_col_idx_t2 = None
    
    for i_scan_t2_header in range(header_row_idx_t1 + 1, len(df_sheet)):
        row_series_scan_t2 = df_sheet.iloc[i_scan_t2_header]
        if row_series_scan_t2.astype(str).str.upper().str.strip().str.contains("RTO CLUSTER", na=False).any():
            header_row_idx_t2 = i_scan_t2_header
            try:
                rto_cluster_col_idx_t2 = \
                    row_series_scan_t2[row_series_scan_t2.astype(str).str.upper().str.strip().str.contains("RTO CLUSTER", na=False)].index[0]
            except IndexError: header_row_idx_t2 = None; break

            title_search_start_row_t2 = max(0, header_row_idx_t2 - 5)
            for j_scan_title_t2 in range(title_search_start_row_t2, header_row_idx_t2): 
                scan_cols_for_title_t2 = list(range(max(0, rto_cluster_col_idx_t2 - 5), min(len(df_sheet.columns), rto_cluster_col_idx_t2 + 5)))
                if 1 not in scan_cols_for_title_t2 and 1 < len(df_sheet.columns): scan_cols_for_title_t2.insert(0,1) 

                for k_col_title_t2 in scan_cols_for_title_t2:
                    if j_scan_title_t2 < len(df_sheet) and k_col_title_t2 < df_sheet.shape[1]:
                        cell_val_title_t2 = df_sheet.iloc[j_scan_title_t2, k_col_title_t2]
                        cell_text_title_t2_upper = clean_text_general(cell_val_title_t2).upper()
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

        for j_col_idx_t1 in range(rto_cluster_col_idx_t1 + 1, end_col_t1):
            if j_col_idx_t1 >= len(df_sheet.columns): break
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

            for j_col_idx_t2 in range(rto_cluster_col_idx_t2 + 1, len(df_sheet.columns)):
                if j_col_idx_t2 >= len(df_sheet.columns): break
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