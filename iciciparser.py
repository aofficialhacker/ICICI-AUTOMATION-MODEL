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
    "SCANIA", "VOLVO"
]
BIKE_MAKES = sorted(list(set(bm.upper() for bm in BIKE_MAKES_RAW)), key=len, reverse=True)
BIKE_MAKE_REGEX = r"\b(" + "|".join(re.escape(bm) for bm in BIKE_MAKES) + r")\b"

SPECIAL_CLUSTER_CODES_RAW = [
    "WB1", "DL", "NON DL RTO", "JK1 RTO", "GJ1 RTO", "UP1 EAST", "UK1 RTO", "UP EAST 1",
    "KA1 RTOS", "KA1 RTO", "TN10", "TN12", "TN02", "TN22", "TN04", "TN06", "TN09",
    "TN18", "TN19", "TN20", "KA01-05", "OD1", "PIMPRI", "PIMPRICHINCHWAD",
    "DELHI SURROUNDING RTO", "GJ1"
]
SPECIAL_CLUSTER_CODES = sorted(list(set(scc.upper() for scc in SPECIAL_CLUSTER_CODES_RAW)), key=len, reverse=True)
SPECIAL_CLUSTER_REGEX = r"\b(" + "|".join(re.escape(scc) for scc in SPECIAL_CLUSTER_CODES) + r")\b"

VEHICLE_CATEGORIES_MAP = {
    "GCV": "GCV", "SCV": "GCV", "LCV": "GCV", "MHCV": "GCV",
    "PCV": "PCV",
    "MISC D CE": "MISC", "MIsc D CE": "MISC", "MIS D CE": "MISC",
}
VEHICLE_CATEGORY_REGEX = r"\b(" + "|".join(re.escape(k) for k in VEHICLE_CATEGORIES_MAP.keys()) + r")\b"

SPECIFIC_VEHICLES_RAW = [
    "TANKER", "TIPPER", "TRUCK", "TRAILER", "DUMPER", "CRANES", "TRACTOR", "TRACTER",
    "SCHOOL BUS", "STAFF BUS", "BUS", "TAXI", "CE", "BACKHOELOADER"
]
SPECIFIC_VEHICLES = sorted(list(set(sv.upper() for sv in SPECIFIC_VEHICLES_RAW)), key=len, reverse=True)
SPECIFIC_VEHICLE_REGEX = r"\b(" + "|".join(re.escape(sv) for sv in SPECIFIC_VEHICLES) + r")\b"

FUEL_TYPES_RAW = ["ELECTRIC", "PETROL", "CNG", "BIFUEL", "DIESEL"]
FUEL_TYPES = sorted(list(set(ft.upper() for ft in FUEL_TYPES_RAW)), key=len, reverse=True)
FUEL_TYPE_REGEX = r"\b(" + "|".join(re.escape(ft) for ft in FUEL_TYPES) + r")\b"

AGE_KEYWORD_PATTERNS = [
    (re.compile(r"\bNEW\b", re.IGNORECASE), "NEW"),
    (re.compile(r"\bOLD\b", re.IGNORECASE), "OLD"),
    (re.compile(r"\b(1-5\s*(?:YRS?|YEARS?|AGE))\b", re.IGNORECASE), "1-5 YRS"),
    (re.compile(r"\b(>\s*5\s*(?:YRS?|YEARS?|AGE))\b", re.IGNORECASE), ">5 YRS"),
    (re.compile(r"\b(ABOVE\s*5\s*(?:YRS?|YEARS?|AGE))\b", re.IGNORECASE), ">5 YRS"),
    (re.compile(r"\b(1-6\s*(?:YRS?|YEARS?|AGE))\b", re.IGNORECASE), "1-6 YRS"),
    (re.compile(r"\b(\d+\s*-\s*\d+\s*(?:YRS?|YEARS?|AGE))\b", re.IGNORECASE), lambda m: m.group(1).upper()),
    (re.compile(r"\b([<>]=?\s*\d+\s*(?:YRS?|YEARS?|AGE))\b", re.IGNORECASE), lambda m: m.group(1).upper()),
    (re.compile(r"\b(UPTO\s*\d+\s*(?:YRS?|YEARS?|AGE))\b", re.IGNORECASE), lambda m: m.group(0).upper()),
    (re.compile(r"\b(\d+\s*\+\s*(?:YRS?|YEARS?|AGE))\b", re.IGNORECASE), lambda m: m.group(0).upper()),
    (re.compile(r"\b(\d+(?:ST|ND|RD|TH)\s*YEAR)\b", re.IGNORECASE), lambda m: m.group(1).upper()),
]

GVW_REGEX_PATTERNS = [
    re.compile(r"([<>]=?)\s*(\d+(?:\.\d+)?)\s*GVW", re.IGNORECASE),
    re.compile(r"(\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?)\s*T\b", re.IGNORECASE),
    re.compile(r"([<>]=?)\s*(\d+(?:\.\d+)?)\s*T\b", re.IGNORECASE),
]

SEATING_CAP_REGEX_PATTERNS_CONTEXTUAL = [
    (re.compile(r"([<>]=?)\s*(\d+)\s*UPTO\s*(\d+)\s*SEATER", re.IGNORECASE), lambda m: f"{m.group(1)}{m.group(2)} UPTO {m.group(3)}"),
    (re.compile(r"([<>]=?)\s*(\d+)\b"), lambda m: f"{m.group(1)}{m.group(2)}"),
    (re.compile(r"(\d+\s*-\s*\d+)\b"), lambda m: m.group(1)),
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
PLAN_TYPE_REGEX = r"\b(" + "|".join(re.escape(k) for k in PLAN_TYPE_KEYWORDS.keys()) + r")\b"

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
        for j in range(len(df.columns)):
            cell_text_cleaned = clean_text_general(df.iloc[i, j]).upper()
            match = re.search(r"CV\s*AGENCY\s*GRID\s*([A-Z]+\'?\d{2,4})", cell_text_cleaned)
            if match:
                slab_month = match.group(1)
                print(f"DEBUG: extract_slab_month_from_df: Extracted slab_month: {slab_month}")
                return slab_month
    print("DEBUG: extract_slab_month_from_df: Slab month not found.")
    return None

def extract_pure_percentage_val(text_segment):
    text_segment_upper = clean_text_general(text_segment).upper()
    match = re.search(r"(\d+(?:\.\d+)?)\s*%?", text_segment_upper)
    if match:
        return match.group(1) + "%"
    return None

def parse_main_table_header(header_text_full):
    context = {"bike_makes_main": [], "remarks_main": [], "age_main": None, "plan_type_main": None, "veh_type_main": None}
    text_upper = clean_text_general(header_text_full).upper()
    print(f"DEBUG: parse_main_table_header: Parsing main table header: '{header_text_full}' -> '{text_upper}'")

    veh_type_match = re.search(VEHICLE_CATEGORY_REGEX, text_upper)
    if veh_type_match:
        context["veh_type_main"] = VEHICLE_CATEGORIES_MAP.get(veh_type_match.group(1))

    plan_type_match = re.search(PLAN_TYPE_REGEX, text_upper)
    if plan_type_match:
        context["plan_type_main"] = PLAN_TYPE_KEYWORDS.get(plan_type_match.group(1))

    age_match = re.search(r"([<>]=?\s*\d+\s*(?:YRS?|YEARS?|AGE))", text_upper)
    if age_match:
        context["age_main"] = age_match.group(1)
    
    bike_makes_found = re.findall(BIKE_MAKE_REGEX, text_upper)
    if bike_makes_found:
         context["bike_makes_main"] = list(set(bike_makes_found))

    remark_match = re.search(r"\((.*?)\)", header_text_full)
    if remark_match:
        context["remarks_main"].append(f"({remark_match.group(1)})")
    elif header_text_full:
        context["remarks_main"].append(header_text_full)
        
    print(f"DEBUG: parse_main_table_header: Parsed main table header context: {context}")
    return context

def parse_column_header_text(header_cell_text_original):
    base_details = defaultdict(lambda: None)
    text_upper = clean_text_general(header_cell_text_original).upper()
    base_details["remarks_col_header"] = []
    print(f"DEBUG: parse_column_header_text: Parsing column header: '{header_cell_text_original}' -> '{text_upper}'")

    base_details["product_type"] = "COMMERCIAL VEHICLE"

    veh_type_match = re.search(VEHICLE_CATEGORY_REGEX, text_upper)
    if veh_type_match:
        cat_key = veh_type_match.group(1)
        base_details["veh_type"] = VEHICLE_CATEGORIES_MAP[cat_key]
        if base_details["veh_type"] == "PCV": base_details["product_type"] = "PASSENGER CARRYING VEHICLE"
        elif base_details["veh_type"] == "MISC":
            base_details["product_type"] = "MISCELLANEOUS VEHICLE"
            if "CE" in cat_key: base_details["vehicle"] = "CE"
    
    if not base_details["vehicle"]:
        vehicle_match = re.search(SPECIFIC_VEHICLE_REGEX, text_upper)
        if vehicle_match:
            matched_vehicle = vehicle_match.group(1)
            base_details["vehicle"] = matched_vehicle
            if "BUS" in matched_vehicle.upper(): base_details["veh_type"] = "PCV"; base_details["product_type"] = "PASSENGER CARRYING VEHICLE"
            elif matched_vehicle.upper() in ["CRANES", "TRACTOR", "TRACTER", "BACKHOELOADER", "CE"]:
                 base_details["veh_type"] = "MISC"; base_details["product_type"] = "MISCELLANEOUS VEHICLE"
            elif matched_vehicle.upper() == "TAXI": base_details["veh_type"] = "PCV"; base_details["product_type"] = "PASSENGER CARRYING VEHICLE"

    if not base_details["vehicle"]:
        if re.search(r"\b3W\b", text_upper, re.IGNORECASE): base_details["vehicle"] = "3W"
        elif re.search(r"\b2W\b", text_upper, re.IGNORECASE): base_details["vehicle"] = "2W"

    for age_pattern, age_val in AGE_KEYWORD_PATTERNS:
        if isinstance(age_val, str) and age_val in ["NEW", "OLD"]:
            if age_pattern.search(text_upper):
                base_details["age"] = age_val
                break
    
    fuel_match = re.search(FUEL_TYPE_REGEX, text_upper)
    if fuel_match: base_details["fuel_type"] = fuel_match.group(1)

    for pattern in GVW_REGEX_PATTERNS:
        gvw_match = pattern.search(text_upper)
        if gvw_match:
            base_details["gvw"] = "".join(gvw_match.groups()) if len(gvw_match.groups()) > 1 else gvw_match.group(1)
            break
    
    if ("BUS" in text_upper or "SEATER" in text_upper or \
        (base_details.get("vehicle") and "BUS" in base_details.get("vehicle", "").upper()) or \
        (base_details.get("veh_type") == "PCV")):
        
        bus_seater_match = re.search(r"\b(BUS|SEATER)\b", text_upper, re.IGNORECASE)
        text_to_search_seating = text_upper
        if bus_seater_match:
            text_to_search_seating = text_upper[bus_seater_match.end():]

        for pattern, resolver in SEATING_CAP_REGEX_PATTERNS_CONTEXTUAL:
            sc_match = pattern.search(text_to_search_seating)
            if sc_match:
                base_details["seating_cap"] = resolver(sc_match)
                print(f"DEBUG: parse_column_header_text: Matched seating_cap: {base_details['seating_cap']} using pattern {pattern.pattern} on text '{text_to_search_seating}'")
                break

    for pattern in ENGINE_TYPE_REGEX_PATTERNS:
        et_match = pattern.search(text_upper)
        if et_match:
            base_details["engine_type"] = et_match.group(0).upper()
            break
            
    bracket_remarks = re.findall(r"\((.*?)\)", header_cell_text_original)
    for br in bracket_remarks:
        base_details["remarks_col_header"].append(f"({br})")

    header_bike_makes = re.findall(BIKE_MAKE_REGEX, text_upper)
    if header_bike_makes:
        base_details["header_bike_makes"] = list(set(header_bike_makes))

    final_details = {k: v for k, v in base_details.items() if v is not None and v != []}
    if "remarks_col_header" in final_details:
        final_details["remarks_col_header"] = " ".join(final_details["remarks_col_header"]).strip()
    
    print(f"DEBUG: parse_column_header_text: Parsed column header details: {dict(final_details)}")
    return dict(final_details)


def parse_percentage_cell_text(cell_text_original, base_header_details, rto_cluster_from_row, main_table_context_global):
    results = []
    cell_text_cleaned_upper = clean_text_general(cell_text_original).upper()
    print(f"DEBUG: parse_percentage_cell_text: Parsing cell text: '{cell_text_original}' -> '{cell_text_cleaned_upper}' for RTO: {rto_cluster_from_row}")
    print(f"DEBUG: parse_percentage_cell_text: Base header details: {base_header_details}")
    print(f"DEBUG: parse_percentage_cell_text: Main table context: {main_table_context_global}")

    if cell_text_cleaned_upper in ["DECLINE", "NO BUSINESS", "CC", "NO BIZ", "#REF!", "TBD", ""]:
        print(f"DEBUG: parse_percentage_cell_text: Cell is non-data ('{cell_text_original}'). Skipping.")
        return results

    main_context_bike_make_list = main_table_context_global.get("bike_makes_main") if main_table_context_global else [None]
    if not main_context_bike_make_list: main_context_bike_make_list = [None]

    for current_main_context_bike_make in main_context_bike_make_list:
        print(f"DEBUG: parse_percentage_cell_text: Processing with main_context_bike_make: {current_main_context_bike_make}")
        
        current_details = base_header_details.copy()
        current_details["cluster_code"] = rto_cluster_from_row
        current_details["remark"] = []
        if base_header_details.get("remarks_col_header"):
            current_details["remark"].append(base_header_details["remarks_col_header"])
        if cell_text_original:
             current_details["remark"].append(str(cell_text_original))

        if main_table_context_global:
            if main_table_context_global.get("veh_type_main") and not current_details.get("veh_type"):
                current_details["veh_type"] = main_table_context_global["veh_type_main"]
            if main_table_context_global.get("age_main") and not current_details.get("age"):
                current_details["age"] = main_table_context_global["age_main"]
            if main_table_context_global.get("plan_type_main") and not current_details.get("plan_type"):
                current_details["plan_type"] = main_table_context_global["plan_type_main"]
            if main_table_context_global.get("remarks_main"):
                current_details["remark"].extend(main_table_context_global["remarks_main"])
            if current_main_context_bike_make:
                current_details["bike_make"] = current_main_context_bike_make

        dl_non_dl_rules = [] # Correctly initialized
        dl_pattern = r"DL\s*-\s*(\d+(?:\.\d+)?%?)"
        nondl_pattern = r"NON\s*DL\s*RTO\s*-\s*(\d+(?:\.\d+)?%?)"
        
        dl_match = re.search(dl_pattern, cell_text_cleaned_upper, re.IGNORECASE)
        nondl_match = re.search(nondl_pattern, cell_text_cleaned_upper, re.IGNORECASE)

        # Temporary list to hold results for DL/NonDL for this current_details base
        dl_nondl_specific_results = []

        if dl_match or nondl_match:
            temp_dl_rules_for_this_iteration = []
            if dl_match:
                temp_dl_rules_for_this_iteration.append({"cluster_code": "DL", "po_percent": dl_match.group(1)})
            if nondl_match:
                temp_dl_rules_for_this_iteration.append({"cluster_code": "NON DL RTO", "po_percent": nondl_match.group(1)})
            
            for rule in temp_dl_rules_for_this_iteration:
                specific_entry = current_details.copy()
                specific_entry["cluster_code"] = rule["cluster_code"]
                specific_entry["po_percent"] = rule["po_percent"] if "%" in rule["po_percent"] else rule["po_percent"] + "%"
                
                # Clean up remarks for this specific entry
                if isinstance(specific_entry.get("remark"), list):
                    specific_entry["remark"] = " ".join(list(dict.fromkeys(filter(None,specific_entry["remark"])))).strip()
                dl_nondl_specific_results.append(specific_entry)
            
            if dl_nondl_specific_results:
                results.extend(dl_nondl_specific_results)
                print(f"DEBUG: parse_percentage_cell_text: Handled DL/NonDL pattern for cell. Added {len(dl_nondl_specific_results)} results.")
                continue # Skips further parsing for this main_context_bike_make if DL/NonDL found

        primary_po_percent = extract_pure_percentage_val(cell_text_cleaned_upper)
        if primary_po_percent:
            current_details["po_percent"] = primary_po_percent
        else:
            print(f"DEBUG: parse_percentage_cell_text: No primary percentage found in '{cell_text_cleaned_upper}'. Skipping this path.")
            continue

        found_age_in_cell = False
        for age_pattern_re, age_val_resolver in AGE_KEYWORD_PATTERNS:
            age_match = age_pattern_re.search(cell_text_cleaned_upper)
            if age_match:
                current_details["age"] = age_val_resolver(age_match) if callable(age_val_resolver) else age_val_resolver
                found_age_in_cell = True
                break
        
        plan_type_match_cell = re.search(PLAN_TYPE_REGEX, cell_text_cleaned_upper, re.IGNORECASE)
        if plan_type_match_cell:
            current_details["plan_type"] = PLAN_TYPE_KEYWORDS.get(plan_type_match_cell.group(1).upper())

        for pattern_et_cell in ENGINE_TYPE_REGEX_PATTERNS:
            et_match_cell = pattern_et_cell.search(cell_text_cleaned_upper)
            if et_match_cell:
                current_details["engine_type"] = et_match_cell.group(0).upper()
                break

        fuel_match_cell = re.search(FUEL_TYPE_REGEX, cell_text_cleaned_upper, re.IGNORECASE)
        if fuel_match_cell:
            current_details["fuel_type"] = fuel_match_cell.group(1).upper()

        scc_match_cell = re.search(SPECIAL_CLUSTER_REGEX, cell_text_cleaned_upper, re.IGNORECASE)
        if scc_match_cell and "ONLY" in cell_text_cleaned_upper:
            current_details["cluster_code"] = scc_match_cell.group(1).upper()
            
        cell_bike_makes = list(set(re.findall(BIKE_MAKE_REGEX, cell_text_cleaned_upper)))
        
        temp_results_for_bike_makes = []
        if cell_bike_makes:
            if current_details.get("bike_make") and current_details["bike_make"] in cell_bike_makes:
                temp_results_for_bike_makes.append(current_details.copy())
            elif not current_details.get("bike_make"):
                for cbm in cell_bike_makes:
                    entry_var = current_details.copy()
                    entry_var["bike_make"] = cbm
                    temp_results_for_bike_makes.append(entry_var)
            else: 
                  if current_main_context_bike_make and current_main_context_bike_make not in cell_bike_makes:
                      print(f"DEBUG: parse_percentage_cell_text: Main context bike_make '{current_main_context_bike_make}' not in cell_bike_makes '{cell_bike_makes}'. Skipping this variant.")
                      continue 
                  if not current_main_context_bike_make:
                      for cbm in cell_bike_makes:
                          entry_var = current_details.copy()
                          entry_var["bike_make"] = cbm
                          temp_results_for_bike_makes.append(entry_var)
                  else: 
                      temp_results_for_bike_makes.append(current_details.copy())

        elif base_header_details.get("header_bike_makes") and not current_details.get("bike_make"):
            for hbm in base_header_details["header_bike_makes"]:
                entry_var = current_details.copy()
                entry_var["bike_make"] = hbm
                temp_results_for_bike_makes.append(entry_var)
        else:
            temp_results_for_bike_makes.append(current_details.copy())

        for res_entry in temp_results_for_bike_makes:
            if isinstance(res_entry.get("remark"), list):
                res_entry["remark"] = " ".join(list(dict.fromkeys(filter(None,res_entry["remark"])))).strip()
            results.append(res_entry)

    print(f"DEBUG: parse_percentage_cell_text: Total results for cell after main_context_bike_make loop: {len(results)}")
    return results


def process_sheet(df_sheet, sheet_name):
    all_rows_for_sheet = []
    slab_month = extract_slab_month_from_df(df_sheet)
    print(f"INFO: process_sheet: Processing sheet: {sheet_name}, Slab Month: {slab_month}")

    header_row_idx_t1 = find_header_row(df_sheet, "RTO CLUSTER")
    if header_row_idx_t1 is None:
        print(f"WARN: process_sheet: RTO CLUSTER header not found for Table 1 in sheet {sheet_name}. Skipping.")
        return []

    rto_cluster_col_idx_t1 = df_sheet.iloc[header_row_idx_t1][df_sheet.iloc[header_row_idx_t1].astype(str).str.contains("RTO CLUSTER", case=False, na=False)].index[0]
    
    header_row_idx_t2 = None
    main_table2_context = None
    rto_cluster_col_idx_t2 = None
    
    for i in range(header_row_idx_t1 + 1, len(df_sheet)):
        is_second_header = df_sheet.iloc[i].astype(str).str.contains("RTO CLUSTER", case=False, na=False).any()
        if is_second_header:
            header_row_idx_t2 = i
            rto_cluster_col_idx_t2 = df_sheet.iloc[i][df_sheet.iloc[i].astype(str).str.contains("RTO CLUSTER", case=False, na=False)].index[0]
            for j_scan_main_header in range(max(0, i - 5), i): 
                for k_col_scan_main_header in range(min(10, len(df_sheet.columns))): 
                    cell_val_scan = df_sheet.iloc[j_scan_main_header, k_col_scan_main_header]
                    cell_text_upper_scan = clean_text_general(cell_val_scan).upper()
                    if "GRID" in cell_text_upper_scan and ("MHCV" in cell_text_upper_scan or "LCV" in cell_text_upper_scan or "AOTP" in cell_text_upper_scan) :
                        main_table2_context = parse_main_table_header(str(cell_val_scan))
                        break 
                if main_table2_context: break
            break

    print(f"INFO: process_sheet: Processing Table 1 (Header row: {header_row_idx_t1})")
    end_row_t1 = header_row_idx_t2 if header_row_idx_t2 is not None else len(df_sheet)
    for i_row_t1 in range(header_row_idx_t1 + 1, end_row_t1):
        rto_cluster_val_t1_orig = df_sheet.iloc[i_row_t1, rto_cluster_col_idx_t1]
        rto_cluster_val_t1 = clean_text_general(rto_cluster_val_t1_orig).upper()
        if not rto_cluster_val_t1 or "RTO CLUSTER" in rto_cluster_val_t1: break 

        for j_col_idx_t1, col_header_text_t1_orig in df_sheet.iloc[header_row_idx_t1].items():
            if j_col_idx_t1 <= rto_cluster_col_idx_t1 : continue 
            if pd.isna(col_header_text_t1_orig) or clean_text_general(str(col_header_text_t1_orig)) == "": continue

            base_details = parse_column_header_text(str(col_header_text_t1_orig))
            base_details["slab_month"] = slab_month
            
            cell_value_t1_orig = df_sheet.iloc[i_row_t1, j_col_idx_t1]
            
            parsed_rows = parse_percentage_cell_text(str(cell_value_t1_orig), base_details, str(rto_cluster_val_t1_orig), None)
            all_rows_for_sheet.extend(parsed_rows)

    if header_row_idx_t2 is not None and rto_cluster_col_idx_t2 is not None:
        print(f"INFO: process_sheet: Processing Table 2 (Header row: {header_row_idx_t2}) with Main Context: {main_table2_context}")
        for i_row_t2 in range(header_row_idx_t2 + 1, len(df_sheet)):
            rto_cluster_val_t2_orig = df_sheet.iloc[i_row_t2, rto_cluster_col_idx_t2]
            rto_cluster_val_t2 = clean_text_general(rto_cluster_val_t2_orig).upper()
            if not rto_cluster_val_t2 or "RTO CLUSTER" in rto_cluster_val_t2: break

            for j_col_idx_t2, col_header_text_t2_orig in df_sheet.iloc[header_row_idx_t2].items():
                if j_col_idx_t2 <= rto_cluster_col_idx_t2: continue
                if pd.isna(col_header_text_t2_orig) or clean_text_general(str(col_header_text_t2_orig)) == "": continue

                base_details_t2 = parse_column_header_text(str(col_header_text_t2_orig))
                base_details_t2["slab_month"] = slab_month
                
                cell_value_t2_orig = df_sheet.iloc[i_row_t2, j_col_idx_t2]
                parsed_rows_t2 = parse_percentage_cell_text(str(cell_value_t2_orig), base_details_t2, str(rto_cluster_val_t2_orig), main_table2_context)
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