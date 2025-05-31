#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import re
import numpy as np
import math

# --- Configuration & Hardcoded Values ---

# Placeholder lists - These should be updated with actual lists if provided
# Example bike makes mentioned in the text
BIKE_MAKES = ["TATA", "EICHER", "BAJAJ", "TVS", "AL", "MARUTI"] # Added MARUTI based on debug output
# Example special cluster codes mentioned
SPECIAL_CLUSTER_CODES = ["WB1", "DL", "NON DL"] 

OUTPUT_COLUMNS = [
    "cluster_code", "bike_make", "model", "plan_type", "engine_type", "fuel_type",
    "plan_subtype", "add_on", "plan_term", "business_slab", "age", "po_percent",
    "slab_month", "remark", "product_type", "ncb", "vehicle", "veh_type",
    "seating_cap", "gvw"
]

# Keywords mapping to output columns
VEHICLE_KEYWORDS = {
    "TANKER": ("MISC", "Tanker"), "TIPPER": ("MISC", "Tipper"), "TRUCK": ("MISC", "Truck"),
    "TRAILER": ("MISC", "Trailer"), "DUMPER": ("MISC", "Dumper"), "CRANES": ("MISC", "Cranes"),
    "TRACTOR": ("MISC", "Tractor"), "TRACTER": ("MISC", "Tractor"), # Handle typo
    "BUS": ("PCV", "Bus"), "SCHOOL BUS": ("PCV", "School Bus"), "STAFF BUS": ("PCV", "Staff Bus"),
    "TAXI": ("PCV", "Taxi")
}

VEH_TYPE_KEYWORDS = {
    "GCV": ("GCV", "Commercial Vehicle"),
    "SCV": ("GCV", "Commercial Vehicle"), # SCV maps to GCV veh_type
    "LCV": ("GCV", None), # LCV maps to GCV veh_type, product_type might be inferred elsewhere
    "MHCV": ("GCV", None),
    "PCV": ("PCV", None)
}

FUEL_TYPE_KEYWORDS = ["PETROL", "CNG", "BIFUEL", "DIESEL", "ELECTRIC"]
AGE_KEYWORDS = ["NEW", "OLD"]
PLAN_TYPE_KEYWORDS = {
    "AOTP": "SATP", "SATP": "SATP", "TP": "SATP",
    "ON OD": "SAOD",
    "COMP": "Comp"
}

# --- Regular Expressions ---
# Regex to find percentages (number optionally followed by %)
# Captures the number and the optional % separately
PERCENT_REGEX = re.compile(r"(\d{1,3}(?:\.\d+)?)\s*(%?)")
# Regex for age patterns like "1-5 yrs", ">5 yrs", "1-6years", "1-5age"
AGE_PATTERN_REGEX = re.compile(r"((\d+)\s*-\s*(\d+)|([<>]=?)\s*(\d+)|(above|below)\s*(\d+))\s*(?:yrs?|years?|age)?", re.IGNORECASE)
# Regex for GVW patterns like "<2450 GVW", "3.5-7.5T", ">40T"
GVW_PATTERN_REGEX = re.compile(r"(([<>]=?)\s*\d+(?:\.\d+)?|\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?)\s*(?:T|GVW)", re.IGNORECASE)
# Regex for HP patterns like "<50 HP", ">50 HP"
HP_PATTERN_REGEX = re.compile(r"(([<>]=?|above|below)\s*\d+)\s*HP", re.IGNORECASE)
# Regex for CC patterns like "<=1000cc"
CC_PATTERN_REGEX = re.compile(r"(([<>]=?)\s*\d+)\s*CC", re.IGNORECASE)
# Regex for Seating capacity patterns like "<18", "18-36", ">36 seater"
# Made more specific to avoid matching GVW by looking for 'seater' or context
SEATING_CAP_REGEX = re.compile(r"(([<>]=?)\s*\d+|\d+\s*-\s*\d+|\d+\s*upto\s*\d+)\s*(?:seater|capacity)?", re.IGNORECASE)
# Regex for text in parentheses
PARENTHESIS_REGEX = re.compile(r"\((.*?)\)")
# Regex for special cluster codes with "only" or associated with percentage
# Looks for 'only MAKE in CODE', 'MAKE in CODE', 'CODE-PERCENT%', 'PERCENT% on CODE'
CLUSTER_CODE_REGEX = re.compile(
    r"(?:only\s+)?(?:({make})\s+in\s+)?({code})\b" # only MAKE in CODE or only CODE or MAKE in CODE or CODE
    r"|" # OR
    r"\b({code})\s*-\s*{percent}" # CODE-PERCENT%
    r"|" # OR
    r"{percent}\s+(?:on\s+)?\(?\b({code})\)?" # PERCENT% on CODE
    .format(make="|".join(BIKE_MAKES), code="|".join(SPECIAL_CLUSTER_CODES), percent=PERCENT_REGEX.pattern),
    re.IGNORECASE
)
# Regex specifically for the 'only MAKE in CODE' pattern to identify cell-wide overrides
CLUSTER_CODE_ONLY_REGEX = re.compile(r"only\s+(?:({make})\s+in\s+)?({code})\b".format(make="|".join(BIKE_MAKES), code="|".join(SPECIAL_CLUSTER_CODES)), re.IGNORECASE)

# Regex for 3W, 4W, 2W
WHEELER_REGEX = re.compile(r"\b([234]W)\b", re.IGNORECASE)
# Regex to find age and percentage pairs on the same line (more robust)
# Group 1: Full age pattern (e.g., "1-5 yrs", ">5 yrs")
# Group 8: Percentage number
# Group 9: Percentage symbol (%)
AGE_PERCENT_PAIR_REGEX = re.compile(r"^.*?" + AGE_PATTERN_REGEX.pattern + r".*?" + PERCENT_REGEX.pattern, re.IGNORECASE | re.MULTILINE)

# Fixed regex for percentage-keyword pairs to avoid duplicate percentage capture
# Example: "45% on TATA", "50% on new", "others 16%"
PERCENT_KEYWORD_REGEX = re.compile(
    # Case 1: Percent ... (Keyword)
    r"(\d{{1,3}}(?:\.\d+)?)\s*(%?)\s+(?:on\s+)?\(?\b({0})\)?"
    r"|" # OR
    # Case 2: (others) ... Percent
    r"\b(others)\s+(\d{{1,3}}(?:\.\d+)?)\s*(%?)"
    .format("|".join(BIKE_MAKES + AGE_KEYWORDS)),
    re.IGNORECASE
)

# --- Helper Functions ---

def deduplicate_columns(columns):
    """Appends suffixes like .1, .2 to duplicate column names."""
    counts = {}
    new_columns = []
    for col in columns:
        clean_col = safe_string(col) # Ensure consistent type/cleaning
        if clean_col in counts:
            counts[clean_col] += 1
            new_columns.append(f"{clean_col}.{counts[clean_col]}")
        else:
            counts[clean_col] = 0
            new_columns.append(clean_col)
    return new_columns

def safe_string(val):
    """Converts value to string, handling NaN/None."""
    if pd.isna(val):
        return ""
    # Handle potential float formatting issues from Excel
    if isinstance(val, float) and val.is_integer():
        return str(int(val)).strip()
    return str(val).strip()

def find_header_row(df, max_rows=15):
    """Identifies header row by finding the first row with likely header keywords or lack of percentages."""
    header_row_index = -1
    potential_headers = []
    rto_col_idx = -1
    found_rto_in_row = -1

    # First pass: Find rows containing "RTO CLUSTER"
    for i, row in df.head(max_rows).iterrows():
        row_str = " ".join(safe_string(x) for x in row.tolist()).upper()
        if "RTO CLUSTER" in row_str:
            potential_headers.append(i)
            # Try to get the column index immediately
            try:
                # Find the first column index where the cell contains 'RTO CLUSTER'
                rto_col_idx_candidate = next((idx for idx, cell in enumerate(row) if "RTO CLUSTER" in safe_string(cell).upper()), -1)
                if rto_col_idx_candidate != -1:
                    # Store the first found RTO column index and its row
                    if found_rto_in_row == -1:
                        found_rto_in_row = i
                        rto_col_idx = rto_col_idx_candidate
            except Exception:
                pass # Error finding index, continue search

    # If RTO cluster was found, assume the row it was found in is the header
    if found_rto_in_row != -1:
        header_row_index = found_rto_in_row
        print(f"Found 'RTO CLUSTER' in row {header_row_index}, column index {rto_col_idx}")
    else:
        # Fallback: Use previous heuristic if RTO CLUSTER text not found directly
        print("Warning: 'RTO CLUSTER' text not found directly in cells. Using heuristic...")
        for i, row in df.head(max_rows).iterrows():
            non_numeric_count = 0
            percentage_count = 0
            row_str = " ".join(safe_string(x) for x in row.tolist()).upper()

            for cell in row:
                s_cell = safe_string(cell)
                if s_cell:
                    is_numeric = False
                    try:
                        num_val = float(s_cell.replace("%", ""))
                        is_numeric = True
                        # Check if it looks like a percentage (0-1 range or has %)
                        if "%" in s_cell or (0 < num_val <= 1 and "." in s_cell) or num_val > 1:
                           percentage_count += 1
                    except ValueError:
                        pass # Not numeric
                    if not is_numeric:
                        non_numeric_count += 1

            # Heuristic: If a row has many non-numeric entries and few percentages
            if non_numeric_count > len(row) / 2 and percentage_count < 3:
                 potential_headers.append(i)

        if potential_headers:
            header_row_index = potential_headers[-1] # Assume last potential header
            print(f"Heuristic identified potential header row: {header_row_index}")
            # Try finding RTO cluster column index in this assumed header row
            header_content = df.iloc[header_row_index].apply(safe_string).str.upper()
            try:
                rto_col_idx = header_content[header_content.str.contains("RTO CLUSTER", na=False)].index[0]
                print(f"Found 'RTO CLUSTER' in heuristic header row {header_row_index}, column index {rto_col_idx}")
            except IndexError:
                print(f"Warning: Could not find 'RTO CLUSTER' in heuristic header row {header_row_index}.")
                rto_col_idx = -1 # Mark as not found
        else:
             header_row_index = -1 # Could not identify header

    # Final check if header/RTO column still not found
    if header_row_index == -1 or rto_col_idx == -1:
         print("Error: Could not reliably determine header row or RTO Cluster column. Cannot proceed.")
         return -1, -1, None

    print(f"Final Identified Header Row Index: {header_row_index}")
    print(f"Final Identified RTO Cluster Column Index: {rto_col_idx}")
    # Return the original column name/label from the DataFrame's columns
    return header_row_index, rto_col_idx, df.columns[rto_col_idx]

def parse_header_string(header_str):
    """Parses a header string to extract keywords and patterns."""
    parsed_info = {col: set() for col in OUTPUT_COLUMNS}
    remarks = []
    upper_header = header_str.upper()

    # --- Extract Keywords --- 
    # Veh Type & Product Type
    for keyword, (veh_type_val, prod_type_val) in VEH_TYPE_KEYWORDS.items():
        if keyword in upper_header:
            if veh_type_val: parsed_info["veh_type"].add(veh_type_val)
            if prod_type_val: parsed_info["product_type"].add(prod_type_val)

    # Vehicle (and associated Veh Type)
    for keyword, (veh_type_val, vehicle_val) in VEHICLE_KEYWORDS.items():
        # Check for whole word match if keyword is short like BUS, TAXI
        pattern = r"\b" + keyword + r"\b" if len(keyword) <= 4 else keyword
        if re.search(pattern, upper_header):
            if veh_type_val: parsed_info["veh_type"].add(veh_type_val)
            if vehicle_val: parsed_info["vehicle"].add(vehicle_val)
            # Special case for BUS - capture full name like "School Bus"
            if keyword == "BUS":
                match = re.search(r"(SCHOOL\s+BUS|STAFF\s+BUS|BUS)", header_str, re.IGNORECASE)
                if match: parsed_info["vehicle"].add(match.group(1).title())

    # Fuel Type
    for keyword in FUEL_TYPE_KEYWORDS:
        if keyword in upper_header:
            parsed_info["fuel_type"].add(keyword.title())

    # Age
    for keyword in AGE_KEYWORDS:
        if keyword in upper_header:
            parsed_info["age"].add(keyword.title())

    # Plan Type
    for keyword, plan_val in PLAN_TYPE_KEYWORDS.items():
        if keyword in upper_header:
            parsed_info["plan_type"].add(plan_val)

    # Wheeler type
    wheeler_match = WHEELER_REGEX.search(upper_header)
    if wheeler_match:
        parsed_info["vehicle"].add(wheeler_match.group(1))

    # MISC CE
    if "MISC" in upper_header and "CE" in upper_header:
        parsed_info["veh_type"].add("MISC")
        parsed_info["vehicle"].add("CE")

    # --- Extract Patterns --- 
    # Important: Process patterns *after* keywords to potentially refine or override

    # GVW Pattern
    gvw_match = GVW_PATTERN_REGEX.search(header_str)
    if gvw_match: parsed_info["gvw"].add(gvw_match.group(1).strip())

    # HP Pattern (Engine Type)
    hp_match = HP_PATTERN_REGEX.search(header_str)
    if hp_match: parsed_info["engine_type"].add(hp_match.group(1).strip() + " HP")

    # CC Pattern (Engine Type)
    cc_match = CC_PATTERN_REGEX.search(header_str)
    if cc_match: parsed_info["engine_type"].add(cc_match.group(1).strip() + "cc")

    # Seating Capacity Pattern - check context (e.g., if PCV or Bus/Taxi found)
    is_pcv_context = False
    if parsed_info.get("veh_type") and "PCV" in parsed_info["veh_type"]:
        is_pcv_context = True
    if parsed_info.get("vehicle") and any(v in parsed_info["vehicle"] for v in ["Bus", "School Bus", "Staff Bus", "Taxi"]):
        is_pcv_context = True

    if is_pcv_context:
        seating_match = SEATING_CAP_REGEX.search(header_str)
        if seating_match:
             # Avoid adding if it looks like a GVW value matched loosely
             potential_seat_cap = seating_match.group(1).strip()
             if not gvw_match or potential_seat_cap != gvw_match.group(1).strip():
                 parsed_info["seating_cap"].add(potential_seat_cap)

    # --- Final Processing --- 
    # Remarks from parentheses
    paren_matches = PARENTHESIS_REGEX.findall(header_str)
    for match in paren_matches:
        remarks.append(match.strip())
    if remarks:
        parsed_info["remark"].add("; ".join(remarks))

    # Convert sets to lists or single values
    final_parsed = {}
    for key, value_set in parsed_info.items():
        if value_set:
            if len(value_set) == 1:
                final_parsed[key] = list(value_set)[0]
            else:
                 # Allow multiple values for these specific fields
                 if key in ["fuel_type", "vehicle", "remark", "engine_type"]:
                      final_parsed[key] = sorted(list(value_set)) # Sort for consistency
                 else:
                      # For others, take the first one found (or apply specific logic if needed)
                      final_parsed[key] = sorted(list(value_set))[0]

    # Ensure product_type is set if veh_type indicates Commercial
    if final_parsed.get("veh_type") == "GCV" and "product_type" not in final_parsed:
        final_parsed["product_type"] = "Commercial Vehicle"

    return final_parsed

def parse_percentage_cell(cell_str, header_info):
    """Parses a percentage cell, handling multiple lines, keywords, and patterns.
       Returns a list of dictionaries, each representing a row to be added.
    """
    results = [] # List to store results for potentially multiple rows from one cell
    lines = [line.strip() for line in cell_str.split("\n") if line.strip()]
    
    # --- Pre-parse Cell-wide Information --- 
    cell_remarks = []
    cell_cluster_override = None
    cell_cluster_override_make = None
    override_line_indices = set()
    non_rule_lines = [] # Store lines that are not rules or overrides

    # Extract remarks from parentheses first and mark lines
    paren_matches = PARENTHESIS_REGEX.findall(cell_str)
    for match in paren_matches:
        cell_remarks.append(match.strip())
        for i, line in enumerate(lines):
            if f"({match})" in line:
                override_line_indices.add(i)

    # Check for cell-wide cluster override ('only MAKE in CODE')
    # This rule applies to *all* rows generated from this cell if found
    for i, line in enumerate(lines):
        if i in override_line_indices: continue # Skip lines with remarks in parens
        cluster_override_match = CLUSTER_CODE_ONLY_REGEX.search(line)
        if cluster_override_match:
            potential_make, potential_code = cluster_override_match.groups()
            code_upper = potential_code.strip().upper()
            if code_upper in SPECIAL_CLUSTER_CODES:
                cell_cluster_override = code_upper
                if potential_make and potential_make.strip().upper() in BIKE_MAKES:
                    cell_cluster_override_make = potential_make.strip().upper()
                print(f"Found cell-wide cluster override: Code={cell_cluster_override}, Make={cell_cluster_override_make} from line: {line}")
                override_line_indices.add(i) # Mark this line as processed
                # Only take the first 'only' rule found
                break 
            else:
                 print(f"Warning: Found 'only' pattern but code '{potential_code}' not in known list. Adding to remarks.")
                 cell_remarks.append(f"Condition found: {cluster_override_match.group(0)}")
                 override_line_indices.add(i) # Mark line as processed

    # --- Process Line by Line for Specific Rules --- 
    for i, line in enumerate(lines):
        # Skip lines already processed (overrides, remarks in parens, etc.)
        if i in override_line_indices:
            continue

        line_age = None
        line_percent = None
        line_make = None
        line_cluster_override = None # Line-specific override (e.g., 45% on WB1)
        line_cluster_override_make = None
        is_rule_line = False # Flag if this line defines a specific rule

        # Check for line-specific cluster code association (e.g., 45% on WB1)
        cluster_match = CLUSTER_CODE_REGEX.search(line)
        if cluster_match:
            matched_make = cluster_match.group(1)
            matched_code = cluster_match.group(2) or cluster_match.group(3) or cluster_match.group(5)
            if matched_code:
                code_upper = matched_code.strip().upper()
                if code_upper in SPECIAL_CLUSTER_CODES:
                    line_cluster_override = code_upper
                    if matched_make and matched_make.strip().upper() in BIKE_MAKES:
                        line_cluster_override_make = matched_make.strip().upper()
                    print(f"Line {i}: Found line-specific cluster association: Code={line_cluster_override}, Make={line_cluster_override_make}")

        # 1. Try matching Age-Percent pairs
        age_percent_match = AGE_PERCENT_PAIR_REGEX.match(line)
        if age_percent_match:
            line_age = age_percent_match.group(1).strip() # Full age pattern
            percent_num = age_percent_match.group(8).strip()
            percent_sym = age_percent_match.group(9).strip()
            line_percent = f"{percent_num}{percent_sym if percent_sym else '%'}"
            is_rule_line = True
            print(f"Line {i}: Found Age-Percent pair: Age='{line_age}', Percent='{line_percent}'")
            # Check for make on the same line
            for make in BIKE_MAKES:
                 if re.search(r"\b" + make + r"\b", line, re.IGNORECASE):
                     line_make = make.title()
                     break

        # 2. If no pair, try finding percent associated with keywords
        if not is_rule_line:
            percent_keyword_match = PERCENT_KEYWORD_REGEX.search(line)
            if percent_keyword_match:
                # Case 1: Percent ... (Keyword)
                if percent_keyword_match.group(3):
                    percent_num = percent_keyword_match.group(1)
                    percent_sym = percent_keyword_match.group(2)
                    keyword = percent_keyword_match.group(3)
                    
                    line_percent = f"{percent_num}{percent_sym if percent_sym else '%'}"
                    is_rule_line = True
                    
                    keyword_upper = keyword.upper()
                    if keyword_upper in BIKE_MAKES:
                        line_make = keyword_upper.title()
                    elif keyword_upper in [a.upper() for a in AGE_KEYWORDS]:
                        line_age = keyword_upper.title()
                    
                # Case 2: 'others' ... Percent
                elif percent_keyword_match.group(4):
                    others_keyword = percent_keyword_match.group(4)
                    percent_num = percent_keyword_match.group(5)
                    percent_sym = percent_keyword_match.group(6)
                    
                    line_percent = f"{percent_num}{percent_sym if percent_sym else '%'}"
                    is_rule_line = True
                    # No specific make for 'others'
                
                print(f"Line {i}: Found Percent-Keyword pair: Percent='{line_percent}', Keyword='{keyword if percent_keyword_match.group(3) else others_keyword}'")

        # 3. If still no rule, try finding just a standalone percentage
        if not is_rule_line:
             percent_match = PERCENT_REGEX.search(line)
             if percent_match:
                 percent_num = percent_match.group(1).strip() if percent_match.group(1) else ""
                 percent_sym = percent_match.group(2).strip() if percent_match.group(2) else ""
                 line_percent = f"{percent_num}{percent_sym if percent_sym else '%'}"
                 is_rule_line = True # Treat standalone percent as a rule
                 print(f"Line {i}: Found standalone Percent: '{line_percent}'")
                 # Check for keywords (Age, Make) on the same line
                 age_match_on_line = AGE_PATTERN_REGEX.search(line)
                 if age_match_on_line:
                     line_age = age_match_on_line.group(1).strip()
                 for make in BIKE_MAKES:
                     if re.search(r"\b" + make + r"\b", line, re.IGNORECASE):
                         line_make = make.title()
                         break
                 # Also check for simple age keywords like 'new'/'old'
                 for age_kw in AGE_KEYWORDS:
                      if re.search(r"\b" + age_kw + r"\b", line, re.IGNORECASE):
                          line_age = age_kw.title()
                          break

        # 4. If it's a rule line, generate the base row(s)
        if is_rule_line and line_percent is not None:
            base_row = header_info.copy()
            base_row["po_percent"] = line_percent
            
            # Apply line-specific findings (Age, Make)
            if line_age: base_row["age"] = line_age
            if line_make: base_row["bike_make"] = line_make

            # Apply CELL-WIDE cluster/make override FIRST (if exists)
            if cell_cluster_override:
                base_row["cluster_code"] = cell_cluster_override
                if cell_cluster_override_make:
                    # Cell override make takes precedence
                    base_row["bike_make"] = cell_cluster_override_make.title()
            # ELSE apply LINE-SPECIFIC cluster/make override (if exists and no cell override)
            elif line_cluster_override:
                base_row["cluster_code"] = line_cluster_override
                if line_cluster_override_make:
                    # Line override make takes precedence over make found elsewhere on line
                    base_row["bike_make"] = line_cluster_override_make.title()
            # ELSE use original cluster code from header
            else:
                 base_row["cluster_code"] = header_info.get("cluster_code")

            # Combine remarks (cell-wide only for now)
            base_row["remark"] = "; ".join(filter(None, cell_remarks))

            # --- Expand based on Header's Multi-value Fields --- 
            rows_to_add = []
            temp_rows = [base_row]

            # Expand by Fuel Type from header
            fuel_types = header_info.get("fuel_type", [None])
            if isinstance(fuel_types, str): fuel_types = [fuel_types]
            expanded_fuel = []
            for row in temp_rows:
                for fuel in fuel_types:
                    new_row = row.copy()
                    new_row["fuel_type"] = fuel
                    expanded_fuel.append(new_row)
            temp_rows = expanded_fuel

            # Expand by Vehicle/Bike Make Rule (Separate Rows)
            vehicles_from_header = header_info.get("vehicle", [None])
            if isinstance(vehicles_from_header, str): vehicles_from_header = [vehicles_from_header]
            current_row_make = base_row.get("bike_make") # Make determined for this rule line/override

            for row in temp_rows:
                # Case 1: Rule line/override determined a bike make
                if current_row_make:
                    new_row = row.copy()
                    new_row["bike_make"] = current_row_make
                    new_row["vehicle"] = None # Vehicle is blank
                    rows_to_add.append(new_row)
                # Case 2: No make from rule line, but vehicles in header
                elif vehicles_from_header != [None]:
                     for veh in vehicles_from_header:
                         new_row = row.copy()
                         new_row["bike_make"] = None # Bike make is blank
                         new_row["vehicle"] = veh
                         rows_to_add.append(new_row)
                # Case 3: Neither bike make nor vehicle applies
                else:
                    new_row = row.copy()
                    new_row["bike_make"] = None
                    new_row["vehicle"] = None
                    rows_to_add.append(new_row)

            results.extend(rows_to_add)

        # 5. If not a rule line, check for decline/no business or add to unparsed
        elif not is_rule_line:
            if line.strip().lower() in ["decline", "no business", "cc"]:
                # Create a row for decline/no business
                base_row = header_info.copy()
                base_row["po_percent"] = line.strip().title()
                # Apply cluster override if present
                if cell_cluster_override:
                    base_row["cluster_code"] = cell_cluster_override
                else:
                    base_row["cluster_code"] = header_info.get("cluster_code")
                base_row["remark"] = "; ".join(filter(None, cell_remarks))
                # Expand this row as well
                rows_to_add = []
                temp_rows = [base_row]
                fuel_types = header_info.get("fuel_type", [None])
                if isinstance(fuel_types, str): fuel_types = [fuel_types]
                expanded_fuel = []
                for row in temp_rows:
                    for fuel in fuel_types:
                        new_row = row.copy(); new_row["fuel_type"] = fuel; expanded_fuel.append(new_row)
                temp_rows = expanded_fuel
                vehicles_from_header = header_info.get("vehicle", [None])
                if isinstance(vehicles_from_header, str): vehicles_from_header = [vehicles_from_header]
                for row in temp_rows:
                    if vehicles_from_header != [None]:
                         for veh in vehicles_from_header:
                             new_row = row.copy(); new_row["bike_make"] = None; new_row["vehicle"] = veh; rows_to_add.append(new_row)
                    else:
                        new_row = row.copy(); new_row["bike_make"] = None; new_row["vehicle"] = None; rows_to_add.append(new_row)
                results.extend(rows_to_add)
            else:
                # Collect unparsed lines (potential remarks for later)
                if line.strip().upper() not in ["EMG"]: # Avoid adding known non-data codes
                    non_rule_lines.append(line)

    # --- Final Handling --- 
    # If no rows were generated but there was content, maybe create a default row?
    if not results and lines and not any(l.strip().lower() in ["decline", "no business", "cc"] for l in lines):
        print(f"Warning: Cell content not parsed into specific rules: {cell_str}")
        # Optionally create a row with the raw content in remarks or po_percent if needed
        pass

    # Add unparsed lines as remarks to all generated rows?
    if non_rule_lines and results:
        unparsed_remark = "; ".join(non_rule_lines)
        for row in results:
            existing_remark = row.get("remark", "")
            if existing_remark:
                row["remark"] = f"{existing_remark}; Unparsed: {unparsed_remark}"
            else:
                row["remark"] = f"Unparsed: {unparsed_remark}"

    # Clean up results - ensure all columns exist
    final_results = []
    for res in results:
        cleaned_res = {col: res.get(col) for col in OUTPUT_COLUMNS}
        final_results.append(cleaned_res)
    
    print(f"Final results count: {len(final_results)}")
    print(f"Final results: {final_results}")
    print("--- End Debugging ---")
    return final_results


# --- Main Processing Logic ---

def process_excel_sheet(df):
    """Processes a single sheet DataFrame based on the algorithm."""
    processed_data = []

    # 1. Find Header Row and RTO Cluster Column
    header_row_idx, rto_col_idx, rto_col_name = find_header_row(df)
    if header_row_idx == -1 or rto_col_idx == -1:
        print("Error: Could not find essential header/columns. Skipping sheet.")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    # 2. Extract actual header values from the identified header row
    actual_headers = df.iloc[header_row_idx].apply(safe_string)
    # Clean the actual headers for use as column names
    cleaned_headers = [str(h).replace('\n', ' ').strip() for h in actual_headers]

    # 3. Get the data starting from the row after the header
    data_df = df.iloc[header_row_idx + 1:].copy()
    # Assign the cleaned actual headers as column names, handling duplicates
    data_df.columns = deduplicate_columns(cleaned_headers)

    # 4. Identify the RTO Cluster column name *within the cleaned (potentially deduped) headers*
    rto_col_name_in_data = None
    try:
        # Find the column name at the previously identified index
        rto_col_name_in_data = data_df.columns[rto_col_idx]
        # Optional: Verify it still looks like an RTO column
        if "RTO CLUSTER" not in rto_col_name_in_data.upper():
             print(f"Warning: Column at index {rto_col_idx} ('{rto_col_name_in_data}') might not be the RTO Cluster column.")
             # Add fallback search using the original (non-deduped) name pattern
             found = False
             original_rto_header = actual_headers.iloc[rto_col_idx]
             for i, col_name in enumerate(data_df.columns):
                 # Check if the potentially deduped name starts with the original cleaned name
                 if col_name.startswith(str(original_rto_header).replace('\n', ' ').strip()) and "RTO CLUSTER" in col_name.upper():
                     rto_col_name_in_data = col_name
                     rto_col_idx = i # Update index if found elsewhere
                     print(f"Found RTO Cluster column as '{rto_col_name_in_data}' at index {i} instead.")
                     found = True
                     break
             if not found:
                 print("Error: Could not confirm RTO Cluster column in data headers after dedup. Skipping sheet.")
                 return pd.DataFrame(columns=OUTPUT_COLUMNS)

    except IndexError:
        print(f"Error: RTO Cluster column index {rto_col_idx} is out of bounds for data columns. Skipping sheet.")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    except Exception as e:
         print(f"Error identifying RTO column name in data_df: {e}")
         return pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Iterate through data rows
    current_cluster_code = None
    for idx, row in data_df.iterrows():
        # Update cluster code if a new one is found in the RTO column
        potential_cluster_val = row[rto_col_name_in_data]
        actual_cluster_scalar = None
        # Handle Series case (less likely with deduped names, but keep for safety)
        if isinstance(potential_cluster_val, pd.Series):
            if not potential_cluster_val.empty:
                actual_cluster_scalar = potential_cluster_val.iloc[0]
        else:
            actual_cluster_scalar = potential_cluster_val

        cluster_val = safe_string(actual_cluster_scalar)

        # Skip rows where RTO cluster is empty or seems like a header remnant
        if not cluster_val or cluster_val.upper() == "RTO CLUSTER":
            continue

        current_cluster_code = cluster_val # Update cluster code for subsequent columns in this row

        # Iterate through columns (potential headers for payout info)
        for col_idx, cell_value in enumerate(row): # Iterate using index
            # Skip RTO column itself
            if col_idx == rto_col_idx:
                continue

            col_header_str = safe_string(data_df.columns[col_idx]) # Get header using index
            cell_str = safe_string(cell_value)

            # Skip empty cells/headers, or cells containing the header text itself
            if not cell_str or not col_header_str or cell_str == col_header_str:
                continue

            # Avoid processing columns that are clearly not payout headers (e.g., 'Sr. No.')
            if re.match(r"Sr\.?\s*No\.?", col_header_str, re.IGNORECASE):
                continue

            # Parse the header for this column
            header_info = parse_header_string(col_header_str)
            header_info["cluster_code"] = current_cluster_code # Set base cluster code for this row

            # Now parse the cell value which contains the percentage and potentially other keywords
            parsed_rows = parse_percentage_cell(cell_str, header_info)

            # Add the generated rows to our results
            processed_data.extend(parsed_rows)

    return pd.DataFrame(processed_data)


def process_excel_file(file_path):
    """Processes all sheets in an Excel file."""
    try:
        xls = pd.ExcelFile(file_path)
        all_data = []
        
        for sheet_name in xls.sheet_names:
            print(f"\nProcessing sheet: {sheet_name}")
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            sheet_data = process_excel_sheet(df)
            if not sheet_data.empty:
                all_data.append(sheet_data)
                print(f"Extracted {len(sheet_data)} rows from sheet '{sheet_name}'")
            else:
                print(f"No data extracted from sheet '{sheet_name}'")
        
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            print(f"\nTotal rows extracted from all sheets: {len(result_df)}")
            return result_df
        else:
            print("No data extracted from any sheet.")
            return pd.DataFrame(columns=OUTPUT_COLUMNS)
    
    except Exception as e:
        print(f"Error processing Excel file: {e}")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)


# --- Main Execution ---

if __name__ == "__main__":
    input_file = input("Enter the path to the input Excel file: ")
    output_file = input("Enter the path for the output Excel file: ")
    
    result_df = process_excel_file(input_file)
    
    if not result_df.empty:
        result_df.to_excel(output_file, index=False)
        print(f"Output saved to {output_file}")
    else:
        print("No data to save. Output file not created.")
