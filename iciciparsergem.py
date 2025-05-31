import pandas as pd
import re
import numpy as np # For handling NaN

# --- Configuration ---
# !!! IMPORTANT: Populate these lists with your actual data !!!
BIKE_MAKES_LIST = [
    "TATA", "AL", "ASHOK LEYLAND", "EICHER", "HERO", "HONDA", "BAJAJ", "TVS", 
    "ROYAL ENFIELD", "YAMAHA", "SUZUKI", "MAHINDRA", "FORCE", "SML", "BHARAT BENZ"
    # Add all relevant bike/vehicle makes
]
RTO_CODES_LIST = [
    "WB1", "DL", "AS01", "MH01", "KA01", "TN01", "UP32", "GJ01", "RJ14",
    "NON DL", "NON-DL" # Add all relevant RTO/cluster codes
] 
# Note: "Non DL" is also handled specifically in parsing logic.

VEHICLE_KEYWORDS_TO_TYPE = {
    "TANKER": "MISC", "TIPPER": "MISC", "TRUCK": "MISC", "TRAILER": "MISC",
    "DUMPER": "MISC", "CRANES": "MISC", "TRACTOR": "MISC", "TRACTER": "MISC",
    "BUS": "PCV", "SCHOOL BUS": "PCV", "STAFF BUS": "PCV",
    "TAXI": "PCV",
    "CE": "MISC" # Typically when "MISC" is also present in header
}

OUTPUT_COLUMNS = [
    "cluster_code", "bike_make", "model", "plan_type", "engine_type", "fuel_type",
    "plan_subtype", "add_on", "plan_term", "business_slab", "age", "po_percent",
    "slab_month", "remark", "product_type", "ncb", "vehicle", "veh_type",
    "seating_cap", "gvw"
]

# --- Helper Functions ---

def clean_text(text):
    """Cleans text by converting to string, stripping whitespace."""
    if pd.isna(text) or text is None:
        return ""
    return str(text).strip()

def parse_gvw(text):
    """Parses GVW values like '<2450 GVW', '3.5-7.5T', '>40T'."""
    text_upper = clean_text(text).upper()
    # Pattern for <, >, <=, >= followed by number and GVW (e.g., "<2450 GVW", ">=3000 GVW")
    match = re.search(r"([<>]=?)\s*(\d+(?:\.\d+)?)\s*GVW", text_upper)
    if match:
        return f"{match.group(1)}{match.group(2)}"
    
    # Pattern for number followed by GVW (e.g., "2450 GVW")
    match = re.search(r"(\d+(?:\.\d+)?)\s*GVW", text_upper)
    if match:
        return match.group(1)

    # Pattern for tonnage like "3.5-7.5T", "7.5-12T", ">40T", ">=7.5T"
    # Handles ranges (e.g., 7.5-12T) or single values with T (e.g., >40T)
    match = re.search(r"([<>]=?)?\s*(\d+(?:\.\d+)?(?:-\d+(?:\.\d+)?)?T)", text_upper)
    if match:
        operator = match.group(1) if match.group(1) else ""
        value = match.group(2)
        return f"{operator}{value}"
    return ""

def parse_engine_hp(text):
    """Parses engine horsepower like '<50 HP', '>50 HP', 'Above 50HP'."""
    text_upper = clean_text(text).upper()
    match = re.search(r"([<>]=?|ABOVE)\s*(\d+)\s*HP", text_upper)
    if match:
        op = match.group(1)
        if op == "ABOVE": op = ">"
        return f"{op}{match.group(2)} HP"
    return ""

def parse_engine_cc(text):
    """Parses engine capacity in CC like '<=1000cc', '>1000cc', '1500CC'."""
    text_upper = clean_text(text).upper()
    match = re.search(r"([<>]=?)\s*(\d+)\s*CC", text_upper)
    if match:
        return f"{match.group(1)}{match.group(2)}CC"
    match = re.search(r"(\d+)\s*CC", text_upper) # numberCC
    if match:
        return f"{match.group(1)}CC"
    return ""

def parse_seating_capacity(text):
    """Parses seating capacity like '<18', '18-36 seater', '>18 upto 36 seater'."""
    text_upper = clean_text(text).upper()
    # Order patterns from more specific to less specific
    patterns = [
        # Matches "> XX UPTO YY SEATER" or ">XX UPTO YY"
        r"(>\s*\d+\s*UPTO\s*\d+)(?:\s*SEATER)?",
        # Matches "XX-YY SEATER" or "XX-YY"
        r"(\d+\s*-\s*\d+)(?:\s*SEATER)?",
        # Matches "<XX SEATER", ">YY SEATER", "<=ZZ SEATER", ">=AA SEATER" or without "SEATER"
        r"([<>]=?\s*\d+)(?:\s*SEATER)?"
    ]
    for p_str in patterns:
        match = re.search(p_str, text_upper)
        if match:
            capacity_str = match.group(1).replace("UPTO", "to").strip()
            return f"{capacity_str} seater"
    return ""

def extract_percentage_from_segment(segment):
    """Extracts a percentage value (e.g., '45' from '45%') from a text segment."""
    segment = clean_text(segment)
    # Prioritize number directly followed by %
    match_direct_percent = re.search(r"(\d+(?:\.\d+)?)\s*%", segment)
    if match_direct_percent:
        return match_direct_percent.group(1)
    
    # If no direct %, look for a number that could be a percentage,
    # often at the beginning or end of a condition.
    # This is heuristic. Example: "Old 20" -> 20
    # Be careful not to pick up numbers that are part of age, gvw etc.
    # For now, rely on explicit "%" or context where percentage is expected.
    # The problem states: "sometimes it can happen that "%" symbol is not mentioned after number but still treat it as percentage."
    # This is tricky. For now, we'll be conservative.
    # Let's assume if a number stands alone or is clearly associated with a rate context.
    # A simple approach: if a number is found and no '%' but it's in a context where % is expected.
    # For now, the parsing of segments like "age X%" or "MAKE Y%" will handle this.
    # This function is mainly for extracting the numeric part once a percentage string is identified.
    
    # Fallback: if the segment itself is just a number (after stripping other context)
    # This is less reliable and should be used carefully.
    # Example: if the segment passed is "45" after isolating it.
    if segment.replace('.', '', 1).isdigit(): # Check if it's a number (int or float)
        return segment
        
    return ""


def parse_age_from_text(text_segment):
    """Parses age conditions like '1-5 yrs', '>5 yrs', 'new', 'old'."""
    text_segment_lower = clean_text(text_segment).lower()
    age_patterns = [
        r"(\d+\s*-\s*\d+\s*(?:yrs|years|yr|age))",      # 1-5 yrs, 1-6 years, 1-5age
        r"((?:>|>=|above)\s*\d+\s*(?:yrs|years|yr|age))", # >5 yrs, >=5 years, above 5 age
        r"((?:<|<=|upto)\s*\d+\s*(?:yrs|years|yr|age))",   # <5 yrs, upto 5 years
        r"\b(new)\b",
        r"\b(old)\b"
    ]
    for pattern in age_patterns:
        match = re.search(pattern, text_segment_lower)
        if match:
            # For 'new'/'old', return capitalized
            if match.group(1) in ["new", "old"]:
                return match.group(1).capitalize()
            return match.group(1)
    return ""

def process_header_string(header_str, base_attrs):
    """Parses a single header string and updates base_attrs."""
    header_lower = header_str.lower()

    # Veh Type & Product Type
    if "gcv" in header_lower or "scv" in header_lower or "lcv" in header_lower or "mhcv" in header_lower:
        base_attrs["veh_type"] = "GCV"
        if "gcv" in header_lower or "scv" in header_lower:
            base_attrs["product_type"] = "Commercial Vehicle"
    if "pcv" in header_lower:
        base_attrs["veh_type"] = "PCV"
        # If "PCVTAXI_ELECTRIC" type combined keywords
        if "taxi" in header_lower: base_attrs["vehicle"] = "Taxi"

    if "misc" in header_lower and "ce" in header_lower:
        base_attrs["veh_type"] = "MISC"
        base_attrs["vehicle"] = "CE"

    # Vehicle & associated veh_type
    # Iterate in a way that longer matches are preferred (e.g., "School Bus" before "Bus")
    sorted_vehicle_keywords = sorted(VEHICLE_KEYWORDS_TO_TYPE.keys(), key=len, reverse=True)
    for vk in sorted_vehicle_keywords:
        if vk.lower() in header_lower:
            base_attrs["vehicle"] = vk # Use the original casing from VEHICLE_KEYWORDS_TO_TYPE
            # Only set veh_type if not already set by a more general category like GCV/PCV
            if not base_attrs["veh_type"] or base_attrs["veh_type"] == "MISC": # Allow override if current is MISC
                 base_attrs["veh_type"] = VEHICLE_KEYWORDS_TO_TYPE[vk]
            break # Found primary vehicle

    # 3W, 4W, 2W (if no specific vehicle already found)
    if not base_attrs["vehicle"]:
        if "3w" in header_lower: base_attrs["vehicle"] = "3W"
        elif "4w" in header_lower: base_attrs["vehicle"] = "4W"
        elif "2w" in header_lower: base_attrs["vehicle"] = "2W"

    # Age
    if "new" in header_lower: base_attrs["age"] = "New"
    if "old" in header_lower: base_attrs["age"] = "Old"

    # Fuel type (can be multiple)
    header_fuel_types = []
    if "electric" in header_lower: header_fuel_types.append("Electric")
    if "petrol" in header_lower: header_fuel_types.append("Petrol")
    if "cng" in header_lower: header_fuel_types.append("CNG")
    if "diesel" in header_lower: header_fuel_types.append("Diesel")
    if "bifuel" in header_lower: header_fuel_types.append("Bifuel")
    if header_fuel_types: base_attrs["_header_fuel_types"] = header_fuel_types # Store temporarily

    # GVW
    gvw_val = parse_gvw(header_str) # Pass original case for parsing consistency
    if gvw_val: base_attrs["gvw"] = gvw_val
    
    # Engine Type (HP/CC)
    hp_val = parse_engine_hp(header_str)
    cc_val = parse_engine_cc(header_str)
    engine_parts = []
    if hp_val: engine_parts.append(hp_val)
    if cc_val: engine_parts.append(cc_val)
    if engine_parts: base_attrs["engine_type"] = " ".join(engine_parts)

    # Plan Type
    if any(pt.lower() in header_lower for pt in ["aotp", "satp", "tp"]):
        base_attrs["plan_type"] = "SATP"
    elif "on od" in header_lower: base_attrs["plan_type"] = "SAOD"
    elif "comp" in header_lower or "comprehensive" in header_lower : base_attrs["plan_type"] = "Comp"
    
    # Seating Capacity (if Bus is also mentioned in this header)
    if "bus" in header_lower:
        sc_val = parse_seating_capacity(header_str)
        if sc_val: base_attrs["seating_cap"] = sc_val

    # Bike makes from header (e.g., "TATA & AL only")
    header_bike_makes = []
    # Use regex to find whole words to avoid partial matches (e.g. "AL" in "Mahindra ALFA")
    for bm in BIKE_MAKES_LIST:
        if re.search(r'\b' + re.escape(bm.lower()) + r'\b', header_lower):
            header_bike_makes.append(bm)
    if header_bike_makes: base_attrs["_header_bike_makes"] = header_bike_makes


def parse_data_cell_into_segments(data_cell_text, rto_cluster_base):
    """
    Parses a data cell that might contain multiple conditions and percentages.
    Returns a list of dictionaries, each representing a segment with its own attributes.
    Example: "1-5 yrs 45% \n >5 yrs 55%" -> two segments
    Example: "26% (old, Tata & Eicher) 20% (Old, others)" -> three segments (Tata, Eicher, others)
    Example: "DL-30%, Non DL RTO-50%" -> two segments
    """
    segments = []
    cell_lower = data_cell_text.lower()

    # Pattern 1: "RTO_CODE-PERCENTAGE, RTO_CODE-PERCENTAGE" (e.g., DL-30%, Non DL RTO-50%)
    # Use finditer to get all matches for RTO codes
    # Ensure RTO_CODES_LIST includes "DL", "Non DL" etc.
    # Regex to find RTO codes followed by a percentage
    # (?:rto\s*)? allows optional "RTO" string
    # (\d+(?:\.\d+)?)\s*%? captures percentage value
    rto_percentage_pattern = re.compile(
        r"(\b(?:NON\s*DL|DL|" + "|".join(re.escape(code) for code in RTO_CODES_LIST) + r")\b)" # RTO Code
        r"(?:\s*RTO)?\s*(?:-|–)?\s*"  # Optional RTO text and hyphen/dash
        r"(\d+(?:\.\d+)?)\s*%?",      # Percentage
        re.IGNORECASE
    )
    
    rto_matches = list(rto_percentage_pattern.finditer(data_cell_text))
    if len(rto_matches) > 1 or (len(rto_matches) == 1 and ("non dl" in rto_matches[0].group(1).lower() or "dl" in rto_matches[0].group(1).lower())): # If multiple distinct RTOs or specific DL/Non-DL
        for match in rto_matches:
            segment = {"remark_detail": match.group(0)}
            rto_code_found = match.group(1).upper()
            # Normalize "NON DL"
            if "NON" in rto_code_found and "DL" in rto_code_found:
                segment["cluster_code"] = "Non DL"
            else:
                segment["cluster_code"] = rto_code_found

            segment["po_percent"] = match.group(2)
            segments.append(segment)
        if segments: return segments # Prioritize this pattern if matched

    # Pattern 2: "PERCENTAGE on MAKE \n others PERCENTAGE" (e.g., 45% on TATA \n others 16%)
    # This requires splitting lines and then parsing.
    lines = data_cell_text.split('\n')
    if "others" in cell_lower and len(lines) > 1:
        processed_others_pattern = False
        # First line for specific make(s)
        first_line_lower = lines[0].lower()
        first_line_percent_match = re.search(r"(\d+(?:\.\d+)?)\s*%?", lines[0])
        if first_line_percent_match:
            percent_val = first_line_percent_match.group(1)
            makes_in_line = []
            for bm in BIKE_MAKES_LIST:
                if re.search(r'\b' + re.escape(bm.lower()) + r'\b', first_line_lower):
                    makes_in_line.append(bm)
            
            if makes_in_line:
                for make in makes_in_line:
                    segments.append({
                        "bike_make": make, 
                        "po_percent": percent_val, 
                        "remark_detail": lines[0].strip()
                    })
                processed_others_pattern = True

        # Subsequent line(s) for "others"
        for i in range(1, len(lines)):
            if "others" in lines[i].lower():
                others_percent_match = re.search(r"(\d+(?:\.\d+)?)\s*%?", lines[i])
                if others_percent_match:
                    segments.append({
                        "bike_make": "", # Blank for others
                        "po_percent": others_percent_match.group(1),
                        "remark_detail": lines[i].strip()
                    })
                    processed_others_pattern = True
        if processed_others_pattern and segments: return segments


    # Pattern 3: "PERCENTAGE (condition1, condition2) PERCENTAGE (condition3)"
    # E.g., "26% (old, Tata & Eicher) 20% (Old, others)"
    # This is complex. We can try to find all occurrences of "X% (conditions)"
    # or "conditions Y%"
    # Regex to find "percentage followed by optional (conditions)" or "conditions followed by percentage"
    # This will capture blocks like "26% (old, Tata & Eicher)" and "20% (Old, others)"
    # Or "1-5 yrs 45%" and ">5 yrs 55%"
    
    # General segment finder: looks for a percentage and associated text
    # It can be "text PERCENTAGE text" or "PERCENTAGE text" or "text PERCENTAGE"
    # We'll iterate through lines, and within lines, try to identify these blocks.
    
    # Split by common delimiters that might separate distinct rules, like "\n" or sometimes "," if not part of a range.
    # For now, primarily use newline.
    
    potential_clauses = []
    current_clause = ""
    for line in data_cell_text.split('\n'):
        line = line.strip()
        if not line: continue

        # Try to identify if this line starts a new "percentage rule"
        # A line is a new rule if it contains its own percentage, or starts with common conditions like age.
        # This is heuristic.
        if re.search(r"\d+\s*%", line) or parse_age_from_text(line):
            if current_clause:
                potential_clauses.append(current_clause.strip())
            current_clause = line
        else: # Append to current clause if it seems to be a continuation
            current_clause += " " + line
    if current_clause:
        potential_clauses.append(current_clause.strip())

    if not potential_clauses: # If no newlines, treat whole cell as one clause
        potential_clauses.append(data_cell_text.strip())

    for clause_text in potential_clauses:
        segment = {"remark_detail": clause_text} # Default remark is the clause itself
        clause_lower = clause_text.lower()

        # Extract Percentage
        percent_match = re.search(r"(\d+(?:\.\d+)?)\s*%?", clause_text) # Find number possibly with %
        if percent_match:
            segment["po_percent"] = percent_match.group(1)
        else: # If no explicit percentage in this clause, it might be a condition for a previous one or an error. Skip.
            # However, the rule "sometimes % is not mentioned" means we might need to infer.
            # For now, require a number that looks like a percentage.
            # If the clause is just "Old 20", we need to be sure "20" is the percent.
            # Let's assume if a number is found, it's the percent for this clause.
            lone_number_match = re.search(r"\b(\d+(?:\.\d+)?)\b", clause_text) # Match standalone number
            # Ensure this number isn't part of age (e.g. "5" in ">5 yrs") or gvw etc.
            # This is tricky. A simple way is to extract known patterns first.
            temp_text_for_lone_num = clause_text
            age_in_clause_for_check = parse_age_from_text(clause_lower)
            if age_in_clause_for_check:
                temp_text_for_lone_num = temp_text_for_lone_num.replace(age_in_clause_for_check, "") # Remove age part
            
            # Add similar removals for gvw, cc, hp if needed before lone number search.
            # For now, this is a basic check.
            lone_number_match_refined = re.search(r"\b(\d+(?:\.\d+)?)\b", temp_text_for_lone_num)
            if lone_number_match_refined:
                 segment["po_percent"] = lone_number_match_refined.group(1)
            else:
                continue # No clear percentage for this clause.


        # Extract Age
        age_val = parse_age_from_text(clause_lower)
        if age_val: segment["age"] = age_val

        # Extract Bike Makes
        # "26% (old, Tata & Eicher)" -> makes are Tata, Eicher for this 26%
        makes_in_clause = []
        text_for_make_search = clause_text # Search in original case for BIKE_MAKES_LIST
        # Check for "excluding/except" for makes in this specific clause
        is_excluding = "excluding" in clause_lower or "except" in clause_lower
        
        for bm in BIKE_MAKES_LIST:
            if re.search(r'\b' + re.escape(bm) + r'\b', text_for_make_search, re.IGNORECASE):
                if not is_excluding:
                    makes_in_clause.append(bm)
        
        if makes_in_clause:
            segment["_segment_bike_makes"] = makes_in_clause
        elif "others" in clause_lower and not is_excluding : # Handle "others" if no specific makes found
             segment["bike_make"] = "" # Explicitly blank for "others"

        # Extract RTO code override: "only TATA in WB1"
        # This means this segment's cluster_code should be WB1.
        only_rto_match = re.search(r"\b(only)\b.*?(\b(?:WB1|DL|AS01)\b)", clause_lower, re.IGNORECASE) # Add more RTOs
        if not only_rto_match: # Handle "WB1only" or "WB1 only"
             only_rto_match = re.search(r"(\b(?:WB1|DL|AS01)\b)(only)\b", clause_lower, re.IGNORECASE) # Add more RTOs
        
        if only_rto_match:
            # The RTO code could be group 2 or group 1 depending on the regex match
            rto_code_in_clause = only_rto_match.group(2) if len(only_rto_match.groups()) > 1 and only_rto_match.group(2) else only_rto_match.group(1)
            # Check if this rto_code_in_clause is a valid one from RTO_CODES_LIST (case insensitive)
            found_rto = None
            for r_code in RTO_CODES_LIST:
                if r_code.lower() == rto_code_in_clause.lower():
                    found_rto = r_code
                    break
            if found_rto:
                segment["cluster_code"] = found_rto
        
        # If segment has a po_percent, add it
        if segment.get("po_percent"):
            segments.append(segment)

    # If after all pattern matching, no segments were created,
    # but the cell has content, treat the whole cell as one basic segment.
    if not segments and data_cell_text:
        basic_segment = {"remark_detail": data_cell_text}
        basic_percent = extract_percentage_from_segment(data_cell_text) # Try to get a %
        if basic_percent : basic_segment["po_percent"] = basic_percent
        # Try to get age and makes for this basic segment too
        basic_age = parse_age_from_text(data_cell_text.lower())
        if basic_age: basic_segment["age"] = basic_age
        
        basic_makes = []
        is_excluding_basic = "excluding" in data_cell_text.lower() or "except" in data_cell_text.lower()
        for bm in BIKE_MAKES_LIST:
            if re.search(r'\b' + re.escape(bm) + r'\b', data_cell_text, re.IGNORECASE):
                if not is_excluding_basic:
                    basic_makes.append(bm)
        if basic_makes: basic_segment["_segment_bike_makes"] = basic_makes
        elif "others" in data_cell_text.lower() and not is_excluding_basic:
            basic_segment["bike_make"] = ""

        if basic_segment.get("po_percent"): # Only add if we found a percentage
            segments.append(basic_segment)
            
    return segments


# --- Main Processing Function ---
def process_excel_table(df_table, main_header_attrs=None):
    """Processes a single table DataFrame."""
    all_output_rows = []
    
    header_row_idx = -1
    rto_column_idx = -1

    # Find the header row (containing "RTO Cluster")
    for r_idx in range(min(15, len(df_table))): # Search in top rows of this specific table
        row_values_lower = [clean_text(c).lower() for c in df_table.iloc[r_idx].tolist()]
        if "rto cluster" in row_values_lower:
            header_row_idx = r_idx
            rto_column_idx = row_values_lower.index("rto cluster")
            break
    
    if header_row_idx == -1:
        print(f"Warning: 'RTO Cluster' not found in a table segment. Skipping this segment.")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    column_header_texts = df_table.iloc[header_row_idx].fillna('').astype(str)
    
    # Data rows start after the identified header row
    for r_idx in range(header_row_idx + 1, len(df_table)):
        current_data_row = df_table.iloc[r_idx]
        base_rto_cluster_code = clean_text(current_data_row.iloc[rto_column_idx])

        if not base_rto_cluster_code: # Skip if RTO cluster is empty for this row
            continue

        # Iterate through each data cell in the row (corresponding to a column header)
        for c_idx in range(len(df_table.columns)):
            if c_idx == rto_column_idx: # Skip the RTO cluster column itself
                continue

            header_text_for_cell = clean_text(column_header_texts.iloc[c_idx])
            data_cell_original_text = clean_text(current_data_row.iloc[c_idx])

            if not data_cell_original_text: # Skip empty data cells
                continue

            # 1. Initialize base attributes for this combination
            current_base_attrs = {col: "" for col in OUTPUT_COLUMNS}
            if main_header_attrs: # Apply attributes from main grid header if present
                current_base_attrs.update(main_header_attrs)
            
            current_base_attrs["cluster_code"] = base_rto_cluster_code # Default from row

            # 2. Parse the column header for this specific data cell
            process_header_string(header_text_for_cell, current_base_attrs)
            
            # Temporarily store header-derived makes and fuels before cell processing
            header_derived_makes = current_base_attrs.pop("_header_bike_makes", [])
            header_derived_fuels = current_base_attrs.pop("_header_fuel_types", [])

            # 3. Parse the data cell content, which may yield multiple segments (conditions/rows)
            cell_segments = parse_data_cell_into_segments(data_cell_original_text, base_rto_cluster_code)

            if not cell_segments: # If cell parsing yields nothing, but cell had text, log or skip
                # This might happen if cell is just text without a clear percentage or parsable structure
                # Create a fallback row with remark if cell had text but no segments.
                fb_row = current_base_attrs.copy()
                fb_row["remark"] = (fb_row["remark"] + " " + f"Unparsed cell: {data_cell_original_text}").strip()
                fb_row["po_percent"] = data_cell_original_text # Put raw cell content as PO percent as fallback
                # all_output_rows.append(fb_row) # Decided to only add rows with parsed segments
                continue


            # 4. For each segment from the data cell, generate output row(s)
            for segment_attrs in cell_segments:
                # Start with a copy of attributes derived from column header and main header
                row_template = current_base_attrs.copy()
                
                # Apply segment-specific attributes (from cell parsing)
                # These can override values from header or main_header_attrs
                if segment_attrs.get("cluster_code"): # Cell segment can override RTO
                    row_template["cluster_code"] = segment_attrs["cluster_code"]
                if segment_attrs.get("age"):
                    row_template["age"] = segment_attrs["age"]
                if segment_attrs.get("po_percent"):
                    row_template["po_percent"] = segment_attrs["po_percent"]
                
                # Remarks from cell
                # Parentheses content from original cell
                parentheses_content = re.findall(r"\((.*?)\)", data_cell_original_text)
                if parentheses_content:
                    row_template["remark"] = (row_template["remark"] + " " + " ".join(parentheses_content)).strip()
                
                # "Excluding", "Except" lines from original cell
                if "excluding" in data_cell_original_text.lower() or "except" in data_cell_original_text.lower():
                    for line in data_cell_original_text.split('\n'):
                        if "excluding" in line.lower() or "except" in line.lower():
                            row_template["remark"] = (row_template["remark"] + " " + line.strip()).strip()
                
                # "new and 1st year" from original cell
                if "new and 1st year" in data_cell_original_text.lower():
                    row_template["remark"] = (row_template["remark"] + " new and 1st year").strip()
                    if not row_template["age"]: row_template["age"] = "New" # Ensure "New" is set for age

                # Add segment's own remark detail if any (e.g. the clause text)
                if segment_attrs.get("remark_detail"):
                     # Avoid duplicating if remark_detail is the whole original cell and parentheses already captured it
                    if not (segment_attrs["remark_detail"] == data_cell_original_text and parentheses_content):
                         row_template["remark"] = (row_template["remark"] + " | " + segment_attrs["remark_detail"]).strip().lstrip(" |")


                # Determine bike makes and fuels for final row generation
                # Segment makes take precedence. If none, use header makes. If none, iterate once with blank.
                final_bike_makes = segment_attrs.get("_segment_bike_makes", header_derived_makes if header_derived_makes else [""])
                if "_segment_bike_makes" in segment_attrs and segment_attrs["_segment_bike_makes"] is None: # Explicit "others"
                    final_bike_makes = [""]
                
                # Fuel: For now, assume fuel types only come from header. Cell specific fuel not handled yet.
                final_fuel_types = header_derived_fuels if header_derived_fuels else [""]

                # Iterate for each combination of make and fuel
                for make_to_use in final_bike_makes:
                    for fuel_to_use in final_fuel_types:
                        final_row_data = row_template.copy()
                        final_row_data["bike_make"] = make_to_use
                        final_row_data["fuel_type"] = fuel_to_use

                        # Rule: "bike_make and vehicle shouldn't come as same row entry..."
                        # If both are substantially set (not just placeholders from init)
                        # This applies if base_attrs had a vehicle and we are now setting a specific bike_make.
                        if final_row_data["bike_make"] and final_row_data["vehicle"]:
                            # One row with bike_make, vehicle blank
                            row_with_make = final_row_data.copy()
                            row_with_make["vehicle"] = ""
                            all_output_rows.append({col: row_with_make.get(col, "") for col in OUTPUT_COLUMNS})

                            # One row with vehicle, bike_make blank
                            row_with_vehicle = final_row_data.copy()
                            row_with_vehicle["bike_make"] = ""
                            all_output_rows.append({col: row_with_vehicle.get(col, "") for col in OUTPUT_COLUMNS})
                        else:
                            all_output_rows.append({col: final_row_data.get(col, "") for col in OUTPUT_COLUMNS})
                            
    return pd.DataFrame(all_output_rows, columns=OUTPUT_COLUMNS)


def main():
    file_path = input("Enter the path to the Excel file: ")
    try:
        try:
            # Read all sheets if multiple, or the first one.
            # For this problem, we assume data is on the first sheet or user specifies.
            # Let's target the first sheet by default.
            xls = pd.ExcelFile(file_path)
            sheet_name = xls.sheet_names[0] # Use the first sheet
            df_full_sheet = pd.read_excel(xls, sheet_name=sheet_name, header=None)
        except Exception as e_read:
            print(f"Error reading Excel file: {e_read}. Ensure the file path is correct and format is supported.")
            return

        print(f"File '{file_path}' (sheet: '{sheet_name}') read successfully. Processing...")
        
        # --- Detect multiple tables based on "RTO Cluster" occurrences ---
        table_header_indices = [] # Stores (original_index, header_text_of_rto_cluster_cell)
        # Scan for "RTO Cluster" to identify potential table header rows
        for idx, row in df_full_sheet.iterrows():
            # Check first few cells (e.g., up to 10) for "RTO Cluster"
            for cell_pos, cell_value in enumerate(row.iloc[:10].astype(str)):
                if "rto cluster" in cell_value.lower():
                    # Check if this is a *new* table or part of the same header block
                    # A simple heuristic: if this "RTO Cluster" is far from the previous one, it's a new table.
                    # Or if it's the first one.
                    if not table_header_indices or (idx > table_header_indices[-1][0] + 5) : # Arbitrary 5 rows gap
                         table_header_indices.append((idx, cell_value))
                    break 
        
        print(f"Found {len(table_header_indices)} potential table start(s) based on 'RTO Cluster'.")

        all_processed_dfs = []
        main_header_overall_attrs = {} # For very top header like "MHCV-AOTP GRID (> 5 Years, TATA & AL only)"

        if not table_header_indices:
            print("No 'RTO Cluster' found. Attempting to process the sheet as a single table.")
            # This case might mean the sheet doesn't follow the expected format.
            # We can try to process it, but results might be unreliable.
            # For now, we'll assume at least one RTO cluster header is expected.
            # If you want to process sheets without it, the logic in process_excel_table needs adjustment.
            # For now, let's process the whole sheet if no specific tables are found.
            # This might be what the user wants if the structure is simpler.
            processed_df = process_excel_table(df_full_sheet.copy())
            if not processed_df.empty:
                all_processed_dfs.append(processed_df)
        else:
            for i in range(len(table_header_indices)):
                table_start_row_original_idx = table_header_indices[i][0]
                rto_header_text = table_header_indices[i][1] # The cell content with "RTO Cluster"

                # --- Check for a "Main Grid Header" above this table's RTO Cluster row ---
                # Example: "MHCV-AOTP GRID (> 5 Years, TATA & AL only)"
                # This header would apply to all rows generated from THIS table.
                current_table_main_header_attrs = {}
                if table_start_row_original_idx > 0:
                    # Look at the row(s) immediately above table_start_row_original_idx
                    # For simplicity, check 1-2 rows above.
                    for look_back_rows in range(1, 3): # Check 1 and 2 rows above
                        if table_start_row_original_idx - look_back_rows >= 0:
                            potential_main_header_row = df_full_sheet.iloc[table_start_row_original_idx - look_back_rows]
                            # A main header is likely a single cell spanning much of the width or containing keywords.
                            # Heuristic: check the first cell if it's not empty and doesn't look like data.
                            first_cell_main_header = clean_text(potential_main_header_row.iloc[0])
                            # If the first cell is not empty and the rest of the row is mostly empty, or it contains distinctive keywords
                            is_likely_main_header = False
                            if first_cell_main_header:
                                if all(pd.isna(potential_main_header_row.iloc[j]) for j in range(1, len(potential_main_header_row))):
                                     is_likely_main_header = True # First cell has text, rest empty
                                # Add more checks: if it contains "GRID", "ONLY", specific makes, age conditions etc.
                                if any(kword in first_cell_main_header.lower() for kword in ["grid", "only", "years"]) or \
                                   any(bm.lower() in first_cell_main_header.lower() for bm in BIKE_MAKES_LIST):
                                    is_likely_main_header = True
                                
                            if is_likely_main_header:
                                print(f"  Potential main grid header found for table at {table_start_row_original_idx}: '{first_cell_main_header}'")
                                # Parse this main_header_text to extract attributes
                                process_header_string(first_cell_main_header, current_table_main_header_attrs)
                                break # Found main header for this table

                # Define the slice of the DataFrame for the current table
                # It starts from the row containing "RTO Cluster" (or the main grid header if found just above)
                # For simplicity, we'll slice from the RTO cluster row itself.
                # The process_excel_table function expects the RTO cluster row to be at its df_table.iloc[0] or nearby.
                
                slice_start_idx = table_start_row_original_idx
                slice_end_idx = table_header_indices[i+1][0] if (i+1) < len(table_header_indices) else len(df_full_sheet)
                
                current_table_df = df_full_sheet.iloc[slice_start_idx:slice_end_idx].reset_index(drop=True)
                
                print(f"\nProcessing Table {i+1} (Original rows {slice_start_idx}-{slice_end_idx-1})...")
                if current_table_main_header_attrs:
                    print(f"  Applying main header attributes: {current_table_main_header_attrs}")

                processed_df = process_excel_table(current_table_df.copy(), current_table_main_header_attrs)
                if not processed_df.empty:
                    all_processed_dfs.append(processed_df)
        
        if all_processed_dfs:
            final_df = pd.concat(all_processed_dfs, ignore_index=True)
            final_df = final_df.fillna("") # Clean up NaNs for output
            
            # Deduplicate if necessary (exact duplicates)
            final_df = final_df.drop_duplicates()

            output_file_path = "output_processed_grid.xlsx"
            final_df.to_excel(output_file_path, index=False)
            print(f"\nProcessing complete. Output saved to '{output_file_path}'")
            print(f"Total rows generated: {len(final_df)}")
        else:
            print("No data was processed or generated.")

    except FileNotFoundError:
        print(f"Error: File not found at '{file_path}'. Please check the path and try again.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
