import pandas as pd
import re
import os
import sys

# Define constants for output columns
OUTPUT_COLUMNS = [
    "cluster_code", "bike_make", "model", "plan_type", "engine_type", "fuel_type",
    "plan_subtype", "add_on", "plan_term", "business_slab", "age", "po_percent",
    "slab_month", "remark", "product_type", "ncb", "vehicle", "veh_type",
    "seating_cap", "gvw"
]

# Keywords and patterns (case-insensitive where appropriate)
BIKE_MAKES = [
    "TATA", "M&M", "MAHINDRA & MAHINDRA", "MAHINDRA", "ASHOK LEYLAND", "AL", "EICHER",
    "MARUTI SUPER CARRY", "MARUTI", "BAJAJ", "ATUL", "TVS", "PIAGGIO", "TOYOTA",
    "FORCE MOTORS", "SML ISUZU", "DAIMLER", "BHARATBENZ", "MAN", "SCANIA", "VOLVO"
]
BIKE_MAKES_LOWER = [bm.lower() for bm in BIKE_MAKES]

RTO_SUB_CODES = [
    "WB1", "DL RTO", "NON DL RTO", "DL", "GJ1", "JK1", "KA1", "OD1", "TN1", "UP EAST 1", "UP1", "UK1",
    "KA01-05", "TN10", "TN12", "TN02", "TN22", "TN04", "TN06", "TN09", "TN18", "TN19", "TN20", "TN11", "TN14",
    "PUNE RTO", "PIMPRI", "PIMPRI CHINCHWAD", "DELHI SURROUNDING RTO"
]
RTO_SUB_CODES_LOWER = [rc.lower() for rc in RTO_SUB_CODES]

# Global list to store all row data
all_rows_data = []

def get_slab_month(filepath):
    filename = os.path.basename(filepath)
    name_part = os.path.splitext(filename)[0].lower()
    month_map = {
        'jan': 'Jan', 'feb': 'Feb', 'mar': 'Mar', 'apr': 'Apr', 'may': 'May', 'jun': 'Jun',
        'jul': 'Jul', 'aug': 'Aug', 'sep': 'Sep', 'oct': 'Oct', 'nov': 'Nov', 'dec': 'Dec'
    }
    match = re.search(r"(\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b)\s*'?(\d{2})", name_part)
    if match:
        month_short = match.group(1)[:3]
        year = match.group(2)
        return f"{month_map.get(month_short, month_short.capitalize())}{year}"
    
    match_month_only = re.search(r"(\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b)", name_part)
    if match_month_only:
        month_short = match_month_only.group(1)[:3]
        print(f"DEBUG: Slab month found (month only): {month_map.get(month_short, month_short.capitalize())}")
        return f"{month_map.get(month_short, month_short.capitalize())}_unknown_year"
        
    print(f"DEBUG: Slab month not reliably found in '{filename}', returning 'unknown_month_year'")
    return "unknown_month_year"


def parse_header_keywords(header_text_orig, entry):
    header_text = str(header_text_orig)
    print(f"DEBUG: Parsing header: '{header_text}' for entry: {entry}")
    header_lower = header_text.lower()
    if "remark" not in entry or entry["remark"] is None:
        entry["remark"] = ""

    if any(kw in header_lower for kw in ["gcv", "scv", "lcv", "mhcv"]):
        entry["product_type"] = "Commercial Vehicle"

    if "gcv" in header_lower: entry["veh_type"] = "GCV"
    if "scv" in header_lower: entry["veh_type"] = "GCV" 
    if "lcv" in header_lower: entry["veh_type"] = "GCV" 
    if "mhcv" in header_lower: entry["veh_type"] = "GCV" 
    
    if "pcvtaxi" in header_lower.replace(" ", "").replace("_", ""):
        entry["veh_type"] = "PCV"
        entry["vehicle"] = "Taxi"
    elif "pcv" in header_lower: entry["veh_type"] = "PCV"
    
    if "misc d ce" in header_lower or ("misc" in header_lower and "ce" in header_lower):
        entry["veh_type"] = "MISC"
        entry["vehicle"] = "CE"

    if "3w" in header_lower: entry["vehicle"] = "3W"
    elif "2w" in header_lower: entry["vehicle"] = "2W"

    vehicles_keywords_map = {
        "school bus": "School Bus", "staff bus": "Staff Bus", "bus": "Bus",
        "tanker": "Tanker", "tipper": "Tipper", "truck": "Truck", "trailer": "Trailer",
        "dumper": "Dumper", "cranes": "Cranes", "tractor": "Tractor", "tracter": "Tractor", 
        "taxi": "Taxi"
    }
    sorted_vehicle_kws = sorted(vehicles_keywords_map.keys(), key=len, reverse=True)
    for kw in sorted_vehicle_kws:
        if kw in header_lower:
            val = vehicles_keywords_map[kw]
            entry["vehicle"] = val
            if "bus" in kw.lower() or "taxi" in kw.lower(): 
                entry["veh_type"] = "PCV"
            elif val != "CE" and entry.get("veh_type","").upper() not in ["GCV", "PCV", "MISC"]:
                 entry["veh_type"] = "MISC"
            break

    if "new" in header_lower: entry["age"] = "New"
    if "old" in header_lower: entry["age"] = "Old"

    if "electric" in header_lower: entry["fuel_type"] = "Electric"
    
    fuel_types_map = {"petrol": "Petrol", "cng": "CNG", "diesel": "Diesel", "bifuel": "Bifuel"}
    found_fuels_header = []
    for ft_kw, ft_val in fuel_types_map.items():
        if ft_kw in header_lower:
            found_fuels_header.append(ft_val)
    if found_fuels_header:
        entry["fuel_type"] = "/".join(sorted(list(set(found_fuels_header))))

    gvw_patterns = [
        r"([<>]=?\s*\d+)\s*GVW",
        r"(\d+\.?\d*\s*-\s*\d+\.?\d*\s*T)\b",
        r"([<>]=?\s*\d+\s*T)\b"
    ]
    for pattern in gvw_patterns:
        gvw_match = re.search(pattern, header_text, re.IGNORECASE)
        if gvw_match:
            entry["gvw"] = re.sub(r"\s+", "", gvw_match.group(1))
            break
    
    hp_match = re.search(r"([<>]=?\s*\d+)\s*HP", header_text, re.IGNORECASE)
    if hp_match: entry["engine_type"] = re.sub(r"\s+", "", hp_match.group(1))
    
    cc_match = re.search(r"([<>]=?\s*\d+)\s*CC", header_text, re.IGNORECASE)
    if cc_match: entry["engine_type"] = re.sub(r"\s+", "", cc_match.group(1))

    if entry.get("vehicle","").lower().endswith("bus"): 
        cap_patterns_ordered = [
            (r"(>\s*18\s*upto\s*36\s*seater)", ">18 upto 36 seater"), 
            (r"(<\s*18)", "<18"), 
            (r"(18\s*-\s*36)", "18-36"), 
            (r"(>\s*36)", ">36"), 
            (r"(>\s*18)", ">18") 
        ]
        for pattern_str, val_str in cap_patterns_ordered:
            cap_match = re.search(pattern_str, header_text, re.IGNORECASE)
            if cap_match:
                entry["seating_cap"] = val_str
                break
    
    if "comp" in header_lower:
        entry["plan_type"] = "Comp"
    elif "aotp" in header_lower or "satp" in header_lower or "tp" in header_lower :
        entry["plan_type"] = "SATP"
    elif "on od" in header_lower:
        entry["plan_type"] = "SAOD"

    header_remark_parts = []
    if entry.get("remark"): header_remark_parts.append(entry["remark"])

    paren_remarks_header = re.findall(r"\((.*?)\)", header_text)
    for pr_h in paren_remarks_header:
        is_make_in_paren = any(bm.lower() in pr_h.lower() for bm in BIKE_MAKES)
        if not is_make_in_paren:
             header_remark_parts.append(f"({pr_h})")

    excl_match = re.search(r"(excluding[\s\S]*)", header_text, re.IGNORECASE)
    if excl_match: header_remark_parts.append(excl_match.group(1).strip())
    
    except_match = re.search(r"(except[\s\S]*)", header_text, re.IGNORECASE)
    if except_match: header_remark_parts.append(except_match.group(1).strip())
    
    if header_remark_parts:
        entry["remark"] = " | ".join(sorted(list(set(filter(None, header_remark_parts)))))
    print(f"DEBUG: Entry after header parse: {entry}")


def process_cell_content(cell_text_original, base_entry_from_header, header_text_for_context):
    print(f"DEBUG: Cell Processing START: '{cell_text_original}' for Header: '{header_text_for_context}' with Base: {base_entry_from_header}")
    if pd.isna(cell_text_original) or str(cell_text_original).strip() == "":
        print(f"DEBUG: Cell is NaN or empty, skipping.")
        return

    cell_text_str = str(cell_text_original).strip()
    
    direct_value_match = re.match(r"^(CC|Decline|IRDA)\b(.*)", cell_text_str, re.IGNORECASE)
    if direct_value_match:
        entry = base_entry_from_header.copy()
        entry["po_percent"] = direct_value_match.group(1).upper()
        remark_after_direct = direct_value_match.group(2).strip()
        
        current_remarks = []
        if entry.get("remark"): current_remarks.append(entry["remark"])
        if remark_after_direct: current_remarks.append(remark_after_direct)
        
        paren_remarks_direct = re.findall(r"\((.*?)\)", remark_after_direct)
        for prd in paren_remarks_direct:
            current_remarks.append(f"({prd})")
        
        entry["remark"] = " | ".join(sorted(list(set(filter(None, current_remarks)))))
        all_rows_data.append(entry)
        print(f"DEBUG: Appended direct value (CC/Decline/IRDA): {entry}")
        return

    lines = [line.strip() for line in cell_text_str.split('\n') if line.strip()]
    if not lines: # If cell was spaces or only newlines after strip
        entry = base_entry_from_header.copy()
        # If cell_text_str had content before split (e.g. just " "), use it
        # otherwise it's truly empty
        po_val = cell_text_str if cell_text_str else "" 
        if re.match(r"^\d+\.?\d*$", po_val) and '%' not in po_val : po_val += "%"
        entry["po_percent"] = po_val
        all_rows_data.append(entry)
        print(f"DEBUG: Appended whitespace/empty line cell as po_percent: {entry}")
        return

    segment_interpretations = [] 

    for line_idx, line_content in enumerate(lines):
        print(f"DEBUG: Cell Processing Line {line_idx + 1}/{len(lines)}: '{line_content}'")
        
        percent_finds = list(re.finditer(r"(\d+\.?\d*%?)", line_content))

        if not percent_finds: # Line has no percentage, treat as a general condition/remark for the cell
            interp = {}
            current_line_text = line_content
            
            sorted_rto_codes_desc = sorted(RTO_SUB_CODES, key=len, reverse=True)
            sorted_makes_desc = sorted(BIKE_MAKES, key=len, reverse=True)

            for rsc_orig in sorted_rto_codes_desc:
                rsc_l = rsc_orig.lower()
                if "only" in current_line_text.lower() and rsc_l in current_line_text.lower():
                    interp["cluster_code"] = rsc_orig
                    for bm_orig_only in sorted_makes_desc:
                        if re.search(r'\b' + re.escape(bm_orig_only.lower()) + r'\b' + r'.*only.*' + re.escape(rsc_l), current_line_text.lower()) or \
                           re.search(re.escape(rsc_l) + r'.*only.*' + r'\b' + re.escape(bm_orig_only.lower()) + r'\b', current_line_text.lower()) or \
                           re.search(r'only.*' + r'\b' + re.escape(bm_orig_only.lower()) + r'\b' + r'.*' + re.escape(rsc_l), current_line_text.lower()):
                            interp["bike_make"] = bm_orig_only
                            break 
                    break 
            
            if "bike_make" not in interp: # if not set by "only RTO MAKE"
                for bm_orig_gen in sorted_makes_desc:
                    if re.search(r'\b' + re.escape(bm_orig_gen.lower()) + r'\b', current_line_text.lower()):
                        interp["bike_make"] = bm_orig_gen
                        current_line_text = re.sub(r'\b' + re.escape(bm_orig_gen.lower()) + r'\b', "", current_line_text, flags=re.IGNORECASE, count=1).strip(" ,()")
                        break
            
            age_patterns_cell = [
                (r"(\d+\s*-\s*\d+\s*(?:yrs|years|age))\b", lambda m: m.group(1).replace("age","yrs").replace("years","yrs").replace(" ","")),
                (r"(>\s*\d+\s*(?:yrs|years|age))\b", lambda m: m.group(1).replace("age","yrs").replace("years","yrs").replace(" ","")),
                (r"(above\s+\d+\s*(?:yr|year|age)s?)\b", lambda m: m.group(1).replace("age","yrs").replace("years","yrs").replace(" ","")),
                (r"\b(new)\b", lambda m: "New"), (r"\b(old)\b", lambda m: "Old")
            ]
            for pattern, formatter in age_patterns_cell:
                age_match = re.search(pattern, current_line_text, re.IGNORECASE)
                if age_match:
                    interp["age"] = formatter(age_match)
                    current_line_text = re.sub(pattern, "", current_line_text, flags=re.IGNORECASE, count=1).strip(" ,()")
                    break
            
            if "comp" in current_line_text.lower(): interp["plan_type"] = "Comp"
            elif "aotp" in current_line_text.lower() or "satp" in current_line_text.lower() or "tp" in current_line_text.lower():
                interp["plan_type"] = "SATP"
            elif "on od" in current_line_text.lower(): interp["plan_type"] = "SAOD"

            current_line_text_cleaned = current_line_text.strip(", ")
            if current_line_text_cleaned: # Add any remaining text as remark for this line
                interp["line_remark"] = (interp.get("line_remark","") + " " + current_line_text_cleaned).strip()
            
            if interp: # If this non-percentage line yielded any interpretation
                segment_interpretations.append(interp)
            continue # Move to next line


        # Line has percentages, process segments
        for i, find in enumerate(percent_finds):
            # segment_text_start = find.start() # Not directly used for segment slicing this way
            segment_text_end = percent_finds[i+1].start() if i + 1 < len(percent_finds) else len(line_content)
            segment_text = line_content[find.start() : segment_text_end].strip()

            if not segment_text: continue

            interp = {}
            current_segment_po_percent = find.group(1)
            if not current_segment_po_percent.endswith('%'): # Add % if missing, e.g. "15" -> "15%"
                 current_segment_po_percent += "%"
            interp["po_percent"] = current_segment_po_percent
            
            # Text associated with this percentage (after the digits, within this segment)
            associated_text = segment_text[len(find.group(1)):].strip(" ,()").strip() # Use original find.group(1) length
            
            # Plan type from associated text
            if "comp" in associated_text.lower(): interp["plan_type"] = "Comp"
            elif "aotp" in associated_text.lower() or "satp" in associated_text.lower() or "tp" in associated_text.lower():
                 interp["plan_type"] = "SATP"
            elif "on od" in associated_text.lower(): interp["plan_type"] = "SAOD"

            # Age from associated text
            age_patterns_cell_assoc = [
                (r"(\d+\s*-\s*\d+\s*(?:yrs|years|age))\b", lambda m: m.group(1).replace("age","yrs").replace("years","yrs").replace(" ","")),
                (r"(>\s*\d+\s*(?:yrs|years|age))\b", lambda m: m.group(1).replace("age","yrs").replace("years","yrs").replace(" ","")),
                (r"(above\s+\d+\s*(?:yr|year|age)s?)\b", lambda m: m.group(1).replace("age","yrs").replace("years","yrs").replace(" ","")),
                (r"\b(new)\b", lambda m: "New"), (r"\b(old)\b", lambda m: "Old")
            ]
            cleaned_assoc_text_for_age_make = associated_text # Work on a copy
            for pattern, formatter in age_patterns_cell_assoc:
                age_match = re.search(pattern, cleaned_assoc_text_for_age_make, re.IGNORECASE)
                if age_match:
                    interp["age"] = formatter(age_match)
                    cleaned_assoc_text_for_age_make = re.sub(pattern, "", cleaned_assoc_text_for_age_make, flags=re.IGNORECASE, count=1).strip(" ,()")
                    break
            
            # Bike make from associated text
            sorted_bike_makes_cell_desc = sorted(BIKE_MAKES, key=len, reverse=True)
            found_makes_in_segment = []
            
            temp_text_for_multi_make = cleaned_assoc_text_for_age_make
            loop_guard_multi_make = 0 
            while loop_guard_multi_make < 20: 
                loop_guard_multi_make += 1
                make_found_this_iteration = False
                for bm_k_cell in sorted_bike_makes_cell_desc:
                    bm_k_l_cell = bm_k_cell.lower()
                    if re.search(r'\b' + re.escape(bm_k_l_cell) + r'\b', temp_text_for_multi_make.lower()):
                        found_makes_in_segment.append(bm_k_cell)
                        temp_text_for_multi_make = re.sub(r'\b' + re.escape(bm_k_l_cell) + r'\b', "", temp_text_for_multi_make, flags=re.IGNORECASE, count=1).strip(" ,()")
                        make_found_this_iteration = True
                        break 
                if not make_found_this_iteration:
                    break 
            
            cleaned_assoc_text_for_age_make = temp_text_for_multi_make.strip(" ,()")


            if found_makes_in_segment:
                # Create multiple interpretations if multiple makes are associated with THIS percentage segment
                for make_idx, make_seg in enumerate(found_makes_in_segment):
                    make_interp = interp.copy() # Start with base interp for this %
                    make_interp["bike_make"] = make_seg
                    # Remark is what's left of associated_text after ALL makes, age, plan type are processed
                    if cleaned_assoc_text_for_age_make: # Add remaining text as remark for this specific make-entry
                        make_interp["line_remark"] = (make_interp.get("line_remark","") + " " + cleaned_assoc_text_for_age_make).strip()
                    segment_interpretations.append(make_interp)
                interp = {} # Reset interp as it's been split by makes for this segment.
            
            if interp and "po_percent" in interp: # If interp still has po_percent (i.e., no makes split it, or it's the base for "others")
                 # Remaining associated_text is remark for this percentage
                if cleaned_assoc_text_for_age_make: # Check if it's not just spaces or empty
                    interp["line_remark"] = (interp.get("line_remark","") + " " + cleaned_assoc_text_for_age_make).strip()
                segment_interpretations.append(interp)
    
    # Fallback if cell_text_str was not empty but segment_interpretations is empty after processing lines
    # This means the lines themselves didn't yield specific po_percent segments (e.g., just text "XYZ Corp")
    if not segment_interpretations and cell_text_str:
        entry = base_entry_from_header.copy()
        po_val = cell_text_str
        if re.match(r"^\d+\.?\d*$", po_val) and '%' not in po_val : po_val += "%" 
        entry["po_percent"] = po_val
        
        paren_remarks_cell_fallback = re.findall(r"\((.*?)\)", cell_text_str)
        existing_remark = entry.get("remark","")
        for pr_fb in paren_remarks_cell_fallback:
            existing_remark = (existing_remark + f" ({pr_fb})").strip()
        entry["remark"] = existing_remark if existing_remark else None 
        all_rows_data.append(entry)
        print(f"DEBUG: Appended unparsed/fallback cell as po_percent (no segments found in lines): {entry}")
        return

    # Consolidate interpretations into final rows
    # Apply general conditions from non-percentage lines to percentage lines within the same cell
    general_conditions_from_cell_lines = {}
    temp_segment_interpretations = [] # To hold only segments that have a po_percent

    for seg_interp_item in segment_interpretations:
        if "po_percent" not in seg_interp_item: # This is a general condition line from the cell
            general_conditions_from_cell_lines.update(seg_interp_item) # Collect all general attributes
        else:
            temp_segment_interpretations.append(seg_interp_item) # This is a percentage-bearing segment
    segment_interpretations = temp_segment_interpretations # Now only contains % segments


    for seg_interp_final in segment_interpretations: # Iterate over segments that define a po_percent
        entry = base_entry_from_header.copy()
        entry.update(general_conditions_from_cell_lines) # Apply general conditions from other lines in same cell first
        entry.update(seg_interp_final) # Then apply this segment's specifics (which might override)

        # Aggregate remarks: base_header_remark + general_cell_line_remark + specific_segment_remark
        final_remarks_list = []
        if base_entry_from_header.get("remark"): final_remarks_list.append(base_entry_from_header["remark"])
        if general_conditions_from_cell_lines.get("line_remark"): final_remarks_list.append(general_conditions_from_cell_lines["line_remark"])
        if seg_interp_final.get("line_remark"): final_remarks_list.append(seg_interp_final["line_remark"])
        
        # Add parenthetical content from original cell_text_str if not already captured by more specific line_remarks
        original_paren_remarks_cell = re.findall(r"\((.*?)\)", cell_text_str)
        for opr_cell in original_paren_remarks_cell:
            formatted_opr = f"({opr_cell})"
            # Check if this parenthetical remark is already part of the collected remarks
            # This is to avoid duplicating remarks if line_remark logic already extracted it.
            is_already_captured = any(formatted_opr in remark_part for remark_part in final_remarks_list if remark_part)
            if not is_already_captured:
                 final_remarks_list.append(formatted_opr)

        unique_remarks = sorted(list(set(filter(None, final_remarks_list))))
        entry["remark"] = " | ".join(unique_remarks) if unique_remarks else None


        # Handle "others" keyword logic. If "bike_make" is "others", it should be blank.
        if str(entry.get("bike_make","")).lower() == "others":
            entry["bike_make"] = "" # Blank out if "others"

        all_rows_data.append(entry)
        print(f"DEBUG: Appended processed segment from cell: {entry}")

    # Handle case where cell ONLY contained general conditions (no po_percent defined within the cell itself)
    # And there were no segments that generated a po_percent
    if not any("po_percent" in si for si in segment_interpretations) and general_conditions_from_cell_lines and not temp_segment_interpretations:
        entry = base_entry_from_header.copy()
        entry.update(general_conditions_from_cell_lines)
        # po_percent might be inherited from base_entry_from_header if it was set by a "direct value match" earlier,
        # or use the original cell_text_str if it looks like a percentage or is just text.
        # This primarily covers cases where a cell says e.g. "TATA only" without a % directly.
        # The po_percent should ideally come from the base_entry_from_header or be the raw cell text if it's a value.
        # If general_conditions_from_cell_lines is populated, it means lines were parsed but none had a percentage.
        # In this specific block, it implies cell_text_str did not match direct_value nor percent_finds logic in lines.
        # So, cell_text_str is likely the intended po_percent if it's a value, or a remark.
        
        # The instruction is "if instead of percentage value ... you find something else ... included as it is in po_percent column"
        # So, if no po_percent was derived from segments, use the original cell_text_str
        final_po_percent = cell_text_str
        if re.match(r"^\d+\.?\d*$", final_po_percent) and '%' not in final_po_percent : final_po_percent += "%"
        entry["po_percent"] = final_po_percent

        final_remarks_list_gen_only = []
        if base_entry_from_header.get("remark"): final_remarks_list_gen_only.append(base_entry_from_header["remark"])
        if general_conditions_from_cell_lines.get("line_remark"): final_remarks_list_gen_only.append(general_conditions_from_cell_lines["line_remark"])
        original_paren_remarks_gen_cell = re.findall(r"\((.*?)\)", cell_text_str) # From original cell text
        for opr_g_cell in original_paren_remarks_gen_cell:
            formatted_opr_g = f"({opr_g_cell})"
            if formatted_opr_g not in " | ".join(final_remarks_list_gen_only): # Avoid duplicates
                final_remarks_list_gen_only.append(formatted_opr_g)
        
        unique_remarks_gen_only = sorted(list(set(filter(None, final_remarks_list_gen_only))))
        entry["remark"] = " | ".join(unique_remarks_gen_only) if unique_remarks_gen_only else None
        
        all_rows_data.append(entry)
        print(f"DEBUG: Appended cell with only general conditions (no specific % in cell lines): {entry}")


def process_file(filepath):
    global all_rows_data
    all_rows_data = [] 

    slab_month = get_slab_month(filepath)
    print(f"DEBUG: Processing file: {filepath} for slab_month: {slab_month}")

    try:
        excel_file = pd.ExcelFile(filepath)
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return None
    except Exception as e:
        print(f"Error reading Excel file {filepath}: {e}")
        return None

    print(f"DEBUG: Available sheet names: {excel_file.sheet_names}")
    for sheet_name in excel_file.sheet_names:
        print(f"DEBUG: Processing sheet: {sheet_name}")
        try:
            df = excel_file.parse(sheet_name, header=None)
            print(f"DEBUG: Sheet '{sheet_name}' loaded with shape: {df.shape}")
            print(f"DEBUG: DataFrame head for sheet '{sheet_name}':\n{df.head(10).to_string()}")
        except Exception as e:
            print(f"Error parsing sheet {sheet_name} in {filepath}: {e}")
            continue

        header_row_idx = -1
        rto_cluster_col_idx = -1
        
        for i in range(min(20, len(df))): # Scan top 20 rows
            row_values = df.iloc[i].tolist()
            row_values_str_lower = [str(val).strip().lower() for val in row_values] # Strip before lower
            print(f"DEBUG: Scanning row {i} for header: {row_values_str_lower}")
            if "rto cluster" in row_values_str_lower:
                header_row_idx = i
                try:
                    rto_cluster_col_idx = row_values_str_lower.index("rto cluster")
                    print(f"DEBUG: Found 'rto cluster' in sheet '{sheet_name}' at row_idx={header_row_idx}, col_idx={rto_cluster_col_idx}")
                except ValueError:
                    print(f"WARN: 'rto cluster' text reported in row {i} but index not found. This should not happen.")
                    header_row_idx = -1 
                    continue 
                break 
        
        if header_row_idx == -1:
            print(f"DEBUG: 'RTO cluster' main header not found in sheet '{sheet_name}'. Skipping this sheet for structured parsing.")
            continue
        
        main_header_series = df.iloc[header_row_idx]
        print(f"DEBUG: Main header series identified: {main_header_series.tolist()}")

        # --- Iterate through data rows ---
        for i in range(header_row_idx + 1, len(df)):
            data_row = df.iloc[i]
            # Ensure rto_cluster_col_idx is valid for data_row
            if rto_cluster_col_idx >= len(data_row):
                print(f"DEBUG: Skipping data_row {i} as rto_cluster_col_idx {rto_cluster_col_idx} is out of bounds for row length {len(data_row)}")
                continue
            current_rto_cluster_val = data_row.iloc[rto_cluster_col_idx]
            print(f"DEBUG: Processing data_row index {i}, RTO Cluster Value: '{current_rto_cluster_val}'")

            if pd.isna(current_rto_cluster_val) or str(current_rto_cluster_val).strip() == "":
                print(f"DEBUG: Skipping data_row {i} due to empty RTO cluster value.")
                continue

            # --- Process First Table Structure ---
            second_table_start_col_idx = len(main_header_series) # Default to end
            for col_scan_idx in range(rto_cluster_col_idx + 1, len(main_header_series)):
                # Ensure col_scan_idx is valid for main_header_series
                if col_scan_idx >= len(main_header_series): break
                header_content_scan = str(main_header_series.iloc[col_scan_idx]).strip().lower()
                if "rto cluster" == header_content_scan: # Exact match after strip and lower
                    second_table_start_col_idx = col_scan_idx
                    print(f"DEBUG: Second 'rto cluster' found at column index {second_table_start_col_idx}, delimiting tables.")
                    break
            
            print(f"DEBUG: First table processing columns from {rto_cluster_col_idx + 1} to {second_table_start_col_idx -1}")
            for j in range(rto_cluster_col_idx + 1, second_table_start_col_idx):
                 # Ensure j is valid for main_header_series and data_row
                if j >= len(main_header_series) or j >= len(data_row):
                    print(f"DEBUG: Table1: Skipping column {j} as it's out of bounds for header or data row.")
                    continue
                column_header_text = str(main_header_series.iloc[j])
                cell_value = data_row.iloc[j]
                print(f"DEBUG: Table1: RTO='{current_rto_cluster_val}', Header='{column_header_text}', CellValue='{cell_value}'")

                if pd.isna(cell_value) or str(cell_value).strip() == "" or str(cell_value).strip().upper() == "#REF!":
                    print(f"DEBUG: Table1: Skipping cell ({i},{j}) due to NaN/empty/#REF!")
                    continue

                current_base_entry = {"cluster_code": str(current_rto_cluster_val).strip(), "slab_month": slab_month}
                parse_header_keywords(column_header_text, current_base_entry)
                process_cell_content(cell_value, current_base_entry, column_header_text)

            # --- Process Second Table Structure (if detected) ---
            if second_table_start_col_idx < len(main_header_series):
                print(f"DEBUG: Processing second table starting at column index {second_table_start_col_idx}")
                second_grid_title_text = ""
                title_search_end_row = header_row_idx 
                title_col_search_start = max(0, second_table_start_col_idx - 5) 
                title_col_search_end = min(len(df.columns), second_table_start_col_idx + 5) 

                for r_idx in range(title_search_end_row):
                    for c_idx in range(title_col_search_start, title_col_search_end):
                        if r_idx < len(df) and c_idx < df.shape[1]: # Check bounds
                            title_candidate_val = str(df.iloc[r_idx, c_idx])
                            if "MHCV-AOTP GRID" in title_candidate_val: # Case sensitive as per example
                                second_grid_title_text = title_candidate_val
                                print(f"DEBUG: Second grid title found at ({r_idx},{c_idx}): '{second_grid_title_text}'")
                                break
                    if second_grid_title_text: break
                
                # Fallback title search (often in cell B of pre-header row)
                if not second_grid_title_text and header_row_idx > 0 and df.shape[1] > 1 :
                    if 1 < df.shape[1]: # Check if column 1 exists
                        candidate_title = str(df.iloc[header_row_idx -1, 1]) 
                        if "MHCV-AOTP GRID" in candidate_title:
                            second_grid_title_text = candidate_title
                            print(f"DEBUG: Second grid title (fallback B column) found: '{second_grid_title_text}'")


                second_grid_defaults = {"slab_month": slab_month}
                sg_makes_from_title = []
                if second_grid_title_text:
                    title_lower = second_grid_title_text.lower()
                    if "> 5 years" in title_lower or ">5 years" in title_lower: second_grid_defaults["age"] = "> 5 Years"
                    
                    if "tata" in title_lower: sg_makes_from_title.append("TATA")
                    if re.search(r"\bal\b", title_lower) or "ashok leyland" in title_lower:
                        if "ashok leyland" in title_lower: sg_makes_from_title.append("ASHOK LEYLAND")
                        else: sg_makes_from_title.append("AL")
                    sg_makes_from_title = list(set(sg_makes_from_title)) 
                    print(f"DEBUG: Second grid title derived makes: {sg_makes_from_title}")

                    if "aotp" in title_lower: second_grid_defaults["plan_type"] = "SATP"
                    if "mhcv" in title_lower: second_grid_defaults["veh_type"] = "GCV" # As per rule
                
                # RTO cluster for the second table part of the current data row
                if second_table_start_col_idx >= len(data_row):
                    print(f"DEBUG: Skipping second table for data_row {i} as second_table_start_col_idx is out of bounds.")
                    continue
                second_grid_rto_val = data_row.iloc[second_table_start_col_idx]
                print(f"DEBUG: Second table RTO value for this row: '{second_grid_rto_val}'")
                if pd.isna(second_grid_rto_val) or str(second_grid_rto_val).strip() == "":
                    print(f"DEBUG: Skipping second table processing for data_row {i} due to empty RTO for second table.")
                    continue

                for k in range(second_table_start_col_idx + 1, len(main_header_series)):
                    if k >= len(main_header_series) or k >= len(data_row):
                        print(f"DEBUG: Table2: Skipping column {k} as it's out of bounds for header or data row.")
                        continue
                    sg_col_header_text = str(main_header_series.iloc[k])
                    sg_cell_value = data_row.iloc[k]
                    print(f"DEBUG: Table2: RTO='{second_grid_rto_val}', Header='{sg_col_header_text}', CellValue='{sg_cell_value}'")


                    if pd.isna(sg_cell_value) or str(sg_cell_value).strip() == "" or str(sg_cell_value).strip().upper() == "#REF!":
                        print(f"DEBUG: Table2: Skipping cell ({i},{k}) due to NaN/empty/#REF!")
                        continue
                    
                    # Logic for applying makes from title or processing once
                    if sg_makes_from_title:
                        for make_from_title in sg_makes_from_title:
                            base_entry_sg = {"cluster_code": str(second_grid_rto_val).strip(), "slab_month": slab_month}
                            base_entry_sg.update(second_grid_defaults.copy()) 
                            base_entry_sg["bike_make"] = make_from_title # Override/set make
                            parse_header_keywords(sg_col_header_text, base_entry_sg) # Header specific overrides after title defaults
                            process_cell_content(sg_cell_value, base_entry_sg, sg_col_header_text)
                    else: # No specific makes from title, process once
                        base_entry_sg = {"cluster_code": str(second_grid_rto_val).strip(), "slab_month": slab_month}
                        base_entry_sg.update(second_grid_defaults.copy())
                        parse_header_keywords(sg_col_header_text, base_entry_sg)
                        process_cell_content(sg_cell_value, base_entry_sg, sg_col_header_text)

    if not all_rows_data:
        print(f"Warning: No data extracted from file {filepath}. The sheet structure might not match expected patterns or data cells were all skippable.")
        return None
        
    output_df = pd.DataFrame(all_rows_data, columns=OUTPUT_COLUMNS)
    return output_df

if __name__ == '__main__':
    input_excel_path = input("Please provide the path to the ICICI CV grid Excel file: ")

    if not os.path.exists(input_excel_path):
        print(f"Error: The file '{input_excel_path}' does not exist.")
        sys.exit(1)

    # Sort BIKE_MAKES and RTO_SUB_CODES by length descending for more accurate regex matching
    BIKE_MAKES.sort(key=len, reverse=True)
    RTO_SUB_CODES.sort(key=len, reverse=True)
    
    print("DEBUG: Starting file processing...")
    final_df = process_file(input_excel_path)

    if final_df is not None and not final_df.empty:
        output_filename = os.path.splitext(os.path.basename(input_excel_path))[0] + "_processed.xlsx"
        output_path = os.path.join(os.path.dirname(input_excel_path), output_filename)
        try:
            final_df.to_excel(output_path, index=False)
            print(f"Processing complete. Output saved to: {output_path}")
        except Exception as e:
            print(f"Error saving output Excel file: {e}")
    elif final_df is not None and final_df.empty: 
        print("Processing finished, but no data was extracted into the final DataFrame. all_rows_data was likely empty.")
    else: 
        print("Processing failed or no data could be extracted (final_df is None).")

    print("DEBUG: Script finished.")