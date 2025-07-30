"""
rdo_parser_v4.py
Config‑driven streaming parser for LGU valuation workbooks.

* Keeps original `clean_value`, carry‑forward & “ALL OTHER” logic.
* Replaces:
    - find_location_components      → location_state_updater (uses config)
    - find_column_headers           → n/a (column indices now explicit)
    - table‑detection loop          → single pass over all rows
* Dropped annotations for now.
"""

from __future__ import annotations
import os, re, json
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple, Optional

# ---------------------------------------------------------------------------
# -------------------------  SHARED HELPERS  --------------------------------
# ---------------------------------------------------------------------------

LABEL_BLANK = "BLANK"   # retained for internal tests only

def clean_value(value, feature: bool = False) -> str | float:
    """(Same as your old helper, lightly trimmed – still removes DO No., cont., revised, etc.)."""
    EFFECTIVITY_DATE_PATTERN = re.compile(r"(D\.?\s*O\s*\.?\s*No|Effec(?:t)?ivity Date)\s*.*", re.IGNORECASE)
    DO_NO_PATTERN            = re.compile(r'^no\.\s*\d+\s*-\s*', re.IGNORECASE)
    CONTINUATION_PATTERN     = re.compile(r"\s*-*\s*(\s*\(cont\s*\.?\)|(?:\()?\s*continued\s*(?:\)?)|(?:\()?\s*continuation\s*(?:\))?|(?:\()?\s*continaution\s*(?:\))?)",
                                          re.IGNORECASE)
    REVISED_PATTERN          = re.compile(r"\s*-+\s*revised.*", re.IGNORECASE)

    try:
        return round(float(value), 3)
    except (ValueError, TypeError):
        value = str(value)
        if value.lower() == 'nan':
            return ''
        value = re.sub(r"^\s*:\s*", "", value.strip())
        value = DO_NO_PATTERN.sub('', value).strip()
        value = CONTINUATION_PATTERN.sub("", value).strip()
        value = REVISED_PATTERN.sub("", value).strip()
        value = value.rstrip(' _')
        if not feature:
            value = EFFECTIVITY_DATE_PATTERN.sub("", value).strip()
        return value

# ---------------------------------------------------------------------------
# -----------------------  CONFIG HELPER UTILS  -----------------------------
# ---------------------------------------------------------------------------

def _compile_regex_list(cfg_list: List[Dict[str, Any]]) -> List[Tuple[re.Pattern, int]]:
    """Utility: from a list of {'regex': str, 'group': int} build [(compiled, group_num), ...]."""
    compiled = []
    for item in cfg_list:
        compiled.append((re.compile(item["regex"], re.IGNORECASE), item.get("group", 1)))
    return compiled

def _coerce_numeric(val: str | float | int,
                    numeric_cfg: Dict[str, Any]) -> Tuple[str | float, bool, Optional[str]]:
    """Return (clean_value, is_numeric, warning_str|None)."""
    allow = set(numeric_cfg.get("allow", []))
    if val in allow or (isinstance(val, str) and val.strip() in allow):
        return val, False, None

    if isinstance(val, str):
        tmp = val
        for th in numeric_cfg.get("thousands", [","]):
            tmp = tmp.replace(th, "")
        tmp = tmp.replace(numeric_cfg.get("decimal", "."), ".")
        try:
            num = round(float(tmp), numeric_cfg.get("round", 3))
            return num, True, None
        except ValueError:
            return val, False, f"non‑numeric '{val}'"
    if isinstance(val, (int, float)):
        return round(float(val), numeric_cfg.get("round", 3)), True, None
    return val, False, f"non‑numeric '{val}'"

# ---------------------------------------------------------------------------
# -----------------------  MAIN PARSE FUNCTION  -----------------------------
# ---------------------------------------------------------------------------

def parse_file(excel_path: str,
               cfg: Dict[str, Any],
               *,
               audit: bool = False) -> pd.DataFrame:
    """
    Parse a single workbook according to cfg and return the tidy DataFrame.
    If audit=True and cfg['output']['audit_log'] is not None, writes JSONL events.
    """
    # ---- 1) Select sheet ----------------------------------------------------
    sheet_spec = cfg["sheet"]["select"]
    engine     = cfg["sheet"].get("engine", "auto")

    xl = pd.ExcelFile(excel_path, engine=None if engine == "auto" else engine)
    if sheet_spec == -1:
        # highest "Sheet<number>"
        sheet_name = max((s for s in xl.sheet_names if s.lower().startswith("sheet")),
                         key=lambda x: int(re.search(r'\d+', x).group()))
    elif isinstance(sheet_spec, int):
        sheet_name = xl.sheet_names[sheet_spec]
    else:
        sheet_name = sheet_spec

    df = xl.parse(sheet_name, header=None)
    data = df.to_numpy()

    # ---- 2) Pre‑compile config pieces --------------------------------------
    col_idx = cfg["columns"]
    loc_cfg = cfg["location"]
    province_rules  = _compile_regex_list(loc_cfg.get("province_rules", []))
    city_rules      = _compile_regex_list(loc_cfg.get("city_rules", []))
    barangay_rules  = _compile_regex_list(loc_cfg.get("barangay_rules", []))
    combined_label_re = re.compile(loc_cfg["combined"]["label_row_regex"], re.IGNORECASE) if loc_cfg["combined"]["enabled"] else None

    row_filters = cfg["row_filters"]
    header_regexes = [re.compile(r, re.IGNORECASE) for r in row_filters["headers"].get("regex_any", [])]
    header_plain   = set(s.lower() for s in row_filters["headers"].get("plain_any", []))
    drop_row_regex = [re.compile(r, re.IGNORECASE) for r in row_filters.get("drop_if_row_contains_regex", [])]

    carry_cfg  = cfg["carry_forward"]
    all_other_triggers = tuple(t.upper() for t in carry_cfg["all_other"]["triggers"]) if carry_cfg["all_other"]["enabled"] else ()

    clas_cfg   = cfg["validation"]["classification"]
    whitelist  = set(x.upper() for x in clas_cfg["whitelist"].get("values", []))
    fuzzy_on   = clas_cfg["whitelist"]["enabled"] and clas_cfg["whitelist"]["fuzzy"]["enabled"]
    fuzzy_thr  = clas_cfg["whitelist"]["fuzzy"]["threshold"]
    if fuzzy_on:
        try:
            from rapidfuzz import process, fuzz   # lazy import
        except ImportError:
            fuzzy_on = False  # fallback silently

    numeric_cfg = cfg["cleaning"]["coerce_numeric"]["zv_sqm"]

    # ---- 3) Streaming row processor ----------------------------------------
    state = {
        "province": None, "city": None, "barangay": None,
        "combined_pending": False,
        "last_street": None, "last_vicinity": None,
        "all_other_vicinity": None,
    }

    output_rows : List[List[Any]] = []
    audit_fp = None
    if audit and cfg["output"].get("audit_log"):
        audit_fp = open(cfg["output"]["audit_log"], "w", encoding="utf-8")

    def log_event(ev: Dict[str, Any]):
        if audit_fp: audit_fp.write(json.dumps(ev, ensure_ascii=False) + "\n")

    for r_idx, row in enumerate(data):
        cells = [str(x) if not pd.isna(x) else "" for x in row]
        joined = " ".join(cells).strip()

        # =================== 3.1 Combined descriptor handling ================
        if state["combined_pending"]:
            _parse_combined_value_row(cells, loc_cfg["combined"], state)
            state["combined_pending"] = False
            log_event({"row": r_idx, "type": "COMBINED_LOC_VALS",
                       "province": state["province"], "city": state["city"],
                       "barangay": state["barangay"]})
            continue
        if combined_label_re and combined_label_re.search(joined):
            state["combined_pending"] = True
            log_event({"row": r_idx, "type": "COMBINED_LOC_LABEL"})
            continue

        # =================== 3.2 Per‑component location rules ===============
        loc_matched = False
        for pat, grp in province_rules:
            m = pat.search(joined)
            if m:
                state["province"] = clean_value(m.group(grp))
                loc_matched = True
                break
        for pat, grp in city_rules:
            m = pat.search(joined)
            if m:
                state["city"] = clean_value(m.group(grp))
                loc_matched = True
                break
        for pat, grp in barangay_rules:
            m = pat.search(joined)
            if m:
                state["barangay"] = clean_value(m.group(grp))
                loc_matched = True
                break
        if loc_matched:
            log_event({"row": r_idx, "type": "LOC_UPDATE",
                       "province": state["province"], "city": state["city"],
                       "barangay": state["barangay"]})
            continue

        # =================== 3.3 Header / skip tests ========================
        if any(regex.search(joined) for regex in header_regexes) or \
           any(tok in cell.lower() for tok in header_plain for cell in cells):
            log_event({"row": r_idx, "type": "HDR_SKIP"})
            continue
        if any(regex.search(joined) for regex in drop_row_regex):
            log_event({"row": r_idx, "type": "DROP_SKIP"})
            continue

        # =================== 3.4 Candidate data row =========================
        try:
            street        = row[col_idx["street"]]
            vicinity      = row[col_idx["vicinity"]]
            classification= row[col_idx["classification"]]
            zv_sqm_raw    = row[col_idx["zv_sqm"]]
        except IndexError:
            # row shorter than expected columns
            continue

        # ---------- carry‑forward street ------------------------------------
        is_street_blank = pd.isna(street) or not str(street).strip()
        if is_street_blank and carry_cfg["street"]:
            street = state["last_street"]
        else:
            state["last_street"] = street
            state["all_other_vicinity"] = None  # reset on new street

        # ---------- carry‑forward vicinity ----------------------------------
        is_vic_blank = pd.isna(vicinity) or not str(vicinity).strip()
        if is_vic_blank and carry_cfg["vicinity"]:
            vicinity = state["last_vicinity"]
        else:
            state["last_vicinity"] = vicinity

        # ---------- ALL OTHER handling --------------------------------------
        street_str = str(street) if street is not None else ""
        is_all_other = street_str.strip().upper().startswith(all_other_triggers)
        if is_all_other:
            if not is_vic_blank:
                state["all_other_vicinity"] = vicinity
            vicinity = state["all_other_vicinity"] or vicinity

        # ---------- Basic "is row data?" rule -------------------------------
        if all((pd.isna(classification) or str(classification).strip() == "",
                pd.isna(zv_sqm_raw) or str(zv_sqm_raw).strip() == "")):
            # not a data row
            continue

        # ---------- Clean / coerce fields -----------------------------------
        street_clean = clean_value(street, feature=True)
        vic_clean    = clean_value(vicinity, feature=True)
        class_clean  = clean_value(classification, feature=True)

        zv_sqm_clean, zv_num_ok, zv_warn = _coerce_numeric(zv_sqm_raw, numeric_cfg)

        # ---------- Classification whitelist / fuzzy ------------------------
        class_warn = None
        if clas_cfg["whitelist"]["enabled"] and isinstance(class_clean, str):
            class_up = class_clean.upper()
            if class_up not in whitelist:
                if fuzzy_on and class_up.strip():
                    best, score = process.extractOne(class_clean, whitelist, scorer=fuzz.partial_ratio)
                    if score >= fuzzy_thr:
                        class_warn = f"fuzzy‑mapped '{class_clean}'→'{best}' ({score})"
                        class_clean = best
                    else:
                        class_warn = f"unknown classification '{class_clean}'"
                else:
                    class_warn = f"unknown classification '{class_clean}'"

        # ---------- Append to output ----------------------------------------
        output_rows.append([
            state["province"], state["city"], state["barangay"],
            street_clean, vic_clean, class_clean, zv_sqm_clean
        ])

        log_event({"row": r_idx, "type": "DATA",
                   "warnings": [w for w in (zv_warn, class_warn) if w]})

    # -----------------------------------------------------------------------
    if audit_fp: audit_fp.close()
    out_df = pd.DataFrame(output_rows, columns=[
        "Province", "City/Municipality", "Barangay",
        "Street/Subdivision", "Vicinity", "Classification", "ZV/SQM"
    ])
    return out_df

# ---------------------------------------------------------------------------
# ----------------------  COMBINED LOC PARSER -------------------------------
# ---------------------------------------------------------------------------

def _parse_combined_value_row(cells: List[str], comb_cfg: Dict[str, Any], state: Dict[str, Any]):
    """
    Interpret the row following the combined label row, depending on value_row_mode.
    Modes:
      * prefixed_colons  → each cell starts with ':' then value in order province, city, barangay
      * same_row_split   → first non‑blank cell contains "PROV / CITY / BAR" string to split
    """
    mode = comb_cfg["value_row_mode"]
    if mode == "prefixed_colons":
        ordered_components = comb_cfg["value_order"]
        values = []
        for cell in cells:
            cell = cell.strip()
            if cell.startswith(":"):
                val = clean_value(cell.lstrip(":").strip())
                values.append(val)
        # assign if available
        for comp, val in zip(ordered_components, values):
            state[comp] = val
    elif mode == "same_row_split":
        nonblank = next((c for c in cells if c.strip()), "")
        parts = re.split("|".join(map(re.escape, comb_cfg["split_delimiters"])), nonblank)
        parts = [clean_value(p.strip()) for p in parts if p.strip()]
        for comp, val in zip(comb_cfg["value_order"], parts):
            state[comp] = val
    else:
        raise ValueError(f"Unknown combined value_row_mode '{mode}'")
