#!/usr/bin/env python3
"""
StatCan SDMX Explorer
=====================
A terminal UI for browsing Statistics Canada's SDMX web service.
Uses the "structure specific" data format (SDMX-ML 2.1).

Features
--------
• Fetch data by dataflow ID + dimension key, or by vector ID
• Fetch & browse the DSD: navigate dimensions, inspect all codes,
  copy a code value directly into the query key builder
• In-memory + on-disk cache (TTL-aware) for all HTTP responses
• Lightweight DataFrame-style operations (sort, filter, describe)
• Time-series ASCII sparkline in the data view
• Export results to CSV

Navigation uses plain letters only — no function keys — so it works
cleanly in any terminal, browser dev-tools, or IDE console.

Key map summary
---------------
  1  Help    2  Query    3  Results    4  DSD    5  Cache    6  Search

Number keys 1-6 navigate between screens from anywhere.

Dependencies: Python 3.8+ stdlib only (curses, urllib, xml, json,
              hashlib, csv, pathlib, datetime, threading)
"""

from __future__ import annotations

import csv
import curses
import hashlib
import json
import math
import pathlib
import textwrap
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
BASE_URL         = "https://www150.statcan.gc.ca/t1/wds/sdmx/statcan/rest"
WDS_BASE_URL     = "https://www150.statcan.gc.ca/t1/wds/rest"
CACHE_DIR        = pathlib.Path.home() / ".cache" / "sdmx_explorer"
CACHE_TTL_DATA   = 3_600     # 1 h  – data endpoints
CACHE_TTL_STRUCT = 86_400    # 24 h – structure / DSD endpoints
CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# Tiny DataFrame-style container  (no pandas / polars needed)
# ──────────────────────────────────────────────────────────────────────────────
class Frame:
    """Minimal column-oriented data frame used for display and export."""

    def __init__(self, columns: List[str], rows: List[List[Any]]):
        self.columns = columns
        self.rows    = rows

    @classmethod
    def empty(cls) -> "Frame":
        return cls([], [])

    def col(self, name: str) -> List[Any]:
        idx = self.columns.index(name)
        return [r[idx] for r in self.rows]

    def filter_rows(self, col: str, value: Any) -> "Frame":
        idx = self.columns.index(col)
        return Frame(self.columns, [r for r in self.rows if r[idx] == value])

    def sort(self, col: str, descending: bool = False) -> "Frame":
        idx = self.columns.index(col)
        try:
            s = sorted(self.rows,
                       key=lambda r: (r[idx] is None, r[idx]),
                       reverse=descending)
        except TypeError:
            s = sorted(self.rows, key=lambda r: str(r[idx]), reverse=descending)
        return Frame(self.columns, s)

    def describe(self, col: str) -> Dict[str, float]:
        vals = [r[self.columns.index(col)] for r in self.rows]
        nums = [float(v) for v in vals if v is not None]
        if not nums:
            return {}
        nums.sort()
        n    = len(nums)
        mean = sum(nums) / n
        var  = sum((x - mean) ** 2 for x in nums) / n if n > 1 else 0
        med  = nums[n // 2] if n % 2 else (nums[n // 2 - 1] + nums[n // 2]) / 2
        return {"count": n, "min": nums[0], "max": nums[-1],
                "mean": mean, "stdev": math.sqrt(var), "median": med}

    def to_csv(self, path: str) -> None:
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(self.columns)
            w.writerows(self.rows)

    def __len__(self) -> int:
        return len(self.rows)


# ──────────────────────────────────────────────────────────────────────────────
# HTTP Cache
# ──────────────────────────────────────────────────────────────────────────────
class Cache:
    """File-based HTTP cache with per-entry TTL stored as JSON envelopes."""

    @staticmethod
    def _key(url: str) -> pathlib.Path:
        digest = hashlib.sha256(url.encode()).hexdigest()
        return CACHE_DIR / f"{digest}.json"

    @classmethod
    def get(cls, url: str) -> Optional[str]:
        p = cls._key(url)
        if not p.exists():
            return None
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if time.time() < data["expires"]:
                return data["body"]
        except Exception:
            pass
        return None

    @classmethod
    def put(cls, url: str, body: str, ttl: int) -> None:
        try:
            cls._key(url).write_text(
                json.dumps({"url": url,
                            "expires": time.time() + ttl,
                            "body": body}),
                encoding="utf-8",
            )
        except Exception:
            pass

    @classmethod
    def clear(cls) -> int:
        n = 0
        for f in CACHE_DIR.glob("*.json"):
            try:
                f.unlink()
                n += 1
            except Exception:
                pass
        return n

    @classmethod
    def stats(cls) -> Tuple[int, int]:
        files = list(CACHE_DIR.glob("*.json"))
        total = sum(f.stat().st_size for f in files if f.exists())
        return len(files), total


# ──────────────────────────────────────────────────────────────────────────────
# SDMX HTTP client
# ──────────────────────────────────────────────────────────────────────────────
class SDMXClient:
    SS_ACCEPT  = "application/vnd.sdmx.structurespecificdata+xml;version=2.1"
    STR_ACCEPT = "application/vnd.sdmx.structure+xml;version=2.1"

    @classmethod
    def _fetch(cls, url: str, accept: str, ttl: int) -> str:
        cached = Cache.get(url)
        if cached is not None:
            return cached
        req = urllib.request.Request(url, headers={"Accept": accept})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"HTTP {e.code}: {e.reason}  [{url}]")
        except Exception as e:
            raise RuntimeError(f"Network error: {e}  [{url}]")
        Cache.put(url, body, ttl)
        return body

    @classmethod
    def fetch_data(cls, dataflow_id: str, key: str = "",
                   start_period: str = "", end_period: str = "",
                   last_n: int = 0, first_n: int = 0) -> str:
        params: Dict[str, str] = {}
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        if last_n  > 0:
            params["lastNObservations"] = str(last_n)
        if first_n > 0:
            params["firstNObservations"] = str(first_n)
        url = f"{BASE_URL}/data/DF_{dataflow_id}/{key}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        return cls._fetch(url, cls.SS_ACCEPT, CACHE_TTL_DATA)

    @classmethod
    def fetch_vector(cls, vector_id: str, start_period: str = "",
                     end_period: str = "", last_n: int = 0) -> str:
        vid = vector_id.lstrip("vV")
        params: Dict[str, str] = {}
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        if last_n > 0:
            params["lastNObservations"] = str(last_n)
        url = f"{BASE_URL}/vector/v{vid}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        return cls._fetch(url, cls.SS_ACCEPT, CACHE_TTL_DATA)

    @classmethod
    def fetch_structure(cls, dataflow_id: str) -> str:
        url = f"{BASE_URL}/structure/Data_Structure_{dataflow_id}"
        return cls._fetch(url, cls.STR_ACCEPT, CACHE_TTL_STRUCT)

    @classmethod
    def fetch_cube_list(cls) -> str:
        """Fetch getAllCubesListLite — returns raw JSON string."""
        url = f"{WDS_BASE_URL}/getAllCubesListLite"
        return cls._fetch(url, "application/json", CACHE_TTL_STRUCT)


# ──────────────────────────────────────────────────────────────────────────────
# XML parsers
# ──────────────────────────────────────────────────────────────────────────────
NS_STRUCT = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
NS_COMMON = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common"
NS_XML    = "http://www.w3.org/XML/1998/namespace"


def _strip_ns(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag


def parse_structure_specific(xml_text: str) -> Frame:
    """Parse SDMX-ML 2.1 Structure Specific Data → Frame."""
    root     = ET.fromstring(xml_text)
    rows:     List[List[Any]] = []
    columns:  List[str]       = []
    dim_cols: List[str]       = []

    for dataset in root:
        if _strip_ns(dataset.tag) != "DataSet":
            continue
        for series in dataset:
            if _strip_ns(series.tag) != "Series":
                continue
            attrs = dict(series.attrib)
            if not columns:
                dim_cols = [k for k in attrs if not k.startswith("{")]
                columns  = dim_cols + ["TIME_PERIOD", "OBS_VALUE"]
            dim_vals = [attrs.get(c, "") for c in dim_cols]
            for obs in series:
                if _strip_ns(obs.tag) != "Obs":
                    continue
                tp  = obs.attrib.get("TIME_PERIOD", "")
                raw = obs.attrib.get("OBS_VALUE")
                try:
                    ov: Any = float(raw) if raw is not None else None
                except ValueError:
                    ov = raw
                rows.append(dim_vals + [tp, ov])

    return Frame(columns or [], rows)


# Type aliases for DSD
DSDCodelist  = Dict[str, str]   # {code_id: label}
DSDDimension = Dict[str, Any]   # {id, position, codelist_id, codes, name, is_time}
DSDResult    = Dict[str, Any]   # {dataflow_id, dimensions, attributes, codelists, flat}


def parse_dsd(xml_text: str) -> DSDResult:
    """
    Parse a full DSD XML response.

    Returns a dict with keys:
      dataflow_id  – string
      dimensions   – list of DSDDimension (sorted by position)
      attributes   – list of DSDDimension (attributes)
      codelists    – {codelist_id: {code_id: label}}
      flat         – {dim_or_attr_id: {code_id: label}}  (quick lookup)
    """
    root = ET.fromstring(xml_text)

    # 1. Collect all codelists
    codelists: Dict[str, DSDCodelist] = {}
    for cl in root.iter(f"{{{NS_STRUCT}}}Codelist"):
        cl_id = cl.get("id", "")
        codes: DSDCodelist = {}
        for code in cl.findall(f"{{{NS_STRUCT}}}Code"):
            code_id = code.get("id", "")
            label   = ""
            for name_el in code.findall(f"{{{NS_COMMON}}}Name"):
                lang = name_el.get(f"{{{NS_XML}}}lang", "")
                if not label or lang == "en":
                    label = name_el.text or ""
                if lang == "en":
                    break
            codes[code_id] = label
        codelists[cl_id] = codes

    # 2. Parse DataStructure elements
    dimensions: List[DSDDimension] = []
    attributes: List[DSDDimension] = []
    dataflow_id = ""

    def _cl_ref_from(elem) -> str:
        for lr in elem.iter(f"{{{NS_STRUCT}}}LocalRepresentation"):
            for er in lr.iter(f"{{{NS_STRUCT}}}Enumeration"):
                for ref in er:
                    v = ref.get("id", "")
                    if v:
                        return v
        return ""

    for ds in root.iter(f"{{{NS_STRUCT}}}DataStructure"):
        dataflow_id = ds.get("id", "")

        for comp_list in ds.iter(f"{{{NS_STRUCT}}}DimensionList"):
            for dim in comp_list:
                local = _strip_ns(dim.tag)
                if local not in ("Dimension", "TimeDimension"):
                    continue
                dim_id  = dim.get("id", "")
                pos     = int(dim.get("position", 0))
                cl_ref  = _cl_ref_from(dim)
                # Try to get a human name from the concept identity
                concept_name = dim_id
                for ci in dim.iter(f"{{{NS_STRUCT}}}ConceptIdentity"):
                    for ref in ci:
                        concept_name = ref.get("id", dim_id)
                codes = codelists.get(cl_ref, {})
                dimensions.append({
                    "id":           dim_id,
                    "position":     pos,
                    "codelist_id":  cl_ref,
                    "codes":        codes,
                    "name":         concept_name,
                    "is_time":      local == "TimeDimension",
                })

        for comp_list in ds.iter(f"{{{NS_STRUCT}}}AttributeList"):
            for attr_el in comp_list.iter(f"{{{NS_STRUCT}}}Attribute"):
                attr_id = attr_el.get("id", "")
                cl_ref  = _cl_ref_from(attr_el)
                codes   = codelists.get(cl_ref, {})
                attributes.append({
                    "id":          attr_id,
                    "codelist_id": cl_ref,
                    "codes":       codes,
                    "name":        attr_id,
                    "is_time":     False,
                })

    dimensions.sort(key=lambda d: d["position"])

    flat: Dict[str, DSDCodelist] = {d["id"]: d["codes"] for d in dimensions}
    flat.update({a["id"]: a["codes"] for a in attributes})

    return {
        "dataflow_id": dataflow_id,
        "dimensions":  dimensions,
        "attributes":  attributes,
        "codelists":   codelists,
        "flat":        flat,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Sparkline
# ──────────────────────────────────────────────────────────────────────────────
SPARK_CHARS = " ▁▂▃▄▅▆▇█"


def sparkline(values: List[Optional[float]], width: int = 40) -> str:
    nums = [v for v in values if v is not None]
    if not nums:
        return "─" * width
    lo, hi = min(nums), max(nums)
    rng = hi - lo or 1
    out = []
    for v in values[-width:]:
        if v is None:
            out.append("?")
        else:
            idx = int((v - lo) / rng * (len(SPARK_CHARS) - 1))
            out.append(SPARK_CHARS[idx])
    return "".join(out)


# ──────────────────────────────────────────────────────────────────────────────
# Application state
# ──────────────────────────────────────────────────────────────────────────────
class AppState:
    def __init__(self):
        # Screen mode: query | results | series | dsd | help | cache
        self.mode: str = "query"

        # ── Query form ────────────────────────────────────────────────────────
        self.dataflow_id:  str = "17100005"
        self.dim_key:      str = ""
        self.vector_id:    str = ""
        self.start_period: str = ""
        self.end_period:   str = ""
        self.last_n:       str = ""
        self.query_type:   str = "cube"
        self.form_cursor:  int = 0
        self.form_fields       = [
            "query_type", "dataflow_id", "dim_key", "vector_id",
            "start_period", "end_period", "last_n",
        ]
        self.editing:  bool = False
        self.edit_buf: str  = ""

        # ── Results ───────────────────────────────────────────────────────────
        self.frame:          Optional[Frame]     = None
        self.dsd:            Optional[DSDResult] = None
        self.error:          str                 = ""
        self.loading:        bool                = False
        self.status_msg:     str                 = (
            "Ready — 1 help  2 query  3 results  4 DSD  5 cache  6 search  |  q quit"
        )

        self.table_offset: int  = 0
        self.cursor_row:   int  = 0
        self.sort_col:     int  = -1
        self.sort_desc:    bool = False

        self.filter_text:    str             = ""
        self.filter_mode:    bool            = False
        self.filtered_frame: Optional[Frame] = None

        # ── Labels checkbox ───────────────────────────────────────────────────
        self.show_labels: bool = True   # join DSD code labels into results

        # ── DSD browser ───────────────────────────────────────────────────────
        self.dsd_level:        str  = "dims"   # "dims" | "codes"
        self.dsd_dim_cursor:   int  = 0
        self.dsd_code_cursor:  int  = 0
        self.dsd_code_offset:  int  = 0
        self.dsd_filter:       str  = ""
        self.dsd_filter_mode:  bool = False
        # Per-dimension selections: {dim_id: set of selected code IDs}
        # Empty set means "All" (wildcard). Populated when DSD is loaded.
        self.dsd_selections: Dict[str, set] = {}

        # ── Table search ─────────────────────────────────────────────────
        self.cube_list: List[Dict] = []        # full list from getAllCubesListLite
        self.cube_list_loaded: bool = False
        self.cube_list_loading: bool = False
        self.search_query: str = ""
        self.search_cursor: int = 0
        self.search_offset: int = 0


STATE = AppState()


# ──────────────────────────────────────────────────────────────────────────────
# Colour pairs
# ──────────────────────────────────────────────────────────────────────────────
C_NORMAL   = 1
C_HEADER   = 2
C_ACCENT   = 3
C_DIM_VAL  = 4
C_NUM      = 5
C_TIME     = 6
C_SELECTED = 7
C_ERROR    = 8
C_SUCCESS  = 9
C_TITLE    = 10
C_DIM2     = 11
C_WARN     = 12
C_BORDER   = 13
C_SPARK    = 14
C_LABEL    = 15
C_MUTED    = 16


def init_colors() -> None:
    curses.start_color()
    curses.use_default_colors()
    bg = -1
    for pair, fg, _bg in [
        (C_NORMAL,   curses.COLOR_WHITE,   bg),
        (C_HEADER,   curses.COLOR_BLACK,   curses.COLOR_CYAN),
        (C_ACCENT,   curses.COLOR_CYAN,    bg),
        (C_DIM_VAL,  curses.COLOR_MAGENTA, bg),
        (C_NUM,      curses.COLOR_GREEN,   bg),
        (C_TIME,     curses.COLOR_BLUE,    bg),
        (C_SELECTED, curses.COLOR_BLACK,   curses.COLOR_WHITE),
        (C_ERROR,    curses.COLOR_RED,     bg),
        (C_SUCCESS,  curses.COLOR_GREEN,   bg),
        (C_TITLE,    curses.COLOR_CYAN,    bg),
        (C_DIM2,     curses.COLOR_YELLOW,  bg),
        (C_WARN,     curses.COLOR_YELLOW,  bg),
        (C_BORDER,   curses.COLOR_BLUE,    bg),
        (C_SPARK,    curses.COLOR_CYAN,    bg),
        (C_LABEL,    curses.COLOR_WHITE,   bg),
        (C_MUTED,    curses.COLOR_BLUE,    bg),
    ]:
        try:
            curses.init_pair(pair, fg, _bg)
        except Exception:
            pass


def cp(pair: int, bold: bool = False) -> int:
    attr = curses.color_pair(pair)
    if bold:
        attr |= curses.A_BOLD
    return attr


# ──────────────────────────────────────────────────────────────────────────────
# Low-level drawing helpers
# ──────────────────────────────────────────────────────────────────────────────
def safe_addstr(win, y: int, x: int, text: str, attr: int = 0) -> None:
    h, w = win.getmaxyx()
    if y < 0 or y >= h or x >= w:
        return
    if x < 0:
        text = text[-x:]
        x = 0
    avail = w - x
    if avail <= 0:
        return
    try:
        win.addstr(y, x, text[:avail], attr)
    except curses.error:
        pass


def hline(win, y: int, x: int, length: int, attr: int = 0) -> None:
    safe_addstr(win, y, x, "─" * length, attr)


def draw_box(win, y: int, x: int, h: int, w: int,
             title: str = "", attr: int = 0) -> None:
    if h < 2 or w < 2:
        return
    safe_addstr(win, y,         x,       "╭" + "─" * (w - 2) + "╮", attr)
    safe_addstr(win, y + h - 1, x,       "╰" + "─" * (w - 2) + "╯", attr)
    for row in range(1, h - 1):
        safe_addstr(win, y + row, x,         "│", attr)
        safe_addstr(win, y + row, x + w - 1, "│", attr)
    if title:
        t  = f" {title} "
        tx = x + max(1, (w - len(t)) // 2)
        safe_addstr(win, y, tx, t, attr | curses.A_BOLD)


def draw_scrollbar(win, top_y: int, height: int, x: int,
                   total: int, offset: int) -> None:
    if total <= height or height <= 0:
        return
    thumb = max(1, height * height // total)
    thumb_y = top_y + int((height - thumb) * offset / max(1, total - height))
    for sy in range(height):
        ch = "█" if thumb_y <= top_y + sy < thumb_y + thumb else "░"
        safe_addstr(win, top_y + sy, x, ch, cp(C_BORDER))


# ──────────────────────────────────────────────────────────────────────────────
# Shared chrome: top nav bar + bottom status bar
# ──────────────────────────────────────────────────────────────────────────────
_NAV_ITEMS = [
    ("1 Help",    "help"),
    ("2 Query",   "query"),
    ("3 Results", "results"),
    ("4 DSD",     "dsd"),
    ("5 Cache",   "cache"),
    ("6 Search",  "search"),
]


def draw_chrome(win) -> None:
    h, w = win.getmaxyx()

    # Top bar background
    safe_addstr(win, 0, 0, " " * w, cp(C_HEADER, bold=True))
    safe_addstr(win, 0, 0, "  StatCan SDMX Explorer", cp(C_HEADER, bold=True))

    # Navigation tabs (right-aligned)
    tab_x = w - 2
    for key, mode in reversed(_NAV_ITEMS):
        label = f" {key} "
        tab_x -= len(label) + 1
        attr = cp(C_SELECTED) if STATE.mode == mode else cp(C_HEADER)
        safe_addstr(win, 0, tab_x, label, attr)

    quit_hint = " | q Quit "
    safe_addstr(win, 0, max(2, tab_x - len(quit_hint)), quit_hint, cp(C_HEADER))

    # Bottom status bar
    last = h - 1
    safe_addstr(win, last, 0, " " * w, cp(C_MUTED))
    count, nbytes = Cache.stats()
    right = f"  cache: {count} files · {nbytes // 1024} KB  "
    safe_addstr(win, last, 2,
                STATE.status_msg[:w - len(right) - 4], cp(C_MUTED))
    safe_addstr(win, last, w - len(right) - 1, right, cp(C_MUTED))


# ──────────────────────────────────────────────────────────────────────────────
# Query form
# ──────────────────────────────────────────────────────────────────────────────
FIELD_LABELS = {
    "query_type":   "Query type    ",
    "dataflow_id":  "Dataflow ID   ",
    "dim_key":      "Dimension key ",
    "vector_id":    "Vector ID     ",
    "start_period": "Start period  ",
    "end_period":   "End period    ",
    "last_n":       "Last N obs    ",
}
FIELD_HINTS = {
    "query_type":   "'cube' or 'vector'",
    "dataflow_id":  "e.g. 17100005",
    "dim_key":      "e.g. 1.1.1  or  ..1  (empty = auto-wildcard all dims)",
    "vector_id":    "e.g. v466668  or  466668",
    "start_period": "e.g. 2015  or  2022-01",
    "end_period":   "e.g. 2023  or  2023-12",
    "last_n":       "integer (0 = all)",
}


def draw_query(win) -> None:
    h, w = win.getmaxyx()
    win.erase()
    draw_chrome(win)
    draw_box(win, 1, 1, h - 2, w - 2, "Query Parameters", cp(C_BORDER))

    is_cube = STATE.query_type.lower() != "vector"
    y = 3
    safe_addstr(win, y, 4, "Examples:", cp(C_LABEL, bold=True))
    for i, ex in enumerate([
        "Cube  : DF_17100005 / key = 1.1.1",
        "Cube  : DF_17100005 / key = ..1   (wildcard, all geographies)",
        "Cube  : DF_17100005 / key = 1.2+3.1  start=2015  end=2023",
        "Vector: v466668",
    ]):
        safe_addstr(win, y + 1 + i, 6, ex, cp(C_MUTED))

    form_y = y + 7
    for i, field in enumerate(STATE.form_fields):
        if field in ("dim_key", "dataflow_id") and not is_cube:
            continue
        if field == "vector_id" and is_cube:
            continue
        fy = form_y + i * 2
        if fy >= h - 6:
            break

        label    = FIELD_LABELS[field]
        hint     = FIELD_HINTS[field]
        value    = getattr(STATE, field)
        selected = (STATE.form_cursor == i)
        editing  = selected and STATE.editing

        safe_addstr(win, fy, 4, label,
                    cp(C_ACCENT, bold=True) if selected else cp(C_LABEL))
        box_w = min(42, w - 28)
        disp  = (STATE.edit_buf if editing else value)[:box_w - 2]
        ba    = (cp(C_SELECTED) if editing else
                 cp(C_ACCENT)   if selected else cp(C_NORMAL))
        safe_addstr(win, fy, 4 + len(label) + 1, f"[ {disp:<{box_w-2}} ]", ba)
        safe_addstr(win, fy, 4 + len(label) + box_w + 4,
                    f"← {hint}", cp(C_MUTED))

    act_y = form_y + len(STATE.form_fields) * 2 + 1
    if act_y < h - 5:
        safe_addstr(win, act_y, 4,
            "↑↓ navigate   Enter edit field   Esc cancel   Space toggle cube/vector",
            cp(C_DIM2))
    if act_y + 1 < h - 5:
        safe_addstr(win, act_y + 1, 4,
            "g  fetch data   d  fetch DSD   3  results   q quit",
            cp(C_DIM2))

    # ── Labels checkbox ──────────────────────────────────────────────────────
    cb_y = act_y + 3
    cb_box = "[x]" if STATE.show_labels else "[ ]"
    safe_addstr(win, cb_y, 4, cb_box,
                cp(C_SUCCESS, bold=True) if STATE.show_labels else cp(C_MUTED))
    safe_addstr(win, cb_y, 8, " Join dimension labels in results",
                cp(C_LABEL) if STATE.show_labels else cp(C_MUTED))
    safe_addstr(win, cb_y, 42, "  x = toggle", cp(C_DIM2))

    if STATE.dsd:
        ndims = len(STATE.dsd.get("dimensions", []))
        sel_summary = ""
        if STATE.dsd_selections:
            n_filtered = sum(1 for s in STATE.dsd_selections.values() if s)
            if n_filtered:
                sel_summary = f"  ({n_filtered} dim(s) filtered)"
        safe_addstr(win, cb_y + 2, 4,
            f"✓ DSD loaded: {ndims} dimensions  (4 to select){sel_summary}",
            cp(C_SUCCESS))

    if STATE.loading:
        safe_addstr(win, cb_y + (4 if STATE.dsd else 3) + 1, 4,
                    "⏳  Loading …", cp(C_WARN, bold=True))
    if STATE.error:
        ey = cb_y + (5 if STATE.dsd else 4) + (1 if STATE.loading else 0)
        for j, line in enumerate(textwrap.wrap(f"✖  {STATE.error}", w - 10)[:3]):
            safe_addstr(win, ey + j, 4, line, cp(C_ERROR))


# ──────────────────────────────────────────────────────────────────────────────
# Results table
# ──────────────────────────────────────────────────────────────────────────────
COL_WIDTHS: Dict[str, int] = {"TIME_PERIOD": 13, "OBS_VALUE": 16}
DIM_COL_W  = 12


def visible_frame() -> Optional[Frame]:
    if STATE.filter_text and STATE.filtered_frame is not None:
        return STATE.filtered_frame
    return STATE.frame


def apply_filter() -> None:
    if not STATE.filter_text or STATE.frame is None:
        STATE.filtered_frame = None
        return
    txt  = STATE.filter_text.lower()
    rows = [r for r in STATE.frame.rows
            if any(txt in str(v).lower() for v in r)]
    STATE.filtered_frame = Frame(STATE.frame.columns, rows)


def _col_w(col: str) -> int:
    if col in COL_WIDTHS:
        return COL_WIDTHS[col]
    if col.endswith("_LABEL"):
        return 24
    return max(DIM_COL_W, len(col) + 2)


def draw_results(win) -> None:
    h, w = win.getmaxyx()
    win.erase()
    draw_chrome(win)

    frame = visible_frame()

    if STATE.loading:
        safe_addstr(win, h // 2, max(0, (w - 18) // 2),
                    "⏳  Loading data …", cp(C_WARN, bold=True))
        return

    if STATE.error and frame is None:
        safe_addstr(win, 3, 3, f"Error: {STATE.error}", cp(C_ERROR))
        safe_addstr(win, 5, 3, "2  back to query", cp(C_MUTED))
        return

    if frame is None:
        safe_addstr(win, h // 2, 4,
                    "No data yet — press 2 then g to fetch",
                    cp(C_MUTED))
        return

    # Filter bar
    fl = "Filter: "
    fv = (STATE.filter_text if STATE.filter_text
          else ("▌ type to filter" if STATE.filter_mode else "(/ to filter)"))
    safe_addstr(win, 1, 2, fl, cp(C_LABEL, bold=True))
    safe_addstr(win, 1, 2 + len(fl), fv,
                cp(C_ACCENT) if STATE.filter_text else cp(C_MUTED))
    n_info = f"{len(frame)} rows"
    if STATE.filter_text and STATE.frame:
        n_info += f" (of {len(STATE.frame)})"
    safe_addstr(win, 1, 2 + len(fl) + 44, n_info, cp(C_DIM2))

    if 0 <= STATE.sort_col < len(frame.columns):
        sd = "▼" if STATE.sort_desc else "▲"
        si = f"sort: {frame.columns[STATE.sort_col]} {sd}"
        safe_addstr(win, 1, w - len(si) - 3, si, cp(C_MUTED))

    # Column headers
    hy = 2
    hline(win, hy, 1, w - 2, cp(C_BORDER))
    x = 2
    for ci, col in enumerate(frame.columns):
        if x >= w - 3:
            break
        cw  = _col_w(col)
        hdr = col[:cw].ljust(cw)
        if ci == STATE.sort_col:
            hdr = hdr[:-1] + ("▼" if STATE.sort_desc else "▲")
        safe_addstr(win, hy, x, hdr, cp(C_HEADER, bold=True))
        x += cw + 1
    hline(win, hy + 1, 1, w - 2, cp(C_BORDER))

    # Rows
    table_y  = hy + 2
    vis_rows = h - table_y - 2
    total    = len(frame)

    if total:
        STATE.table_offset = max(0, min(STATE.table_offset, total - 1))
        STATE.cursor_row   = max(0, min(STATE.cursor_row,   total - 1))
        if STATE.cursor_row < STATE.table_offset:
            STATE.table_offset = STATE.cursor_row
        elif STATE.cursor_row >= STATE.table_offset + vis_rows:
            STATE.table_offset = STATE.cursor_row - vis_rows + 1

    for ri in range(vis_rows):
        ai = ri + STATE.table_offset
        if ai >= total:
            break
        row       = frame.rows[ai]
        is_cursor = (ai == STATE.cursor_row)
        x = 2
        for ci, col in enumerate(frame.columns):
            if x >= w - 3:
                break
            cw  = _col_w(col)
            val = row[ci] if ci < len(row) else ""
            if is_cursor:
                attr = cp(C_SELECTED)
            elif col == "OBS_VALUE":
                attr = cp(C_NUM)
            elif col == "TIME_PERIOD":
                attr = cp(C_TIME)
            elif col.endswith("_LABEL"):
                attr = cp(C_MUTED)
            else:
                attr = cp(C_DIM_VAL) if ci % 2 == 0 else cp(C_DIM2)
            cell = (str(val) if val is not None else "·")[:cw].ljust(cw)
            safe_addstr(win, table_y + ri, x, cell, attr)
            x += cw + 1

    draw_scrollbar(win, table_y, vis_rows, w - 2, total, STATE.table_offset)

    hline(win, h - 2, 1, w - 2, cp(C_BORDER))
    safe_addstr(win, h - 2, 2,
        "↑↓ navigate   Enter series detail   s sort   / filter   e export CSV",
        cp(C_MUTED))


# ──────────────────────────────────────────────────────────────────────────────
# Series detail
# ──────────────────────────────────────────────────────────────────────────────
def draw_series_detail(win) -> None:
    h, w = win.getmaxyx()
    win.erase()
    draw_chrome(win)

    frame = visible_frame()
    if frame is None or STATE.cursor_row >= len(frame):
        safe_addstr(win, 5, 4, "No row selected.", cp(C_MUTED))
        return

    row      = frame.rows[STATE.cursor_row]
    dim_cols = [c for c in frame.columns
                if c not in ("TIME_PERIOD", "OBS_VALUE") and not c.endswith("_LABEL")]

    key_vals = tuple(row[frame.columns.index(c)] for c in dim_cols)
    s_rows   = [r for r in frame.rows
                if tuple(r[frame.columns.index(c)] for c in dim_cols) == key_vals]

    try:
        tp_idx = frame.columns.index("TIME_PERIOD")
        ov_idx = frame.columns.index("OBS_VALUE")
    except ValueError:
        safe_addstr(win, 5, 4, "Missing TIME_PERIOD or OBS_VALUE column.",
                    cp(C_ERROR))
        return

    s_rows.sort(key=lambda r: str(r[tp_idx]))
    times  = [r[tp_idx] for r in s_rows]
    values: List[Optional[float]] = []
    for r in s_rows:
        v = r[ov_idx]
        try:
            values.append(float(v) if v is not None else None)
        except (TypeError, ValueError):
            values.append(None)

    draw_box(win, 1, 1, h - 2, w - 2, "Series Detail", cp(C_BORDER))
    y = 3

    flat = (STATE.dsd or {}).get("flat", {})

    # Dimensions section
    safe_addstr(win, y, 4, "Dimensions", cp(C_ACCENT, bold=True))
    y += 1
    hline(win, y, 4, min(60, w - 8), cp(C_BORDER))
    y += 1
    for col in dim_cols:
        val   = row[frame.columns.index(col)]
        label = flat.get(col, {}).get(str(val), "")
        disp  = str(val) + (f"  {label}" if label else "")
        safe_addstr(win, y, 4,  f"{col:<22}", cp(C_LABEL))
        safe_addstr(win, y, 26, disp[:w - 30], cp(C_DIM_VAL))
        y += 1
    y += 1

    # Statistics section
    safe_addstr(win, y, 4, "Statistics", cp(C_ACCENT, bold=True))
    y += 1
    hline(win, y, 4, min(60, w - 8), cp(C_BORDER))
    y += 1
    stats = Frame(frame.columns, s_rows).describe("OBS_VALUE")
    if stats:
        for lbl, val_s in [
            ("Count",  f"{int(stats['count'])}"),
            ("Min",    f"{stats['min']:,.4f}"),
            ("Max",    f"{stats['max']:,.4f}"),
            ("Mean",   f"{stats['mean']:,.4f}"),
            ("Median", f"{stats['median']:,.4f}"),
            ("StdDev", f"{stats['stdev']:,.4f}"),
        ]:
            safe_addstr(win, y, 4,  f"{lbl:<10}", cp(C_LABEL))
            safe_addstr(win, y, 14, val_s, cp(C_NUM))
            y += 1
    y += 1

    # Sparkline
    spark_w = min(w - 10, 64)
    if y + 2 < h - 5:
        safe_addstr(win, y, 4, "Trend", cp(C_ACCENT, bold=True))
        y += 1
        safe_addstr(win, y, 4, sparkline(values, spark_w), cp(C_SPARK))
        if times:
            safe_addstr(win, y + 1, 4, str(times[0]), cp(C_TIME))
            rl = str(times[-1])
            safe_addstr(win, y + 1, 4 + spark_w - len(rl), rl, cp(C_TIME))
        y += 3

    # Observations table
    if y + 3 < h - 4:
        safe_addstr(win, y, 4, "Observations", cp(C_ACCENT, bold=True))
        y += 1
        hline(win, y, 4, 36, cp(C_BORDER))
        y += 1
        safe_addstr(win, y, 4, f"{'TIME_PERIOD':<14} {'OBS_VALUE':>16}",
                    cp(C_HEADER, bold=True))
        y += 1
        max_y = h - 4
        for i, (t, v) in enumerate(zip(times, values)):
            if y + i >= max_y:
                safe_addstr(win, y + i, 4,
                            f"… {len(times) - i} more rows", cp(C_MUTED))
                break
            vs = f"{v:,.4f}" if v is not None else "—"
            safe_addstr(win, y + i, 4,  f"{str(t):<14}", cp(C_TIME))
            safe_addstr(win, y + i, 18, f"{vs:>16}", cp(C_NUM))

    hline(win, h - 2, 1, w - 2, cp(C_BORDER))
    safe_addstr(win, h - 2, 2,
                "Esc / Backspace  return to results table", cp(C_MUTED))


# ──────────────────────────────────────────────────────────────────────────────
# DSD browser
# ──────────────────────────────────────────────────────────────────────────────
def _dsd_all_items() -> List[Tuple[str, str, bool]]:
    """Returns [(id, display_name, is_attr)] for dims + attrs."""
    if not STATE.dsd:
        return []
    dims  = STATE.dsd.get("dimensions", [])
    attrs = STATE.dsd.get("attributes", [])
    items: List[Tuple[str, str, bool]] = []
    for d in dims:
        name = d.get("name", d["id"])
        items.append((d["id"], name, False))
    for a in attrs:
        name = a.get("name", a["id"])
        items.append((a["id"], name, True))
    return items


def _num_sort_key(code_id: str):
    """Sort key: numeric if possible, else lowercase string."""
    try:
        return (0, int(code_id), "")
    except ValueError:
        return (1, 0, code_id.lower())


def _dsd_codes_for_cursor() -> List[Tuple[str, str]]:
    """Numerically-sorted + filtered [(code_id, label)] for the dim/attr at cursor."""
    if not STATE.dsd:
        return []
    all_items = _dsd_all_items()
    if STATE.dsd_dim_cursor >= len(all_items):
        return []
    item_id = all_items[STATE.dsd_dim_cursor][0]
    codes   = STATE.dsd["flat"].get(item_id, {})
    items   = sorted(codes.items(), key=lambda x: _num_sort_key(x[0]))
    if STATE.dsd_filter:
        txt   = STATE.dsd_filter.lower()
        items = [(k, v) for k, v in items
                 if txt in k.lower() or txt in v.lower()]
    return items


def _build_key_from_selections() -> str:
    """
    Build an SDMX dimension key from STATE.dsd_selections.
    For each non-time dimension (in position order):
      - empty set  -> "" (wildcard for that position)
      - {a, b, c}  -> "a+b+c"
    Positions joined by ".".
    If no DSD is loaded, returns "".
    """
    if not STATE.dsd:
        return ""
    dims = [d for d in STATE.dsd.get("dimensions", []) if not d.get("is_time")]
    parts = []
    for d in dims:
        sel = STATE.dsd_selections.get(d["id"], set())
        if sel:
            sorted_codes = sorted(sel, key=_num_sort_key)
            parts.append("+".join(sorted_codes))
        else:
            parts.append("")
    return ".".join(parts)


def draw_dsd(win) -> None:
    h, w = win.getmaxyx()
    win.erase()
    draw_chrome(win)

    if STATE.dsd is None:
        safe_addstr(win, h // 2, 4,
            "No DSD loaded.  Go to query form (2) and press d.",
            cp(C_MUTED))
        return

    all_items = _dsd_all_items()
    n_items   = len(all_items)
    codes     = _dsd_codes_for_cursor()

    # Current dimension info for right pane
    cur_item    = all_items[STATE.dsd_dim_cursor] if all_items else None
    cur_dim_id  = cur_item[0] if cur_item else ""
    cur_dim_nm  = cur_item[1] if cur_item else ""
    cur_is_attr = cur_item[2] if cur_item else False
    cur_sel     = STATE.dsd_selections.get(cur_dim_id, set()) if not cur_is_attr else set()

    # ── Left pane: dimension / attribute list ─────────────────────────────────
    pane_w = max(32, w // 3)
    n_dims  = len(STATE.dsd.get("dimensions", []))
    n_attrs = len(STATE.dsd.get("attributes", []))
    n_filt  = sum(1 for d in STATE.dsd.get("dimensions", [])
                  if not d.get("is_time") and STATE.dsd_selections.get(d["id"]))
    title_l = f"Dims({n_dims}) Attrs({n_attrs})"
    if n_filt:
        title_l += f" [{n_filt} filtered]"
    draw_box(win, 1, 0, h - 2, pane_w, title_l, cp(C_BORDER))

    vis_dim = h - 5
    dim_off = max(0, STATE.dsd_dim_cursor - vis_dim + 1)               if STATE.dsd_dim_cursor >= vis_dim else 0

    for i in range(vis_dim):
        ai = i + dim_off
        if ai >= n_items:
            break
        did, dname, is_attr = all_items[ai]
        is_left_sel = (ai == STATE.dsd_dim_cursor)

        if is_left_sel and STATE.dsd_level == "dims":
            row_attr = cp(C_SELECTED)
        elif is_left_sel:
            row_attr = cp(C_ACCENT)
        elif is_attr:
            row_attr = cp(C_MUTED)
        else:
            row_attr = cp(C_LABEL)

        prefix   = "A " if is_attr else "D "
        dim_sel  = STATE.dsd_selections.get(did, set()) if not is_attr else set()
        n_codes  = len(STATE.dsd["flat"].get(did, {}))
        if dim_sel:
            sel_str = f"[{len(dim_sel)}/{n_codes}]"
        else:
            sel_str = "[All]"
        # name truncated to fit, sel_str right-padded
        avail  = pane_w - 6 - len(sel_str) - 1
        dname_t = dname[:avail] if len(dname) > avail else dname
        line   = f" {prefix}{dname_t:<{avail}} {sel_str}"[:pane_w - 2]
        safe_addstr(win, 3 + i, 1, line.ljust(pane_w - 2), row_attr)
        # Highlight sel_str in accent when filtered
        if dim_sel and not is_left_sel:
            sx = 1 + 2 + avail + 1  # position of sel_str
            safe_addstr(win, 3 + i, sx, sel_str, cp(C_ACCENT))

    draw_scrollbar(win, 3, vis_dim, pane_w - 1, n_items, dim_off)

    # Key preview at bottom of left pane
    key_preview = _build_key_from_selections()
    kp_label = "Key: "
    kp_val   = key_preview if key_preview else "(all)"
    safe_addstr(win, h - 3, 2, kp_label, cp(C_LABEL, bold=True))
    safe_addstr(win, h - 3, 2 + len(kp_label),
                kp_val[:pane_w - 8], cp(C_ACCENT))

    # ── Right pane: codes ─────────────────────────────────────────────────────
    rx = pane_w
    rw = w - pane_w
    if rw < 10:
        return

    n_sel_right = len(cur_sel)
    sel_count   = (f"{n_sel_right} selected" if n_sel_right else "All (none selected)")
    draw_box(win, 1, rx, h - 2, rw,
             f" {cur_dim_id} — {cur_dim_nm} ", cp(C_BORDER))

    # Filter bar
    fl_label = "Filter: "
    fl_val   = (STATE.dsd_filter if STATE.dsd_filter
                else ("▌ type to filter" if STATE.dsd_filter_mode
                      else "(/ to filter)"))
    safe_addstr(win, 2, rx + 2, fl_label, cp(C_LABEL, bold=True))
    safe_addstr(win, 2, rx + 2 + len(fl_label), fl_val,
                cp(C_ACCENT) if STATE.dsd_filter else cp(C_MUTED))
    count_str = f"{len(codes)} codes  {sel_count}"
    safe_addstr(win, 2, max(rx + 2 + len(fl_label) + len(fl_val) + 2,
                            rx + rw - len(count_str) - 2),
                count_str, cp(C_DIM2))

    # Header row — checkbox col + code id + label
    safe_addstr(win, 3, rx + 2,
                f"  {'Code ID':<12}  {'Label'}", cp(C_HEADER, bold=True))
    hline(win, 4, rx + 1, rw - 2, cp(C_BORDER))

    vis_codes = h - 8
    if codes:
        STATE.dsd_code_cursor = max(0, min(STATE.dsd_code_cursor, len(codes) - 1))
        if STATE.dsd_code_cursor < STATE.dsd_code_offset:
            STATE.dsd_code_offset = STATE.dsd_code_cursor
        elif STATE.dsd_code_cursor >= STATE.dsd_code_offset + vis_codes:
            STATE.dsd_code_offset = STATE.dsd_code_cursor - vis_codes + 1

    for i in range(vis_codes):
        ci = i + STATE.dsd_code_offset
        if ci >= len(codes):
            break
        cid, clabel = codes[ci]
        is_cursor   = (ci == STATE.dsd_code_cursor and STATE.dsd_level == "codes")
        is_checked  = (cid in cur_sel) if not cur_is_attr else False

        if is_cursor:
            row_attr = cp(C_SELECTED)
        elif is_checked:
            row_attr = cp(C_SUCCESS)
        else:
            row_attr = cp(C_NORMAL)

        # Checkbox glyph: [x] checked, [ ] unchecked; attrs show no checkbox
        if cur_is_attr:
            cb = "   "
        else:
            cb = "[x]" if is_checked else "[ ]"

        line = f"{cb} {cid:<12}  {clabel}"[:rw - 4]
        safe_addstr(win, 5 + i, rx + 2, line.ljust(rw - 4), row_attr)
        # Colour just the checkbox distinctly when not on cursor row
        if not is_cursor and is_checked:
            safe_addstr(win, 5 + i, rx + 2, cb, cp(C_SUCCESS, bold=True))

    draw_scrollbar(win, 5, vis_codes, rx + rw - 2,
                   len(codes), STATE.dsd_code_offset)

    # Hint bar
    hline(win, h - 2, 0, w, cp(C_BORDER))
    if STATE.dsd_level == "dims":
        hints = ("↑↓ select dim   → / Enter  open codes   "
                 "a reset All   2 apply & go to query")
    else:
        if cur_is_attr:
            hints = ("↑↓ navigate   ← / Esc  back   / filter   2 query")
        else:
            hints = ("↑↓ navigate   Space toggle   a reset All   "
                     "← / Esc back   / filter   2 apply & query")
    safe_addstr(win, h - 2, 2, hints, cp(C_MUTED))


# ──────────────────────────────────────────────────────────────────────────────
# Cache panel
# ──────────────────────────────────────────────────────────────────────────────
def draw_cache(win) -> None:
    h, w = win.getmaxyx()
    win.erase()
    draw_chrome(win)
    draw_box(win, 1, 1, h - 2, w - 2, "Cache Manager", cp(C_BORDER))

    count, nbytes = Cache.stats()
    y = 3
    safe_addstr(win, y, 4, "On-disk HTTP response cache",
                cp(C_ACCENT, bold=True))
    y += 2
    for lbl, val in [
        ("Entries",      str(count)),
        ("Total size",   f"{nbytes // 1024} KB  ({nbytes:,} bytes)"),
        ("Location",     str(CACHE_DIR)),
        ("Data TTL",     f"{CACHE_TTL_DATA} s  ({CACHE_TTL_DATA // 60} min)"),
        ("Structure TTL",f"{CACHE_TTL_STRUCT} s  ({CACHE_TTL_STRUCT // 3600} h)"),
    ]:
        safe_addstr(win, y, 4,  f"{lbl:<18}", cp(C_LABEL))
        safe_addstr(win, y, 22, val, cp(C_NORMAL))
        y += 1
    y += 2
    safe_addstr(win, y, 4, "c  Clear all cache entries", cp(C_DIM2, bold=True))
    if STATE.status_msg.startswith("Cache cleared"):
        safe_addstr(win, y + 2, 4, STATE.status_msg, cp(C_SUCCESS, bold=True))


# ──────────────────────────────────────────────────────────────────────────────
# Help screen
# ──────────────────────────────────────────────────────────────────────────────
HELP_TEXT = """\
╭──────────────────────────────────────────────────────────────╮
│               StatCan SDMX Explorer — Help                   │
╰──────────────────────────────────────────────────────────────╯

GLOBAL NAVIGATION  (number keys — active outside text-edit / filter mode)
  1              Help (this screen)
  2              Query form
  3              Results table
  4              DSD browser  (after fetching a DSD)
  5              Cache manager
  6              Table search
  q              Quit

QUERY FORM
  ↑ / ↓          Move between fields
  Enter          Begin editing the focused field
  Esc            Cancel editing
  Space          Toggle query type  (cube ↔ vector)
  x              Toggle [x] Join labels checkbox — when on, human-readable
                 labels are added next to each dimension code in results
  g              Execute query — fetches data, auto-fetches DSD, and
                 (if labels checkbox is on) joins labels into results
  d              Fetch DSD only (populates DSD browser selections)

RESULTS TABLE
  ↑ / ↓          Move cursor row
  PgUp / PgDn    Jump 10 rows
  Enter          Open series detail view
  s              Cycle sort column forward
  S              Cycle sort column backward
  /              Enter filter mode (type to filter all columns)
  Esc            Clear filter / exit filter mode
  e              Export visible rows to CSV

SERIES DETAIL
  Esc / Backspace  Return to results table

DSD BROWSER  (after fetching the DSD on the query form)
  Dimension key is built automatically from your selections here.
  "All" (no selections) = wildcard; selected codes are joined with +.

  Left pane — dimension / attribute list
    ↑ / ↓        Select dimension or attribute
    → / Enter    Open code list for the selected dimension
    /            Open code list and enter filter mode
    a            Reset ALL dimensions to "All" (clear every selection)

  Right pane — code list (sorted numerically)
    ↑ / ↓        Navigate codes
    PgUp / PgDn  Jump 10 codes
    Space        Toggle selection of the highlighted code  (multi-select)
    a            Reset THIS dimension to "All" (clear its selection)
    /            Filter codes by ID or label text
    Esc          Clear filter or return to left pane
    ←            Return to left pane

  2 applies selections → dim key, then goes to query form

CACHE MANAGER
  c              Clear all cached HTTP responses

TABLE SEARCH
  Type           Filter tables by name or product ID (live search)
  ↑ / ↓          Navigate results  (also j / k)
  PgUp / PgDn    Jump 10 rows
  Enter          Select table → populates query form, switches to it
  Backspace      Erase last character
  r              (Re)load the full table list  (cached 24 h)
  Note: list auto-loads on first keystroke

──────────────────────────────────────────────────────────────

SDMX KEY SYNTAX  (Dimension key field)
  Dimensions separated by dots:  Geo.Sex.Age = 1.1.1
  Wildcard a position:           .1.138  (all geographies)
  OR values with +:              1.2+3.1  (Sex = 2 or 3)
  All series:                    leave key empty — wildcards
                                 are filled in automatically

Press any key to return to the query form."""


def draw_help(win) -> None:
    h, w = win.getmaxyx()
    win.erase()
    draw_chrome(win)
    for i, line in enumerate(HELP_TEXT.split("\n")):
        y = 2 + i
        if y >= h - 1:
            break
        if line.startswith("╭") or line.startswith("╰") or line.startswith("│"):
            attr = cp(C_ACCENT, bold=True)
        elif line.startswith("─"):
            attr = cp(C_BORDER)
        elif line and not line[0].isspace() and line.endswith(":"):
            attr = cp(C_LABEL, bold=True)
        elif line.startswith("  1") or line.startswith("  2") \
                or line.startswith("  3") or line.startswith("  4") \
                or line.startswith("  5") or line.startswith("  6") \
                or line.startswith("  ↑") \
                or line.startswith("  ←") or line.startswith("  →") \
                or line.startswith("  /") or line.startswith("  s") \
                or line.startswith("  S") or line.startswith("  e") \
                or line.startswith("  g") or line.startswith("  d") \
                or line.startswith("  c") or line.startswith("  q") \
                or line.startswith("  P") or line.startswith("  Esc") \
                or line.startswith("  Sp") or line.startswith("  En"):
            attr = cp(C_DIM2)
        elif "Tip:" in line:
            attr = cp(C_ACCENT)
        elif line.startswith("  "):
            attr = cp(C_MUTED)
        else:
            attr = cp(C_NORMAL)
        safe_addstr(win, y, 2, line[:w - 3], attr)


# ──────────────────────────────────────────────────────────────────────────────
# Background fetch helpers
# ──────────────────────────────────────────────────────────────────────────────
def _bg(fn) -> None:
    threading.Thread(target=fn, daemon=True).start()


def _resolve_key(key: str, dataflow_id: str) -> str:
    """
    If key is empty, build a full-wildcard key based on the number of
    non-time dimensions in the DSD (e.g. 3 dims → "..").
    If a DSD is already loaded for this dataflow, use it; otherwise fetch it.
    Falls back to returning the original key if anything goes wrong.
    """
    if key.strip():
        return key.strip()
    try:
        # Use cached DSD if available for the right dataflow, else fetch
        dsd = STATE.dsd
        if dsd is None or dsd.get("dataflow_id", "") != f"Data_Structure_{dataflow_id}":
            xml = SDMXClient.fetch_structure(dataflow_id)
            dsd = parse_dsd(xml)
        dims = [d for d in dsd.get("dimensions", []) if not d.get("is_time")]
        n = len(dims)
        return "." * (n - 1) if n > 1 else ("" if n == 0 else "")
    except Exception:
        return key


def _join_dsd_labels(frame: Frame, dsd: DSDResult) -> Frame:
    """
    For every dimension column in frame that has a codelist in the DSD,
    insert a new  <DIM>_LABEL  column immediately after it containing the
    human-readable label for each code value.  Columns with no codelist
    (or where all codes are unknown) are left unchanged.
    """
    flat     = dsd.get("flat", {})
    new_cols: List[str]       = []
    new_rows: List[List[Any]] = [[] for _ in frame.rows]

    for ci, col in enumerate(frame.columns):
        new_cols.append(col)
        for ri, row in enumerate(frame.rows):
            new_rows[ri].append(row[ci] if ci < len(row) else "")

        # Only add label columns for non-TIME, non-OBS dimension columns
        if col in ("TIME_PERIOD", "OBS_VALUE"):
            continue
        codelist = flat.get(col)
        if not codelist:
            continue
        # Check at least one row actually resolves to a non-empty label
        has_label = any(
            codelist.get(str(row[ci] if ci < len(row) else ""), "")
            for row in frame.rows
        )
        if not has_label:
            continue
        label_col = f"{col}_LABEL"
        new_cols.append(label_col)
        for ri, row in enumerate(frame.rows):
            code = str(row[ci] if ci < len(row) else "")
            new_rows[ri].append(codelist.get(code, ""))

    return Frame(new_cols, new_rows)


def do_fetch_data() -> None:
    STATE.loading        = True
    STATE.error          = ""
    STATE.frame          = None
    STATE.filtered_frame = None
    STATE.status_msg     = "Fetching data …"
    try:
        ln = int(STATE.last_n) if STATE.last_n.strip().isdigit() else 0

        if STATE.query_type.lower() == "vector":
            xml = SDMXClient.fetch_vector(
                STATE.vector_id,
                start_period=STATE.start_period,
                end_period=STATE.end_period,
                last_n=ln,
            )
        else:
            # Resolve empty key to wildcards based on DSD dimension count
            resolved_key = _resolve_key(STATE.dim_key, STATE.dataflow_id)
            xml = SDMXClient.fetch_data(
                STATE.dataflow_id,
                key=resolved_key,
                start_period=STATE.start_period,
                end_period=STATE.end_period,
                last_n=ln,
            )

        frame = parse_structure_specific(xml)

        # Auto-fetch DSD; optionally join labels into the results frame
        STATE.status_msg = "Data loaded — fetching DSD for labels …"
        try:
            dsd_xml   = SDMXClient.fetch_structure(STATE.dataflow_id)
            dsd       = parse_dsd(dsd_xml)
            # Only reset selections if DSD changed (different dataflow)
            if STATE.dsd is None or STATE.dsd.get("dataflow_id") != dsd.get("dataflow_id"):
                STATE.dsd = dsd
                _init_dsd_selections()
            else:
                STATE.dsd = dsd
            if STATE.show_labels:
                frame = _join_dsd_labels(frame, dsd)
        except Exception:
            pass  # DSD join is best-effort; raw data is still shown

        STATE.frame        = frame
        STATE.cursor_row   = 0
        STATE.table_offset = 0
        STATE.sort_col     = -1
        STATE.filtered_frame = None
        ndims = len(STATE.dsd.get("dimensions", [])) if STATE.dsd else "?"
        STATE.status_msg = (
            f"Loaded {len(frame)} rows · {len(frame.columns)} columns"
            f" · {ndims} dimensions — 4 to explore DSD"
        )
        STATE.mode = "results"
    except Exception as e:
        STATE.error      = str(e)
        STATE.status_msg = "Fetch error — see query form (2)"
    finally:
        STATE.loading = False


def _init_dsd_selections() -> None:
    """Reset per-dimension selections to All (empty set) for current DSD."""
    if not STATE.dsd:
        return
    dims = [d for d in STATE.dsd.get("dimensions", []) if not d.get("is_time")]
    STATE.dsd_selections = {d["id"]: set() for d in dims}


def do_fetch_dsd() -> None:
    STATE.loading    = True
    STATE.status_msg = "Fetching DSD …"
    try:
        xml       = SDMXClient.fetch_structure(STATE.dataflow_id)
        STATE.dsd = parse_dsd(xml)
        _init_dsd_selections()
        # Sync dim_key to reflect fresh All-wildcard selections
        STATE.dim_key = _build_key_from_selections()
        ndims     = len(STATE.dsd.get("dimensions", []))
        nattrs    = len(STATE.dsd.get("attributes", []))
        STATE.status_msg = (
            f"DSD loaded: {ndims} dimensions, {nattrs} attributes"
            " — 4 to explore"
        )
    except Exception as e:
        STATE.error      = str(e)
        STATE.status_msg = "DSD fetch error"
    finally:
        STATE.loading = False


# ──────────────────────────────────────────────────────────────────────────────
# Input handlers
# ──────────────────────────────────────────────────────────────────────────────
def handle_query(ch: int) -> None:
    n = len(STATE.form_fields)
    if STATE.editing:
        if ch in (curses.KEY_ENTER, 10, 13):
            setattr(STATE, STATE.form_fields[STATE.form_cursor], STATE.edit_buf)
            STATE.editing = False
        elif ch == 27:
            STATE.editing = False
        elif ch in (curses.KEY_BACKSPACE, 127, 8):
            STATE.edit_buf = STATE.edit_buf[:-1]
        elif 32 <= ch < 127:
            STATE.edit_buf += chr(ch)
    else:
        if ch in (curses.KEY_UP, ord("k")):
            STATE.form_cursor = (STATE.form_cursor - 1) % n
        elif ch in (curses.KEY_DOWN, ord("j")):
            STATE.form_cursor = (STATE.form_cursor + 1) % n
        elif ch in (curses.KEY_ENTER, 10, 13):
            STATE.edit_buf = getattr(STATE, STATE.form_fields[STATE.form_cursor])
            STATE.editing  = True
        elif ch == ord(" "):
            STATE.query_type = "vector" if STATE.query_type == "cube" else "cube"
        elif ch in (ord("x"), ord("X")):
            STATE.show_labels = not STATE.show_labels
            STATE.status_msg  = (
                "Labels ON — will join DSD labels into results"
                if STATE.show_labels else
                "Labels OFF — raw code IDs only"
            )
        elif ch in (ord("g"), ord("G")):
            if not STATE.loading:
                _bg(do_fetch_data)
        elif ch in (ord("d"), ord("D")):
            if not STATE.loading:
                _bg(do_fetch_dsd)


def handle_results(ch: int) -> None:
    frame  = visible_frame()
    n_rows = len(frame) if frame else 0

    if STATE.filter_mode:
        if ch == 27:
            STATE.filter_text = ""
            STATE.filter_mode = False
            apply_filter()
        elif ch in (curses.KEY_BACKSPACE, 127, 8):
            STATE.filter_text = STATE.filter_text[:-1]
            apply_filter()
        elif ch in (curses.KEY_ENTER, 10, 13):
            STATE.filter_mode = False
        elif 32 <= ch < 127:
            STATE.filter_text += chr(ch)
            apply_filter()
        return

    if ch == ord("/"):
        STATE.filter_mode = True
        return
    if ch == 27:
        STATE.filter_text = ""
        STATE.filter_mode = False
        apply_filter()
        return

    if ch in (curses.KEY_UP, ord("k")):
        STATE.cursor_row = max(0, STATE.cursor_row - 1)
    elif ch in (curses.KEY_DOWN, ord("j")):
        STATE.cursor_row = min(n_rows - 1, STATE.cursor_row + 1) if n_rows else 0
    elif ch == curses.KEY_PPAGE:
        STATE.cursor_row = max(0, STATE.cursor_row - 10)
    elif ch == curses.KEY_NPAGE:
        STATE.cursor_row = min(n_rows - 1, STATE.cursor_row + 10) if n_rows else 0
    elif ch in (curses.KEY_ENTER, 10, 13):
        if frame and 0 <= STATE.cursor_row < len(frame):
            STATE.mode = "series"
    elif ch in (ord("s"), ord("S")):
        if frame:
            nc    = len(frame.columns)
            delta = -1 if ch == ord("S") else 1
            nc_   = (STATE.sort_col + delta) % nc
            if nc_ == STATE.sort_col:
                STATE.sort_desc = not STATE.sort_desc
            else:
                STATE.sort_col  = nc_
                STATE.sort_desc = False
            STATE.frame = STATE.frame.sort(
                frame.columns[STATE.sort_col], STATE.sort_desc)
            apply_filter()
            STATE.cursor_row = STATE.table_offset = 0
    elif ch in (ord("e"), ord("E")):
        if frame:
            ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = f"sdmx_export_{ts}.csv"
            frame.to_csv(path)
            STATE.status_msg = f"Exported {len(frame)} rows → {path}"


def _dsd_apply_selections() -> None:
    """Rebuild STATE.dim_key from current DSD selections and show status."""
    key = _build_key_from_selections()
    STATE.dim_key    = key
    n_filt = sum(1 for s in STATE.dsd_selections.values() if s)
    if n_filt == 0:
        STATE.status_msg = "Selections reset to All — key cleared (fetch all series)"
    else:
        STATE.status_msg = f"Key applied: {key!r}  ({n_filt} dimension(s) filtered)"


def handle_dsd(ch: int) -> None:
    all_items = _dsd_all_items()
    n_items   = len(all_items)

    # Current dimension context
    cur_item    = all_items[STATE.dsd_dim_cursor] if all_items else None
    cur_dim_id  = cur_item[0] if cur_item else ""
    cur_is_attr = cur_item[2] if cur_item else True   # attrs are not selectable

    if STATE.dsd_filter_mode:
        if ch == 27:
            STATE.dsd_filter      = ""
            STATE.dsd_filter_mode = False
            STATE.dsd_code_cursor = 0
            STATE.dsd_code_offset = 0
        elif ch in (curses.KEY_BACKSPACE, 127, 8):
            STATE.dsd_filter      = STATE.dsd_filter[:-1]
            STATE.dsd_code_cursor = 0
            STATE.dsd_code_offset = 0
        elif ch in (curses.KEY_ENTER, 10, 13):
            STATE.dsd_filter_mode = False
        elif 32 <= ch < 127:
            STATE.dsd_filter     += chr(ch)
            STATE.dsd_code_cursor = 0
            STATE.dsd_code_offset = 0
        return

    if STATE.dsd_level == "dims":
        if ch in (curses.KEY_UP, ord("k")):
            STATE.dsd_dim_cursor = max(0, STATE.dsd_dim_cursor - 1)
        elif ch in (curses.KEY_DOWN, ord("j")):
            STATE.dsd_dim_cursor = min(n_items - 1, STATE.dsd_dim_cursor + 1)
        elif ch in (curses.KEY_RIGHT, curses.KEY_ENTER, 10, 13,
                    ord("l"), ord(" ")):
            STATE.dsd_level       = "codes"
            STATE.dsd_code_cursor = 0
            STATE.dsd_code_offset = 0
            STATE.dsd_filter      = ""
        elif ch == ord("/"):
            STATE.dsd_level       = "codes"
            STATE.dsd_code_cursor = 0
            STATE.dsd_code_offset = 0
            STATE.dsd_filter      = ""
            STATE.dsd_filter_mode = True
        elif ch in (ord("a"), ord("A")):
            # Reset all dimensions to "All"
            _init_dsd_selections()
            _dsd_apply_selections()

    else:   # dsd_level == "codes"
        codes = _dsd_codes_for_cursor()
        n_c   = len(codes)

        if ch in (curses.KEY_UP, ord("k")):
            STATE.dsd_code_cursor = max(0, STATE.dsd_code_cursor - 1)
        elif ch in (curses.KEY_DOWN, ord("j")):
            STATE.dsd_code_cursor = min(n_c - 1,
                                        STATE.dsd_code_cursor + 1) if n_c else 0
        elif ch == curses.KEY_PPAGE:
            STATE.dsd_code_cursor = max(0, STATE.dsd_code_cursor - 10)
        elif ch == curses.KEY_NPAGE:
            STATE.dsd_code_cursor = min(n_c - 1,
                                        STATE.dsd_code_cursor + 10) if n_c else 0
        elif ch in (curses.KEY_LEFT, ord("h")):
            STATE.dsd_level       = "dims"
            STATE.dsd_filter      = ""
            STATE.dsd_filter_mode = False
        elif ch == 27:
            if STATE.dsd_filter:
                STATE.dsd_filter      = ""
                STATE.dsd_code_cursor = 0
                STATE.dsd_code_offset = 0
            else:
                STATE.dsd_level = "dims"
        elif ch == ord("/"):
            STATE.dsd_filter_mode = True
            STATE.dsd_filter      = ""
            STATE.dsd_code_cursor = 0
            STATE.dsd_code_offset = 0
        elif ch == ord(" ") and not cur_is_attr:
            # Toggle selection of highlighted code
            if codes and 0 <= STATE.dsd_code_cursor < n_c:
                code_id = codes[STATE.dsd_code_cursor][0]
                sel = STATE.dsd_selections.setdefault(cur_dim_id, set())
                if code_id in sel:
                    sel.discard(code_id)
                else:
                    sel.add(code_id)
                _dsd_apply_selections()
                # Advance cursor to next code for quick multi-select
                if STATE.dsd_code_cursor < n_c - 1:
                    STATE.dsd_code_cursor += 1
        elif ch in (ord("a"), ord("A")) and not cur_is_attr:
            # Reset this dimension to All (clear selection)
            STATE.dsd_selections[cur_dim_id] = set()
            _dsd_apply_selections()
        elif ch in (ord("c"), ord("C")):
            # Legacy: copy highlighted code id to dim_key as before
            if codes and 0 <= STATE.dsd_code_cursor < n_c:
                code_id = codes[STATE.dsd_code_cursor][0]
                STATE.status_msg = (
                    f"Tip: use Space to toggle selections; "
                    f"key is now: {STATE.dim_key!r}"
                )



# ──────────────────────────────────────────────────────────────────────────────
# Table search
# ──────────────────────────────────────────────────────────────────────────────
WDS_FREQ_LABELS = {
    1: "Annual", 2: "Semi-annual", 4: "Quarterly", 6: "Bi-monthly",
    7: "Monthly every 4 wks", 9: "Every 2 months", 10: "Monthly",
    11: "Semi-monthly", 12: "Bi-weekly", 13: "Weekly", 14: "Daily",
    15: "Occasional", 16: "Infrequent",
}


def _search_filtered() -> List[Dict]:
    """Return cube list entries matching the current search query (case-insensitive)."""
    items = STATE.cube_list
    q = STATE.search_query.strip().lower()
    if not q:
        return items
    parts = q.split()
    out = []
    for item in items:
        haystack = (
            str(item.get("productId", "")) + " " +
            item.get("cubeTitleEn", "").lower()
        )
        if all(p in haystack for p in parts):
            out.append(item)
    return out


def do_fetch_cube_list() -> None:
    STATE.cube_list_loading = True
    STATE.status_msg        = "Fetching table list from StatCan WDS …"
    try:
        raw             = SDMXClient.fetch_cube_list()
        data            = json.loads(raw)
        STATE.cube_list = data
        STATE.cube_list_loaded   = True
        STATE.status_msg = (
            f"Table list loaded: {len(data):,} tables — type to search"
        )
    except Exception as e:
        STATE.status_msg = f"Table list fetch error: {e}"
    finally:
        STATE.cube_list_loading = False


def draw_search(win) -> None:
    h, w = win.getmaxyx()
    win.erase()
    draw_chrome(win)
    draw_box(win, 1, 1, h - 2, w - 2, "Table Search", cp(C_BORDER))

    # ── Search box ────────────────────────────────────────────────────────────
    sq_label = "Search: "
    safe_addstr(win, 3, 4, sq_label, cp(C_LABEL, bold=True))
    box_w = w - 4 - len(sq_label) - 6
    sq_disp = STATE.search_query[-box_w:] if len(STATE.search_query) > box_w else STATE.search_query
    safe_addstr(win, 3, 4 + len(sq_label),
                f"[ {sq_disp:<{box_w}} ]", cp(C_SELECTED))

    # Status / loader line
    results = _search_filtered()
    n_total = len(STATE.cube_list)

    if STATE.cube_list_loading:
        safe_addstr(win, 4, 4, "⏳  Downloading table list …", cp(C_WARN, bold=True))
    elif not STATE.cube_list_loaded:
        safe_addstr(win, 4, 4,
                    "Press r to load table list  (one-time download, cached 24 h)",
                    cp(C_MUTED))
    else:
        count_str = (f"{len(results):,} of {n_total:,} tables"
                     if STATE.search_query.strip() else
                     f"{n_total:,} tables  — type to filter")
        safe_addstr(win, 4, 4, count_str, cp(C_DIM2))

    # ── Column header ─────────────────────────────────────────────────────────
    hline(win, 5, 2, w - 4, cp(C_BORDER))
    id_w   = 12
    freq_w = 11
    date_w = 12
    title_w = max(10, w - id_w - freq_w - date_w - 8)
    safe_addstr(win, 6, 4,
                f"{'Product ID':<{id_w}} {'Title':<{title_w}} {'Freq':<{freq_w}} {'End date':<{date_w}}",
                cp(C_HEADER, bold=True))
    hline(win, 7, 2, w - 4, cp(C_BORDER))

    # ── Results list ──────────────────────────────────────────────────────────
    list_top    = 8
    list_height = h - list_top - 3
    n_results   = len(results)

    if n_results:
        STATE.search_cursor = max(0, min(STATE.search_cursor, n_results - 1))
        if STATE.search_cursor < STATE.search_offset:
            STATE.search_offset = STATE.search_cursor
        elif STATE.search_cursor >= STATE.search_offset + list_height:
            STATE.search_offset = STATE.search_cursor - list_height + 1

    for i in range(list_height):
        ri = i + STATE.search_offset
        if ri >= n_results:
            break
        item      = results[ri]
        pid       = str(item.get("productId", ""))
        title     = item.get("cubeTitleEn", "")
        freq_code = item.get("frequencyCode", 0)
        freq      = WDS_FREQ_LABELS.get(freq_code, str(freq_code))
        end_date  = str(item.get("cubeEndDate", ""))[:10]
        archived  = str(item.get("archived", "0")) != "0"
        is_cursor = (ri == STATE.search_cursor)

        if is_cursor:
            row_attr  = cp(C_SELECTED)
            id_attr   = cp(C_SELECTED)
            date_attr = cp(C_SELECTED)
        elif archived:
            row_attr  = cp(C_MUTED)
            id_attr   = cp(C_MUTED)
            date_attr = cp(C_MUTED)
        else:
            row_attr  = cp(C_NORMAL)
            id_attr   = cp(C_ACCENT)
            date_attr = cp(C_TIME)

        title_trunc = title[:title_w]
        freq_trunc  = freq[:freq_w]
        line = f"{pid:<{id_w}} {title_trunc:<{title_w}} {freq_trunc:<{freq_w}} {end_date:<{date_w}}"
        safe_addstr(win, list_top + i, 4, line[:w - 6].ljust(w - 6), row_attr)
        if not is_cursor:
            safe_addstr(win, list_top + i, 4, f"{pid:<{id_w}}", id_attr)
            safe_addstr(win, list_top + i, 4 + id_w + 1 + title_w + 1 + freq_w + 1,
                        f"{end_date:<{date_w}}", date_attr)

    draw_scrollbar(win, list_top, list_height, w - 3, n_results, STATE.search_offset)

    # ── Hint bar ──────────────────────────────────────────────────────────────
    hline(win, h - 2, 1, w - 2, cp(C_BORDER))
    if not STATE.cube_list_loaded:
        hints = "r  load table list   q quit"
    else:
        hints = "type to filter   ↑↓ navigate   Enter select → query form   Backspace erase   r reload"
    safe_addstr(win, h - 2, 3, hints, cp(C_MUTED))


def handle_search(ch: int) -> None:
    """Handle keypresses on the search screen."""
    results = _search_filtered()
    n = len(results)

    if ch in (ord("r"), ord("R")):
        if not STATE.cube_list_loading:
            # Force a fresh fetch by clearing the cache entry, then reload
            _bg(do_fetch_cube_list)
        return

    if ch in (curses.KEY_UP, ord("k")):
        STATE.search_cursor = max(0, STATE.search_cursor - 1)
    elif ch in (curses.KEY_DOWN, ord("j")):
        STATE.search_cursor = min(n - 1, STATE.search_cursor + 1) if n else 0
    elif ch == curses.KEY_PPAGE:
        STATE.search_cursor = max(0, STATE.search_cursor - 10)
    elif ch == curses.KEY_NPAGE:
        STATE.search_cursor = min(n - 1, STATE.search_cursor + 10) if n else 0
    elif ch in (curses.KEY_ENTER, 10, 13):
        if results and 0 <= STATE.search_cursor < n:
            item = results[STATE.search_cursor]
            pid  = str(item.get("productId", ""))
            # Populate query form and switch to it
            STATE.dataflow_id  = pid
            STATE.query_type   = "cube"
            STATE.dim_key      = ""
            STATE.dsd          = None
            STATE.dsd_selections = {}
            STATE.status_msg   = (
                f"Selected table {pid}: {item.get('cubeTitleEn', '')[:60]}"
            )
            STATE.mode = "query"
    elif ch in (curses.KEY_BACKSPACE, 127, 8):
        STATE.search_query  = STATE.search_query[:-1]
        STATE.search_cursor = 0
        STATE.search_offset = 0
    elif 32 <= ch < 127:
        # Auto-load list on first keystroke if not yet loaded
        if not STATE.cube_list_loaded and not STATE.cube_list_loading:
            _bg(do_fetch_cube_list)
        STATE.search_query  += chr(ch)
        STATE.search_cursor  = 0
        STATE.search_offset  = 0

# ──────────────────────────────────────────────────────────────────────────────
# Master input dispatcher
# ──────────────────────────────────────────────────────────────────────────────
def handle_input(ch: int) -> bool:
    """
    Returns False to quit the application.

    Navigation: digit keys 1-6 (active outside text-edit modes)
      1 → help    2 → query    3 → results    4 → dsd    5 → cache    6 → search
    """
    # ── Digit navigation (1-5) — active outside text-edit modes ─────────────────
    in_text_mode = (STATE.editing
                    or STATE.filter_mode
                    or STATE.dsd_filter_mode)
    if not in_text_mode:
        nav = {ord("1"): "help", ord("2"): "query", ord("3"): "results",
               ord("4"): "dsd",  ord("5"): "cache", ord("6"): "search"}
        if ch in nav:
            dest = nav[ch]
            if dest == "dsd" and STATE.dsd is None:
                STATE.status_msg = (
                    "No DSD loaded — go to query form (2) and press d"
                )
            else:
                # When leaving DSD browser to query form, sync key from selections
                if STATE.mode == "dsd" and dest == "query" and STATE.dsd:
                    _dsd_apply_selections()
                STATE.mode = dest
            return True

    # Bare ESC → cancel editing if active, otherwise ignored
    if ch == 27:
        if STATE.editing:
            STATE.editing = False
        elif STATE.filter_mode:
            STATE.filter_text = ""
            STATE.filter_mode = False
            apply_filter()
        elif STATE.dsd_filter_mode:
            STATE.dsd_filter      = ""
            STATE.dsd_filter_mode = False
        return True

    # ── Global quit ───────────────────────────────────────────────────────────
    if ch in (ord("q"), ord("Q")) and not in_text_mode \
            and STATE.mode not in ("help", "search"):
        return False

    # ── Mode dispatch ─────────────────────────────────────────────────────────
    mode = STATE.mode
    if mode == "query":
        handle_query(ch)
    elif mode == "results":
        handle_results(ch)
    elif mode == "series":
        if ch in (curses.KEY_BACKSPACE, 127, 8):
            STATE.mode = "results"
    elif mode == "dsd":
        if STATE.dsd:
            handle_dsd(ch)
    elif mode == "help":
        STATE.mode = "query"   # any key returns
    elif mode == "cache":
        if ch in (ord("c"), ord("C")):
            n = Cache.clear()
            STATE.status_msg = f"Cache cleared — {n} entries removed"
    elif mode == "search":
        handle_search(ch)

    return True


# ──────────────────────────────────────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────────────────────────────────────
def main(stdscr) -> None:
    init_colors()
    curses.curs_set(0)
    stdscr.timeout(150)   # ms — non-blocking for loading spinner refresh

    draw_fns = {
        "query":   draw_query,
        "results": draw_results,
        "series":  draw_series_detail,
        "dsd":     draw_dsd,
        "help":    draw_help,
        "cache":   draw_cache,
        "search":  draw_search,
    }

    while True:
        stdscr.erase()
        draw_fns.get(STATE.mode, draw_query)(stdscr)
        stdscr.refresh()

        ch = stdscr.getch()
        if ch == -1:
            continue   # timeout tick
        if not handle_input(ch):
            break


def run() -> None:
    print("StatCan SDMX Explorer")
    print(f"Cache directory: {CACHE_DIR}")
    print()
    print("Navigation — press a number key from anywhere:")
    print("  1  Help    2  Query    3  Results    4  DSD")
    print("  5  Cache   6  Search   q  Quit")
    print()
    print("Press Ctrl+C to force-exit.\n")
    try:
        curses.wrapper(main)
    except KeyboardInterrupt:
        pass
    print("\nGoodbye!")


if __name__ == "__main__":
    run()
