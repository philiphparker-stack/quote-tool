import os
import json
import base64
import re
import uuid
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import List, Dict, Any, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from reportlab.lib import colors

from PIL import Image

# ============================================================
# Paths
# ============================================================
APP_ROOT = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(APP_ROOT, ".."))
WEB_ROOT = os.path.join(PROJECT_ROOT, "web")

ITEMS_JSON = os.path.join(WEB_ROOT, "items.json")
IMAGES_DIR = os.path.join(WEB_ROOT, "assets", "items")

EMSER_LOGO_CANDIDATES = [
    os.path.join(WEB_ROOT, "assets", "emserlogo.png"),
    os.path.join(WEB_ROOT, "assets", "items", "emserlogo.png"),
]

COMING_SOON_IMAGE = os.path.join(IMAGES_DIR, "comingsoon.png")

SAVED_QUOTES_DIR = os.path.join(PROJECT_ROOT, "data", "saved_quotes")
SAVED_QUOTE_RETENTION_DAYS = 45

# ============================================================
# Security
# ============================================================
APP_PASSWORD = os.getenv("QUOTE_TOOL_PASSWORD", "sterlina")

# ============================================================
# App + CORS
# ============================================================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs(SAVED_QUOTES_DIR, exist_ok=True)

# ============================================================
# Request models
# ============================================================
class CustomItemReq(BaseModel):
    name: str
    manufacturer: Optional[str] = None
    category: Optional[str] = None
    id: Optional[str] = None
    uom: Optional[str] = "ea"
    price: Optional[float] = None


class SelectedItemReq(BaseModel):
    id: str
    pricing_mode: Optional[str] = "oow"  # oow / direct / both
    oow_price: Optional[Any] = None
    direct_price: Optional[Any] = None
    variant_size: Optional[str] = None
    variant_sku: Optional[str] = None


class GenerateReq(BaseModel):
    program: str = "TEST"
    customer_name: Optional[str] = None
    effective_date: Optional[str] = None
    layout_mode: Optional[str] = "grid"
    categorize_by: Optional[str] = "category"

    # legacy support
    price_mode: Optional[str] = "oow"
    item_ids: List[str] = []
    price_overrides: Optional[Dict[str, Any]] = None

    # new per-item support
    items: Optional[List[SelectedItemReq]] = None

    custom_items: Optional[List[CustomItemReq]] = None
    customer_logo_data: Optional[str] = None


class SaveQuoteReq(BaseModel):
    quote_id: Optional[str] = None
    quote_name: str
    program: str = "TEST"
    customer_name: Optional[str] = None
    effective_date: Optional[str] = None
    layout_mode: Optional[str] = "grid"
    categorize_by: Optional[str] = "category"
    items: Optional[List[SelectedItemReq]] = None
    custom_items: Optional[List[CustomItemReq]] = None
    customer_logo_data: Optional[str] = None


# ============================================================
# Helpers: auth
# ============================================================
def norm(s: Any) -> str:
    return str(s).strip() if s is not None else ""


def require_password(request: Request):
    supplied = norm(request.headers.get("X-Quote-Password"))
    if supplied != APP_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ============================================================
# Helpers: data loading
# ============================================================
def load_items_list() -> List[Dict[str, Any]]:
    if not os.path.exists(ITEMS_JSON):
        return []
    try:
        with open(ITEMS_JSON, "r", encoding="utf-8") as f:
            items = json.load(f)
        return items if isinstance(items, list) else []
    except Exception:
        return []


def load_items_map() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for it in load_items_list():
        item_id = norm(it.get("id"))
        if item_id and item_id not in out:
            out[item_id] = it
    return out


def build_filters(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    mfr_set = set()
    cat_set = set()
    cats_by_mfr: Dict[str, set] = {}

    for it in items:
        mfr = norm(it.get("manufacturer") or it.get("mfr") or it.get("brand"))
        cat = norm(it.get("category"))
        if mfr:
            mfr_set.add(mfr)
            cats_by_mfr.setdefault(mfr, set())
            if cat:
                cats_by_mfr[mfr].add(cat)
        if cat:
            cat_set.add(cat)

    manufacturers = sorted(mfr_set, key=str.lower)
    categories = sorted(cat_set, key=str.lower)
    categories_by_manufacturer = {
        m: sorted(list(cats_by_mfr.get(m, set())), key=str.lower)
        for m in manufacturers
    }

    return {
        "manufacturers": manufacturers,
        "categories": categories,
        "categories_by_manufacturer": categories_by_manufacturer,
    }


# ============================================================
# Search helpers
# ============================================================
def normalize_search_text(s: Any) -> str:
    s = norm(s).lower()
    s = s.replace('"', " ")
    s = s.replace("'", " ")
    s = s.replace("/", " ")
    s = s.replace("-", " ")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def tokenize_search_text(s: Any) -> List[str]:
    s = normalize_search_text(s)
    return [tok for tok in s.split(" ") if tok]


def item_search_blob(it: Dict[str, Any]) -> str:
    parts = [
        it.get("name", ""),
        it.get("id", ""),
        it.get("manufacturer", ""),
        it.get("category", ""),
        it.get("search_terms", ""),
        it.get("aliases", ""),
    ]
    return normalize_search_text(" ".join([norm(p) for p in parts]))


def query_matches_item(q: str, it: Dict[str, Any]) -> bool:
    if not q:
        return True

    q_norm = normalize_search_text(q)
    q_tokens = tokenize_search_text(q_norm)
    blob = item_search_blob(it)

    if q_norm in blob:
        return True

    blob_tokens = set(tokenize_search_text(blob))
    if q_tokens and all(tok in blob_tokens for tok in q_tokens):
        return True

    for tok in q_tokens:
        if not any(bt.startswith(tok) or tok in bt for bt in blob_tokens):
            return False
    return True


def filter_items(
    items: List[Dict[str, Any]],
    manufacturer: str = "",
    category: str = "",
    q: str = "",
    limit: int = 300,
) -> List[Dict[str, Any]]:
    manufacturer = norm(manufacturer)
    category = norm(category)
    q = norm(q)

    out = []
    for it in items:
        mfr = norm(it.get("manufacturer") or it.get("mfr") or it.get("brand"))
        cat = norm(it.get("category"))
        name = norm(it.get("name"))
        sku = norm(it.get("id") or it.get("sku"))

        if manufacturer and mfr.lower() != manufacturer.lower():
            continue
        if category and cat.lower() != category.lower():
            continue
        if q and not query_matches_item(q, it):
            continue

        out.append({
            "id": sku,
            "name": name,
            "manufacturer": mfr,
            "category": cat,
            "uom": norm(it.get("uom") or "ea"),
            "price_direct": it.get("price_direct"),
            "price_oow": it.get("price_oow"),
            "image": norm(it.get("image")),
            "aliases": norm(it.get("aliases")),
            "search_terms": norm(it.get("search_terms")),
            "has_variants": bool(it.get("has_variants")),
            "variants": it.get("variants") if isinstance(it.get("variants"), list) else [],
        })

        if len(out) >= limit:
            break

    return out


# ============================================================
# Category labels / order
# ============================================================
CATEGORY_LABELS = {
    "THINSET": "THINSETS",
    "GROUT": "GROUTS",
    "CAULK": "CAULK",
    "ADHESIVE": "ADHESIVES",
    "BACKERBOARD": "BACKERBOARD",
    "TRIM": "TRIMS / METALS",
    "DRAINS": "DRAINS",
    "MASTIC": "MASTICS",
}

CATEGORY_ORDER = {
    "THINSET": 10,
    "GROUT": 20,
    "CAULK": 30,
    "TRIM": 40,
}


def pretty_category(cat: str) -> str:
    cat = norm(cat).upper()
    return CATEGORY_LABELS.get(cat, cat)


def category_sort_key(cat: str) -> int:
    cat = norm(cat).upper()
    return CATEGORY_ORDER.get(cat, 999)


MANUFACTURER_DISPLAY_NAMES = {
    "CUSTOM BUILDING PRODUCTS": "Custom",
    "CUSTOM BUILDING PRODUCTS, INC.": "Custom",
}


def pretty_manufacturer(name: str) -> str:
    raw = norm(name)
    if not raw:
        return ""
    return MANUFACTURER_DISPLAY_NAMES.get(raw.upper(), raw)


# ============================================================
# Price helpers
# ============================================================
def try_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s.replace("$", "").replace(",", ""))
    except Exception:
        return None


def normalize_pricing_mode(value: Any) -> str:
    mode = norm(value).lower()
    return mode if mode in {"oow", "direct", "both"} else "oow"


def validate_selected_item_pricing(items: Optional[List[SelectedItemReq]]) -> None:
    missing: List[str] = []

    for selected in (items or []):
        pricing_mode = normalize_pricing_mode(selected.pricing_mode)
        item_label = norm(selected.variant_sku) or norm(selected.id) or "Selected item"

        if pricing_mode in {"oow", "both"} and try_float(selected.oow_price) is None:
            missing.append(f"{item_label}: Warehouse price is required")
            continue

        if pricing_mode in {"direct", "both"} and try_float(selected.direct_price) is None:
            missing.append(f"{item_label}: Direct price is required")

    if missing:
        preview = "; ".join(missing[:8])
        if len(missing) > 8:
            preview += f"; and {len(missing) - 8} more"
        raise HTTPException(
            status_code=400,
            detail=f"Every selected catalog item must have a typed price. {preview}.",
        )


def get_numeric_price(it: Dict[str, Any], mode: str) -> Optional[float]:
    raw = it.get("price_direct") if mode == "direct" else it.get("price_oow")
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except Exception:
        return None


def fmt_single_price(label: str, value: Optional[float], uom: str) -> str:
    if value is None:
        return ""
    return f"{label}: ${value:.2f}/{uom}"


def get_price_lines(it: Dict[str, Any], legacy_mode: str) -> List[str]:
    uom = norm(it.get("uom") or "ea")

    pricing_mode = normalize_pricing_mode(it.get("_pricing_mode"))
    if norm(it.get("_pricing_mode")):
        oow_price = try_float(it.get("_override_oow_price"))
        direct_price = try_float(it.get("_override_direct_price"))

        lines: List[str] = []
        if pricing_mode in {"oow", "both"}:
            txt = fmt_single_price("Warehouse", oow_price, uom)
            if txt:
                lines.append(txt)
        if pricing_mode in {"direct", "both"}:
            txt = fmt_single_price("Direct", direct_price, uom)
            if txt:
                lines.append(txt)
        return lines

    price_val = get_numeric_price(it, legacy_mode)
    if price_val is None:
        return []

    label = "Direct" if legacy_mode == "direct" else "Warehouse"
    return [fmt_single_price(label, price_val, uom)]


# ============================================================
# Saved quotes helpers
# ============================================================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().replace(microsecond=0).isoformat()


def parse_iso_datetime(value: Any) -> Optional[datetime]:
    value = norm(value)
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def safe_quote_id(raw: str) -> str:
    raw = norm(raw)
    if not raw:
        return ""
    if not re.fullmatch(r"[A-Za-z0-9_-]+", raw):
        return ""
    return raw


def saved_quote_path(quote_id: str) -> str:
    return os.path.join(SAVED_QUOTES_DIR, f"{quote_id}.json")


def purge_expired_saved_quotes():
    cutoff = utc_now() - timedelta(days=SAVED_QUOTE_RETENTION_DAYS)

    if not os.path.exists(SAVED_QUOTES_DIR):
        return

    for name in os.listdir(SAVED_QUOTES_DIR):
        if not name.lower().endswith(".json"):
            continue

        path = os.path.join(SAVED_QUOTES_DIR, name)
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)

            dt = (
                parse_iso_datetime(payload.get("updated_at"))
                or parse_iso_datetime(payload.get("created_at"))
            )

            if dt is None:
                try:
                    stat = os.stat(path)
                    dt = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                except Exception:
                    dt = None

            if dt is not None and dt < cutoff:
                os.remove(path)
        except Exception:
            continue


def build_display_items_for_saved_quote(
    selected_items: List[SelectedItemReq],
    items_map: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()

    for selected in selected_items:
        item_id = norm(selected.id)
        base = items_map.get(item_id)
        if not base:
            continue
        if item_id in seen_ids:
            continue
        seen_ids.add(item_id)

        out.append({
            "id": norm(base.get("id")),
            "name": norm(base.get("name")),
            "manufacturer": norm(base.get("manufacturer") or base.get("mfr") or base.get("brand")),
            "category": norm(base.get("category")),
            "uom": norm(base.get("uom") or "ea"),
            "price_direct": base.get("price_direct"),
            "price_oow": base.get("price_oow"),
            "image": norm(base.get("image")),
            "aliases": norm(base.get("aliases")),
            "search_terms": norm(base.get("search_terms")),
            "has_variants": bool(base.get("has_variants")),
            "variants": base.get("variants") if isinstance(base.get("variants"), list) else [],
        })

    return out


def serialize_selected_items(items: Optional[List[SelectedItemReq]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for selected in (items or []):
        pricing_mode = normalize_pricing_mode(selected.pricing_mode)

        out.append({
            "id": norm(selected.id),
            "pricing_mode": pricing_mode,
            "oow_price": selected.oow_price,
            "direct_price": selected.direct_price,
            "variant_size": norm(selected.variant_size),
            "variant_sku": norm(selected.variant_sku),
        })
    return out


def serialize_custom_items(items: Optional[List[CustomItemReq]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for custom in (items or []):
        out.append({
            "name": norm(custom.name),
            "manufacturer": norm(custom.manufacturer),
            "category": norm(custom.category),
            "id": norm(custom.id),
            "uom": norm(custom.uom) or "ea",
            "price": custom.price,
        })
    return out



def build_saved_quote_searchable_text(summary: Dict[str, Any]) -> str:
    effective_date = norm(summary.get("effective_date"))
    date_parts: List[str] = [effective_date]

    if effective_date:
        try:
            parsed = datetime.strptime(effective_date, "%Y-%m-%d")
            date_parts.extend([
                parsed.strftime("%m/%d/%Y"),
                parsed.strftime("%m/%d/%y"),
                parsed.strftime("%-m/%-d/%Y") if os.name != "nt" else parsed.strftime("%#m/%#d/%Y"),
                parsed.strftime("%-m/%-d/%y") if os.name != "nt" else parsed.strftime("%#m/%#d/%y"),
                parsed.strftime("%B %d %Y"),
                parsed.strftime("%b %d %Y"),
                parsed.strftime("%B %Y"),
                parsed.strftime("%b %Y"),
            ])
        except Exception:
            pass

    return normalize_search_text(
        " ".join([
            summary.get("quote_id", ""),
            summary.get("quote_name", ""),
            summary.get("customer_name", ""),
            summary.get("program", ""),
            *date_parts,
        ])
    )


def saved_quote_matches_query(summary: Dict[str, Any], q: str) -> bool:
    q_norm = normalize_search_text(q or "")
    if not q_norm:
        return True

    searchable = build_saved_quote_searchable_text(summary)
    if not searchable:
        return False

    if q_norm in searchable:
        return True

    q_tokens = tokenize_search_text(q_norm)
    searchable_tokens = tokenize_search_text(searchable)

    if not q_tokens or not searchable_tokens:
        return False

    for token in q_tokens:
        if not any(
            token == searchable_token
            or searchable_token.startswith(token)
            or token in searchable_token
            for searchable_token in searchable_tokens
        ):
            return False

    return True


def saved_quote_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "quote_id": norm(payload.get("quote_id")),
        "quote_name": norm(payload.get("quote_name")),
        "customer_name": norm(payload.get("customer_name")),
        "program": norm(payload.get("program")),
        "effective_date": norm(payload.get("effective_date")),
        "layout_mode": norm(payload.get("layout_mode")) or "grid",
        "categorize_by": norm(payload.get("categorize_by")) or "category",
        "created_at": norm(payload.get("created_at")),
        "updated_at": norm(payload.get("updated_at")),
        "item_count": len(payload.get("items") or []) + len(payload.get("custom_items") or []),
    }


def load_saved_quote_payload(quote_id: str) -> Dict[str, Any]:
    quote_id = safe_quote_id(quote_id)
    if not quote_id:
        raise HTTPException(status_code=400, detail="Invalid saved quote ID.")

    path = saved_quote_path(quote_id)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Saved quote not found.")

    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            raise ValueError("Invalid saved quote format.")
        return payload
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Could not read saved quote.")


# ============================================================
# PDF image compression
# ============================================================
IMAGE_CACHE: Dict[str, ImageReader] = {}
LOGO_CACHE: Dict[str, ImageReader] = {}


def compress_pil_to_reader(img: Image.Image, max_px: int = 180, quality: int = 55) -> ImageReader:
    img = img.convert("RGBA")
    img.thumbnail((max_px, max_px))

    bg = Image.new("RGB", img.size, (255, 255, 255))
    bg.paste(img, mask=img.split()[-1])

    bio = BytesIO()
    bg.save(bio, format="JPEG", optimize=True, quality=quality)
    bio.seek(0)
    return ImageReader(bio)


def get_image_reader_from_path(path: str, max_px: int = 180, quality: int = 55) -> Optional[ImageReader]:
    if not path:
        return None

    key = f"{path}|{max_px}|{quality}"
    if key in IMAGE_CACHE:
        return IMAGE_CACHE[key]

    if not os.path.exists(path):
        return None

    try:
        img = Image.open(path)
        reader = compress_pil_to_reader(img, max_px=max_px, quality=quality)
        IMAGE_CACHE[key] = reader
        return reader
    except Exception:
        return None


def decode_logo_data(customer_logo_data: Optional[str]) -> Optional[ImageReader]:
    if not customer_logo_data:
        return None

    key = str(hash(customer_logo_data))
    if key in LOGO_CACHE:
        return LOGO_CACHE[key]

    try:
        data = customer_logo_data
        if "," in data:
            data = data.split(",", 1)[1]
        raw = base64.b64decode(data)
        img = Image.open(BytesIO(raw))
        reader = compress_pil_to_reader(img, max_px=360, quality=65)
        LOGO_CACHE[key] = reader
        return reader
    except Exception:
        return None


def get_emser_logo_reader() -> Optional[ImageReader]:
    for candidate in EMSER_LOGO_CANDIDATES:
        reader = get_image_reader_from_path(candidate, max_px=360, quality=65)
        if reader:
            return reader
    return None


def resolve_item_image_path(image_value: str) -> str:
    image_value = norm(image_value).replace("\\", "/").strip()
    if not image_value:
        return COMING_SOON_IMAGE
    candidate = os.path.join(IMAGES_DIR, image_value)
    if os.path.exists(candidate):
        return candidate
    return COMING_SOON_IMAGE if os.path.exists(COMING_SOON_IMAGE) else ""


# ============================================================
# Text fitting
# ============================================================
def safe_ellipsis_fit(
    c: canvas.Canvas,
    text: str,
    max_w: float,
    font_name: str,
    size: float,
) -> str:
    text = norm(text)
    if not text:
        return ""

    if c.stringWidth(text, font_name, size) <= max_w:
        return text

    words = text.split()
    if len(words) > 1:
        out = ""
        for word in words:
            trial = word if not out else out + " " + word
            if c.stringWidth(trial + "…", font_name, size) <= max_w:
                out = trial
            else:
                break
        if out:
            return out + "…"

    out = text
    while out and c.stringWidth(out + "…", font_name, size) > max_w:
        out = out[:-1]
    return (out + "…") if out else "…"


def fit_one_line(
    c: canvas.Canvas,
    text: str,
    max_w: float,
    font_name: str,
    start_size: float = 11.0,
    min_size: float = 6.0,
) -> Tuple[str, float]:
    text = norm(text)
    if not text:
        return "", start_size

    size = start_size
    while size >= min_size:
        if c.stringWidth(text, font_name, size) <= max_w:
            return text, size
        size -= 0.25

    return safe_ellipsis_fit(c, text, max_w, font_name, min_size), min_size


def fit_lines(
    c: canvas.Canvas,
    text: str,
    max_w: float,
    font_name: str,
    max_lines: int,
    start_size: float,
    min_size: float,
) -> Tuple[List[str], float]:
    words = norm(text).split()
    if not words:
        return [], start_size

    size = start_size
    while size >= min_size:
        lines: List[str] = []
        current = ""

        for w in words:
            trial = w if not current else current + " " + w
            if c.stringWidth(trial, font_name, size) <= max_w:
                current = trial
            else:
                if current:
                    lines.append(current)
                    current = w
                else:
                    current = w

        if current:
            lines.append(current)

        if len(lines) <= max_lines and all(c.stringWidth(line, font_name, size) <= max_w for line in lines):
            return lines, size

        size -= 0.25

    size = min_size
    lines = []
    current = ""

    for idx, w in enumerate(words):
        trial = w if not current else current + " " + w
        if c.stringWidth(trial, font_name, size) <= max_w:
            current = trial
        else:
            lines.append(current)
            current = w
            if len(lines) == max_lines - 1:
                rest = " ".join(words[idx:])
                current = safe_ellipsis_fit(c, rest, max_w, font_name, size)
                lines.append(current)
                return lines, size

    if current:
        lines.append(current)

    if len(lines) > max_lines:
        lines = lines[:max_lines]
        lines[-1] = safe_ellipsis_fit(c, lines[-1], max_w, font_name, size)

    return lines, size


# ============================================================
# PDF layout helpers
# ============================================================
BRAND_BLUE = colors.HexColor("#0F3A6D")
BRAND_BLUE_DARK = colors.HexColor("#0A2D55")
BRAND_GOLD = colors.HexColor("#BC8644")
SOFT_TEXT = colors.HexColor("#5F6B7A")


def draw_placeholder_image(c: canvas.Canvas, x: float, y: float, w: float, h: float):
    c.setStrokeColor(colors.Color(0, 0, 0, alpha=0.18))
    c.setFillColor(colors.Color(0, 0, 0, alpha=0.04))
    c.roundRect(x, y, w, h, 5, stroke=1, fill=1)
    c.setStrokeColor(colors.Color(0, 0, 0, alpha=0.25))
    c.line(x + 2, y + 2, x + w - 2, y + h - 2)
    c.line(x + 2, y + h - 2, x + w - 2, y + 2)


def draw_page_footer(c: canvas.Canvas, W: float, page_num: int):
    footer_y = 20
    c.setStrokeColor(colors.Color(0, 0, 0, alpha=0.14))
    c.setLineWidth(0.6)
    c.line(30, footer_y + 10, W - 30, footer_y + 10)

    c.setFillColor(SOFT_TEXT)
    c.setFont("Helvetica", 8.5)
    c.drawString(30, footer_y, "Emser Tile  |  www.emser.com  |  For ordering contact your Emser representative")
    c.drawRightString(W - 30, footer_y, f"Page {page_num}")


def draw_header(
    c: canvas.Canvas,
    W: float,
    H: float,
    program: str,
    effective_date: str,
    customer_name: str,
    customer_logo_reader: Optional[ImageReader] = None,
):
    c.setFillColor(BRAND_BLUE)
    c.rect(0, H - 20, W, 20, stroke=0, fill=1)

    emser_reader = get_emser_logo_reader()
    if emser_reader:
        try:
            c.drawImage(
                emser_reader,
                30,
                H - 74,
                width=150,
                height=44,
                preserveAspectRatio=True,
                mask="auto",
            )
        except Exception:
            pass

    if customer_logo_reader:
        try:
            c.drawImage(
                customer_logo_reader,
                W - 170,
                H - 72,
                width=138,
                height=42,
                preserveAspectRatio=True,
                mask="auto",
            )
        except Exception:
            pass

    c.setFillColor(BRAND_BLUE_DARK)
    c.setFont("Helvetica-Bold", 18)
    c.drawCentredString(W / 2, H - 38, "Product Pricing Summary")

    c.setFillColor(SOFT_TEXT)
    c.setFont("Helvetica-Bold", 10)
    c.drawCentredString(W / 2, H - 52, "EMSER TILE")

    left_x = 30
    right_x = W - 30
    row1_y = H - 96
    row2_y = H - 111

    c.setFont("Helvetica-Bold", 9.4)
    c.setFillColor(colors.black)
    c.drawString(left_x, row1_y, f"Customer: {customer_name or '—'}")

    c.drawRightString(right_x, row1_y, f"Effective Date: {effective_date or '—'}")

    divider_y = H - 132
    c.setStrokeColor(colors.Color(0, 0, 0, alpha=0.18))
    c.setLineWidth(0.9)
    c.line(30, divider_y, W - 30, divider_y)

    return divider_y


def draw_category_header(c: canvas.Canvas, x: float, y_top: float, width: float, label: str):
    header_h = 18
    c.setFillColor(BRAND_GOLD)
    c.setStrokeColor(BRAND_GOLD)
    c.roundRect(x, y_top - header_h, width, header_h, 6, stroke=1, fill=1)

    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 10)
    c.drawString(x + 10, y_top - 11.8, label)
    return header_h


def draw_card(
    c: canvas.Canvas,
    x: float,
    y_top: float,
    card_w: float,
    card_h: float,
    it: Dict[str, Any],
    fallback_mode: str,
):
    c.setFillColor(colors.Color(0, 0, 0, alpha=0.05))
    c.roundRect(x + 1.6, y_top - card_h - 1.6, card_w, card_h, 9, stroke=0, fill=1)

    c.setStrokeColor(colors.Color(0, 0, 0, alpha=0.16))
    c.setFillColor(colors.white)
    c.roundRect(x, y_top - card_h, card_w, card_h, 9, stroke=1, fill=1)

    pad = 8
    inner_x = x + pad
    inner_w = card_w - (pad * 2)

    title_lines, title_size = fit_lines(
        c,
        norm(it.get("name")),
        inner_w,
        "Helvetica-Bold",
        max_lines=3,
        start_size=7.2,
        min_size=5.5,
    )

    title_y = y_top - 14
    c.setFillColor(BRAND_BLUE_DARK)
    c.setFont("Helvetica-Bold", title_size)
    for i, line in enumerate(title_lines[:3]):
        c.drawString(inner_x, title_y - (i * (title_size + 1.2)), line)

    title_block_h = max(1, len(title_lines)) * (title_size + 1.2)
    price_start_y = title_y - title_block_h - 6

    price_lines = get_price_lines(it, fallback_mode)
    price_size = 8.4 if len(price_lines) > 1 else 10.4
    price_gap = price_size + 1.4

    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", price_size)
    for idx, line in enumerate(price_lines[:2]):
        c.drawString(inner_x, price_start_y - (idx * price_gap), line)

    img_size = 36
    img_x = inner_x
    extra_price_space = max(0, (len(price_lines) - 1) * price_gap)
    img_y = y_top - card_h + 16 - extra_price_space

    img_path = resolve_item_image_path(norm(it.get("image")))
    img_reader = get_image_reader_from_path(img_path, max_px=180, quality=55) if img_path else None

    if img_reader:
        try:
            c.drawImage(
                img_reader,
                img_x,
                img_y,
                width=img_size,
                height=img_size,
                preserveAspectRatio=True,
                mask="auto",
                anchor="sw",
            )
        except Exception:
            draw_placeholder_image(c, img_x, img_y, img_size, img_size)
    else:
        draw_placeholder_image(c, img_x, img_y, img_size, img_size)

    meta_x = img_x + img_size + 6
    meta_w = card_w - (meta_x - x) - pad

    mfr_lines, mfr_size = fit_lines(
        c,
        pretty_manufacturer(norm(it.get("manufacturer"))),
        meta_w,
        "Helvetica",
        max_lines=2,
        start_size=5.4,
        min_size=4.6,
    )

    line_gap = 0.8
    mfr_start_y = img_y + 18

    c.setFillColor(SOFT_TEXT)
    c.setFont("Helvetica", mfr_size)
    for i, line in enumerate(mfr_lines[:2]):
        c.drawString(meta_x, mfr_start_y - (i * (mfr_size + line_gap)), line)

    sku_txt, sku_size = fit_one_line(
        c,
        norm(it.get("id")),
        meta_w,
        "Helvetica",
        start_size=5.2,
        min_size=4.5,
    )
    c.setFillColor(colors.Color(0, 0, 0, alpha=0.58))
    c.setFont("Helvetica", sku_size)

    sku_y = mfr_start_y - ((len(mfr_lines) - 1) * (mfr_size + line_gap)) - mfr_size - 0.4
    c.drawString(meta_x, sku_y, sku_txt)


def draw_compact_row(
    c: canvas.Canvas,
    x: float,
    y_top: float,
    width: float,
    row_h: float,
    it: Dict[str, Any],
    fallback_mode: str,
    categorize_by: str = "category",
):
    c.setStrokeColor(colors.Color(0, 0, 0, alpha=0.12))
    c.setFillColor(colors.white)
    c.rect(x, y_top - row_h, width, row_h, stroke=1, fill=1)

    img_x = x + 8
    img_w = 26
    img_h = 26
    img_y = y_top - ((row_h + img_h) / 2)

    img_path = resolve_item_image_path(norm(it.get("image")))
    img_reader = get_image_reader_from_path(img_path, max_px=120, quality=55) if img_path else None

    if img_reader:
        try:
            c.drawImage(
                img_reader,
                img_x,
                img_y,
                width=img_w,
                height=img_h,
                preserveAspectRatio=True,
                mask="auto",
                anchor="sw",
            )
        except Exception:
            draw_placeholder_image(c, img_x, img_y, img_w, img_h)
    else:
        draw_placeholder_image(c, img_x, img_y, img_w, img_h)

    name_x = x + 44
    sku_x = x + 290
    detail_x = x + 360
    price_x = x + 455

    text_y = y_top - 21

    c.setFillColor(BRAND_BLUE_DARK)
    name_txt, name_size = fit_one_line(c, norm(it.get("name")), 238, "Helvetica-Bold", 9.0, 6.0)
    c.setFont("Helvetica-Bold", name_size)
    c.drawString(name_x, text_y, name_txt)

    c.setFillColor(colors.Color(0, 0, 0, alpha=0.74))
    c.setFont("Helvetica", 7.3)
    sku_txt, _ = fit_one_line(c, norm(it.get("id")), 84, "Helvetica", 7.3, 6.0)
    c.drawString(sku_x, text_y, sku_txt)

    if norm(categorize_by).lower() == "manufacturer":
        detail_value = pretty_category(norm(it.get("category")) or "OTHER")
    else:
        detail_value = pretty_manufacturer(norm(it.get("manufacturer")))

    detail_txt, _ = fit_one_line(c, detail_value, 88, "Helvetica", 7.1, 5.8)
    c.drawString(detail_x, text_y, detail_txt)

    price_lines = get_price_lines(it, fallback_mode)
    c.setFont("Helvetica-Bold", 7.3)
    c.setFillColor(colors.black)
    price_y = text_y
    for line in price_lines[:2]:
        draw_txt, _ = fit_one_line(c, line, 125, "Helvetica-Bold", 7.1, 5.8)
        c.drawString(price_x, price_y, draw_txt)
        price_y -= 9


def group_items_for_pdf(
    items: List[Dict[str, Any]],
    categorize_by: str = "category",
) -> List[Tuple[str, List[Dict[str, Any]]]]:
    mode = norm(categorize_by).lower()

    if mode == "manufacturer":
        items_sorted = sorted(
            items,
            key=lambda it: (
                norm(it.get("manufacturer")).lower() or "zzzz",
                category_sort_key(it.get("category", "")),
                norm(it.get("category")).upper(),
                norm(it.get("name")).lower(),
            ),
        )

        grouped: Dict[str, List[Dict[str, Any]]] = {}
        labels: Dict[str, str] = {}
        for it in items_sorted:
            group_key = norm(it.get("manufacturer")).lower() or "other"
            grouped.setdefault(group_key, []).append(it)
            labels[group_key] = norm(it.get("manufacturer")) or "OTHER"

        ordered_keys = sorted(grouped.keys(), key=lambda k: labels.get(k, k).lower())
        return [(labels[key], grouped[key]) for key in ordered_keys]

    items_sorted = sorted(
        items,
        key=lambda it: (
            category_sort_key(it.get("category", "")),
            norm(it.get("category")).upper(),
            norm(it.get("manufacturer")).lower(),
            norm(it.get("name")).lower(),
        ),
    )

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for it in items_sorted:
        cat = norm(it.get("category")).upper() or "OTHER"
        grouped.setdefault(cat, []).append(it)

    ordered_cats = sorted(grouped.keys(), key=lambda c: (category_sort_key(c), c))
    return [(pretty_category(cat), grouped[cat]) for cat in ordered_cats]


def is_half_width_group(items: List[Dict[str, Any]]) -> bool:
    return len(items) <= 2


def estimate_half_width_section_height(card_h: float) -> float:
    header_h = 18
    return header_h + 8 + card_h


def draw_half_width_category_section(
    c: canvas.Canvas,
    x: float,
    y_top: float,
    section_w: float,
    group_label: str,
    cat_items: List[Dict[str, Any]],
    fallback_mode: str,
    card_w: float,
    card_h: float,
    gutter: float,
):
    header_h = draw_category_header(c, x, y_top, section_w, group_label)
    y = y_top - (header_h + 8)

    # Left-align cards inside the half-width section.
    start_x = x

    if len(cat_items) == 1:
        draw_card(c, start_x, y, card_w, card_h, cat_items[0], fallback_mode)
    else:
        draw_card(c, start_x, y, card_w, card_h, cat_items[0], fallback_mode)
        draw_card(c, start_x + card_w + gutter, y, card_w, card_h, cat_items[1], fallback_mode)

    return estimate_half_width_section_height(card_h)


def build_pdf_grid(
    items: List[Dict[str, Any]],
    fallback_mode: str,
    customer_logo_data: Optional[str] = None,
    program: str = "TEST",
    customer_name: str = "",
    effective_date: str = "",
    categorize_by: str = "category",
) -> bytes:
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=letter, pageCompression=1)
    W, H = letter

    cols = 4
    left = 30
    right = 30
    bottom = 34
    gutter = 8
    section_gap = 12
    row_gap = 9

    usable_w = W - left - right
    card_w = (usable_w - gutter * (cols - 1)) / cols
    card_h = 112

    half_section_w = (usable_w - section_gap) / 2
    half_section_inner_gutter = max(4, half_section_w - (card_w * 2))

    customer_logo_reader = decode_logo_data(customer_logo_data)
    page_num = 1

    header_divider_y = draw_header(
        c, W, H,
        program=program,
        effective_date=effective_date,
        customer_name=customer_name,
        customer_logo_reader=customer_logo_reader,
    )
    y = header_divider_y - 10

    groups = group_items_for_pdf(items, categorize_by=categorize_by)
    min_full_section_space = 18 + 8 + card_h + row_gap
    min_half_section_space = estimate_half_width_section_height(card_h)

    def new_page():
        nonlocal y, page_num, header_divider_y
        draw_page_footer(c, W, page_num)
        c.showPage()
        page_num += 1
        header_divider_y = draw_header(
            c, W, H,
            program=program,
            effective_date=effective_date,
            customer_name=customer_name,
            customer_logo_reader=customer_logo_reader,
        )
        y = header_divider_y - 10

    i = 0
    while i < len(groups):
        group_label, cat_items = groups[i]

        # ----------------------------------------------------
        # Half-width categories: 1 or 2 items
        # These can pair side-by-side on the same row.
        # ----------------------------------------------------
        if is_half_width_group(cat_items):
            next_pair = None
            if i + 1 < len(groups):
                next_group_label, next_cat_items = groups[i + 1]
                if is_half_width_group(next_cat_items):
                    next_pair = (next_group_label, next_cat_items)

            needed_height = min_half_section_space

            if y - needed_height < bottom:
                new_page()

            used_left = draw_half_width_category_section(
                c=c,
                x=left,
                y_top=y,
                section_w=half_section_w,
                group_label=group_label,
                cat_items=cat_items,
                fallback_mode=fallback_mode,
                card_w=card_w,
                card_h=card_h,
                gutter=half_section_inner_gutter,
            )

            used_right = 0
            if next_pair:
                used_right = draw_half_width_category_section(
                    c=c,
                    x=left + half_section_w + section_gap,
                    y_top=y,
                    section_w=half_section_w,
                    group_label=next_pair[0],
                    cat_items=next_pair[1],
                    fallback_mode=fallback_mode,
                    card_w=card_w,
                    card_h=card_h,
                    gutter=half_section_inner_gutter,
                )

            y -= max(used_left, used_right if next_pair else 0) + 6

            if next_pair:
                i += 2
            else:
                i += 1
            continue

        # ----------------------------------------------------
        # Full-width categories: 3+ items
        # ----------------------------------------------------
        if y - min_full_section_space < bottom:
            new_page()

        label = group_label
        header_h = draw_category_header(c, left, y, usable_w, label)
        y -= (header_h + 8)

        x = left
        col = 0

        for it in cat_items:
            if col == 0 and (y - card_h) < bottom:
                new_page()
                if y - min_full_section_space < bottom:
                    new_page()
                header_h = draw_category_header(c, left, y, usable_w, label + " (cont.)")
                y -= (header_h + 8)

            draw_card(c, x, y, card_w, card_h, it, fallback_mode)

            col += 1
            if col == cols:
                col = 0
                x = left
                y -= (card_h + row_gap)
            else:
                x += (card_w + gutter)

        if col != 0:
            y -= (card_h + row_gap)
        else:
            y -= 4

        i += 1

    draw_page_footer(c, W, page_num)
    c.save()
    pdf = buf.getvalue()
    buf.close()
    return pdf


def build_pdf_compact(
    items: List[Dict[str, Any]],
    fallback_mode: str,
    customer_logo_data: Optional[str] = None,
    program: str = "TEST",
    customer_name: str = "",
    effective_date: str = "",
    categorize_by: str = "category",
) -> bytes:
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=letter, pageCompression=1)
    W, H = letter

    left = 30
    right = 30
    bottom = 34
    usable_w = W - left - right
    row_gap = 4
    row_h = 50
    section_header_h = 18
    section_header_gap = 8
    column_header_h = 10
    section_start_buffer = 6

    customer_logo_reader = decode_logo_data(customer_logo_data)
    page_num = 1

    header_divider_y = draw_header(
        c, W, H,
        program=program,
        effective_date=effective_date,
        customer_name=customer_name,
        customer_logo_reader=customer_logo_reader,
    )

    y = header_divider_y - 10
    groups = group_items_for_pdf(items, categorize_by=categorize_by)

    def new_page():
        nonlocal y, page_num, header_divider_y
        draw_page_footer(c, W, page_num)
        c.showPage()
        page_num += 1
        header_divider_y = draw_header(
            c, W, H,
            program=program,
            effective_date=effective_date,
            customer_name=customer_name,
            customer_logo_reader=customer_logo_reader,
        )
        y = header_divider_y - 10

    def draw_compact_column_headings(y_top: float) -> float:
        c.setFont("Helvetica-Bold", 7.2)
        c.setFillColor(SOFT_TEXT)
        c.drawString(left + 44, y_top - 2, "Description")
        c.drawString(left + 290, y_top - 2, "SKU")
        third_heading = "Category" if norm(categorize_by).lower() == "manufacturer" else "Manufacturer"
        c.drawString(left + 360, y_top - 2, third_heading)
        c.drawString(left + 455, y_top - 2, "Pricing")
        return y_top - column_header_h

    min_section_start_space = (
        section_header_h
        + section_header_gap
        + column_header_h
        + row_h
        + row_gap
        + section_start_buffer
    )

    def ensure_section_start_room():
        nonlocal y
        if y - min_section_start_space < bottom:
            new_page()

    for group_label, cat_items in groups:
        label = group_label

        ensure_section_start_room()

        draw_category_header(c, left, y, usable_w, label)
        y -= (section_header_h + section_header_gap)
        y = draw_compact_column_headings(y)

        for it in cat_items:
            if y - row_h < bottom:
                new_page()
                ensure_section_start_room()
                draw_category_header(c, left, y, usable_w, label + " (cont.)")
                y -= (section_header_h + section_header_gap)
                y = draw_compact_column_headings(y)

            draw_compact_row(c, left, y, usable_w, row_h, it, fallback_mode, categorize_by=categorize_by)
            y -= (row_h + row_gap)

        y -= 4

    draw_page_footer(c, W, page_num)
    c.save()
    pdf = buf.getvalue()
    buf.close()
    return pdf


# ============================================================
# Custom item helpers
# ============================================================
def build_custom_item(custom: CustomItemReq, index_num: int) -> Dict[str, Any]:
    name = norm(custom.name)
    if not name:
        raise HTTPException(status_code=400, detail="Custom item name is required.")

    custom_id = norm(custom.id) or f"CUSTOM-{index_num}"
    category = norm(custom.category).upper() or "OTHER"
    manufacturer = norm(custom.manufacturer) or "CUSTOM ITEM"
    uom = norm(custom.uom) or "ea"

    price_val: Optional[float] = None
    if custom.price is not None and str(custom.price).strip() != "":
        try:
            price_val = float(custom.price)
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid custom item price for {name}.")

    return {
        "id": custom_id,
        "name": name,
        "manufacturer": manufacturer,
        "category": category,
        "uom": uom,
        "price_direct": None,
        "price_oow": price_val,
        "image": "",
        "aliases": "",
        "search_terms": "",
        "_pricing_mode": "oow",
        "_override_oow_price": price_val,
        "_override_direct_price": None,
    }


# ============================================================
# Routes
# ============================================================
@app.get("/health")
def health():
    emser_logo_found = any(os.path.exists(p) for p in EMSER_LOGO_CANDIDATES)
    return {
        "ok": True,
        "items_json_path": ITEMS_JSON,
        "items_json_exists": os.path.exists(ITEMS_JSON),
        "images_dir_path": IMAGES_DIR,
        "images_dir_exists": os.path.exists(IMAGES_DIR),
        "emser_logo_candidates": EMSER_LOGO_CANDIDATES,
        "emser_logo_found": emser_logo_found,
        "coming_soon_exists": os.path.exists(COMING_SOON_IMAGE),
        "password_protected": True,
        "saved_quotes_dir": SAVED_QUOTES_DIR,
        "saved_quotes_dir_exists": os.path.exists(SAVED_QUOTES_DIR),
        "saved_quote_retention_days": SAVED_QUOTE_RETENTION_DAYS,
    }


@app.get("/filters")
def get_filters(request: Request, manufacturer: Optional[str] = Query(default=None)):
    require_password(request)

    items = load_items_list()
    if not items:
        return {"manufacturers": [], "categories": [], "categories_by_manufacturer": {}}

    filt = build_filters(items)

    if manufacturer:
        m = norm(manufacturer)
        return {
            "manufacturer": m,
            "categories": filt["categories_by_manufacturer"].get(m, [])
        }

    return filt


@app.get("/items")
def get_items(
    request: Request,
    manufacturer: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=300, ge=1, le=2000),
):
    require_password(request)

    items = load_items_list()
    if not items:
        return {"items": []}

    filtered = filter_items(
        items,
        manufacturer=manufacturer or "",
        category=category or "",
        q=q or "",
        limit=limit,
    )
    return {"items": filtered}


@app.get("/saved-quotes")
def list_saved_quotes(
    request: Request,
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
):
    require_password(request)
    purge_expired_saved_quotes()

    results: List[Dict[str, Any]] = []

    if not os.path.exists(SAVED_QUOTES_DIR):
        return {"quotes": []}

    for name in os.listdir(SAVED_QUOTES_DIR):
        if not name.lower().endswith(".json"):
            continue

        path = os.path.join(SAVED_QUOTES_DIR, name)
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if not isinstance(payload, dict):
                continue
        except Exception:
            continue

        summary = saved_quote_summary(payload)
        if q and not saved_quote_matches_query(summary, q):
            continue

        results.append(summary)

    results.sort(key=lambda x: norm(x.get("updated_at")), reverse=True)
    return {"quotes": results[:limit]}


@app.get("/saved-quotes/{quote_id}")
def get_saved_quote(request: Request, quote_id: str):
    require_password(request)
    purge_expired_saved_quotes()

    payload = load_saved_quote_payload(quote_id)
    return {"quote": payload}


@app.post("/saved-quotes")
def save_quote(request: Request, req: SaveQuoteReq):
    require_password(request)
    purge_expired_saved_quotes()

    quote_name = norm(req.quote_name)
    if not quote_name:
        raise HTTPException(status_code=400, detail="Quote name is required.")

    if not (req.items or req.custom_items):
        raise HTTPException(status_code=400, detail="Select at least one item or add a custom item before saving.")

    validate_selected_item_pricing(req.items)

    quote_id = safe_quote_id(req.quote_id or "") or uuid.uuid4().hex
    path = saved_quote_path(quote_id)

    now_iso = utc_now_iso()
    created_at = now_iso

    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f)
            created_at = norm(existing.get("created_at")) or now_iso
        except Exception:
            created_at = now_iso

    items_map = load_items_map()
    serialized_items = serialize_selected_items(req.items)
    serialized_custom_items = serialize_custom_items(req.custom_items)
    display_items = build_display_items_for_saved_quote(req.items or [], items_map)

    payload = {
        "quote_id": quote_id,
        "quote_name": quote_name,
        "customer_name": norm(req.customer_name),
        "program": norm(req.program) or "TEST",
        "effective_date": norm(req.effective_date),
        "layout_mode": norm(req.layout_mode).lower() if norm(req.layout_mode).lower() in {"grid", "compact"} else "grid",
        "categorize_by": norm(req.categorize_by).lower() if norm(req.categorize_by).lower() in {"category", "manufacturer"} else "category",
        "items": serialized_items,
        "custom_items": serialized_custom_items,
        "display_items": display_items,
        "customer_logo_data": req.customer_logo_data,
        "created_at": created_at,
        "updated_at": now_iso,
    }

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
    except Exception:
        raise HTTPException(status_code=500, detail="Could not save quote.")

    return {
        "ok": True,
        "quote": payload,
        "summary": saved_quote_summary(payload),
    }


@app.post("/generate")
def generate(request: Request, req: GenerateReq):
    require_password(request)

    validate_selected_item_pricing(req.items)

    items_map = load_items_map()
    picked: List[Dict[str, Any]] = []

    if req.items:
        for selected in req.items:
            item_id = norm(selected.id)
            if item_id not in items_map:
                continue

            base = dict(items_map[item_id])
            pricing_mode = normalize_pricing_mode(selected.pricing_mode)

            variant_sku = norm(selected.variant_sku)
            variant_size = norm(selected.variant_size)
            if variant_sku:
                base["id"] = variant_sku
            if variant_size:
                base["name"] = f'{norm(base.get("name"))} {variant_size}'.strip()

            base["_pricing_mode"] = pricing_mode
            base["_override_oow_price"] = try_float(selected.oow_price)
            base["_override_direct_price"] = try_float(selected.direct_price)
            picked.append(base)

    elif req.item_ids:
        legacy_mode = norm(req.price_mode).lower()
        if legacy_mode not in ("direct", "oow"):
            legacy_mode = "oow"

        for item_id in req.item_ids:
            if item_id in items_map:
                base = dict(items_map[item_id])
                base["_pricing_mode"] = legacy_mode
                override_raw = (req.price_overrides or {}).get(item_id)
                if legacy_mode == "oow":
                    base["_override_oow_price"] = try_float(override_raw)
                    base["_override_direct_price"] = None
                else:
                    base["_override_direct_price"] = try_float(override_raw)
                    base["_override_oow_price"] = None
                picked.append(base)

    custom_items = req.custom_items or []
    for idx, custom in enumerate(custom_items, start=1):
        picked.append(build_custom_item(custom, idx))

    if not picked:
        raise HTTPException(status_code=400, detail="No valid selected items found.")

    fallback_mode = norm(req.price_mode).lower()
    if fallback_mode not in ("direct", "oow"):
        fallback_mode = "oow"

    layout_mode = norm(req.layout_mode).lower()
    if layout_mode not in {"grid", "compact"}:
        layout_mode = "grid"

    categorize_by = norm(req.categorize_by).lower()
    if categorize_by not in {"category", "manufacturer"}:
        categorize_by = "category"

    if layout_mode == "compact":
        pdf_bytes = build_pdf_compact(
            items=picked,
            fallback_mode=fallback_mode,
            customer_logo_data=req.customer_logo_data,
            program=norm(req.program) or "TEST",
            customer_name=norm(req.customer_name),
            effective_date=norm(req.effective_date),
            categorize_by=categorize_by,
        )
    else:
        pdf_bytes = build_pdf_grid(
            items=picked,
            fallback_mode=fallback_mode,
            customer_logo_data=req.customer_logo_data,
            program=norm(req.program) or "TEST",
            customer_name=norm(req.customer_name),
            effective_date=norm(req.effective_date),
            categorize_by=categorize_by,
        )

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="quote.pdf"'},
    )


# ============================================================
# Serve frontend
# ============================================================
app.mount("/assets", StaticFiles(directory=os.path.join(WEB_ROOT, "assets")), name="assets")


@app.get("/")
def serve_frontend():
    return FileResponse(os.path.join(WEB_ROOT, "index.html"))
