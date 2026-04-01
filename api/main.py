import os
import json
import base64
import re
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


class GenerateReq(BaseModel):
    program: str = "TEST"
    customer_name: Optional[str] = None
    effective_date: Optional[str] = None
    layout_mode: Optional[str] = "grid"

    # legacy support
    price_mode: Optional[str] = "oow"
    item_ids: List[str] = []
    price_overrides: Optional[Dict[str, Any]] = None

    # new per-item support
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

    pricing_mode = norm(it.get("_pricing_mode")).lower()
    if pricing_mode in {"oow", "direct", "both"}:
        oow_price = try_float(it.get("_override_oow_price"))
        if oow_price is None:
            oow_price = try_float(it.get("price_oow"))

        direct_price = try_float(it.get("_override_direct_price"))
        if direct_price is None:
            direct_price = try_float(it.get("price_direct"))

        lines: List[str] = []
        if pricing_mode in {"oow", "both"}:
            txt = fmt_single_price("OOW", oow_price, uom)
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

    label = "Direct" if legacy_mode == "direct" else "OOW"
    return [fmt_single_price(label, price_val, uom)]


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
    c.drawCentredString(W / 2, H - 38, "Local Market Stocking Program")

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

    c.drawRightString(right_x, row1_y, f"Program: {program or 'TEST'}")
    c.drawRightString(right_x, row2_y, f"Effective Date: {effective_date or '—'}")

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
        norm(it.get("manufacturer")),
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
    mfr_x = x + 382
    price_x = x + 490

    text_y = y_top - 21

    c.setFillColor(BRAND_BLUE_DARK)
    name_txt, name_size = fit_one_line(c, norm(it.get("name")), 238, "Helvetica-Bold", 9.0, 6.0)
    c.setFont("Helvetica-Bold", name_size)
    c.drawString(name_x, text_y, name_txt)

    c.setFillColor(colors.Color(0, 0, 0, alpha=0.74))
    c.setFont("Helvetica", 7.3)
    sku_txt, _ = fit_one_line(c, norm(it.get("id")), 84, "Helvetica", 7.3, 6.0)
    c.drawString(sku_x, text_y, sku_txt)

    mfr_txt, _ = fit_one_line(c, norm(it.get("manufacturer")), 100, "Helvetica", 7.3, 6.0)
    c.drawString(mfr_x, text_y, mfr_txt)

    price_lines = get_price_lines(it, fallback_mode)
    c.setFont("Helvetica-Bold", 7.3)
    c.setFillColor(colors.black)
    price_y = text_y
    for line in price_lines[:2]:
        draw_txt, _ = fit_one_line(c, line, 86, "Helvetica-Bold", 7.3, 6.0)
        c.drawString(price_x, price_y, draw_txt)
        price_y -= 9


def group_items_for_pdf(items: List[Dict[str, Any]]) -> List[Tuple[str, List[Dict[str, Any]]]]:
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
    return [(cat, grouped[cat]) for cat in ordered_cats]


def is_small_group(items: List[Dict[str, Any]]) -> bool:
    return len(items) == 1


def estimate_small_section_height(num_items: int, card_h: float, row_gap: float) -> float:
    header_h = 18
    if num_items <= 0:
        return header_h + 8
    cards_block = (num_items * card_h) + ((num_items - 1) * row_gap)
    return header_h + 8 + cards_block


def draw_small_category_section(
    c: canvas.Canvas,
    x: float,
    y_top: float,
    section_w: float,
    cat_key: str,
    cat_items: List[Dict[str, Any]],
    fallback_mode: str,
    card_h: float,
    row_gap: float,
):
    label = pretty_category(cat_key)
    header_h = draw_category_header(c, x, y_top, section_w, label)
    y = y_top - (header_h + 8)

    for it in cat_items:
        draw_card(c, x, y, section_w, card_h, it, fallback_mode)
        y -= (card_h + row_gap)

    return estimate_small_section_height(len(cat_items), card_h, row_gap)


def draw_two_up_category_section(
    c: canvas.Canvas,
    x: float,
    y_top: float,
    section_w: float,
    cat_key: str,
    cat_items: List[Dict[str, Any]],
    fallback_mode: str,
    card_h: float,
    gutter: float,
):
    label = pretty_category(cat_key)
    header_h = draw_category_header(c, x, y_top, section_w, label)
    y = y_top - (header_h + 8)

    two_card_w = (section_w - gutter) / 2

    if len(cat_items) >= 1:
        draw_card(c, x, y, two_card_w, card_h, cat_items[0], fallback_mode)

    if len(cat_items) >= 2:
        draw_card(c, x + two_card_w + gutter, y, two_card_w, card_h, cat_items[1], fallback_mode)

    used_height = header_h + 8 + card_h
    return used_height


def build_pdf_grid(
    items: List[Dict[str, Any]],
    fallback_mode: str,
    customer_logo_data: Optional[str] = None,
    program: str = "TEST",
    customer_name: str = "",
    effective_date: str = "",
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

    small_section_w = (usable_w - section_gap) / 2

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

    groups = group_items_for_pdf(items)
    min_full_section_space = 18 + 8 + card_h + row_gap

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
        cat_key, cat_items = groups[i]

        # ----------------------------------------------------
        # Two-item categories = same category, side-by-side
        # ----------------------------------------------------
        if len(cat_items) == 2:
            needed_height = 18 + 8 + card_h

            if y - needed_height < bottom:
                new_page()

            used_height = draw_two_up_category_section(
                c=c,
                x=left,
                y_top=y,
                section_w=usable_w,
                cat_key=cat_key,
                cat_items=cat_items,
                fallback_mode=fallback_mode,
                card_h=card_h,
                gutter=gutter,
            )

            y -= used_height + 6
            i += 1
            continue

        # ----------------------------------------------------
        # One-item categories can pair side-by-side
        # ----------------------------------------------------
        if is_small_group(cat_items):
            next_pair = None
            if i + 1 < len(groups):
                next_cat_key, next_cat_items = groups[i + 1]
                if is_small_group(next_cat_items):
                    next_pair = (next_cat_key, next_cat_items)

            left_height = estimate_small_section_height(len(cat_items), card_h, row_gap)
            right_height = 0
            if next_pair:
                right_height = estimate_small_section_height(len(next_pair[1]), card_h, row_gap)

            needed_height = max(left_height, right_height if next_pair else 0)

            if y - needed_height < bottom:
                new_page()

            used_left = draw_small_category_section(
                c=c,
                x=left,
                y_top=y,
                section_w=small_section_w,
                cat_key=cat_key,
                cat_items=cat_items,
                fallback_mode=fallback_mode,
                card_h=card_h,
                row_gap=row_gap,
            )

            used_right = 0
            if next_pair:
                used_right = draw_small_category_section(
                    c=c,
                    x=left + small_section_w + section_gap,
                    y_top=y,
                    section_w=small_section_w,
                    cat_key=next_pair[0],
                    cat_items=next_pair[1],
                    fallback_mode=fallback_mode,
                    card_h=card_h,
                    row_gap=row_gap,
                )

            y -= max(used_left, used_right if next_pair else 0) + 6

            if next_pair:
                i += 2
            else:
                i += 1
            continue

        # ----------------------------------------------------
        # Full-width categories
        # ----------------------------------------------------
        if y - min_full_section_space < bottom:
            new_page()

        label = pretty_category(cat_key)
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
    groups = group_items_for_pdf(items)

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

    for cat_key, cat_items in groups:
        label = pretty_category(cat_key)

        section_header_h = 18
        if y - (section_header_h + 8 + row_h) < bottom:
            new_page()

        draw_category_header(c, left, y, usable_w, label)
        y -= (section_header_h + 8)

        c.setFont("Helvetica-Bold", 7.2)
        c.setFillColor(SOFT_TEXT)
        c.drawString(left + 44, y - 2, "Description")
        c.drawString(left + 290, y - 2, "SKU")
        c.drawString(left + 382, y - 2, "Manufacturer")
        c.drawString(left + 490, y - 2, "Pricing")
        y -= 10

        for it in cat_items:
            if y - row_h < bottom:
                new_page()
                draw_category_header(c, left, y, usable_w, label + " (cont.)")
                y -= (section_header_h + 8)
                c.setFont("Helvetica-Bold", 7.2)
                c.setFillColor(SOFT_TEXT)
                c.drawString(left + 44, y - 2, "Description")
                c.drawString(left + 290, y - 2, "SKU")
                c.drawString(left + 382, y - 2, "Manufacturer")
                c.drawString(left + 490, y - 2, "Pricing")
                y -= 10

            draw_compact_row(c, left, y, usable_w, row_h, it, fallback_mode)
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


@app.post("/generate")
def generate(request: Request, req: GenerateReq):
    require_password(request)

    items_map = load_items_map()
    picked: List[Dict[str, Any]] = []

    if req.items:
        for selected in req.items:
            item_id = norm(selected.id)
            if item_id not in items_map:
                continue

            base = dict(items_map[item_id])
            pricing_mode = norm(selected.pricing_mode).lower()
            if pricing_mode not in {"oow", "direct", "both"}:
                pricing_mode = "oow"

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

    if layout_mode == "compact":
        pdf_bytes = build_pdf_compact(
            items=picked,
            fallback_mode=fallback_mode,
            customer_logo_data=req.customer_logo_data,
            program=norm(req.program) or "TEST",
            customer_name=norm(req.customer_name),
            effective_date=norm(req.effective_date),
        )
    else:
        pdf_bytes = build_pdf_grid(
            items=picked,
            fallback_mode=fallback_mode,
            customer_logo_data=req.customer_logo_data,
            program=norm(req.program) or "TEST",
            customer_name=norm(req.customer_name),
            effective_date=norm(req.effective_date),
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
