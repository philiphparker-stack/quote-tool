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
# Request model
# ============================================================
class GenerateReq(BaseModel):
    program: str = "TEST"
    customer_name: Optional[str] = None
    effective_date: Optional[str] = None
    price_mode: str = "direct"
    item_ids: List[str] = []
    customer_logo_data: Optional[str] = None
    price_overrides: Optional[Dict[str, Any]] = None


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
    s = s.replace('"', ' ')
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
    "GROUT": "GROUTS / CAULKING",
    "CAULK": "GROUTS / CAULKING",
    "ADHESIVE": "ADHESIVES",
    "BACKERBOARD": "BACKERBOARD",
    "TRIM": "TRIMS / METALS",
    "DRAINS": "DRAINS",
    "MASTIC": "MASTICS",
}

CATEGORY_ORDER = {
    "BACKERBOARD": 10,
    "THINSET": 20,
    "GROUT": 30,
    "CAULK": 31,
    "ADHESIVE": 40,
    "MASTIC": 41,
    "TRIM": 50,
    "DRAINS": 60,
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
def get_numeric_price(it: Dict[str, Any], mode: str) -> Optional[float]:
    if "_override_price" in it and it["_override_price"] is not None:
        try:
            return float(it["_override_price"])
        except Exception:
            pass

    raw = it.get("price_direct") if mode == "direct" else it.get("price_oow")
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except Exception:
        return None


def fmt_price(it: Dict[str, Any], mode: str) -> str:
    price_val = get_numeric_price(it, mode)
    uom = norm(it.get("uom") or "ea")
    if price_val is None:
        return ""
    return f"${price_val:.2f}/{uom}"


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
LIGHT_LINE = colors.HexColor("#D6DCE5")


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
    mode: str,
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
    price_y = title_y - title_block_h - 6

    price_txt, price_size = fit_one_line(
        c,
        fmt_price(it, mode),
        inner_w,
        "Helvetica-Bold",
        start_size=10.0,
        min_size=8.8,
    )
    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", price_size)
    c.drawString(inner_x, price_y, price_txt)

    img_size = 36
    img_x = inner_x
    img_y = y_top - card_h + 16

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

    # tighter manufacturer/SKU stack
    mfr_lines, mfr_size = fit_lines(
        c,
        norm(it.get("manufacturer")),
        meta_w,
        "Helvetica",
        max_lines=2,
        start_size=5.4,
        min_size=4.6,
    )

    mfr_line_gap = 0.8
    mfr_start_y = img_y + 22

    c.setFillColor(SOFT_TEXT)
    c.setFont("Helvetica", mfr_size)
    for i, line in enumerate(mfr_lines[:2]):
        c.drawString(meta_x, mfr_start_y - (i * (mfr_size + mfr_line_gap)), line)

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

    # put SKU directly under manufacturer block
    sku_y = mfr_start_y - (len(mfr_lines) * (mfr_size + mfr_line_gap)) - 1.5
    c.drawString(meta_x, sku_y, sku_txt)


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


def build_pdf(
    items: List[Dict[str, Any]],
    mode: str,
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
    row_gap = 9

    usable_w = W - left - right
    card_w = (usable_w - gutter * (cols - 1)) / cols
    card_h = 92

    customer_logo_reader = decode_logo_data(customer_logo_data)
    page_num = 1

    header_divider_y = draw_header(
        c,
        W,
        H,
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
            c,
            W,
            H,
            program=program,
            effective_date=effective_date,
            customer_name=customer_name,
            customer_logo_reader=customer_logo_reader,
        )
        y = header_divider_y - 10

    for cat_key, cat_items in groups:
        label = pretty_category(cat_key)

        if y - 22 < bottom:
            new_page()

        header_h = draw_category_header(c, left, y, usable_w, label)
        y -= (header_h + 8)

        x = left
        col = 0

        for it in cat_items:
            if col == 0 and (y - card_h) < bottom:
                new_page()
                header_h = draw_category_header(c, left, y, usable_w, label + " (cont.)")
                y -= (header_h + 8)

            draw_card(c, x, y, card_w, card_h, it, mode)

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

    draw_page_footer(c, W, page_num)

    c.save()
    pdf = buf.getvalue()
    buf.close()
    return pdf


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

    mode = norm(req.price_mode).lower()
    if mode not in ("direct", "oow"):
        raise HTTPException(status_code=400, detail="price_mode must be 'direct' or 'oow'")

    items_map = load_items_map()

    picked = []
    if req.item_ids and items_map:
        for item_id in req.item_ids:
            if item_id in items_map:
                picked.append(dict(items_map[item_id]))

    if not picked:
        raise HTTPException(status_code=400, detail="No valid selected items found.")

    overrides = req.price_overrides or {}
    for it in picked:
        item_id = norm(it.get("id"))
        if item_id in overrides:
            raw = overrides[item_id]
            try:
                if raw is not None and str(raw).strip() != "":
                    it["_override_price"] = float(str(raw).replace("$", "").replace(",", "").strip())
            except Exception:
                pass

    pdf_bytes = build_pdf(
        items=picked,
        mode=mode,
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