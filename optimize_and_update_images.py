import json
from pathlib import Path
from typing import Dict, List, Tuple
from PIL import Image, ImageOps

# ============================================================
# CONFIG
# ============================================================
PROJECT_ROOT = Path(r"C:\Quote Tool Test")
ITEMS_JSON_PATH = PROJECT_ROOT / "web" / "items.json"
IMAGES_ROOT = PROJECT_ROOT / "web" / "assets" / "items"

TARGET_SIZE = (300, 300)
WEBP_QUALITY = 78
BACKGROUND_COLOR = (255, 255, 255)
SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}

# Safety switches
WRITE_JSON_UPDATES = True          # set False to dry-run JSON changes
OVERWRITE_EXISTING_WEBP = True     # if False, skips making .webp when already present
BACKUP_JSON = True                 # writes items.json.bak before changing items.json

# ============================================================
# HELPERS
# ============================================================
def clean_stem(name: str) -> str:
    safe = []
    for ch in name.strip():
        if ch.isalnum() or ch in ("-", "_"):
            safe.append(ch)
        elif ch in (" ", "."):
            safe.append("_")
    out = "".join(safe).strip("_")
    return out or "image"


def optimize_image_to_webp(src_path: Path, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with Image.open(src_path) as img:
        img = ImageOps.exif_transpose(img)
        img = img.convert("RGBA")

        fitted = ImageOps.contain(img, TARGET_SIZE, method=Image.Resampling.LANCZOS)

        canvas = Image.new("RGBA", TARGET_SIZE, BACKGROUND_COLOR + (255,))
        x = (TARGET_SIZE[0] - fitted.width) // 2
        y = (TARGET_SIZE[1] - fitted.height) // 2
        canvas.paste(fitted, (x, y), fitted)

        final_img = canvas.convert("RGB")
        final_img.save(
            out_path,
            format="WEBP",
            quality=WEBP_QUALITY,
            method=6
        )


def load_items() -> List[Dict]:
    if not ITEMS_JSON_PATH.exists():
        raise FileNotFoundError(f"items.json not found: {ITEMS_JSON_PATH}")

    with open(ITEMS_JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("items.json must be a JSON array/list.")

    return data


def save_items(items: List[Dict]) -> None:
    if BACKUP_JSON:
        backup_path = ITEMS_JSON_PATH.with_suffix(".json.bak")
        backup_path.write_text(
            ITEMS_JSON_PATH.read_text(encoding="utf-8"),
            encoding="utf-8"
        )

    with open(ITEMS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)


def build_webp_relative_path(image_value: str) -> str:
    src_rel = Path(image_value.replace("\\", "/"))
    parent = src_rel.parent
    stem = clean_stem(src_rel.stem)
    if str(parent) == ".":
        return f"{stem}.webp"
    return f"{parent.as_posix()}/{stem}.webp"


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    print("=" * 90)
    print("OPTIMIZE IMAGES + UPDATE items.json")
    print("=" * 90)
    print(f"items.json: {ITEMS_JSON_PATH}")
    print(f"images root: {IMAGES_ROOT}")
    print(f"target size: {TARGET_SIZE[0]}x{TARGET_SIZE[1]}")
    print(f"webp quality: {WEBP_QUALITY}")
    print(f"write json updates: {WRITE_JSON_UPDATES}")
    print(f"overwrite existing webp: {OVERWRITE_EXISTING_WEBP}")
    print("=" * 90)

    items = load_items()

    optimized_count = 0
    skipped_count = 0
    missing_count = 0
    updated_json_count = 0
    error_count = 0

    processed_images: Dict[str, str] = {}

    for idx, item in enumerate(items, start=1):
        raw_image = str(item.get("image") or "").strip()

        if not raw_image:
            skipped_count += 1
            continue

        normalized_image = raw_image.replace("\\", "/")

        # Reuse work if multiple items share same image
        if normalized_image in processed_images:
            new_rel_path = processed_images[normalized_image]
            if item.get("image") != new_rel_path:
                item["image"] = new_rel_path
                updated_json_count += 1
            continue

        src_path = IMAGES_ROOT / Path(normalized_image)
        if not src_path.exists():
            print(f"MISSING | {normalized_image}")
            missing_count += 1
            continue

        if src_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
            print(f"SKIP    | Unsupported extension: {normalized_image}")
            skipped_count += 1
            continue

        new_rel_path = build_webp_relative_path(normalized_image)
        out_path = IMAGES_ROOT / Path(new_rel_path)

        try:
            should_optimize = True
            if out_path.exists() and not OVERWRITE_EXISTING_WEBP:
                should_optimize = False

            if should_optimize:
                optimize_image_to_webp(src_path, out_path)
                src_kb = src_path.stat().st_size / 1024
                out_kb = out_path.stat().st_size / 1024
                print(
                    f"OK      | {normalized_image} -> {new_rel_path} "
                    f"| {src_kb:.1f} KB -> {out_kb:.1f} KB"
                )
                optimized_count += 1
            else:
                print(f"EXISTS  | {new_rel_path}")
                skipped_count += 1

            processed_images[normalized_image] = new_rel_path

            if item.get("image") != new_rel_path:
                item["image"] = new_rel_path
                updated_json_count += 1

        except Exception as e:
            print(f"ERROR   | {normalized_image} -> {e}")
            error_count += 1

    if WRITE_JSON_UPDATES:
        save_items(items)
        print("-" * 90)
        print("items.json updated.")
        if BACKUP_JSON:
            print(f"Backup created: {ITEMS_JSON_PATH.with_suffix('.json.bak')}")
    else:
        print("-" * 90)
        print("Dry run only. items.json was NOT changed.")

    print("=" * 90)
    print("SUMMARY")
    print("=" * 90)
    print(f"Optimized images : {optimized_count}")
    print(f"Skipped          : {skipped_count}")
    print(f"Missing          : {missing_count}")
    print(f"JSON updates     : {updated_json_count}")
    print(f"Errors           : {error_count}")
    print("=" * 90)


if __name__ == "__main__":
    main()