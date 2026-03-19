import os
from pathlib import Path
from PIL import Image, ImageOps

# ============================================================
# CONFIG
# ============================================================
INPUT_DIR = Path(r"C:\Quote Tool Test\web\assets\items")
OUTPUT_DIR = Path(r"C:\Quote Tool Test\web\assets\items_optimized")

TARGET_SIZE = (300, 300)
WEBP_QUALITY = 78
BACKGROUND_COLOR = (255, 255, 255)
SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}

# Set to True if you want to skip files already ending in .webp
SKIP_EXISTING_WEBP_INPUTS = False

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


def optimize_one_image(src_path: Path, out_path: Path) -> None:
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


def find_all_images(root: Path):
    return [
        p for p in root.rglob("*")
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS
    ]


def build_output_path(src_path: Path) -> Path:
    relative_path = src_path.relative_to(INPUT_DIR)
    parent_relative = relative_path.parent
    new_name = f"{clean_stem(src_path.stem)}.webp"
    return OUTPUT_DIR / parent_relative / new_name


def main() -> None:
    if not INPUT_DIR.exists():
        print(f"Input folder does not exist: {INPUT_DIR}")
        return

    files = find_all_images(INPUT_DIR)

    if SKIP_EXISTING_WEBP_INPUTS:
        files = [p for p in files if p.suffix.lower() != ".webp"]

    if not files:
        print(f"No supported image files found in: {INPUT_DIR}")
        return

    print(f"Found {len(files)} image(s).")
    print(f"Input:  {INPUT_DIR}")
    print(f"Output: {OUTPUT_DIR}")
    print("-" * 80)

    success = 0
    failed = 0

    for src_path in files:
        try:
            out_path = build_output_path(src_path)
            optimize_one_image(src_path, out_path)

            src_size_kb = src_path.stat().st_size / 1024
            out_size_kb = out_path.stat().st_size / 1024

            print(
                f"OK  | {src_path.relative_to(INPUT_DIR)} -> {out_path.relative_to(OUTPUT_DIR)} "
                f"| {src_size_kb:.1f} KB -> {out_size_kb:.1f} KB"
            )
            success += 1
        except Exception as e:
            print(f"ERR | {src_path.relative_to(INPUT_DIR)} -> {e}")
            failed += 1

    print("-" * 80)
    print(f"Done. Success: {success}, Failed: {failed}")


if __name__ == "__main__":
    main()