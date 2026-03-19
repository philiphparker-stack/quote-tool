import os
from pathlib import Path
from PIL import Image, ImageOps

# ============================================================
# CONFIG
# ============================================================
PROJECT_ROOT = Path(r"C:\Quote Tool Test")
RAW_DIR = PROJECT_ROOT / "raw_images"
OUTPUT_DIR = PROJECT_ROOT / "web" / "assets" / "items"

TARGET_SIZE = (300, 300)   # final canvas size
WEBP_QUALITY = 78          # 70-80 is a good range
BACKGROUND_COLOR = (255, 255, 255)
SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}

# ============================================================
# HELPERS
# ============================================================
def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def clean_stem(name: str) -> str:
    safe = []
    for ch in name.strip():
        if ch.isalnum() or ch in ("-", "_"):
            safe.append(ch)
        elif ch in (" ", "."):
            safe.append("_")
    out = "".join(safe).strip("_")
    return out or "image"


def optimize_one_image(src_path: Path) -> Path:
    out_name = f"{clean_stem(src_path.stem)}.webp"
    out_path = OUTPUT_DIR / out_name

    with Image.open(src_path) as img:
        # honor EXIF rotation
        img = ImageOps.exif_transpose(img)

        # convert to RGBA first for safe handling of transparency
        img = img.convert("RGBA")

        # contain = keeps whole image visible without cropping
        fitted = ImageOps.contain(img, TARGET_SIZE, method=Image.Resampling.LANCZOS)

        # white square background
        canvas = Image.new("RGBA", TARGET_SIZE, BACKGROUND_COLOR + (255,))

        x = (TARGET_SIZE[0] - fitted.width) // 2
        y = (TARGET_SIZE[1] - fitted.height) // 2
        canvas.paste(fitted, (x, y), fitted)

        # save as RGB WebP
        final_img = canvas.convert("RGB")
        final_img.save(
            out_path,
            format="WEBP",
            quality=WEBP_QUALITY,
            method=6
        )

    return out_path


def main() -> None:
    ensure_output_dir()

    if not RAW_DIR.exists():
        print(f"RAW_DIR does not exist: {RAW_DIR}")
        print("Create that folder and drop raw images into it.")
        return

    files = [p for p in RAW_DIR.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS]

    if not files:
        print(f"No supported image files found in: {RAW_DIR}")
        return

    print(f"Found {len(files)} image(s).")
    print(f"Optimized images will be saved to: {OUTPUT_DIR}")
    print("-" * 60)

    success = 0
    failed = 0

    for src_path in files:
        try:
            out_path = optimize_one_image(src_path)
            src_size_kb = src_path.stat().st_size / 1024
            out_size_kb = out_path.stat().st_size / 1024

            print(
                f"OK  | {src_path.name} -> {out_path.name} "
                f"| {src_size_kb:.1f} KB -> {out_size_kb:.1f} KB"
            )
            success += 1
        except Exception as e:
            print(f"ERR | {src_path.name} -> {e}")
            failed += 1

    print("-" * 60)
    print(f"Done. Success: {success}, Failed: {failed}")


if __name__ == "__main__":
    main()