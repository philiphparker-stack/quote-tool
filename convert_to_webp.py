import os
from PIL import Image

# 🔧 root folder (same as before)
IMAGE_DIR = r"C:\Quote Tool Test\web\assets\items"

SUPPORTED_EXTENSIONS = (".png", ".jpg", ".jpeg", ".bmp", ".tiff")

converted = 0
skipped = 0

# 🔥 THIS IS THE KEY CHANGE → os.walk (recursive)
for root, dirs, files in os.walk(IMAGE_DIR):
    for filename in files:
        file_path = os.path.join(root, filename)

        name, ext = os.path.splitext(filename)

        # skip already webp
        if ext.lower() == ".webp":
            skipped += 1
            continue

        if ext.lower() not in SUPPORTED_EXTENSIONS:
            continue

        try:
            img = Image.open(file_path)

            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")

            output_path = os.path.join(root, name + ".webp")

            img.save(output_path, "WEBP", quality=75, method=6)

            print(f"✅ {file_path} → {output_path}")
            converted += 1

        except Exception as e:
            print(f"❌ Failed: {file_path} ({e})")

print("\n--- DONE ---")
print(f"Converted: {converted}")
print(f"Skipped: {skipped}")