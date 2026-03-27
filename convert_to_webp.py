import os
from PIL import Image

IMAGE_DIR = r"C:\Quote Tool Test\web\assets\items"

SUPPORTED_EXTENSIONS = (".png", ".jpg", ".jpeg")

converted = 0

for root, dirs, files in os.walk(IMAGE_DIR):
    for filename in files:
        file_path = os.path.join(root, filename)
        name, ext = os.path.splitext(filename)

        if ext.lower() not in SUPPORTED_EXTENSIONS:
            continue

        try:
            img = Image.open(file_path)

            # 🔥 THIS IS THE FIX
            if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
                img = img.convert("RGBA")

                white_bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
                img = Image.alpha_composite(white_bg, img).convert("RGB")
            else:
                img = img.convert("RGB")

            output_path = os.path.join(root, name + ".webp")

            # overwrite existing broken webp
            img.save(output_path, "WEBP", quality=90, method=6)

            print(f"✅ Fixed: {filename}")
            converted += 1

        except Exception as e:
            print(f"❌ Failed: {filename} ({e})")

print("\nDONE")
print(f"Fixed images: {converted}")