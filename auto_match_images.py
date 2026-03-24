import os
import json
from pathlib import Path

# ============================================================
# PATHS
# ============================================================
PROJECT_ROOT = Path(r"C:\Quote Tool Test")
ITEMS_JSON = PROJECT_ROOT / "web" / "items.json"
IMAGES_ROOT = PROJECT_ROOT / "web" / "assets" / "items"

VALID_EXTS = [".png", ".jpg", ".jpeg", ".webp", ".PNG", ".JPG"]

# ============================================================
# BUILD IMAGE LOOKUP
# ============================================================
def build_image_index():
    image_map = {}

    for root, dirs, files in os.walk(IMAGES_ROOT):
        for file in files:
            ext = Path(file).suffix
            if ext.lower() not in [e.lower() for e in VALID_EXTS]:
                continue

            full_path = Path(root) / file

            # relative path from items folder
            rel_path = full_path.relative_to(IMAGES_ROOT).as_posix()

            # normalize key
            key = Path(file).stem.lower().replace("-", "").replace("_", "").replace(" ", "")

            image_map[key] = rel_path

    return image_map

# ============================================================
# CLEAN STRING
# ============================================================
def normalize(text):
    return text.lower().replace("-", "").replace("_", "").replace(" ", "")

# ============================================================
# MAIN MATCHING
# ============================================================
def main():
    with open(ITEMS_JSON, "r", encoding="utf-8") as f:
        items = json.load(f)

    image_index = build_image_index()

    updated = 0
    matched = 0
    missing = 0

    for item in items:
        name_key = normalize(item.get("name", ""))
        id_key = normalize(item.get("id", ""))

        # try ID first (best match)
        match = image_index.get(id_key)

        # fallback to name
        if not match:
            match = image_index.get(name_key)

        # fallback: contains match
        if not match:
            for k, v in image_index.items():
                if id_key in k or k in id_key:
                    match = v
                    break

        if match:
            item["image"] = match
            updated += 1
            matched += 1
        else:
            missing += 1

    # backup
    backup_path = ITEMS_JSON.with_suffix(".auto_backup.json")
    with open(backup_path, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2)

    # write updated
    with open(ITEMS_JSON, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2)

    print("=" * 60)
    print("AUTO IMAGE MATCH COMPLETE")
    print("=" * 60)
    print(f"Matched   : {matched}")
    print(f"Updated   : {updated}")
    print(f"Missing   : {missing}")
    print("=" * 60)
    print(f"Backup saved to: {backup_path}")

# ============================================================
if __name__ == "__main__":
    main()