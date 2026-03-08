import json

ITEMS_PATH = r"C:\Quote Tool Test\web\items.json"

IMAGE_MAP = {
    "ZHA20003": "hardie12.png",
    "ZHAFHB25": "hardie14.png",
    "ZCUCBLHTW44": "customblendlht.png",
    "ZCUPROLMGR30": "prolite.png",
    "ZCUPROLMWT30": "prolite.png",
    "ZLC0279-0030-21": "trilite.png",
    "ZLC0279-0030-22": "trilite.png",
    "ZLC677643": "prism.png",
}

with open(ITEMS_PATH, "r", encoding="utf-8") as f:
    items = json.load(f)

for item in items:
    item_id = item.get("id", "")
    if item_id in IMAGE_MAP:
        item["image"] = IMAGE_MAP[item_id]

with open(ITEMS_PATH, "w", encoding="utf-8") as f:
    json.dump(items, f, indent=2)

print("Done. items.json updated.")