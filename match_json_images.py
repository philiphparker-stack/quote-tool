import json
import re
from pathlib import Path
from typing import Dict, List, Tuple

# ============================================================
# PATHS
# ============================================================
PROJECT_ROOT = Path(r"C:\Quote Tool Test")
ITEMS_JSON_PATH = PROJECT_ROOT / "web" / "items.json"
IMAGES_ROOT = PROJECT_ROOT / "web" / "assets" / "items"

VALID_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}

# If True, only update rows that are comingsoon / blank / broken.
# If False, also fix rows whose current image path does not exist.
ONLY_FIX_MISSING_OR_COMINGSOON = True

# ============================================================
# HELPERS
# ============================================================
def normalize_text(value: str) -> str:
    value = str(value or "").strip().lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", "", value)
    return value


def tokenize_name(value: str) -> List[str]:
    raw = str(value or "").strip().lower()
    parts = re.split(r"[^a-z0-9]+", raw)
    return [p for p in parts if len(p) >= 3]


def relative_image_path(path: Path) -> str:
    return path.relative_to(IMAGES_ROOT).as_posix()


def build_image_catalog() -> Tuple[List[Dict], Dict[str, List[Path]]]:
    catalog: List[Dict] = []
    by_stem: Dict[str, List[Path]] = {}

    for path in IMAGES_ROOT.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in VALID_EXTENSIONS:
            continue

        rel = relative_image_path(path)
        stem_norm = normalize_text(path.stem)
        folder_norm = normalize_text(path.parent.name)

        record = {
            "path": path,
            "rel": rel,
            "stem_norm": stem_norm,
            "folder_norm": folder_norm,
            "name_tokens": set(tokenize_name(path.stem)),
        }
        catalog.append(record)
        by_stem.setdefault(stem_norm, []).append(path)

    return catalog, by_stem


def image_path_exists(image_value: str) -> bool:
    image_value = str(image_value or "").strip().replace("\\", "/")
    if not image_value:
        return False
    return (IMAGES_ROOT / image_value).exists()


def is_comingsoon(image_value: str) -> bool:
    image_value = str(image_value or "").strip().lower().replace("\\", "/")
    return image_value in {"comingsoon.png", "comingsoon.webp", ""}


def find_best_match(item: Dict, catalog: List[Dict], by_stem: Dict[str, List[Path]]) -> Tuple[str, str]:
    item_id = str(item.get("id") or "").strip()
    item_name = str(item.get("name") or "").strip()
    manufacturer = str(item.get("manufacturer") or "").strip()
    current_image = str(item.get("image") or "").strip().replace("\\", "/")

    id_norm = normalize_text(item_id)
    name_norm = normalize_text(item_name)
    mfr_norm = normalize_text(manufacturer)
    current_stem_norm = normalize_text(Path(current_image).stem) if current_image else ""
    name_tokens = set(tokenize_name(item_name))

    # --------------------------------------------------------
    # 1. Exact stem match to ID
    # --------------------------------------------------------
    if id_norm and id_norm in by_stem and len(by_stem[id_norm]) == 1:
        p = by_stem[id_norm][0]
        return relative_image_path(p), "exact-id-stem"

    # --------------------------------------------------------
    # 2. Exact stem match to current image stem
    # --------------------------------------------------------
    if current_stem_norm and current_stem_norm in by_stem and len(by_stem[current_stem_norm]) == 1:
        p = by_stem[current_stem_norm][0]
        return relative_image_path(p), "exact-current-stem"

    # --------------------------------------------------------
    # 3. Exact stem match to name
    # --------------------------------------------------------
    if name_norm and name_norm in by_stem and len(by_stem[name_norm]) == 1:
        p = by_stem[name_norm][0]
        return relative_image_path(p), "exact-name-stem"

    # --------------------------------------------------------
    # 4. Score-based matching
    # --------------------------------------------------------
    scored: List[Tuple[int, str, str]] = []

    for rec in catalog:
        score = 0

        # Strong signals
        if id_norm and id_norm == rec["stem_norm"]:
            score += 100
        elif id_norm and id_norm in rec["stem_norm"]:
            score += 60

        if current_stem_norm and current_stem_norm == rec["stem_norm"]:
            score += 90
        elif current_stem_norm and current_stem_norm in rec["stem_norm"]:
            score += 50

        if name_norm and name_norm == rec["stem_norm"]:
            score += 85
        elif name_norm and name_norm in rec["stem_norm"]:
            score += 40

        # Manufacturer folder hint
        if mfr_norm:
            if "tec" in mfr_norm and rec["folder_norm"] == "tec":
                score += 20
            elif "laticrete" in mfr_norm and rec["folder_norm"] == "laticrete":
                score += 20
            elif "hardie" in mfr_norm and rec["folder_norm"] == "hardie":
                score += 20
            elif ("usg" in mfr_norm or "unitedstatesgypsum" in mfr_norm) and rec["folder_norm"] == "usg":
                score += 20
            elif ("nationalgypsum" in mfr_norm or "georgiapacific" in mfr_norm) and rec["folder_norm"] == "nationalgyp":
                score += 20
            elif "noble" in mfr_norm and rec["folder_norm"] == "noble":
                score += 20
            elif "emser" in mfr_norm and rec["folder_norm"] == "emser":
                score += 20
            elif "custombuildingproducts" in mfr_norm and rec["folder_norm"] == "custom":
                score += 20
            elif "guru" in mfr_norm and rec["folder_norm"] == "guru":
                score += 20
            elif "raimondi" in mfr_norm and rec["folder_norm"] == "raimondi":
                score += 20
            elif "paper" in mfr_norm and rec["folder_norm"] == "paper":
                score += 20

        # Token overlap from product name
        overlap = len(name_tokens & rec["name_tokens"])
        score += overlap * 5

        if score > 0:
            scored.append((score, rec["rel"], rec["folder_norm"]))

    if not scored:
        return "", "no-match"

    scored.sort(key=lambda x: x[0], reverse=True)

    top_score = scored[0][0]
    top_paths = [s for s in scored if s[0] == top_score]

    if len(top_paths) == 1 and top_score >= 40:
        return top_paths[0][1], f"score-{top_score}"

    if len(top_paths) > 1:
        rels = ", ".join(tp[1] for tp in top_paths[:5])
        return "", f"ambiguous-top-score-{top_score}: {rels}"

    return "", f"low-confidence-top-score-{top_score}"


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    if not ITEMS_JSON_PATH.exists():
        raise FileNotFoundError(f"Could not find items.json at {ITEMS_JSON_PATH}")

    if not IMAGES_ROOT.exists():
        raise FileNotFoundError(f"Could not find images root at {IMAGES_ROOT}")

    with open(ITEMS_JSON_PATH, "r", encoding="utf-8") as f:
        items = json.load(f)

    if not isinstance(items, list):
        raise ValueError("items.json must be a JSON array/list.")

    catalog, by_stem = build_image_catalog()

    backup_path = ITEMS_JSON_PATH.with_suffix(".json.match_backup")
    with open(backup_path, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)

    updated = 0
    skipped_good = 0
    no_match = 0
    ambiguous = 0
    reviewed = 0

    report_lines: List[str] = []
    report_lines.append("MATCH JSON IMAGES REPORT")
    report_lines.append("=" * 80)

    for item in items:
        reviewed += 1

        current_image = str(item.get("image") or "").strip().replace("\\", "/")
        current_exists = image_path_exists(current_image)

        should_fix = False
        if ONLY_FIX_MISSING_OR_COMINGSOON:
            if is_comingsoon(current_image) or not current_exists:
                should_fix = True
        else:
            should_fix = True

        if not should_fix:
            skipped_good += 1
            continue

        new_path, reason = find_best_match(item, catalog, by_stem)

        item_id = str(item.get("id") or "").strip()
        item_name = str(item.get("name") or "").strip()

        if new_path:
            old_path = current_image
            item["image"] = new_path
            updated += 1
            report_lines.append(f"UPDATED   | {item_id} | {item_name} | {old_path} -> {new_path} | {reason}")
        else:
            if reason.startswith("ambiguous"):
                ambiguous += 1
            else:
                no_match += 1
            report_lines.append(f"UNMATCHED | {item_id} | {item_name} | current={current_image} | {reason}")

    with open(ITEMS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)

    report_path = PROJECT_ROOT / "match_json_images_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    print("=" * 80)
    print("MATCH JSON IMAGES COMPLETE")
    print("=" * 80)
    print(f"Reviewed       : {reviewed}")
    print(f"Updated        : {updated}")
    print(f"Skipped good   : {skipped_good}")
    print(f"No match       : {no_match}")
    print(f"Ambiguous      : {ambiguous}")
    print("-" * 80)
    print(f"Backup JSON    : {backup_path}")
    print(f"Report         : {report_path}")
    print("=" * 80)


if __name__ == "__main__":
    main()