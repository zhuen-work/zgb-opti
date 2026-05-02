"""
Move all DELETE NOW files (from spring_clean_delete_list.txt) to a Desktop trash folder.
Safe: moves only, never deletes. Run from anywhere.
"""
import shutil
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")
from datetime import datetime
from pathlib import Path

LIST_FILE  = Path("C:/Users/Zhu-En/spring_clean_delete_list.txt")
TRASH_ROOT = Path("C:/Users/Zhu-En/Desktop")
TRASH_DIR  = TRASH_ROOT / f"spring_clean_trash_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def fmt_size(n):
    for u in ("B","KB","MB","GB","TB"):
        if abs(n) < 1024: return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"

def main():
    paths = [l.strip() for l in LIST_FILE.read_text(encoding="utf-8", errors="replace").splitlines() if l.strip()]
    total = len(paths)
    print(f"Files to move : {total:,}")
    print(f"Trash folder  : {TRASH_DIR}")
    print()

    TRASH_DIR.mkdir(parents=True, exist_ok=True)

    moved = skipped = errors = 0
    moved_bytes = 0

    for i, path_str in enumerate(paths, 1):
        src = Path(path_str)

        # Progress every 1000 files
        if i % 1000 == 0 or i == total:
            pct = i / total * 100
            bar_len = int(40 * i / total)
            bar = "█" * bar_len + "░" * (40 - bar_len)
            print(f"\r  [{bar}] {pct:.1f}%  {i:,}/{total:,}  moved={moved:,}  errors={errors}", end="", flush=True)

        if not src.exists():
            skipped += 1
            continue

        # Preserve relative structure inside trash dir so names don't collide
        try:
            rel = src.relative_to("C:/Users/Zhu-En")
        except ValueError:
            rel = Path(src.name)

        dest = TRASH_DIR / rel
        dest.parent.mkdir(parents=True, exist_ok=True)

        try:
            size = src.stat().st_size
            shutil.move(str(src), str(dest))
            moved += 1
            moved_bytes += size
        except (PermissionError, OSError, shutil.Error):
            errors += 1

    print()
    print()
    print(f"Done!")
    print(f"  Moved   : {moved:,} files  ({fmt_size(moved_bytes)})")
    print(f"  Skipped : {skipped:,}  (already gone)")
    print(f"  Errors  : {errors:,}  (locked / permission denied)")
    print()
    print(f"Trash folder  : {TRASH_DIR}")
    print(f"To permanently delete when satisfied:")
    print(f'  Remove-Item "{TRASH_DIR}" -Recurse -Force')

if __name__ == "__main__":
    main()
