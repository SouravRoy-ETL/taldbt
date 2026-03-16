"""
Project Scanner: discovers all .item, .screenshot, .properties, and context files
in a Talend project directory and classifies them. Cross-platform (Windows/Mac/Linux).
"""
import re
from pathlib import Path


def scan_project(input_dir: str) -> dict:
    """Walk the Talend project directory and return classified file lists."""
    result = {"process_jobs": [], "joblets": [], "contexts": []}
    root = Path(input_dir)

    for item_path in sorted(root.rglob("*.item")):
        rel = item_path.relative_to(root)
        basename = item_path.stem  # filename without .item

        # Extract name and version: "my_job_0.1" → ("my_job", "0.1")
        match = re.match(r"^(.+?)_(\d+\.\d+)$", basename)
        if match:
            name, version = match.group(1), match.group(2)
        else:
            name, version = basename, "0.1"

        # Look for corresponding .screenshot
        screenshot_path = item_path.with_suffix(".screenshot")

        entry = {
            "name": name,
            "version": version,
            "path": str(item_path),
            "rel_path": str(rel),
            "screenshot_path": str(screenshot_path) if screenshot_path.exists() else "",
        }

        # Classify by directory (use forward slashes for consistent matching)
        rel_posix = rel.as_posix().lower()
        if "joblet" in rel_posix:
            result["joblets"].append(entry)
        elif "context" in rel_posix:
            result["contexts"].append(entry)
        elif "process" in rel_posix:
            result["process_jobs"].append(entry)
        else:
            result["process_jobs"].append(entry)

    return result
