#!/usr/bin/env python3
"""
review_hook.py - processes Claude code review JSON output.

Usage:
    python3 scripts/review_hook.py render <json_file> [config_file]   # print markdown review
    python3 scripts/review_hook.py insert <json_file> [config_file]   # insert FIXME comments
"""

import re
import sys
import json
import os
from collections import defaultdict

SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}


def load(json_file):
    with open(json_file) as f:
        raw = json.load(f)
    # Unwrap --output-format json envelope if present
    if "structured_output" in raw:
        return raw["structured_output"]
    return raw


def load_config(config_file=None):
    defaults = {"min_severity": "low", "categories": ["bug", "security", "quality"]}
    if not config_file or not os.path.exists(config_file):
        return defaults
    with open(config_file) as f:
        cfg = json.load(f)
    if cfg.get("min_severity") not in SEVERITY_ORDER:
        cfg["min_severity"] = defaults["min_severity"]
    if not isinstance(cfg.get("categories"), list) or not cfg["categories"]:
        cfg["categories"] = defaults["categories"]
    return cfg


def filter_issues(issues, cfg):
    min_rank = SEVERITY_ORDER[cfg["min_severity"]]
    allowed = set(cfg["categories"])
    return [i for i in issues
            if SEVERITY_ORDER.get(i["severity"], 99) <= min_rank
            and i["category"] in allowed]


def render(data, cfg):
    issues = filter_issues(data.get("issues", []), cfg)
    lines = ["## Code Review", "", f'_{data.get("summary", "")}_', ""]
    if not issues:
        lines.append("No issues found.")
    else:
        for i in sorted(issues, key=lambda x: SEVERITY_ORDER.get(x["severity"], 4)):
            loc = f'`{i["file"]}:{i["chunk_start_line"]}-{i["chunk_end_line"]}`'
            label = f'{i["severity"].upper()} {i["category"].upper()}'
            lines.append(f"- **[{label}]** {loc} — {i['description']}")
    print("\n".join(lines))


def insert(data, cfg):
    issues = filter_issues(data.get("issues", []), cfg)
    by_file = defaultdict(list)
    for issue in issues:
        by_file[issue["file"]].append(issue)

    for filepath, items in by_file.items():
        if not os.path.exists(filepath):
            print(f"  Skipping {filepath} (not found)")
            continue
        with open(filepath) as f:
            lines = f.readlines()
        ext = os.path.splitext(filepath)[1]
        prefix = "//" if ext in (".go", ".js", ".ts", ".c", ".cpp", ".java") else "#"
        # Insert in reverse order so earlier insertions don't shift subsequent line numbers
        for item in sorted(items, key=lambda x: x["chunk_start_line"], reverse=True):
            linenum = max(1, int(item["chunk_start_line"]))
            target_line = lines[linenum - 1] if linenum - 1 < len(lines) else ""
            indentation = target_line[: len(target_line) - len(target_line.lstrip())]
            label = f'{item["severity"].upper()} {item["category"].upper()}'
            comment = item["fixme_comment"].strip()
            # Strip leading comment prefix (// or #) the model may have included
            comment = re.sub(r"^(//|#)\s*", "", comment)
            # Strip any leading FIXME: the model may have included
            comment = re.sub(r"^FIXME\s*[:\-]?\s*", "", comment, flags=re.IGNORECASE)
            lines.insert(linenum - 1, f"{indentation}{prefix} FIXME [{label}]: {comment}\n")
            print(f"  {filepath}:{linenum}: [{label}] {item['fixme_comment']}")
        with open(filepath, "w") as f:
            f.writelines(lines)

    print("\nFIXMEs inserted. Fix them and commit before pushing.")


if __name__ == "__main__":
    if len(sys.argv) < 3 or sys.argv[1] not in ("render", "insert"):
        print(__doc__)
        sys.exit(1)

    cmd, json_file = sys.argv[1], sys.argv[2]
    config_file = sys.argv[3] if len(sys.argv) >= 4 else None
    data = load(json_file)
    cfg = load_config(config_file)

    if cmd == "render":
        render(data, cfg)
    elif cmd == "insert":
        insert(data, cfg)
