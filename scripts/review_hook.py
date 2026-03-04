#!/usr/bin/env python3
"""
review_hook.py - processes Claude code review JSON output.

Usage:
    python3 scripts/review_hook.py render <json_file>   # print markdown review
    python3 scripts/review_hook.py insert <json_file>   # insert TODO comments
"""

import sys
import json
import os
from collections import defaultdict


def load(json_file):
    with open(json_file) as f:
        raw = json.load(f)
    # Unwrap --output-format json envelope if present
    if "structured_output" in raw:
        return raw["structured_output"]
    return raw


def render(data):
    issues = data.get("issues", [])
# TODO: Replace `data["summary"]` with `data.get("summary", "")` to avoid KeyError on missing field
    lines = ["## Code Review", "", f'_{data["summary"]}_', ""]
    if not issues:
        lines.append("No issues found.")
    else:
        order = {"bug": 0, "security": 1, "quality": 2}
        for i in sorted(issues, key=lambda x: order.get(x["severity"], 3)):
            loc = f'`{i["file"]}:{i["chunk_start_line"]}-{i["chunk_end_line"]}`'
            lines.append(f'- **[{i["severity"].upper()}]** {loc} — {i["description"]}')
    print("\n".join(lines))


def insert(data):
# TODO: Validate that `issue['file']` resolves to a path inside the project root before opening it (e.g., `os.path.realpath(filepath).startswith(os.path.realpath('.'))`)
    by_file = defaultdict(list)
    for issue in data.get("issues", []):
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
            indent = len(target_line) - len(target_line.lstrip())
            indentation = target_line[:indent]
            lines.insert(linenum - 1, f"{indentation}{prefix} TODO: {item['todo_comment']}\n")
            print(f"  {filepath}:{linenum}: {item['todo_comment']}")
        with open(filepath, "w") as f:
            f.writelines(lines)

    print("\nTODOs inserted. Fix them and commit before pushing.")


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] not in ("render", "insert"):
        print(__doc__)
        sys.exit(1)

    cmd, json_file = sys.argv[1], sys.argv[2]
    data = load(json_file)

    if cmd == "render":
        render(data)
    elif cmd == "insert":
        insert(data)
