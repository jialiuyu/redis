#!/usr/bin/env python3
"""
Post-process compile_commands.json to use relative paths.

Replaces absolute project root paths with relative paths so the file
is portable across machines. clangd resolves relative paths based on
the location of compile_commands.json (project root).

Usage:
    bear -- make -j$(nproc)
    python3 scripts/fix_compile_commands.py [compile_commands.json]
"""

import json
import os
import sys


def make_relative(path, root):
    """Convert an absolute path to a path relative to root, if applicable."""
    if path.startswith(root):
        rel = os.path.relpath(path, root)
        return rel
    return path


def fix_compile_commands(filepath="compile_commands.json"):
    root = os.path.dirname(os.path.abspath(filepath))

    with open(filepath, "r") as f:
        data = json.load(f)

    for entry in data:
        for key in ("directory", "file", "output"):
            if key in entry:
                entry[key] = make_relative(entry[key], root)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"Fixed {len(data)} entries in {filepath}")


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "compile_commands.json"
    fix_compile_commands(target)
