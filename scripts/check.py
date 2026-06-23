#!/usr/bin/env python3
"""Pre-commit validation for StreamOps Agent.

Validates consistency across tool definitions, config schema, and prompt files.
Run manually or integrate as a pre-commit hook / CI step.

Usage:
    python scripts/check.py
    # or via uv from mcp-server/:
    uv run python ../scripts/check.py
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MCP_SRC = REPO_ROOT / "mcp-server" / "src" / "streamops_mcp"

errors: list[str] = []
warnings: list[str] = []


def check_prompt_files():
    """Verify all prompt files exist and have valid YAML frontmatter."""
    prompt_dir = MCP_SRC / "prompts"
    expected = ["monitor.md", "diagnostic.md", "report.md"]

    for name in expected:
        path = prompt_dir / name
        if not path.exists():
            errors.append(f"Missing prompt file: {path.relative_to(REPO_ROOT)}")
            continue

        content = path.read_text(encoding="utf-8")
        if not content.startswith("---"):
            errors.append(f"Prompt {name} missing YAML frontmatter")
            continue

        try:
            end = content.index("---", 3)
        except ValueError:
            errors.append(f"Prompt {name} has unclosed YAML frontmatter")
            continue

        frontmatter = content[3:end].strip()
        required_keys = ["name", "description"]
        for key in required_keys:
            if f"{key}:" not in frontmatter:
                errors.append(f"Prompt {name} frontmatter missing '{key}'")


def check_runbook_files():
    """Verify runbook files have valid frontmatter with anomaly_type."""
    runbook_dir = MCP_SRC / "prompts" / "runbooks"
    if not runbook_dir.is_dir():
        warnings.append("Runbooks directory not found")
        return

    for path in sorted(runbook_dir.glob("*.md")):
        content = path.read_text(encoding="utf-8")
        if not content.startswith("---"):
            errors.append(f"Runbook {path.name} missing YAML frontmatter")
            continue

        try:
            end = content.index("---", 3)
        except ValueError:
            errors.append(f"Runbook {path.name} has unclosed YAML frontmatter")
            continue

        frontmatter = content[3:end].strip()
        if "anomaly_type:" not in frontmatter:
            errors.append(f"Runbook {path.name} frontmatter missing 'anomaly_type'")


def check_config_defaults():
    """Verify all config fields have defaults (no required-only fields)."""
    config_path = MCP_SRC / "config.py"
    if not config_path.exists():
        errors.append("config.py not found")
        return

    content = config_path.read_text(encoding="utf-8")
    in_class = False
    for line_num, line in enumerate(content.splitlines(), 1):
        if "class StreamOpsConfig" in line:
            in_class = True
            continue
        if in_class and line.strip() and not line.startswith(" ") and not line.startswith("\t"):
            break
        if in_class and ":" in line and "=" not in line and not line.strip().startswith("#") and not line.strip().startswith('"""'):
            field_line = line.strip()
            if field_line and not field_line.startswith("#") and not field_line.startswith('"""'):
                errors.append(f"config.py:{line_num} field without default: {field_line}")


def check_tool_definitions():
    """Verify tool definitions have required fields."""
    tools_path = MCP_SRC / "agent" / "tools.py"
    if not tools_path.exists():
        errors.append("tools.py not found")
        return

    content = tools_path.read_text(encoding="utf-8")
    tool_names = []
    in_tool = False
    current_name = None

    for line in content.splitlines():
        stripped = line.strip()
        if '"name":' in stripped:
            name = stripped.split('"name":')[1].strip().strip('",')
            tool_names.append(name)
            current_name = name
            in_tool = True
        if in_tool and '"description":' in stripped:
            desc = stripped.split('"description":')[1].strip().strip('",')
            if len(desc) < 10:
                warnings.append(f"Tool '{current_name}' has very short description")
            in_tool = False

    if not tool_names:
        errors.append("No tool definitions found in tools.py")
        return

    seen = set()
    for name in tool_names:
        if name in seen:
            errors.append(f"Duplicate tool definition: {name}")
        seen.add(name)


def main():
    print("StreamOps Agent pre-commit checks")
    print("=" * 40)

    check_prompt_files()
    check_runbook_files()
    check_config_defaults()
    check_tool_definitions()

    if warnings:
        print(f"\n{len(warnings)} warning(s):")
        for w in warnings:
            print(f"  WARN: {w}")

    if errors:
        print(f"\n{len(errors)} error(s):")
        for e in errors:
            print(f"  FAIL: {e}")
        sys.exit(1)
    else:
        print(f"\nAll checks passed ({0 if not warnings else len(warnings)} warnings)")
        sys.exit(0)


if __name__ == "__main__":
    main()
