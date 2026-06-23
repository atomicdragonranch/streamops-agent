"""Prompt loader for externalized agent system prompts.

Prompts are stored as markdown files with YAML frontmatter in this directory
(or a custom directory specified by STREAMOPS_AGENT_PROMPT_DIR).
"""

from pathlib import Path

import yaml

from streamops_mcp.config import config

_BUILTIN_DIR = Path(__file__).parent
_cache: dict[str, str] = {}
_metadata_cache: dict[str, dict] = {}


def _resolve_dir() -> Path:
    if config.agent_prompt_dir:
        return Path(config.agent_prompt_dir)
    return _BUILTIN_DIR


def _split_frontmatter(content: str) -> tuple[str, str]:
    """Split markdown content into YAML frontmatter and body.

    Returns (frontmatter_str, body_str). If no frontmatter found,
    returns ("", full_content).
    """
    if not content.startswith("---"):
        return "", content
    end = content.index("---", 3)
    frontmatter = content[3:end].strip()
    body = content[end + 3:].strip()
    return frontmatter, body


def load_prompt(name: str) -> str:
    """Load a prompt's body text by name, stripping YAML frontmatter.

    Caches the result after first load.
    """
    if name in _cache:
        return _cache[name]

    path = _resolve_dir() / f"{name}.md"
    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found: {path}")

    content = path.read_text(encoding="utf-8")
    _, body = _split_frontmatter(content)
    _cache[name] = body
    return body


def load_prompt_metadata(name: str) -> dict:
    """Load a prompt's YAML frontmatter as a dict.

    Caches the result after first load.
    """
    if name in _metadata_cache:
        return _metadata_cache[name]

    path = _resolve_dir() / f"{name}.md"
    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found: {path}")

    content = path.read_text(encoding="utf-8")
    frontmatter_str, _ = _split_frontmatter(content)
    metadata = yaml.safe_load(frontmatter_str) if frontmatter_str else {}
    _metadata_cache[name] = metadata
    return metadata
