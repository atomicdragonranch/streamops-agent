"""Prompt loader for externalized agent system prompts and runbooks.

Prompts are stored as markdown files with YAML frontmatter in this directory
(or a custom directory specified by STREAMOPS_AGENT_PROMPT_DIR).
Runbooks live in the runbooks/ subdirectory, keyed by anomaly type.
"""

import logging
from pathlib import Path

import yaml

from streamops_mcp.config import config

logger = logging.getLogger("streamops-mcp.prompts")

_BUILTIN_DIR = Path(__file__).parent
_cache: dict[str, str] = {}
_metadata_cache: dict[str, dict] = {}
_runbook_cache: dict[str, str] = {}
_runbook_index: dict[str, Path] | None = None


def _resolve_dir() -> Path:
    if config.agent_prompt_dir:
        return Path(config.agent_prompt_dir)
    return _BUILTIN_DIR


def _resolve_runbook_dir() -> Path:
    if config.agent_runbook_dir:
        return Path(config.agent_runbook_dir)
    return _resolve_dir() / "runbooks"


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


def _build_runbook_index() -> dict[str, Path]:
    """Scan the runbooks directory and index files by anomaly_type from frontmatter."""
    global _runbook_index
    if _runbook_index is not None:
        return _runbook_index

    runbook_dir = _resolve_runbook_dir()
    _runbook_index = {}

    if not runbook_dir.is_dir():
        logger.debug("Runbook directory not found: %s", runbook_dir)
        return _runbook_index

    for path in sorted(runbook_dir.glob("*.md")):
        content = path.read_text(encoding="utf-8")
        fm_str, _ = _split_frontmatter(content)
        if fm_str:
            meta = yaml.safe_load(fm_str)
            if meta and "anomaly_type" in meta:
                _runbook_index[meta["anomaly_type"]] = path

    logger.info("Indexed %d runbook(s)", len(_runbook_index))
    return _runbook_index


def load_runbook(anomaly_type: str) -> str | None:
    """Load the runbook body for a given anomaly type.

    Returns None if no runbook exists for the anomaly type.
    """
    if anomaly_type in _runbook_cache:
        return _runbook_cache[anomaly_type]

    index = _build_runbook_index()
    path = index.get(anomaly_type)
    if path is None:
        return None

    content = path.read_text(encoding="utf-8")
    _, body = _split_frontmatter(content)
    _runbook_cache[anomaly_type] = body
    return body


def list_runbooks() -> list[str]:
    """Return available anomaly types that have runbooks."""
    return sorted(_build_runbook_index().keys())
