"""Minimal settings module for the wrds_crsp_compustat package.

Provides configuration via environment variables or .env file.
"""

import os
from pathlib import Path

from decouple import Config, RepositoryEnv, undefined
from decouple import config as _config_decouple


def find_project_root():
    """Find the project root directory by looking for marker files."""
    current_dir = Path(__file__).parent.parent.resolve()
    markers = ["pyproject.toml", ".env", "requirements.txt", ".git", "LICENSE"]

    while True:
        for marker in markers:
            marker_path = current_dir / marker
            if marker_path.exists():
                return current_dir
        parent_dir = current_dir.parent
        if parent_dir == current_dir:
            break
        current_dir = parent_dir

    return Path(__file__).parent.parent.resolve()


def load_config():
    """Load configuration from .env file."""
    project_root = find_project_root()
    env_file = project_root / ".env"
    if env_file.is_file():
        return Config(repository=RepositoryEnv(str(env_file)))
    return _config_decouple


_config = load_config()
_project_root = find_project_root()

# Default paths
_defaults = {
    "BASE_DIR": _project_root,
    "DATA_DIR": _project_root / "_data",
    "OUTPUT_DIR": _project_root / "_output",
}


def config(var_name, default=undefined, cast=undefined):
    """Get configuration value with fallback to defaults."""
    # Check environment/decouple first
    env_sentinel = object()
    env_value = _config(var_name, default=env_sentinel)
    if env_value is not env_sentinel:
        if cast is not undefined:
            return cast(env_value)
        return env_value

    # Check defaults
    if var_name in _defaults:
        value = _defaults[var_name]
        if cast is not undefined:
            return cast(value)
        return value

    # Use provided default
    return _config(var_name, default=default, cast=cast)
