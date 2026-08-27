"""Chart Agent pattern library. Detectors self-register on import via registry.load_builtin()."""
from .base import PatternDetector, PatternOccurrence  # noqa: F401
from . import registry  # noqa: F401
