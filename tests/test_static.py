"""Architecture rules checked on the source, not at runtime."""

import ast
from pathlib import Path

APP = Path(__file__).resolve().parent.parent / "app"


def imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text())
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    return names


def test_domain_is_framework_free():
    forbidden = {"fastapi", "aiosqlite", "prometheus_client", "asyncio"}
    assert not imported_modules(APP / "domain.py") & forbidden


def test_repository_never_blocks_the_event_loop():
    assert "sqlite3" not in imported_modules(APP / "repository.py")


def test_sql_only_interpolates_module_constants():
    tree = ast.parse((APP / "repository.py").read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            for value in node.values:
                if isinstance(value, ast.FormattedValue):
                    assert isinstance(value.value, ast.Name)
                    assert value.value.id == "_FENCE"
