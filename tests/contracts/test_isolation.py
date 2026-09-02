import ast
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CONTRACTS_DIRECTORY = REPOSITORY_ROOT / "src" / "enervision_contracts"

# Un consumer doit pouvoir importer les contrats sans heriter du client HTTP, du client
# Kafka ni de l'outillage du collecteur. C'est la raison d'etre du paquet separe.
FORBIDDEN_ROOT_MODULES = frozenset(
    {
        "enervision_etl",
        "requests",
        "urllib3",
        "confluent_kafka",
        "prometheus_client",
        "structlog",
        "typer",
    }
)

CONTRACT_MODULES = sorted(CONTRACTS_DIRECTORY.glob("*.py"))


def imported_root_modules(module_path: Path) -> set[str]:
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            roots.add(node.module.split(".")[0])
    return roots


def test_the_contracts_package_is_not_empty() -> None:
    assert CONTRACT_MODULES, "aucun module de contrat trouve, le chemin a du changer"


@pytest.mark.parametrize("module_path", CONTRACT_MODULES, ids=lambda path: path.name)
def test_a_contract_module_pulls_no_infrastructure_dependency(module_path: Path) -> None:
    forbidden = imported_root_modules(module_path) & FORBIDDEN_ROOT_MODULES

    assert not forbidden, (
        f"{module_path.name} importe {sorted(forbidden)}, "
        "ce qui obligerait les consumers a installer ces dependances"
    )


@pytest.mark.parametrize("module_path", CONTRACT_MODULES, ids=lambda path: path.name)
def test_a_contract_module_never_reaches_outside_its_package(module_path: Path) -> None:
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    escaping_imports = [
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.level > 1
    ]

    assert not escaping_imports, (
        f"{module_path.name} remonte hors du paquet des contrats : {escaping_imports}"
    )
