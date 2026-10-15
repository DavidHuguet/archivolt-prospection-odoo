"""Point d'entrée du worker — déploiement géré Mistral Cloud.

Cherche le SDK sous tous ses noms d'import possibles puis démarre le worker.
"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "src"))


def _find_sdk():
    """Retourne le module SDK exposant run_worker / discover_workflows."""
    import importlib

    names = [
        "mistralai_workflows",
        "mistralai.workflows",
        "mistralai_workflows.worker",
        "mistralai_workflows.sdk",
    ]
    for n in names:
        try:
            mod = importlib.import_module(n)
        except Exception:
            continue
        for attr in ("run_worker",):
            if hasattr(mod, attr):
                return mod
    # dernier recours : chercher l'attribut dans le package mistralai
    try:
        import mistralai

        for attr in ("run_worker", "discover_workflows"):
            if hasattr(mistralai, attr):
                return mistralai
    except Exception:
        pass
    raise RuntimeError(
        "SDK mistralai-workflows introuvable ; modules dispo : "
        + ", ".join(sorted(m.name for m in __import__("pkgutil").iter_modules()))
    )


async def main() -> None:
    sdk = _find_sdk()

    # Découverte automatique si disponible, sinon liste explicite
    discovered = None
    if hasattr(sdk, "discover_workflows"):
        try:
            discovered = sdk.discover_workflows()
        except Exception:
            discovered = None
    if not discovered:
        from workflows.prospection_odoo import ProspectionOdooWorkflow

        discovered = [ProspectionOdooWorkflow]

    await sdk.run_worker(discovered)


if __name__ == "__main__":
    asyncio.run(main())
