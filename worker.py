"""Point d'entrée du worker pour le déploiement géré Mistral Cloud."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))


async def main() -> None:
    import mistralai_workflows
    from workflows.prospection_odoo import ProspectionOdooWorkflow

    await mistralai_workflows.run_worker([ProspectionOdooWorkflow])


if __name__ == "__main__":
    asyncio.run(main())
