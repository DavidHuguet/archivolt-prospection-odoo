"""Workflow Archi-Volt : prospection data gouvernance régionale -> activité Odoo.

Chaque exécution :
1. Génère une liste d'ETI / PME / startups en Bretagne, Pays de la Loire,
   Normandie et Nouvelle-Aquitaine qui recherchent des compétences en data
   gouvernance ou qui ont de gros projets data.
2. Se connecte à l'Odoo Archi-Volt (API externe XML-RPC, clé API dans ODOO_API_KEY).
3. Crée une activité « À faire » sur la tâche « Suivi commercial » avec la
   liste en note.

Note technique : activity_schedule prend ses paramètres en kwargs
(summary, note, date_deadline, act_type_id...), pas en dict positionnel.
"""

import os
from datetime import date, timedelta

import mistralai.workflows as workflows
from pydantic import BaseModel

ODOO_URL = os.environ.get("ODOO_URL", "https://archi-volt.odoo.com").rstrip("/")
ODOO_TASK_NAME = os.environ.get("ODOO_TASK_NAME", "Suivi commercial")
ODOO_ACTIVITY_TYPE_NAME = os.environ.get("ODOO_ACTIVITY_TYPE_NAME", "À faire")

PROMPT = (
    "Liste des acteurs Bretagne, Pays de la Loire, Normandie, Nouvelle-Aquitaine "
    "d'ETI, PME, Startup, qui recherchent des compétences en data gouvernance ou "
    "qui ont des difficultés ou des gros projets data. "
    "Pour chaque entreprise : nom, région, secteur, pourquoi elle est pertinente "
    "(recrutement data gouvernance / projet data identifié), et un indice de "
    "pertinence 1-5. Liste structurée, 10 entreprises maximum, en français."
)


class ProspectionInput(BaseModel):
    """Entrée du workflow (fournie par le schedule)."""

    odoo_db: str = ""
    odoo_user: str = ""
    agent_id: str = ""


class OdooActivityResult(BaseModel):
    activity_id: int = 0
    task_id: int = 0


@workflows.activity()
async def generer_liste_entreprises(agent_id: str) -> str:
    """Génère la liste de prospection via un agent Mistral (web search) ou le modèle seul."""
    from mistralai.workflows.client import get_mistral_client

    mistral = get_mistral_client()

    if agent_id:
        try:
            response = await mistral.agents.complete_async(
                agent_id=agent_id,
                messages=[{"role": "user", "content": PROMPT}],
            )
            return response.choices[0].message.content
        except Exception:
            pass  # repli sur le chat simple ci-dessous

    response = await mistral.chat.complete_async(
        model="mistral-large-latest",
        messages=[{"role": "user", "content": PROMPT}],
    )
    return response.choices[0].message.content


@workflows.activity()
async def creer_activite_odoo(note: str, odoo_db: str, odoo_user: str) -> OdooActivityResult:
    """Crée une activité « À faire » sur la tâche « Suivi commercial » d'Odoo (XML-RPC)."""
    import asyncio
    import xmlrpc.client

    api_key = os.environ["ODOO_API_KEY"]
    common_url = f"{ODOO_URL}/xmlrpc/2/common"
    object_url = f"{ODOO_URL}/xmlrpc/2/object"

    def _run() -> OdooActivityResult:
        common = xmlrpc.client.ServerProxy(common_url)
        uid = common.authenticate(odoo_db, odoo_user, api_key, {})
        if not uid:
            raise RuntimeError(
                f"Authentification Odoo refusée. Vérifie : base='{odoo_db}', "
                f"utilisateur='{odoo_user}', clé API (secret ODOO_API_KEY)."
            )

        models = xmlrpc.client.ServerProxy(object_url)

        def call(model: str, method: str, args: list, kwargs: dict | None = None):
            return models.execute_kw(odoo_db, uid, api_key, model, method, args, kwargs or {})

        # 1. Tâche cible
        tasks = call(
            "project.task",
            "search_read",
            [[["name", "ilike", ODOO_TASK_NAME]], ["id", "name"]],
            {"limit": 1},
        )
        if not tasks:
            raise RuntimeError(f"Tâche Odoo « {ODOO_TASK_NAME} » introuvable")
        task_id = tasks[0]["id"]

        # 2. Type d'activité « À faire »
        types = call(
            "mail.activity.type",
            "search_read",
            [[["name", "ilike", ODOO_ACTIVITY_TYPE_NAME], ["active", "=", True]], ["id"]],
            {"limit": 1},
        )
        act_kwargs = {"act_type_id": types[0]["id"]} if types else {}

        # 3. Planification de l'activité (paramètres en kwargs, pas en positionnel)
        due = (date.today() + timedelta(days=7)).isoformat()
        call(
            "project.task",
            "activity_schedule",
            [[task_id]],
            {
                **act_kwargs,
                "summary": "Prospection data gouvernance - entreprises régionales",
                "note": note[:60000],
                "date_deadline": due,
            },
        )

        # 4. Récupérer l'id de l'activité créée
        acts = call(
            "mail.activity",
            "search_read",
            [[["res_model", "=", "project.task"], ["res_id", "=", task_id]], ["id"]],
            {"order": "id desc", "limit": 1},
        )
        activity_id = acts[0]["id"] if acts else 0

        return OdooActivityResult(activity_id=activity_id, task_id=task_id)

    return await asyncio.to_thread(_run)


@workflows.workflow.define(
    name="prospection-odoo",
    workflow_display_name="Prospection data gouvernance -> Odoo",
    workflow_description=(
        "Liste hebdomadaire des entreprises régionales (Bretagne, Pays de la Loire, "
        "Normandie, Nouvelle-Aquitaine) recrutant en data gouvernance, ajoutée comme "
        "activité « À faire » sur la tâche Odoo « Suivi commercial »."
    ),
)
class ProspectionOdooWorkflow:
    @workflows.workflow.entrypoint
    async def run(self, input: ProspectionInput) -> dict:
        liste = await generer_liste_entreprises(input.agent_id)
        result = await creer_activite_odoo(liste, input.odoo_db, input.odoo_user)
        return {
            "odoo_task_id": result.task_id,
            "activity_id": result.activity_id,
            "activite": "créée",
            "apercu_note": liste[:500],
        }
