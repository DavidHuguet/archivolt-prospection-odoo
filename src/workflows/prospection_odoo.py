"""Workflow Archi-Volt : prospection data gouvernance régionale -> activité Odoo.

Chaque exécution :
1. Lance une recherche web sur les entreprises régionales (Bretagne, Pays de
   la Loire, Normandie, Nouvelle-Aquitaine) recrutant en data gouvernance ou
   portant de gros projets data.
2. Génère à partir des résultats une liste de prospection en texte brut.
3. Se connecte à l'Odoo Archi-Volt (API externe XML-RPC, clé API dans ODOO_API_KEY).
4. Crée une activité « À faire » (type To-Do) sur la tâche « Suivi commercial »
   avec la liste en note, échéance à la date du jour.

Notes techniques :
- Odoo 19 : mail.activity.create avec res_model_id (ir.model) + res_id,
  activity_type_id, date_deadline.
- Le type « À faire » de l'interface FR s'appelle « To-Do » en base.
- « Suivi commercial » est une tâche récurrente : on cible l'occurrence EN COURS
  (la plus récente dont le début est passé).
- Mode test : si input.note_test est fournie, la génération est sautée.
- Mode diagnostic : note_test = "DUMP_TYPES" -> liste les types d'activité Odoo.
"""

import asyncio
import os
import re
from datetime import date, timedelta

import mistralai.workflows as workflows
from pydantic import BaseModel

ODOO_URL = os.environ.get("ODOO_URL", "https://archi-volt.odoo.com").rstrip("/")
ODOO_TASK_NAME = os.environ.get("ODOO_TASK_NAME", "Suivi commercial")
ODOO_ACTIVITY_TYPE_NAME = os.environ.get("ODOO_ACTIVITY_TYPE_NAME", "To-Do")
ODOO_ACTIVITY_DELAY_DAYS = int(os.environ.get("ODOO_ACTIVITY_DELAY_DAYS", "0"))
GENERATION_TIMEOUT_S = int(os.environ.get("GENERATION_TIMEOUT_S", "300"))

PROMPT = (
    "Génère une liste de prospection pour un cabinet de conseil indépendant en "
    "data gouvernance (Archi-Volt). "
    "Cible : ETI, PME et startups en Bretagne, Pays de la Loire, Normandie et "
    "Nouvelle-Aquitaine qui recherchent des compétences en data gouvernance ou "
    "qui ont des difficultés ou de gros projets data (RGPD, qualité de données, "
    "IA, transformation digitale). "
    "Appuie-toi sur les résultats de recherche web fournis (offres d'emploi, "
    "actualités, projets annoncés) ; pour chaque entreprise, cite la source "
    "(site / média et date) entre parenthèses après le champ Pourquoi. "
    "FORMAT DE SORTIE IMPÉRATIF : texte brut, sans aucun markdown (pas de #, "
    "pas de **, pas de tableaux, pas de puces markdown). "
    "Structure exacte : "
    "une première ligne 'PROSPECTION DATA GOUVERNANCE - ENTREPRISES REGIONALES' ; "
    "puis pour chaque entreprise, un bloc numéroté sur 4 lignes : "
    "ligne 1 : numéro. Nom - Région : ... - Secteur : ... ; "
    "ligne 2 : ligne de tirets ; "
    "ligne 3 : 'Pourquoi : ' suivi de la raison et de sa source entre parenthèses ; "
    "ligne 4 : 'Pertinence : X/5'. "
    "Une ligne vide entre chaque bloc. 10 entreprises maximum, en français, "
    "classées de la plus pertinente à la moins pertinente."
)


class ProspectionInput(BaseModel):
    """Entrée du workflow (fournie par le schedule)."""

    odoo_db: str = ""
    odoo_user: str = ""
    agent_id: str = ""
    note_test: str = ""  # mode test : si remplie, saute la génération


class OdooActivityResult(BaseModel):
    activity_id: int = 0
    task_id: int = 0


@workflows.activity()
async def generer_liste_entreprises(agent_id: str) -> str:
    """Recherche web puis génération de la liste de prospection.

    1. Recherche web (outil natif Mistral) sur les recrutements et projets
       data gouvernance dans les régions cibles.
    2. Synthèse en liste structurée à partir des résultats sourcés.
    Repli : appel simple sans recherche si l'outil web n'est pas disponible.
    """
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
            pass  # repli sur la recherche web ci-dessous

    try:
        response = await mistral.chat.complete_async(
            model="mistral-small-latest",
            messages=[{"role": "user", "content": PROMPT}],
            tools=[{"type": "web_search"}],
        )
        return response.choices[0].message.content
    except Exception:
        pass  # repli : sans recherche web (mémoire du modèle)

    response = await mistral.chat.complete_async(
        model="mistral-small-latest",
        messages=[{"role": "user", "content": PROMPT}],
    )
    return response.choices[0].message.content


def _nettoyer_note(note: str) -> str:
    """Nettoyage markdown résiduel : la note doit être du texte brut lisible."""
    note = re.sub(r"#+\s*", "", note)
    note = note.replace("**", "").replace("__", "")
    note = re.sub(r"^[ \t]*[-*][ \t]+", "- ", note, flags=re.M)
    note = re.sub(r"\n{3,}", "\n\n", note)
    return note


@workflows.activity()
async def lister_types_activite(odoo_db: str, odoo_user: str) -> list:
    """Diagnostic : liste tous les types d'activité Odoo (id, nom, catégorie, délai)."""
    import xmlrpc.client

    api_key = os.environ["ODOO_API_KEY"]

    def _run() -> list:
        common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
        uid = common.authenticate(odoo_db, odoo_user, api_key, {})
        if not uid:
            raise RuntimeError("Authentification Odoo refusée (diagnostic).")
        models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
        return models.execute_kw(
            odoo_db, uid, api_key,
            "mail.activity.type", "search_read",
            [[]],
            {"fields": ["id", "name", "category", "delay_count", "delay_unit", "active", "icon"], "order": "id"},
        )

    return await asyncio.to_thread(_run)


@workflows.activity()
async def creer_activite_odoo(note: str, odoo_db: str, odoo_user: str) -> OdooActivityResult:
    """Crée une activité « À faire » (To-Do) sur la tâche « Suivi commercial » d'Odoo (XML-RPC)."""
    import xmlrpc.client

    api_key = os.environ["ODOO_API_KEY"]
    common_url = f"{ODOO_URL}/xmlrpc/2/common"
    object_url = f"{ODOO_URL}/xmlrpc/2/object"
    note = _nettoyer_note(note)

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

        # 1. Tâche cible : occurrence EN COURS de la tâche récurrente.
        #    La plus récente dont le début est passé.
        today = date.today().isoformat()
        tasks = call(
            "project.task",
            "search_read",
            [[
                ["name", "ilike", ODOO_TASK_NAME],
                ["planned_date_begin", "<=", f"{today} 23:59:59"],
            ], ["id", "name"]],
            {"limit": 1, "order": "planned_date_begin desc"},
        )
        if not tasks:
            tasks = call(
                "project.task",
                "search_read",
                [[["name", "ilike", ODOO_TASK_NAME]], ["id", "name"]],
                {"limit": 1, "order": "id desc"},
            )
        if not tasks:
            raise RuntimeError(f"Tâche Odoo « {ODOO_TASK_NAME} » introuvable")
        task_id = tasks[0]["id"]

        # 2. Type d'activité To-Do (« À faire » en interface FR).
        #    Recherche par nom exact, puis par icône fa-check,
        #    sinon échec explicite (jamais de repli sur Mail).
        types = call(
            "mail.activity.type",
            "search_read",
            [[
                ["name", "in", [ODOO_ACTIVITY_TYPE_NAME, "To Do", "Todo", "To-do", "To Do"]],
                ["active", "=", True],
            ], ["id", "name", "category", "icon"]],
            {"limit": 1},
        )
        if not types:
            types = call(
                "mail.activity.type",
                "search_read",
                [[["icon", "=", "fa-check"], ["active", "=", True]], ["id", "name", "category", "icon"]],
                {"limit": 1},
            )
        if not types:
            raise RuntimeError(
                "Type d'activité « À faire » (To-Do) introuvable dans Odoo"
            )
        act_type_id = types[0]["id"]

        # 3. ir.model id de project.task (Odoo 19 : res_model_id, pas res_model)
        model_ids = call(
            "ir.model",
            "search_read",
            [[["model", "=", "project.task"]], ["id"]],
            {"limit": 1},
        )
        if not model_ids:
            raise RuntimeError("ir.model project.task introuvable")
        res_model_id = model_ids[0]["id"]

        # 4. Création directe de l'activité (champs Odoo 19), échéance = J+0
        due = (date.today() + timedelta(days=ODOO_ACTIVITY_DELAY_DAYS)).isoformat()
        activity_id = call(
            "mail.activity",
            "create",
            [{
                "res_model_id": res_model_id,
                "res_id": task_id,
                "activity_type_id": act_type_id,
                "summary": "Prospection data gouvernance - entreprises régionales",
                "note": note[:60000],
                "date_deadline": due,
                "user_id": uid,
            }],
        )

        return OdooActivityResult(activity_id=activity_id, task_id=task_id)

    return await asyncio.to_thread(_run)


@workflows.workflow.define(
    name="prospection-odoo",
    workflow_display_name="Prospection data gouvernance -> Odoo",
    workflow_description=(
        "Recherche web puis liste hebdomadaire des entreprises régionales "
        "(Bretagne, Pays de la Loire, Normandie, Nouvelle-Aquitaine) recrutant "
        "en data gouvernance, ajoutée comme activité « À faire » sur la tâche "
        "Odoo « Suivi commercial »."
    ),
)
class ProspectionOdooWorkflow:
    @workflows.workflow.entrypoint
    async def run(self, input: ProspectionInput) -> dict:
        # Mode diagnostic : lister les types d'activité
        if input.note_test == "DUMP_TYPES":
            dump = await lister_types_activite(input.odoo_db, input.odoo_user)
            return {"diagnostic_types": dump}
        # Mode test : note fournie -> pas de génération
        if input.note_test:
            liste = input.note_test
        else:
            try:
                liste = await asyncio.wait_for(generer_liste_entreprises(input.agent_id), timeout=GENERATION_TIMEOUT_S)
            except asyncio.TimeoutError:
                raise RuntimeError(
                    f"Génération de la liste interrompue : dépassé {GENERATION_TIMEOUT_S}s. "
                    "Relance l'exécution ou augmente GENERATION_TIMEOUT_S."
                )
        result = await creer_activite_odoo(liste, input.odoo_db, input.odoo_user)
        return {
            "odoo_task_id": result.task_id,
            "activity_id": result.activity_id,
            "activite": "créée",
            "mode_test": bool(input.note_test),
            "apercu_note": liste[:500],
        }
