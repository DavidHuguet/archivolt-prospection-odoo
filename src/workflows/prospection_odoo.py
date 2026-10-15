"""Workflow Archi-Volt : prospection data gouvernance régionale -> activité Odoo.

Chaque exécution :
1. Lance une recherche web (WebSearchTool, exécuté côté plateforme Mistral)
   sur les entreprises régionales (Bretagne, Pays de la Loire, Normandie,
   Nouvelle-Aquitaine) recrutant en data gouvernance ou portant de gros
   projets data — signaux des 6 derniers mois uniquement.
2. Génère à partir des résultats une liste de prospection en texte brut avec
   fiche entreprise (SIREN, siège, APE, effectifs, CA).
3. Se connecte à l'Odoo Archi-Volt (API externe XML-RPC, clé API dans ODOO_API_KEY).
4. Crée une activité « À faire » (type To-Do) sur la tâche « Suivi commercial »
   avec la liste en note, échéance à la date du jour.

Notes techniques :
- Recherche web : agent workflows (mistralai.workflows.plugins.mistralai)
  avec WebSearchTool() et RemoteSession (LocalSession ignorerait l'outil).
- Odoo 19 : mail.activity.create avec res_model_id (ir.model) + res_id,
  activity_type_id, date_deadline.
- Le type « À faire » de l'interface FR s'appelle « To-Do » en base.
- « Suivi commercial » est une tâche récurrente : on cible l'occurrence EN COURS
  (la plus récente dont le début est passé).
- Mode test : si input.note_test est fournie, la génération est sautée.
- Mode diagnostic : note_test = "DUMP_TYPES" -> liste les types d'activité Odoo.
- Mode génération seule : note_test = "GEN_ONLY" -> renvoie la liste générée
  (avec recherche web) sans rien écrire dans Odoo.
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
GENERATION_TIMEOUT_S = int(os.environ.get("GENERATION_TIMEOUT_S", "600"))
GEN_MODEL = os.environ.get("GEN_MODEL", "mistral-medium-latest")

PROMPT = (
    "Génère une liste de prospection pour un cabinet de conseil indépendant en "
    "data gouvernance (Archi-Volt). "
    "CIBLE STRICTE : uniquement des ETI, PME et startups dont le siège ou "
    "l'établissement concerné se situe en Bretagne, Pays de la Loire, Normandie "
    "ou Nouvelle-Aquitaine. "
    "EXCLUS ABSOLUMENT : administrations publiques, ministères, collectivités "
    "territoriales, métropoles, communautés d'agglomération, conseils "
    "régionaux/départementaux, associations, fédérations professionnelnelles, "
    "organismes publics — et toute entreprise dont le site concerné est hors "
    "de ces 4 régions (notamment Centre-Val de Loire, Île-de-France). "
    "Un seul hors-cible décrédibilise toute la liste : quand tu hésites sur "
    "une entreprise, exclue-la. "
    "Signaux recherchés : entreprises recrutant des compétences en data "
    "gouvernance / data management / DPO / data quality, ou portant de gros "
    "projets data (RGPD, qualité de données, IA, transformation digitale). "
    "FRAÎCHEUR OBLIGATOIRE : ne retiens que des signaux des 6 DERNIERS MOIS "
    "(offres d'emploi publiées récemment, projets ou recrutements annoncés "
    "dans les 6 derniers mois). Ignore tout signal plus ancien. Si une "
    "information d'entreprise n'est pas vérifiable dans les 6 derniers mois, "
    "ne l'inclus pas. "
    "NOMBRE ET PRIORITE : objectif 10 entreprises, mais LA QUALITÉ PRIME SUR "
    "LE NOMBRE. Ne JAMAIS inclure une entreprise qui viole un critère "
    "(région, type, fraîcheur) pour atteindre le quota. Mène d'abord des "
    "recherches complémentaires (autres mots-clés : data governance, data "
    "manager, DPO, CIL, data quality, RGPD, référent données ; autres villes "
    "des 4 régions : Rennes, Nantes, Brest, Caen, Rouen, Le Havre, Angers, "
    "Tours est HORS ZONE, Bordeaux, Bayonne, Pau, Limoges, La Rochelle, "
    "Quimper, Vannes, Laval, Le Mans, Cherbourg). "
    "Si après ces recherches tu ne trouves que 6 à 8 entreprises conformes, "
    "c'est acceptable : liste-les toutes. Mieux vaut 7 cibles parfaites que "
    "10 dont certaines hors-cible. "
    "AUCUN DOUBLON : jamais deux fois le même SIREN ni deux entités du même "
    "groupe (ex. si une filiale est déjà listée, ne liste pas la maison mère "
    "ni une autre filiale avec le même SIREN). "
    "Appuie-toi sur les résultats de ta recherche web (offres d'emploi, "
    "actualités, projets annoncés) ; pour chaque entreprise, cite la source "
    "(site / média et date) entre parenthèses après le champ Pourquoi. "
    "FICHE ENTREPRISE OBLIGATOIRE : pour chaque entreprise, recherche son "
    "numéro SIREN, la ville du siège social, son code APE avec son libellé, "
    "son nombre d'employés (dernier connu, préciser l'année) et son dernier "
    "chiffre d'affaires connu (préciser l'année). Si une information est "
    "introuvable, écris 'non trouvé' pour ce champ plutôt qu'une invention. "
    "FORMAT DE SORTIE IMPÉRATIF : texte brut, sans aucun markdown (pas de #, "
    "pas de **, pas de tableaux, pas de puces markdown). "
    "Structure exacte : "
    "une première ligne 'PROSPECTION DATA GOUVERNANCE - ENTREPRISES REGIONALES' ; "
    "puis pour chaque entreprise, un bloc numéroté sur 6 lignes : "
    "ligne 1 : numéro. Nom - Région : ... - Secteur : ... ; "
    "ligne 2 : ligne de tirets ; "
    "ligne 3 : 'SIREN : ... - Siège : ... - APE : ... (libellé)' ; "
    "ligne 4 : 'Effectifs : ... - Dernier CA : ...' ; "
    "ligne 5 : 'Pourquoi : ' suivi de la raison et de sa source entre parenthèses ; "
    "ligne 6 : 'Pertinence : X/5'. "
    "Une ligne vide entre chaque bloc. Réponds en français, classées de la "
    "plus pertinente à la moins pertinente."
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
    """Génération de la liste de prospection avec recherche web réelle.

    Utilise un agent workflows avec WebSearchTool() via RemoteSession :
    l'outil de recherche est exécuté côté plateforme Mistral. En cas d'échec,
    erreur explicite (pas de repli mémoire silencieux).
    """
    import mistralai.workflows.plugins.mistralai as workflows_mistralai
    from mistralai.client.models import WebSearchTool

    agent = workflows_mistralai.Agent(
        id=agent_id or None,
        model=GEN_MODEL,
        name="archivolt-prospection-websearch",
        description=(
            "Recherche web de prospection data gouvernance (régions Ouest & Sud-Ouest)"
        ),
        instructions=(
            "Tu es un assistant de prospection B2B pour Archi-Volt, cabinet de "
            "conseil indépendant en data gouvernance. Utilise la recherche web "
            "pour trouver des signaux récents (6 derniers mois) : offres d'emploi, "
            "annonces de projets, actualités. "
            "ENTREPRISES PRIVÉES UNIQUEMENT (ETI/PME/startups) : jamais "
            "d'administrations, de collectivités, d'associations ni d'organismes "
            "publics. Strictement en Bretagne, Pays de la Loire, Normandie, "
            "Nouvelle-Aquitaine — jamais de Centre-Val de Loire ni d'Île-de-France. "
            "La qualité prime sur le nombre : mieux vaut 6 cibles parfaites que "
            "10 dont une hors-cible. Jamais de doublons (même SIREN). "
            "Réponds toujours en texte brut structuré, sans markdown, avec les "
            "sources citées."
        ),
        tools=[WebSearchTool()],
    )
    session = workflows_mistralai.RemoteSession()
    outputs = await workflows_mistralai.Runner.run(
        agent=agent,
        inputs=PROMPT,
        session=session,
    )
    # Runner.run renvoie les outputs de l'agent : texte final
    if isinstance(outputs, str):
        return outputs
    for attr in ("content", "text", "output"):
        if hasattr(outputs, attr):
            return getattr(outputs, attr)
    return str(outputs)


def _nettoyer_note(note: str) -> str:
    """Nettoyage markdown résiduel : la note doit être du texte brut lisible."""
    note = re.sub(r"#+s*", "", note)
    note = note.replace("**", "").replace("__", "")
    note = re.sub(r"^[ 	]*[-*][ 	]+", "- ", note, flags=re.M)
    note = re.sub(r"
{3,}", "

", note)
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
        # Mode génération seule : liste renvoyée, rien d'écrit dans Odoo
        if input.note_test == "GEN_ONLY":
            try:
                liste = await asyncio.wait_for(generer_liste_entreprises(input.agent_id), timeout=GENERATION_TIMEOUT_S)
            except asyncio.TimeoutError:
                raise RuntimeError(
                    f"Génération interrompue : dépassé {GENERATION_TIMEOUT_S}s."
                )
            return {"generation_seule": liste}
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
