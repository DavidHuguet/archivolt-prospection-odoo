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
- Activité de génération : start_to_close_timeout 1200 s (prod = 15 fiches),
  UNE seule tentative (retry 1) pour éviter les boucles Temporal de 60 min.
- Nombre d'entreprises : input.nb_entreprises (15 en prod, 2 en test GEN_ONLY).
- Odoo 19 : mail.activity.create avec res_model_id (ir.model) + res_id,
  activity_type_id, date_deadline.
- Le type « À faire » de l'interface FR s'appelle « To-Do » en base.
- Mode diagnostic : note_test = "DUMP_TYPES" -> liste les types d'activité Odoo.
- Mode nettoyage : note_test = "CLEANUP" -> supprime les activités de test Odoo.
- Mode génération seule : note_test = "GEN_ONLY" -> renvoie la liste générée
  (avec recherche web) sans rien écrire dans Odoo (2 entreprises par défaut,
  passer nb_entreprises pour changer).
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
GEN_MODEL = os.environ.get("GEN_MODEL", "mistral-medium-latest")

PROMPT_MODELE = (
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
    "Quand tu hésites sur une entreprise, exclue-la. "
    "Signaux recherchés : entreprises recrutant des compétences en data "
    "gouvernance / data management / DPO / data quality, ou portant de gros "
    "projets data (RGPD, qualité de données, IA, transformation digitale). "
    "FRAÎCHEUR OBLIGATOIRE : ne retiens que des signaux des 6 DERNIERS MOIS. "
    "Ignore tout signal plus ancien. "
    "NOMBRE CIBLE : {nb} entreprises. La qualité prime sur le nombre : ne "
    "JAMAIS inclure une entreprise qui viole un critère pour gonfler la liste. "
    "Si tu trouves moins de {nb} entreprises strictement conformes, liste "
    "celles trouvées sans inventer. "
    "AUCUN DOUBLON : jamais deux fois le même SIREN ni deux entités du même "
    "groupe. "
    "Appuie-toi sur les résultats de ta recherche web (offres d'emploi, "
    "actualités, projets annoncés) ; pour chaque entreprise, cite la source "
    "(site / média et date) entre parenthèses après le champ Pourquoi. "
    "FICHE ENTREPRISE : pour chaque entreprise, indique son numéro SIREN, la "
    "ville du siège social, son code APE avec son libellé, son nombre "
    "d'employés (dernier connu, préciser l'année) et son dernier chiffre "
    "d'affaires connu (préciser l'année). Si une information est introuvable "
    "dans les résultats déjà obtenus, écris 'non trouvé' pour ce champ plutôt "
    "qu'une invention (pas de recherche dédiée par champ manquant). "
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
    nb_entreprises: int = 15  # 15 en prod ; GEN_ONLY force 2 sauf si fourni


class OdooActivityResult(BaseModel):
    activity_id: int = 0
    task_id: int = 0


@workflows.activity(
    start_to_close_timeout=timedelta(seconds=1200),
    retry_policy_max_attempts=1,
)
async def generer_liste_entreprises(agent_id: str, nb: int) -> str:
    """Génération de la liste de prospection avec recherche web réelle.

    Timeout 1200 s (prod = 15 fiches), une seule tentative.
    """
    import mistralai.workflows.plugins.mistralai as workflows_mistralai
    from mistralai.client.models import WebSearchTool

    prompt = PROMPT_MODELE.replace("{nb}", str(nb))
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
            "La qualité prime sur le nombre, jamais de doublons (même SIREN). "
            "Réponds toujours en texte brut structuré, sans markdown, avec les "
            "sources citées."
        ),
        tools=[WebSearchTool()],
    )
    session = workflows_mistralai.RemoteSession()
    outputs = await workflows_mistralai.Runner.run(
        agent=agent,
        inputs=prompt,
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
    note = re.sub(r"#+\s*", "", note)
    note = note.replace("**", "").replace("__", "")
    note = re.sub(r"^[ \t]*[-*][ \t]+", "- ", note, flags=re.M)
    note = re.sub(r"\n{3,}", "\n\n", note)
    return note


@workflows.activity()
async def nettoyer_activites_test(odoo_db: str, odoo_user: str) -> dict:
    """Supprime les activités de test sur la tâche « Suivi commercial » (occurrence en cours).

    Cible : activités dont le résumé contient 'TEST' ou 'Prospection data
    gouvernance' (créées pendant la phase de test du workflow), ou la note
    contient 'TEST' ou 'DUMP_TYPES'. Ne touche à rien d'autre.
    """
    import xmlrpc.client

    api_key = os.environ["ODOO_API_KEY"]

    def _run() -> dict:
        common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
        uid = common.authenticate(odoo_db, odoo_user, api_key, {})
        if not uid:
            raise RuntimeError("Authentification Odoo refusée (nettoyage).")
        models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")

        def call(model, method, args, kwargs=None):
            return models.execute_kw(odoo_db, uid, api_key, model, method, args, kwargs or {})

        today = date.today().isoformat()
        tasks = call(
            "project.task", "search_read",
            [[["name", "ilike", ODOO_TASK_NAME],
              ["planned_date_begin", "<=", f"{today} 23:59:59"]],
             ["id", "name"]],
            {"limit": 1, "order": "planned_date_begin desc"},
        )
        if not tasks:
            return {"supprimees": 0, "detail": "tâche introuvable"}
        task_id = tasks[0]["id"]

        model_ids = call(
            "ir.model", "search_read",
            [[["model", "=", "project.task"]], ["id"]],
            {"limit": 1},
        )
        if not model_ids:
            return {"supprimees": 0, "detail": "ir.model introuvable"}
        res_model_id = model_ids[0]["id"]

        acts = call(
            "mail.activity", "search_read",
            [[
                ["res_model_id", "=", res_model_id],
                ["res_id", "=", task_id],
                "|",
                    ["summary", "like", "TEST"],
                    "|",
                        ["summary", "like", "Prospection data gouvernance"],
                        "|",
                            ["note", "like", "TEST"],
                            ["note", "like", "DUMP_TYPES"],
            ], ["id", "summary"]],
            {},
        )
        ids = [a["id"] for a in acts]
        if ids:
            call("mail.activity", "unlink", [ids])
        return {"supprimees": len(ids), "ids": ids, "task_id": task_id}

    return await asyncio.to_thread(_run)


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
        # Mode nettoyage : suppression des activités de test
        if input.note_test == "CLEANUP":
            return await nettoyer_activites_test(input.odoo_db, input.odoo_user)
        # Mode diagnostic : lister les types d'activité
        if input.note_test == "DUMP_TYPES":
            dump = await lister_types_activite(input.odoo_db, input.odoo_user)
            return {"diagnostic_types": dump}
        # Mode génération seule : liste renvoyée, rien d'écrit dans Odoo.
        # Test léger : 2 entreprises par défaut (nb_entreprises pour changer).
        if input.note_test == "GEN_ONLY":
            nb = input.nb_entreprises if input.nb_entreprises > 0 else 2
            liste = await generer_liste_entreprises(input.agent_id, nb)
            return {"generation_seule": liste}
        # Mode test : note fournie -> pas de génération
        if input.note_test:
            liste = input.note_test
        else:
            # Production : 15 entreprises
            nb = input.nb_entreprises if input.nb_entreprises > 0 else 15
            liste = await generer_liste_entreprises(input.agent_id, nb)
        result = await creer_activite_odoo(liste, input.odoo_db, input.odoo_user)
        return {
            "odoo_task_id": result.task_id,
            "activity_id": result.activity_id,
            "activite": "créée",
            "mode_test": bool(input.note_test),
            "apercu_note": liste[:500],
        }
