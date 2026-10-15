# archivolt-prospection-odoo

Workflow Mistral : prospection data gouvernance régionale -> activité Odoo (tâche « Suivi commercial »).

- Workflow : `prospection-odoo` (liste des ETI/PME/startups en Bretagne, Pays de la Loire, Normandie, Nouvelle-Aquitaine en data gouvernance)
- Sortie : activité « À faire » créée sur la tâche Odoo « Suivi commercial » (https://www.archi-volt.eu/odoo)
- Secrets requis (workspace Mistral, binding sur le déploiement) :
  - `ODOO_API_KEY` (secret workspace `odoo_api_key`) : clé API Odoo
- Secrets de connexion (binding) :
  - `ODOO_URL` : https://www.archi-volt.eu/odoo (défaut dans le code)
