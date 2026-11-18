🚆 SNCF Databricks Lab
Projet personnel de Data Engineering permettant de reproduire une architecture moderne de type entreprise basée sur :

GitHub
GitHub Actions
Databricks
Unity Catalog
Databricks Workflows
Power BI
Architecture Medallion (Bronze / Silver / Gold)
🎯 Objectifs
Ce projet a pour objectif de :

Construire une plateforme Data complète autour des données Open Data SNCF
Mettre en place un workflow Databricks industrialisé
Travailler le versioning Git
Automatiser les déploiements avec GitHub Actions
Construire des pipelines Bronze / Silver / Gold
Implémenter des contrôles qualité
Développer des dashboards Power BI
Simuler des incidents afin de pratiquer le debugging Databricks
🏗 Architecture
GitHub
   │
   ▼
GitHub Actions
   │
   ▼
Databricks Asset Bundle
   │
   ▼
Databricks Workflow
   │
   ▼
Bronze
   │
   ▼
Silver
   │
   ▼
Gold
   │
   ▼
Power BI

📂 Structure du projet
Plain Text
sncf-databricks-lab
 
├── .github
│ └── workflows
│ └── deploy.yml
│
├── config
│ ├── dev.json
│ ├── preprod.json
│ └── prod.json
│
├── data
│ ├── raw
│ └── curated
│
├── docs
│
├── notebooks
│ ├── bronze
│ ├── silver
│ ├── quality
│ └── gold
│
├── powerbi
│
├── referentials
│ ├── gares
│ └── regions
│
├── resources
│ └── jobs.yml
│
├── tests
│
├── databricks.yml
│
└── README.md
📊 Jeux de données
Référentiels
Gares de voyageurs

Utilisé comme référentiel principal.

Exemples :

Nom Gare
Code UIC
Trigramme
Coordonnées GPS
Données métier
Régularité mensuelle TER

Indicateurs :

nombre de trains
taux de ponctualité
taux de retard
Régularité mensuelle TGV

Indicateurs :

ponctualité par axe
ponctualité nationale
évolution mensuelle
🥉 Bronze Layer

Objectif :

charger les fichiers bruts
conserver les données telles qu'elles sont fournies

Tables :

Plain Text
bronze.regularite_ter
bronze.regularite_tgv
bronze.ref_gares
🥈 Silver Layer

Objectif :

nettoyage
standardisation
enrichissement

Actions :

suppression des doublons
contrôle qualité
jointure avec les référentiels

Tables :

Plain Text
silver.regularite_ter
silver.regularite_tgv
🥇 Gold Layer

Objectif :

production d'indicateurs métier

KPIs :

ponctualité moyenne
retard moyen
évolution mensuelle
classement des axes
classement des régions

Tables :

Plain Text
gold.kpi_ponctualite
gold.kpi_retards
gold.top_axes
🔄 Workflow Databricks
Plain Text
load_ref_gares
│
load_regularite_ter
│
load_regularite_tgv
│
quality_checks
│
silver_enrichment
│
gold_kpis
✅ Contrôles qualité

Exemples :

vérification des valeurs nulles
vérification des doublons
vérification des taux de ponctualité
validation du schéma
🐞 Cas de debugging

Le projet inclura volontairement des scénarios d'erreurs :

TABLE_NOT_FOUND
Plain Text
Référentiel absent
UNRESOLVED_COLUMN
Plain Text
Nom de colonne incorrect
PERMISSION_DENIED
Plain Text
Erreur Unity Catalog
DATA QUALITY FAILURE
Plain Text
Contrôle qualité bloquant
📈 Power BI

Dashboards :

Dashboard Direction
ponctualité globale
retard moyen
tendance mensuelle
Dashboard Exploitation
top axes
analyse régionale
comparaison TER / TGV
⚙️ Technologies
Python
PySpark
SQL
Delta Lake
Unity Catalog
Databricks Workflows
GitHub
GitHub Actions
Power BI
🚀 Roadmap
 Création du repository
 Import des datasets SNCF
 Création des notebooks Bronze
 Création des notebooks Silver
 Création des notebooks Gold
 Mise en place des tests
 Création du workflow Databricks
 Intégration GitHub Actions
 Création du dashboard Power BI
 Scénarios de debugging

Ensuite fais :

```powershell
git add README.md
git commit -m "Add project README"
git push

