# 🚆 SNCF Databricks Lab

Projet personnel de **Data Engineering** consacré à la conception d’une plateforme de données inspirée des environnements d’entreprise. Le projet s’appuie sur les données Open Data SNCF et sur une architecture **Medallion** pour transformer les données brutes en indicateurs métier exploitables.

## 🎯 Objectifs

- Construire une plateforme data autour des données ouvertes SNCF
- Mettre en place des pipelines **Bronze / Silver / Gold** avec Databricks
- Industrialiser les traitements avec **Databricks Workflows**
- Gérer et déployer les ressources avec **Databricks Asset Bundles**
- Automatiser les déploiements avec **GitHub Actions**
- Ajouter des tests et des contrôles de qualité des données
- Préparer des données pour des tableaux de bord **Power BI**
- Simuler des incidents afin de pratiquer le diagnostic et le debugging

## 🏗️ Architecture cible

```text
┌──────────┐       ┌─────────────────┐       ┌────────────────────────┐
│  GitHub  │ ───▶  │ GitHub Actions  │ ───▶  │ Databricks Asset Bundle│
└──────────┘       └─────────────────┘       └───────────┬────────────┘
                                                        ▼
                                              ┌─────────────────────┐
                                              │ Databricks Workflow │
                                              └──────────┬──────────┘
                                                         ▼
                                              ┌─────────────────────┐
                                              │ Bronze → Silver → Gold│
                                              └──────────┬──────────┘
                                                         ▼
                                                   ┌──────────┐
                                                   │ Power BI │
                                                   └──────────┘
```

## 📂 Structure du projet

```text
sncf-databricks-lab/
├── .github/
│   └── workflows/
│       └── deploy.yml       # Déploiement automatisé
├── config/
│   ├── dev.json             # Configuration développement
│   ├── preprod.json         # Configuration préproduction
│   └── prod.json            # Configuration production
├── data/
│   ├── raw/                 # Données sources
│   └── curated/             # Données préparées
├── docs/                    # Documentation du projet
├── notebooks/
│   ├── bronze/
│   ├── silver/
│   ├── quality/
│   └── gold/
├── powerbi/                 # Éléments du projet Power BI
├── referentials/
│   ├── gares/
│   └── regions/
├── resources/
│   └── jobs.yml             # Ressources des jobs Databricks
├── tests/                   # Tests et contrôles
├── databricks.yml           # Configuration Asset Bundles
└── README.md
```

## 📊 Jeux de données

### Référentiel des gares

Référentiel principal utilisé pour enrichir les données métier. Les attributs peuvent notamment comprendre le nom de la gare, son code UIC, son trigramme et ses coordonnées GPS.

### Régularité TER mensuelle

Jeu de données destiné à analyser la régularité des trains TER, notamment le nombre de trains et les taux de ponctualité et de retard.

### Régularité TGV mensuelle

Jeu de données permettant d’étudier la ponctualité par axe, les résultats nationaux et leur évolution dans le temps.

> Les fichiers et schémas effectivement disponibles peuvent évoluer. Les traitements doivent s’aligner sur les données sources présentes dans le dépôt et leur documentation.

## 🥉 Bronze — ingestion

**Objectif :** charger les fichiers bruts et préserver leur contenu ainsi que leur traçabilité, sans appliquer de transformation métier importante.

Tables cibles :

- `lab_talan.bronze.ref_gares`
- `lab_talan.bronze.frequentation_gares`
- `lab_talan.bronze.regularite_ter`
- `lab_talan.bronze.regularite_tgv`

Notebooks d’ingestion :

- `notebooks/bronze/load_ref_gares.py`
- `notebooks/bronze/load_frequentation_gares.py`
- `notebooks/bronze/load_regularite_ter.py`
- `notebooks/bronze/load_regularite_tgv.py`

Le notebook des gares a été exécuté en développement : la table `lab_talan.bronze.ref_gares` contient 2 792 lignes. Les trois autres notebooks sont prêts pour l’orchestration ; leurs CSV doivent être présents dans le volume `/Volumes/lab_talan/bronze/raw_files/`. Les notebooks conservent les valeurs source en texte, normalisent les noms de colonnes pour Delta et ajoutent `_source_file` et `_ingested_at`.

## 🥈 Silver — qualité et enrichissement

**Objectif :** produire des données fiables, homogènes et prêtes à être agrégées.

Traitements prévus :

- standardisation des noms de colonnes et des types
- détection et suppression des doublons selon des règles explicites
- gestion des valeurs manquantes
- validation des schémas et des valeurs
- enrichissement par jointure avec les référentiels

Tables cibles envisagées :

- `silver.regularite_ter`
- `silver.regularite_tgv`

## 🥇 Gold — indicateurs métier

**Objectif :** fournir des agrégats adaptés à l’analyse et aux tableaux de bord.

Notebooks Gold :

- `notebooks/gold/build_kpi_ponctualite.py` unifie les indicateurs mensuels TER
  par région et TGV par axe. Les métriques propres à un transport restent nulles
  pour l’autre ; le taux TER est calculé à partir des trains en retard et ayant
  circulé.
- `notebooks/gold/build_kpi_retards.py` agrège les volumes TER par mois et
  région, puis calcule les taux d’annulation et de retard à partir des totaux
  (taux pondérés, et non moyenne des taux régionaux).
- `notebooks/gold/build_top_axes.py` classe les axes TGV selon leur régularité
  composite moyenne sur les mois disponibles, avec la ponctualité à l’origine en
  second critère.

Tables produites :

- `lab_talan.gold.kpi_ponctualite`
- `lab_talan.gold.kpi_retards`
- `lab_talan.gold.top_axes`

## 🔄 Workflow Databricks

```text
load_ref_gares
      │
      ├── load_regularite_ter
      └── load_regularite_tgv
                  │
                  ▼
            quality_checks
                  │
                  ▼
          silver_enrichment
                  │
                  ▼
              gold_kpis
```

Les dépendances exactes entre les tâches seront définies dans `resources/jobs.yml`.

## ✅ Contrôles qualité

Les contrôles envisagés portent notamment sur :

- les valeurs nulles dans les colonnes obligatoires
- les doublons selon la clé métier retenue
- les bornes et la cohérence des taux de ponctualité
- la validité des dates et des types
- la conformité au schéma attendu
- la présence des références correspondantes dans les référentiels

Un contrôle bloquant doit faire échouer la tâche concernée et rendre l’anomalie observable dans le workflow.

## 📈 Tableaux de bord Power BI

### Dashboard Direction

- ponctualité globale
- retard moyen
- tendance mensuelle

### Dashboard Exploitation

- classement des axes
- analyse régionale
- comparaison TER / TGV

## 🐞 Scénarios de debugging

Le projet pourra inclure des cas d’erreur contrôlés pour s’entraîner au diagnostic :

| Erreur | Exemple de cause |
|---|---|
| `TABLE_NOT_FOUND` | Table ou référentiel absent |
| `UNRESOLVED_COLUMN` | Nom de colonne incorrect ou schéma inattendu |
| `PERMISSION_DENIED` | Autorisation Unity Catalog manquante |
| `DATA_QUALITY_FAILURE` | Règle de qualité bloquante non respectée |
| `SCHEMA_MISMATCH` | Structure source différente du schéma attendu |

## ⚙️ Technologies

- Python et PySpark
- SQL
- Delta Lake
- Unity Catalog
- Databricks Workflows et Asset Bundles
- Git et GitHub Actions
- Power BI

## 🚀 Déploiement

Le déploiement nécessite un workspace Databricks, la Databricks CLI configurée et les autorisations adaptées à l’environnement cible. Vérifiez la configuration du bundle et des paramètres avant tout déploiement.

Exemple avec la Databricks CLI et une cible `dev` configurée dans `databricks.yml` :

```powershell
databricks auth login
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run --target dev
```

Le workflow `.github/workflows/deploy.yml` exécute les contrôles sur les Pull Requests et déploie la cible `dev` après un push sur `main`. Configurez la variable GitHub Actions `DATABRICKS_HOST` et le secret `DATABRICKS_TOKEN` (PAT de développement) ; ne stockez jamais le token dans le dépôt. Le déploiement utilise un profil temporaire sur le runner GitHub, qui est détruit à la fin du job. Pour la production, préférez une identité de service dédiée plutôt qu’un PAT personnel.

## 🗺️ Roadmap

- [ ] Importer et documenter les jeux de données SNCF
- [ ] Développer les notebooks d’ingestion Bronze
- [ ] Développer les transformations Silver
- [x] Construire les agrégats Gold
- [ ] Ajouter les contrôles qualité et les tests
- [ ] Configurer le workflow Databricks
- [ ] Mettre en place le déploiement GitHub Actions par environnement
- [ ] Créer les dashboards Power BI
- [ ] Ajouter des scénarios de debugging documentés

## 📄 Licence

Aucune licence n’est spécifiée à ce stade. Ajoutez un fichier de licence au dépôt si vous souhaitez préciser les conditions de réutilisation du projet.
