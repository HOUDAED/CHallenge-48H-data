# Data Challenge-48H

Plateforme d’intégration des données de qualité de l’air et météorologiques pour les villes de France.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

## Pipeline

### Exécution de tous les scripts dans l’ordre : récupération des données météo et pollution depuis les open APIs, nettoyage et normalisation des données, réalisation d’une jointure géospatiale des stations, calcul des indices composites, exposition des données via un endpoint POST.

### Execution  en boucle toutes les 60 minutes

#### Worker (script automatisé) suivant execute le pipeline et publie un payload JSON a chaque cycle. L’actualité des données correspond à la dernière heure.

```bash
bash scripts/run_hourly_worker.sh
```

## Les étapes détaillées du pipeline sont décrites dans le fichier details_pipeline.md
## Les indices détaillés sont expliqués dans le fichier indices.md



## Variables d'environnement utiles:

- `INTERVAL_MINUTES` (defaut: `60`)
- `ENDPOINT_URL` (URL de POST cible, si vide les payloads sont ecrits dans `data/processed/outbox`)
- `MAX_CYCLES` (defaut: `0` = infini)
- `RUN_METEO_EACH_CYCLE` (`true`/`false`)
- `POLLUTION_INPUT_CSV` (fichier CSV local pollution a reutiliser sur chaque cycle)

Exemple (1 cycle de test):

```bash
MAX_CYCLES=1 INTERVAL_MINUTES=60 RUN_METEO_EACH_CYCLE=true ENDPOINT_URL='' bash scripts/run_hourly_worker.sh
```

---

## Sources de données

### Données météo — SYNOP (Météo-France)

- **Couverture temporelle :** années 2024 et 2025
- **Source :** `https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/OBS/SYNOP/synop_{année}.csv.gz`
- **Référentiel stations :** `postes_synop.geojson` (coordonnées GPS de chaque station)
- **Couverture géographique :** toutes les stations SYNOP françaises (métropole + outre-mer), sans filtre géographique à l'import
- **Fréquence des observations :** toutes les heures

### Données pollution — LCSQA (INERIS)

- **Source :** `https://object.infra.data.gouv.fr/ineris-prod/lcsqa/air/temps-reel/{date}/FR_E2_{date}.csv`
- **Fallback :** ressource statique data.gouv (`157ceed4-ce03-4c7d-9cd7-ae60ea07417b`)
- **Fenêtre de recherche :** 14 jours en arrière depuis hier (UTC) pour trouver le fichier journalier le plus récent disponible
- **Couverture géographique :** toutes les stations de surveillance de la qualité de l'air en France, sans filtre géographique à l'import
- **Polluants présents :** PM2.5, PM10, NO2, O3, CO, SO2



