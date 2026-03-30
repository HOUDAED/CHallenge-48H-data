# CHallenge-48H-data

## Step 1 - Recuperer les donnees meteorologiques

Ce scope couvre:

- telechargement des observations SYNOP
- telechargement du referentiel des stations meteo
- normalisation des mesures vers le contrat MeteoData
- generation d'un rapport qualite

### Execution rapide Step 1

```bash
bash scripts/run_step1_meteo.sh
```

### Execution manuelle Step 1

```bash
python3 scripts/download_meteo_data.py --config config/meteo_sources.json --output-dir data/raw
python3 scripts/transform_meteo_data.py --input data/raw/synop_2025.csv.gz --output data/processed/meteo_normalized.jsonl --quality-report data/processed/meteo_quality_report.md
```

### Sorties

- data/raw/synop_YYYY.csv.gz
- data/raw/postes_synop.geojson
- data/processed/meteo_normalized.jsonl
- data/processed/meteo_quality_report.md

### Mapping MeteoData

- temperature: champ t converti de Kelvin vers degres C
- humidity: champ u (pourcentage)
- pressure: champ pres (hPa)
- windSpeed: champ ff converti de m/s vers km/h
- windDirection: champ dd converti en cardinal (N, NNE, ...)
- rainfall: champ rr1 (fallback rr3)

## Step 2 - Recuperer les donnees de concentrations des polluants

Ce scope couvre:

- telechargement du dernier fichier journalier FR_E2_YYYY-MM-DD.csv disponible
- transformation des concentrations polluants vers un format normalise
- generation d'un rapport qualite

### Execution rapide Step 2

```bash
bash scripts/run_step2_pollution.sh
```

Le script essaie d'abord les URLs journalieres par date, puis une ressource CSV data.gouv de repli.

Si vous avez deja un fichier CSV pollution (par exemple fourni par un collaborateur):

```bash
bash scripts/run_step2_pollution.sh /chemin/vers/FR_E2_YYYY-MM-DD.csv
```

### Execution manuelle Step 2

```bash
python3 scripts/download_pollution_data.py --config config/pollution_sources.json --output data/raw/pollution/latest_pollution.csv --metadata-output data/raw/pollution/latest_pollution_meta.json
python3 scripts/transform_pollution_data.py --input data/raw/pollution/latest_pollution.csv --output data/processed/pollution_normalized.jsonl --quality-report data/processed/pollution_quality_report.md
```

Ou en forcant une URL directe connue:

```bash
python3 scripts/download_pollution_data.py --url "https://.../FR_E2_YYYY-MM-DD.csv" --output data/raw/pollution/latest_pollution.csv --metadata-output data/raw/pollution/latest_pollution_meta.json
```

### Sorties Step 2

- data/raw/pollution/latest_pollution.csv
- data/raw/pollution/latest_pollution_meta.json
- data/processed/pollution_normalized.jsonl
- data/processed/pollution_quality_report.md

### Mapping PollutantData

- stationId: identifiant station pollution
- timestamp: horodatage normalise en UTC
- coordinates.latitude / coordinates.longitude: coordonnees GPS (si presentes)
- pollution.pm25: PM2.5
- pollution.pm10: PM10
- pollution.no2: NO2
- pollution.o3: O3
- pollution.co: CO
- pollution.so2: SO2

## Step 3 - Faire une jointure geospatiale des stations

Ce scope couvre:

- jointure par stationId quand l'identifiant est commun
- fallback geospatial vers la station meteo la plus proche (rayon max 50 km)
- appariement temporel vers l'observation meteo la plus proche (delta max 6 h)
- generation d'un rapport qualite de jointure

### Execution rapide Step 3

```bash
bash scripts/run_step3_join.sh
```

### Execution manuelle Step 3

```bash
python3 scripts/join_meteo_pollution.py --meteo-input data/processed/meteo_normalized.jsonl --pollution-input data/processed/pollution_normalized.jsonl --stations-geojson data/raw/postes_synop.geojson --output data/processed/station_snapshots.jsonl --quality-report data/processed/join_quality_report.md --max-distance-km 50 --max-time-diff-hours 6
```

### Sorties Step 3

- data/processed/station_snapshots.jsonl
- data/processed/join_quality_report.md

## Step 4 - Calculer les indices composites

```bash
bash scripts/run_step4_indices.sh
```

Manuel :
```bash
python3 scripts/calculate_indices.py --input data/processed/station_snapshots.jsonl --output data/processed/indices_composite.jsonl
```

### Sorties Step 4

- `data/processed/indices_composite.jsonl`

---

## Step 5 - Servir l'API

```bash
bash scripts/run_step5_api.sh
```

Manuel :
```bash
python3 scripts/serve_indices_api.py --data-file data/processed/indices_composite.jsonl --port 8888
```

### Endpoints

| Endpoint | Méthode | Description | Paramètres |
|---|---|---|---|
| `/` | GET | Info service | — |
| `/health` | GET | Health check | — |
| `/api/v1/indices` | GET | Toutes les stations (filtrées) | `stationId`, `city`, `date`, `from`, `to`, `limit` |
| `/api/v1/stations` | GET | Alias de `/indices` | idem |
| `/api/v1/stations/{id}` | GET | Station unique par ID | `city`, `date`, `from`, `to` |

**Filtres disponibles :**

- `city` : correspondance partielle insensible à la casse sur le nom de station (ex. `?city=paris`)
- `date` : format `YYYY-MM-DD`, filtre sur la date exacte du timestamp
- `from` / `to` : plage ISO8601 inclusive
- `stationId` : correspondance exacte
- `limit` : retourne les N premiers résultats

---

## Execution en boucle toutes les 60 minutes

Le worker suivant execute le pipeline et publie un payload JSON a chaque cycle.

```bash
bash scripts/run_hourly_worker.sh
```

Variables d'environnement utiles:

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

---

## Détail ETL — Nettoyage et Normalisation

### Step 1 — Transformation météo (`transform_meteo_data.py`)

#### Nettoyage

Les lignes suivantes sont **écartées** :

- `stationId` manquant (champs testés : `numer_sta`, `geo_id_wmo`, `station_id`)
- `timestamp` manquant ou non parseable
- Valeurs numériques : les chaînes `""`, `"mq"`, `"nan"` (insensible à la casse) sont converties en `None`; les virgules sont remplacées par des points avant le cast en `float`

#### Normalisation champ par champ

| Champ sortie | Champ(s) source CSV | Transformation |
|---|---|---|
| `stationId` | `numer_sta` → `geo_id_wmo` → `station_id` | Valeur brute (stripped) |
| `timestamp` | `date` → `validity_time` → `timestamp` | `dateutil.parser.parse()` → UTC ISO8601 |
| `temperature` | `t` (Kelvin) | `t − 273.15`, arrondi 2 décimales |
| `humidity` | `u` (%) | Valeur brute, défaut `0.0` |
| `pressure` | `pres` → `pmer` (Pa ou hPa) | Si valeur > 2000 → divisé par 100 pour obtenir hPa, arrondi 2 décimales |
| `windSpeed` | `ff` (m/s) | `ff × 3.6`, arrondi 2 décimales |
| `windDirection` | `dd` (degrés 0–360) | `floor(dd / 22.5) % 16` → index dans la rose des vents 16 points (N, NNE, NE, ENE, E, ESE, SE, SSE, S, SSO, SO, OSO, O, ONO, NO, NNO) |
| `rainfall` | `rr1` → `rr3` (mm) | Premier champ non nul ; `None` si aucun |

**Valeurs par défaut en cas de champ manquant :** `0.0` pour tous les numériques, `"N"` pour `windDirection`, `None` pour `rainfall`.

---

### Step 2 — Transformation pollution (`transform_pollution_data.py`)

#### Nettoyage

- **Détection automatique du délimiteur :** compare le nombre de `;` et `,` dans la ligne d'en-tête pour choisir le séparateur
- Lignes **écartées** : `stationId` manquant, `timestamp` manquant ou non parseable, nom de polluant inconnu après normalisation, valeur de concentration absente
- Les enregistrements du même couple `(stationId, timestamp)` sont **regroupés** en une seule ligne multi-polluants

#### Normalisation des noms de polluants

Les variantes suivantes sont toutes reconnues (insensible à la casse, espaces ignorés) :

| Clé normalisée | Synonymes acceptés |
|---|---|
| `pm25` | `pm25`, `pm2.5`, `pm2_5`, `pm 2.5`, `particules pm2.5`, `pm2,5` |
| `pm10` | `pm10`, `pm 10`, `particules pm10` |
| `no2` | `no2`, `dioxyde d'azote`, `dioxyde dazote` |
| `o3` | `o3`, `ozone` |
| `co` | `co`, `monoxyde de carbone` |
| `so2` | `so2`, `dioxyde de soufre` |

#### Normalisation champ par champ

| Champ sortie | Champ(s) source CSV | Transformation |
|---|---|---|
| `stationId` | `station_id`, `code site`, `codesite`, `code_station`, `id_station`, `identifiant_station` | Valeur brute |
| `timestamp` | `timestamp`, `date_heure`, `date de début`, `date_debut`, `date`, `heure` | UTC ISO8601 |
| `coordinates.latitude` | `latitude`, `lat` | `float` ou `None` |
| `coordinates.longitude` | `longitude`, `lon`, `lng` | `float` ou `None` |
| `pollution.*` | `valeur` → `concentration` → `value` → `resultat` → `mesure` | `float` ou `None` |

Tous les champs polluants sont initialisés à `None` ; seuls les polluants effectivement présents dans le CSV sont renseignés.

---

### Step 3 — Jointure géospatiale (`join_meteo_pollution.py`)

#### Logique de jointure

Pour chaque enregistrement pollution :

1. **Correspondance directe par ID** : si le `stationId` pollution existe dans les stations météo → `method: "BY_ID"`, `distanceKm: 0.0`
2. **Plus proche voisin géographique** (fallback) : si aucune correspondance directe, cherche la station météo la plus proche via `scipy.cKDTree` (1-NN)
   - Rayon maximum : **50 km** (Haversine, R = 6371 km)
   - Requiert que la station pollution ait des coordonnées GPS valides
   - → `method: "NEAREST"`, `distanceKm` arrondi 3 décimales
3. **Fenêtre temporelle** : parmi les observations météo de la station retenue, sélectionne la plus proche en temps via `bisect`
   - Delta maximum : **±6 heures**
   - `timeDeltaMinutes` arrondi 2 décimales

Un enregistrement est **ignoré** si : coordonnées pollution manquantes (cas 2), distance > 50 km, ou aucune observation météo dans la fenêtre ±6 h.

---

## Indices composites

Trois indices sont calculés par `calculate_indices.py`, tous dans l'intervalle **[0, 100]**.

### IPN — Indice Pollution Normalisée

Mesure le niveau de pollution par rapport aux seuils réglementaires européens.

**Seuils de normalisation :**

| Polluant | Seuil (µg/m³ sauf CO en mg/m³) |
|---|---|
| PM2.5 | 25 |
| PM10 | 50 |
| NO2 | 200 |
| O3 | 120 |
| SO2 | 125 |
| CO | 10 |

**Formule :**

```
score_p = clamp( (valeur_p / seuil_p) × 100, 0, 100 )   pour chaque polluant p

IPN = moyenne_pondérée(score_p, poids_p)
```

**Pondérations :**

| Polluant | Poids |
|---|---|
| PM2.5 | 0.30 |
| PM10 | 0.20 |
| NO2 | 0.20 |
| O3 | 0.15 |
| SO2 | 0.10 |
| CO | 0.05 |

La moyenne pondérée est recalculée sur les polluants effectivement disponibles (les poids des polluants absents sont exclus du dénominateur). `IPN = None` si aucun polluant n'est disponible.

---

### IMD — Indice Météo Défavorable

Mesure dans quelle mesure les conditions météo **favorisent la stagnation** des polluants (valeur haute = conditions défavorables à la dispersion).

**Composantes :**

| Composante | Formule | Interprétation |
|---|---|---|
| Vent | `clamp(windSpeed / 30.0 × 100, 0, 100)` | Vent fort → bonne dispersion |
| Pluie | `clamp(rainfall / 2.0 × 100, 0, 100)` | Pluie → lessivage |
| Humidité | `clamp(100 − humidity, 0, 100)` | Humidité faible → meilleure dispersion |
| Pression | `clamp(100 − clamp((pression − 1005) / 20 × 100, 0, 100), 0, 100)` | Haute pression (anticyclone) → stagnation |

**Pondérations :**

| Composante | Poids |
|---|---|
| Vent | 0.45 |
| Pluie | 0.20 |
| Humidité | 0.20 |
| Pression | 0.15 |

**Formule finale :**

```
dispersion_score = moyenne_pondérée(composantes)
IMD = clamp(100 − dispersion_score, 0, 100)
```

`IMD = None` si toutes les composantes sont manquantes.

---

### ICAM — Indice Composite Air-Météo

Combine IPN et IMD en un indice de risque global.

```
ICAM = clamp( IPN × 0.75 + IMD × 0.25, 0, 100 )
```

| Composante | Poids |
|---|---|
| IPN (pollution) | 0.75 |
| IMD (météo) | 0.25 |

`ICAM = None` si IPN et IMD sont tous les deux `None`. Si l'un est absent, la moyenne est recalculée sur le poids disponible.

---

### Interprétation des indices

| Valeur | Signification |
|---|---|
| 0–25 | Bonne qualité / conditions favorables |
| 25–50 | Qualité modérée |
| 50–75 | Mauvaise qualité / conditions dégradées |
| 75–100 | Très mauvaise qualité / conditions très défavorables |
