# Détails du pipeline

## Étape 1 — Récupération et normalisation des données météo

### Téléchargement

Les données brutes SYNOP (Météo-France) sont téléchargées depuis data.gouv.fr pour les années 2024 et 2025 :

- Observations horaires : `synop_{année}.csv.gz`
- Référentiel des stations : `postes_synop.geojson` (coordonnées GPS)

```bash
bash scripts/run_step1_meteo.sh
```

Manuel :
```bash
python3 scripts/download_meteo_data.py --config config/meteo_sources.json --output-dir data/raw
python3 scripts/transform_meteo_data.py --input data/raw/synop_2025.csv.gz --output data/processed/meteo_normalized.jsonl --quality-report data/processed/meteo_quality_report.md
```

### Nettoyage

Les lignes suivantes sont écartées :

- `stationId` manquant (champs testés : `numer_sta`, `geo_id_wmo`, `station_id`)
- `timestamp` manquant ou non parseable
- Valeurs numériques invalides : les chaînes `""`, `"mq"`, `"nan"` sont converties en `None`

### Normalisation

| Champ sortie   | Champ source CSV              | Transformation                                                   |
|----------------|-------------------------------|------------------------------------------------------------------|
| `stationId`    | `numer_sta` → `geo_id_wmo`    | Valeur brute                                                     |
| `timestamp`    | `date` → `validity_time`      | UTC ISO8601                                                      |
| `temperature`  | `t` (Kelvin)                  | `t − 273.15`, arrondi 2 décimales                               |
| `humidity`     | `u` (%)                       | Valeur brute, défaut `0.0`                                       |
| `pressure`     | `pres` → `pmer` (Pa ou hPa)   | Si > 2000 → divisé par 100 pour obtenir hPa                     |
| `windSpeed`    | `ff` (m/s)                    | `ff × 3.6` km/h                                                  |
| `windDirection`| `dd` (degrés 0–360)           | Rose des vents 16 points (N, NNE, NE, ENE, E, ESE…)            |
| `rainfall`     | `rr1` → `rr3` (mm)            | Premier champ non nul ; `None` si aucun                         |

### Sorties

- `data/raw/synop_YYYY.csv.gz`
- `data/raw/postes_synop.geojson`
- `data/processed/meteo_normalized.jsonl`
- `data/processed/meteo_quality_report.md`

---

## Étape 2 — Récupération et normalisation des données de pollution

### Téléchargement

Le dernier fichier journalier LCSQA (INERIS) disponible est téléchargé avec une fenêtre de recherche de 14 jours en arrière :

- URL principale : `FR_E2_YYYY-MM-DD.csv`
- Fallback : ressource statique data.gouv

```bash
bash scripts/run_step2_pollution.sh
```

Si un fichier CSV est déjà disponible localement :
```bash
bash scripts/run_step2_pollution.sh /chemin/vers/FR_E2_YYYY-MM-DD.csv
```

### Nettoyage

- Détection automatique du délimiteur (`;` ou `,`)
- Lignes écartées : `stationId` manquant, `timestamp` invalide, polluant inconnu, valeur de concentration absente
- Les mesures du même couple `(stationId, timestamp)` sont regroupées en une seule ligne multi-polluants

### Normalisation des noms de polluants

| Clé normalisée | Synonymes acceptés |
|----------------|--------------------|
| `pm25` | `pm2.5`, `pm2_5`, `pm 2.5`, `particules pm2.5` |
| `pm10` | `pm 10`, `particules pm10` |
| `no2` | `dioxyde d'azote`, `dioxyde dazote` |
| `o3` | `ozone` |
| `co` | `monoxyde de carbone` |
| `so2` | `dioxyde de soufre` |

### Sorties

- `data/raw/pollution/latest_pollution.csv`
- `data/raw/pollution/latest_pollution_meta.json`
- `data/processed/pollution_normalized.jsonl`
- `data/processed/pollution_quality_report.md`

---

## Étape 3 — Jointure géospatiale des stations

Associe chaque enregistrement pollution à l'observation météo la plus proche dans l'espace et dans le temps.

```bash
bash scripts/run_step3_join.sh
```

Manuel :
```bash
python3 scripts/join_meteo_pollution.py \
  --meteo-input data/processed/meteo_normalized.jsonl \
  --pollution-input data/processed/pollution_normalized.jsonl \
  --stations-geojson data/raw/postes_synop.geojson \
  --output data/processed/station_snapshots.jsonl \
  --max-distance-km 50 --max-time-diff-hours 6
```

### Logique de jointure

Pour chaque enregistrement pollution :

1. **Correspondance directe par ID** — si le `stationId` pollution existe dans les stations météo → `method: "BY_ID"`, `distanceKm: 0.0`
2. **Plus proche voisin géographique** (fallback) — recherche via `scipy.cKDTree`, rayon maximum **50 km** (Haversine) → `method: "NEAREST"`
3. **Fenêtre temporelle** — parmi les observations météo de la station retenue, sélection de la plus proche en temps via `bisect`, delta maximum **±6 heures**

Un enregistrement est ignoré si les coordonnées pollution sont absentes (cas 2), si la distance dépasse 50 km, ou si aucune observation météo ne tombe dans la fenêtre ±6 h.

### Sorties

- `data/processed/station_snapshots.jsonl`
- `data/processed/join_quality_report.md`

---

## Étape 4 — Calcul des indices composites

Trois indices sont calculés à partir des snapshots de stations. Voir `indices.md` pour le détail des formules et pondérations.

```bash
bash scripts/run_step4_indices.sh
```

Manuel :
```bash
python3 scripts/calculate_indices.py \
  --input data/processed/station_snapshots.jsonl \
  --output data/processed/indices_composite.jsonl
```

| Indice | Description |
|--------|-------------|
| **IPN** | Indice Pollution Normalisée — niveau de pollution vs seuils UE |
| **IMD** | Indice Météo Défavorable — conditions météo favorisant la stagnation |
| **ICAM** | Indice Composite Air-Météo — 75 % IPN + 25 % IMD |

### Sorties

- `data/processed/indices_composite.jsonl`
- `data/processed/indices_quality_report.md`
