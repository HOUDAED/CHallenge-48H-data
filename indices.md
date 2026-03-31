# Types et calcul détaillé des indices

## Calcul des indices

Trois indices composites sont calculés dans `scripts/calculate_indices.py` à partir de `station_snapshots.jsonl` et écrits dans `indices_composite.jsonl`. Toutes les valeurs sont bornées à **[0, 100]**.

#### IPN — Indice Pollution Normalisée
#### IMD — Indice Météo Défavorable
#### ICAM — Indice Composite Air-Météo
---

### IPN — Indice Pollution Normalisée

Mesure à quel point chaque polluant se rapproche de son seuil réglementaire européen, puis les combine en une moyenne pondérée.

#### Étape 1 — Score par polluant

```
score(p) = 100 × valeur_mesurée(p) / seuil_UE(p)   [borné à 0–100]
```

| Polluant | Seuil UE  | Poids |
|----------|-----------|-------|
| PM2.5    | 25 µg/m³  | 0.30  |
| PM10     | 50 µg/m³  | 0.20  |
| NO₂      | 200 µg/m³ | 0.20  |
| O₃       | 120 µg/m³ | 0.15  |
| SO₂      | 125 µg/m³ | 0.10  |
| CO       | 10 mg/m³  | 0.05  |

#### Étape 2 — Moyenne pondérée (polluants manquants exclus)

```
IPN = Σ(score(p) × poids(p)) / Σ(poids(p) pour p disponibles)
```

> IPN = 100 signifie que tous les polluants atteignent simultanément leur seuil UE. IPN > 100 est borné à 100.

---

### IMD — Indice Météo Défavorable

Mesure à quel point les conditions météorologiques sont défavorables à la dispersion des polluants. IMD élevé = air stagnant/confiné.

#### Étape 1 — Score de dispersion par composante

Chaque composante représente la capacité des conditions météo à disperser les polluants (plus la valeur est haute, meilleure est la dispersion) :

| Composante  | Formule | Poids |
|-------------|---------|-------|
| Vent        | `clamp(vitesse_vent_km_h / 30 × 100, 0, 100)` | 0.45 |
| Pluie       | `clamp(précipitations_mm / 2 × 100, 0, 100)` | 0.20 |
| Humidité    | `clamp(100 − humidité_%, 0, 100)` | 0.20 |
| Pression    | `clamp(100 − pénalité_pression, 0, 100)` où `pénalité_pression = clamp((hPa − 1005) / 20 × 100, 0, 100)` | 0.15 |

#### Étape 2 — Score de dispersion (moyenne pondérée, même formule que l'IPN)

#### Étape 3 — Inversion en score de risque

```
IMD = 100 − score_dispersion
```

> IMD = 0 signifie une dispersion idéale (vent fort, pluie, faible humidité, basse pression). IMD = 100 signifie des conditions totalement stagnantes.

---

### ICAM — Indice Composite Air-Météo

Combine le risque de pollution et le risque météorologique en un seul score.

```
ICAM = 0.75 × IPN + 0.25 × IMD
```

| Composante | Poids |
|------------|-------|
| IPN        | 0.75  |
| IMD        | 0.25  |

Si l'une des composantes est manquante, les poids disponibles sont renormalisés (même logique `weighted_average`).

---

### Gestion des données manquantes

Les trois indices appliquent la même règle de disponibilité partielle : si un polluant ou une variable météo est absent, son poids est exclu et les poids restants sont renormalisés. Si **aucune** entrée n'est disponible, l'indice vaut `null` et est omis de la réponse API (`normalize_indices` dans `serve_indices_api.py:39`).

---

### Origine des valeurs

| Champ | Étape source |
|-------|-------------|
| `pollution.*` | Étape 2 — `transform_pollution_data.py` |
| `meteo.*` | Étape 1 — `transform_meteo_data.py` (Kelvin→°C, m/s→km/h) |
| Jointure des deux | Étape 3 — `join_meteo_pollution.py` |
| Calcul des indices | Étape 4 — `calculate_indices.py` |
| Exposition API | Étape 5 — `serve_indices_api.py` |
