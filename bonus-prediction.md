# Bonus — Prévisions dynamiques (non déployé)

Fonctionnalité prête mais désactivée. Suivre les étapes ci-dessous pour l'activer.

### Context

##### Le pipeline produit actuellement des indices IMD/IPN/ICAM pour les données historiques.
##### L'objectif est d'ajouter une étape de prévision (Step 6) en utilisant la régression linéaire ##### simple, en exploitant au maximum les données historiques disponibles via les deux APIs ouvertes (SYNOP + LCSQA) sur une période d'au moins 2 ans et max 10 ans.
     

#####  La régression porte sur IMD (Indice Météo Défavorable) car :
#####  - Il est calculable depuis les données météo seules (pas besoin de pollution historique)
#####  - Les 189 stations × 10 ans × ~8760 h/an ≈ 16 millions d'échantillons → modèle très robuste

#####  Modèle A — AR global (autorégressif, court terme)

#####  - Features à t : [windSpeed, humidity, pressure, temperature, rainfall, sin(hour), cos(hour), sin(doy), cos(doy)]
#####  - Cible : IMD(t+1) — valeur de l'heure suivante
##### - Entraînement : tous les couples (station, t) de toutes les années → un seul modèle global
#####  - Prévision : pour chaque station, prendre le dernier état météo connu et prédire les 24 prochaines heures en faisant varier uniquement
#####  l'encodage horaire

#####  Modèle B — Tendance linéaire par station (long terme)

#####  - Features : [day_number, 1] (droite simple)
##### - Cible : daily_mean_IMD agrégé par jour
#####  - Entraînement : par station, ~3650 points sur 10 ans → tendance stable
#####  - Prévision : extrapoler les 7 prochains jours — répond à "l'air s'améliore-t-il sur le long #####  terme ?"

---

## Étape 1 — Installer les dépendances

```bash
pip install -r requirements.txt
```

Nouvelles dépendances ajoutées : `streamlit`, `plotly`.

---

## Étape 2 — Générer les prévisions (une première fois manuellement)

```bash
bash scripts/run_step6_forecast.sh
```

Produit `data/processed/forecast.jsonl` — prévisions IMD sur 24h (modèle AR) et 7 jours (tendance linéaire) pour les 189 stations.

---

## Étape 3 — Activer la régénération automatique dans le worker

Dans `scripts/hourly_pipeline_worker.py`, décommenter les deux blocs marqués `[BONUS]` :

**Bloc 1** — dans `execute_pipeline()` (ligne ~133) :
```python
# Avant
# [BONUS] Uncomment to enable forecast generation each cycle
# run_cmd(["bash", "scripts/run_step6_forecast.sh"])

# Après
run_cmd(["bash", "scripts/run_step6_forecast.sh"])
```

**Bloc 2** — dans la boucle principale (ligne ~222) :
```python
# Avant
# [BONUS] Uncomment to enable forecast push after each cycle
# if args.forecast_endpoint_url.strip():
#     run_cmd([
#         "python3", "scripts/push_forecast.py",
#         "--url", args.forecast_endpoint_url.strip(),
#     ])

# Après
if args.forecast_endpoint_url.strip():
    run_cmd([
        "python3", "scripts/push_forecast.py",
        "--url", args.forecast_endpoint_url.strip(),
    ])
```

---

## Étape 4 — Lancer le worker avec les prévisions

```bash
INTERVAL_MINUTES=60 \
ENDPOINT_URL=http://ton-app/api/indices \
FORECAST_ENDPOINT_URL=http://ton-app/api/forecast \
bash scripts/run_hourly_worker.sh
```

À chaque cycle, le worker exécutera Steps 1→2→3→4→6 puis enverra les prévisions en POST.

---

## Étape 5 — Dashboard Streamlit (optionnel)

```bash
bash scripts/run_forecast_dashboard.sh
```

Accessible sur `http://localhost:8501` — se met à jour automatiquement toutes les 60 secondes.

Intégration React :
```html
<iframe src="http://localhost:8501" width="100%" height="600" frameborder="0" />
```

---

## Étape 6 — Pousser les prévisions manuellement (sans le worker)

```bash
# Toutes les stations
python3 scripts/push_forecast.py --url http://ton-app/api/forecast

# Filtrer par station et modèle
python3 scripts/push_forecast.py \
  --url http://ton-app/api/forecast \
  --station 07005 \
  --model AR_global
```


