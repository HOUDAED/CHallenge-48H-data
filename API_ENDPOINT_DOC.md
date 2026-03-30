# Documentation de l'API/endpoint pour les développeurs

## Endpoint de récupération des données stations

- **URL** : [à compléter par l'infra/dev]
- **Méthode** : POST (production) ou GET (init)
- **Payload retourné** : Tableau JSON de `Station` (voir schéma ci-dessous)

### Exemple de réponse

[
  {
    "id": "07005",
    "name": "ABBEVILLE",
    "lat": 50.136,
    "lng": 1.834,
    "pollutants": {
      "no2": 42.0,
      "pm10": 18.0,
      "o3": 55.0,
      "pm25": 12.0
    },
    "meteo": {
      "temperature": 8.2,
      "humidity": 81,
      "pressure": 1012,
      "windSpeed": 12,
      "windDirection": "NNW"
    },
    "indices": [
      {
        "id": "IPN",
        "label": "Indice Pollution",
        "value": 38.44
      }
    ],
    "timestamp": "2024-01-01T00:00:00Z"
  }
]

### Schéma TypeScript

```ts
export interface PollutantData {
   pm25?: number    // µg/m³
   pm10?: number    // µg/m³
   no2?: number     // µg/m³
   o3?: number      // µg/m³
   co?: number      // mg/m³
   so2?: number     // µg/m³
}
export interface MeteoData {
   temperature: number    // °C
   humidity: number       // %
   pressure: number       // hPa
   windSpeed: number      // km/h
   windDirection: string  // ex: "NNW"
   rainfall?: number      // mm
}
export interface CompositeIndex {
   id: string             // ex: "IQA", "ATMO", "custom"
   label: string          // nom affiché
   value: number          // 0–100+
   weights?: Record<string, number>  // pondérations utilisées par l'équipe data
}
export interface Station {
   id: string
   name: string
   lat: number
   lng: number
   pollutants: PollutantData
   meteo: MeteoData
   indices: CompositeIndex[]   // 1 à N indices composites
   timestamp: string           // ISO 8601
}
```

### Initialisation (historique)

- Pour initialiser la base avec les 10 derniers jours :
  - Lancer le script avec l'option `--init-last-days 10`
  - Cela génère un fichier JSON par jour dans `data/processed/outbox/`

### Fréquence d'appel

- Le pipeline est prévu pour être appelé toutes les 60 minutes (CRON ou équivalent).
- Il est idempotent : chaque appel produit les données du moment, sans écrasement intempestif.

### Contact

- Pour toute question sur le format ou l'API, contacter l'équipe data.
