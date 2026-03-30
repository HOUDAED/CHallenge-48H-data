# CLAUDE.md

## Project Overview

Build a Python-based data integration platform that combines air quality pollution data (LCSQA) and meteorological data (SYNOP OMM) for France's 13 largest cities, providing cleaned, normalized datasets via REST API endpoints.

---

## Objectives

1. **Retrieve historical data** from both APIs for years **2024** and **2025** and **2026**
2. **Enable real-time data refresh** from both OpenAPI datasets
3. **Clean and normalize data** so both datasets can be analyzed together
4. **Provide REST API endpoints** with specific TypeScript interface compliance
5. **Implement orchestrator** to manage the entire data pipeline

---

## Target Cities (Top 13)

Paris, Lyon, Marseille, Lille, Toulouse, Bordeaux, Nice, Nantes, Strasbourg, Rennes, Grenoble, Rouen, Toulon

---

## Data Sources

### Air Quality API (LCSQA)
- **Dataset ID:** `5b98b648634f415309d52a50`
- **URL:** https://www.data.gouv.fr/fr/datasets/donnees-temps-reel-de-mesure-des-concentrations-de-polluants-atmospheriques-reglementes-1/
- **Pollutants:** PM2.5, PM10, NO₂, O₃, CO, SO₂
- **Frequency:** Hourly

### Meteorological API (SYNOP OMM)
- **Dataset ID:** `686f8595b351c06a3a790867`
- **URL:** https://www.data.gouv.fr/fr/datasets/archive-synop-omm/
- **Parameters:** Temperature, humidity, pressure, wind (speed/direction), rainfall
- **Frequency:** 3-hourly

---

## Required API Response Interfaces

The API must return data matching these **exact TypeScript interfaces**:

```typescript
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
    weights?: Record<string, number>  // pondérations utilisées
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

---

## Core Requirements

### 1. Data Acquisition
- Fetch historical data (2024, 2025, 2026) from both APIs
- Fetch real-time data with hourly refresh capability
- Identify stations within each city (10-20km radius)

### 2. Data Cleaning Methods
Must handle:
- **Pollution data:** Remove negatives, cap extremes (PM2.5 > 500, NO₂ > 400), handle maintenance periods
- **Meteo data:** Validate ranges (temp: -30 to +50°C, humidity: 0-100%, pressure: 950-1050 hPa)
- **Both:** Remove outliers (IQR method), handle missing values (interpolation), remove duplicates

### 3. Data Normalization Methods
Must provide:
- **Timestamp alignment:** Convert SYNOP 3-hourly → hourly (interpolation)
- **Unit standardization:** All pollutants in µg/m³ (except CO: mg/m³), wind