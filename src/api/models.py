"""
Pydantic models matching the required TypeScript interfaces.
"""
from typing import Optional
from pydantic import BaseModel


class PollutantData(BaseModel):
    pm25: Optional[float] = None   # µg/m³
    pm10: Optional[float] = None   # µg/m³
    no2: Optional[float] = None    # µg/m³
    o3: Optional[float] = None     # µg/m³
    co: Optional[float] = None     # mg/m³
    so2: Optional[float] = None    # µg/m³


class MeteoData(BaseModel):
    temperature: float       # °C
    humidity: float          # %
    pressure: float          # hPa
    windSpeed: float         # km/h
    windDirection: str       # ex: "NNW"
    rainfall: Optional[float] = None  # mm


class CompositeIndex(BaseModel):
    id: str                             # ex: "IQA", "ATMO"
    label: str                          # displayed name
    value: float                        # 0–100+
    weights: Optional[dict[str, float]] = None  # weights used


class Station(BaseModel):
    id: str
    name: str
    lat: float
    lng: float
    pollutants: PollutantData
    meteo: MeteoData
    indices: list[CompositeIndex]
    timestamp: str   # ISO 8601
