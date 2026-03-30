"""
Compute composite air quality indices from pollutant readings.

ATMO index: French national air quality index (0–100+ scale)
IQA: custom weighted index over available pollutants
"""
from __future__ import annotations

import math
from typing import Optional

from .models import CompositeIndex, PollutantData


# --- ATMO index sub-index thresholds ---
# Each row: (threshold_value, sub_index)
# Based on French ATMO breakpoints adapted to 0–100 linear scale.
_ATMO_BREAKPOINTS: dict[str, list[tuple[float, float]]] = {
    "pm25": [(5, 10), (10, 20), (15, 30), (25, 40), (35, 50), (50, 60), (75, 70), (100, 80), (150, 90), (math.inf, 100)],
    "pm10": [(10, 10), (20, 20), (25, 30), (50, 40), (65, 50), (90, 60), (107, 70), (200, 80), (250, 90), (math.inf, 100)],
    "no2":  [(10, 10), (20, 20), (45, 30), (80, 40), (110, 50), (150, 60), (200, 70), (270, 80), (400, 90), (math.inf, 100)],
    "o3":   [(20, 10), (40, 20), (70, 30), (90, 40), (110, 50), (130, 60), (150, 70), (180, 80), (240, 90), (math.inf, 100)],
    "so2":  [(10, 10), (20, 20), (60, 30), (100, 40), (150, 50), (200, 60), (275, 70), (400, 80), (500, 90), (math.inf, 100)],
}

_ATMO_WEIGHTS = {"pm25": 0.3, "pm10": 0.2, "no2": 0.25, "o3": 0.15, "so2": 0.1}


def _sub_index(pollutant: str, value: float) -> float:
    breakpoints = _ATMO_BREAKPOINTS.get(pollutant, [])
    for threshold, idx in breakpoints:
        if value <= threshold:
            return idx
    return 100.0


def compute_atmo(p: PollutantData) -> Optional[CompositeIndex]:
    available: dict[str, float] = {}
    if p.pm25 is not None:
        available["pm25"] = p.pm25
    if p.pm10 is not None:
        available["pm10"] = p.pm10
    if p.no2 is not None:
        available["no2"] = p.no2
    if p.o3 is not None:
        available["o3"] = p.o3
    if p.so2 is not None:
        available["so2"] = p.so2

    if not available:
        return None

    total_weight = sum(_ATMO_WEIGHTS[k] for k in available)
    if total_weight == 0:
        return None

    weighted_sum = sum(_sub_index(k, v) * _ATMO_WEIGHTS[k] for k, v in available.items())
    score = round(weighted_sum / total_weight, 1)
    used_weights = {k: round(_ATMO_WEIGHTS[k] / total_weight, 3) for k in available}

    return CompositeIndex(
        id="ATMO",
        label="Indice ATMO",
        value=score,
        weights=used_weights,
    )


def compute_iqa(p: PollutantData) -> Optional[CompositeIndex]:
    """Simple IQA: max sub-index across all available pollutants (worst-pollutant approach)."""
    values: list[float] = []
    weights: dict[str, float] = {}

    for pollutant, val in [("pm25", p.pm25), ("pm10", p.pm10), ("no2", p.no2),
                            ("o3", p.o3), ("so2", p.so2)]:
        if val is not None:
            si = _sub_index(pollutant, val)
            values.append(si)
            weights[pollutant] = round(si, 1)

    if not values:
        return None

    return CompositeIndex(
        id="IQA",
        label="Indice Qualité Air",
        value=round(max(values), 1),
        weights=weights,
    )


def compute_indices(p: PollutantData) -> list[CompositeIndex]:
    indices = []
    atmo = compute_atmo(p)
    if atmo:
        indices.append(atmo)
    iqa = compute_iqa(p)
    if iqa:
        indices.append(iqa)
    return indices
