"""
API integration tests — run with:
    pytest tests/test_api.py -v
"""
import pytest
from fastapi.testclient import TestClient

from src.api.app import app


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


# ------------------------------------------------------------------ #
# Health
# ------------------------------------------------------------------ #

def test_health(client):
    r = client.get("/api/v1/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["stations"] > 0


# ------------------------------------------------------------------ #
# Station list
# ------------------------------------------------------------------ #

def test_station_ids_returns_list(client):
    r = client.get("/api/v1/station-ids")
    assert r.status_code == 200
    ids = r.json()
    assert isinstance(ids, list)
    assert len(ids) > 0
    assert all(isinstance(i, str) for i in ids)


def test_list_all_stations_shape(client):
    r = client.get("/api/v1/stations")
    assert r.status_code == 200
    stations = r.json()
    assert len(stations) > 0

    # Every entry must match the Station interface
    for s in stations:
        assert "id" in s
        assert "name" in s
        assert "lat" in s and "lng" in s
        assert "pollutants" in s
        assert "meteo" in s
        assert "indices" in s
        assert "timestamp" in s


# ------------------------------------------------------------------ #
# Single station
# ------------------------------------------------------------------ #

def test_get_known_station(client):
    r = client.get("/api/v1/stations/FR01011")
    assert r.status_code == 200
    s = r.json()

    # Identity
    assert s["id"] == "FR01011"
    assert isinstance(s["name"], str) and len(s["name"]) > 0

    # Coordinates
    assert isinstance(s["lat"], float)
    assert isinstance(s["lng"], float)

    # Meteo — required fields must be present and typed
    m = s["meteo"]
    assert isinstance(m["temperature"], float)
    assert isinstance(m["humidity"], float)
    assert isinstance(m["pressure"], float)
    assert isinstance(m["windSpeed"], float)
    assert isinstance(m["windDirection"], str)

    # Pollutants — at least one must be non-null
    p = s["pollutants"]
    non_null = [v for v in p.values() if v is not None]
    assert len(non_null) > 0

    # Indices
    assert len(s["indices"]) > 0
    for idx in s["indices"]:
        assert idx["id"] in ("ATMO", "IQA")
        assert 0 <= idx["value"] <= 200

    # Timestamp ISO 8601
    assert "T" in s["timestamp"]


def test_get_unknown_station_returns_404(client):
    r = client.get("/api/v1/stations/DOES_NOT_EXIST")
    assert r.status_code == 404


# ------------------------------------------------------------------ #
# History
# ------------------------------------------------------------------ #

def test_history_basic(client):
    r = client.get(
        "/api/v1/stations/FR01011/history"
        "?start=2026-03-01&end=2026-03-07"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["station_id"] == "FR01011"
    assert body["count"] > 0
    assert len(body["rows"]) == body["count"]


def test_history_pollutant_filter(client):
    r = client.get(
        "/api/v1/stations/FR01011/history"
        "?start=2026-03-01&end=2026-03-07&pollutants=PM2.5,NO2"
    )
    assert r.status_code == 200
    rows = r.json()["rows"]
    pollutants_returned = {row["pollutant"] for row in rows}
    assert pollutants_returned.issubset({"PM2.5", "NO2"})


def test_history_date_range_respected(client):
    r = client.get(
        "/api/v1/stations/FR01011/history"
        "?start=2026-03-05&end=2026-03-05T23:59:59"
    )
    assert r.status_code == 200
    rows = r.json()["rows"]
    for row in rows:
        assert row["timestamp"].startswith("2026-03-05")


def test_history_unknown_station_returns_404(client):
    r = client.get("/api/v1/stations/UNKNOWN/history?start=2026-03-01&end=2026-03-07")
    assert r.status_code == 404


# ------------------------------------------------------------------ #
# Indices
# ------------------------------------------------------------------ #

def test_indices_known_station(client):
    r = client.get("/api/v1/indices/FR01011")
    assert r.status_code == 200
    indices = r.json()
    assert isinstance(indices, list)
    assert len(indices) > 0

    ids_returned = {i["id"] for i in indices}
    assert ids_returned.issubset({"ATMO", "IQA"})

    for idx in indices:
        assert isinstance(idx["value"], float)
        assert idx["value"] >= 0
        assert isinstance(idx["label"], str)
        if idx["weights"] is not None:
            assert all(isinstance(w, float) for w in idx["weights"].values())


def test_indices_unknown_station_returns_404(client):
    r = client.get("/api/v1/indices/UNKNOWN")
    assert r.status_code == 404


# ------------------------------------------------------------------ #
# Station with CO data
# ------------------------------------------------------------------ #

def test_station_with_co_data(client):
    """FR02008 measures CO — verify the co field is populated."""
    r = client.get("/api/v1/stations/FR02008")
    assert r.status_code == 200
    p = r.json()["pollutants"]
    assert p["co"] is not None
    assert p["co"] >= 0


# ------------------------------------------------------------------ #
# Meteo range validation
# ------------------------------------------------------------------ #

def test_meteo_values_in_physical_range(client):
    r = client.get("/api/v1/stations")
    assert r.status_code == 200
    for s in r.json():
        m = s["meteo"]
        assert -30 <= m["temperature"] <= 50,  f"{s['id']}: temp {m['temperature']}"
        assert 0 <= m["humidity"] <= 100,       f"{s['id']}: humidity {m['humidity']}"
        assert 950 <= m["pressure"] <= 1050,    f"{s['id']}: pressure {m['pressure']}"
        assert m["windSpeed"] >= 0,             f"{s['id']}: windSpeed {m['windSpeed']}"
