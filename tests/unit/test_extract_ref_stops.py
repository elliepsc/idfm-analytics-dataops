"""Unit tests for extract_ref_stops.py."""

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "ingestion"))

import extract_ref_stops as mod


SAMPLE_EXPORT_ROWS = [
    {
        "id": "IDFM:C01727",
        "route_long_name": "RER C",
        "shortname": "C",
        "stop_id": "IDFM:463405",
        "stop_name": "Saint-Michel Notre-Dame",
        "stop_lat": "48.8534",
        "stop_lon": "2.3445",
        "mode": "RER",
        "operatorname": "SNCF",
        "nom_commune": "Paris",
        "code_insee": "75056",
    },
    {
        "id": "IDFM:C01371",
        "route_long_name": "Metro 4",
        "shortname": "4",
        "stop_id": "IDFM:21930",
        "stop_name": "Chatelet",
        "stop_lat": "48.8582",
        "stop_lon": "2.3470",
        "mode": "Metro",
        "operatorname": "RATP",
        "nom_commune": "Paris",
        "code_insee": "75056",
    },
    {
        "id": "IDFM:C00840",
        "route_long_name": "3779",
        "shortname": "3779",
        "stop_id": "IDFM:33310",
        "stop_name": "Mare a Tissier",
        "stop_lat": "48.6284",
        "stop_lon": "2.5164",
        "mode": "Bus",
        "operatorname": "Transdev",
        "nom_commune": "Lieusaint",
        "code_insee": "77251",
    },
]


FIELDS = {
    "line_external_id": "id",
    "line_name": "route_long_name",
    "line_short_name": "shortname",
    "stop_id": "stop_id",
    "stop_name": "stop_name",
    "latitude": "stop_lat",
    "longitude": "stop_lon",
    "transport_mode": "mode",
    "operator": "operatorname",
    "town": "nom_commune",
    "insee_code": "code_insee",
}


def test_normalize_stop_records_filters_metro_and_rer():
    records = mod.normalize_stop_records(
        records=SAMPLE_EXPORT_ROWS,
        fields=FIELDS,
        transport_modes=["Metro", "RER"],
    )

    assert len(records) == 2
    assert {record["transport_mode"] for record in records} == {"METRO", "RER"}
    assert all(isinstance(record["latitude"], float) for record in records)
    assert all(isinstance(record["longitude"], float) for record in records)
    assert records[0]["source"] == "idfm_ref_stops_export"


def test_download_stops_export_requires_api_key():
    with pytest.raises(ValueError, match="IDFM_API_KEY"):
        mod.download_stops_export(
            api_key="",
            dataset_id="arrets-lignes",
            base_url="https://data.iledefrance-mobilites.fr/api/explore/v2.1",
        )


def test_extract_ref_stops_writes_json_and_parquet(monkeypatch, tmp_path):
    config = {
        "idfm": {
            "base_url": "https://data.iledefrance-mobilites.fr/api/explore/v2.1",
            "datasets": {
                "ref_stops": {
                    "id": "arrets-lignes",
                    "fields": FIELDS,
                }
            },
        }
    }

    monkeypatch.setattr(mod, "load_config", lambda: config)
    monkeypatch.setattr(mod, "download_stops_export", lambda **kwargs: SAMPLE_EXPORT_ROWS)
    monkeypatch.setenv("IDFM_API_KEY", "test-key")

    json_path = mod.extract_ref_stops(
        output_dir=tmp_path,
        output_format="json",
        transport_modes=["Metro", "RER"],
        basename="ref_stops",
    )
    parquet_path = mod.extract_ref_stops(
        output_dir=tmp_path,
        output_format="parquet",
        transport_modes=["Metro", "RER"],
        basename="ref_stops_workbench",
    )

    assert json_path.exists()
    assert parquet_path.exists()
    assert "metro_rer" in json_path.name
    assert json_path.name.startswith("ref_stops-filtered_")
    assert parquet_path.suffix == ".parquet"

    with open(json_path, encoding="utf-8") as f:
        json_rows = json.load(f)
    parquet_rows = pd.read_parquet(parquet_path).to_dict(orient="records")

    assert len(json_rows) == 2
    assert len(parquet_rows) == 2
