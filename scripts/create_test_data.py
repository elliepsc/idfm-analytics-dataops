"""
create_test_data.py — Generate local fixture data for Great Expectations validation.

Creates a minimal JSON file at data/raw/validations_test_<date>.json that
validate_data_quality.py uses to run GE expectations locally in CI
(without requiring a live BigQuery connection).

Called by: .github/workflows/data-quality.yml (Great Expectations validate step)
Run locally: make ge-validate
"""
import json
import pathlib

pathlib.Path("data/raw").mkdir(parents=True, exist_ok=True)
test_data = [
    {
        "date": "2024-01-01",
        "stop_id": "401",
        "stop_name": "CHATELET",
        "ticket_type": "Navigo",
        "validation_count": 1000,
    },
    {
        "date": "2024-01-02",
        "stop_id": "402",
        "stop_name": "GARE DU NORD",
        "ticket_type": "Navigo",
        "validation_count": 1500,
    },
]
pathlib.Path("data/raw/validations_test_2024-01-01.json").write_text(
    json.dumps(test_data)
)
print("Test data created")
