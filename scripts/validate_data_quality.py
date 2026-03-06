"""
Great Expectations - Validation pipeline IDFM
"""

import json
import logging
import sys
from pathlib import Path

from great_expectations.expectations import (
    ExpectColumnToExist,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToMatchRegex,
    ExpectColumnValuesToNotBeNull,
)

import great_expectations as gx

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_or_create_suite(context):
    """Crée ou récupère la suite d expectations."""
    try:
        return context.suites.get("validations_rail_quality")
    except Exception:
        return context.suites.add_or_update(
            gx.ExpectationSuite(
                name="validations_rail_quality",
                expectations=[
                    ExpectColumnToExist(column="date"),
                    ExpectColumnToExist(column="stop_id"),
                    ExpectColumnToExist(column="validation_count"),
                    ExpectColumnToExist(column="ticket_type"),
                    ExpectColumnValuesToNotBeNull(column="date"),
                    ExpectColumnValuesToNotBeNull(column="stop_id"),
                    ExpectColumnValuesToNotBeNull(column="validation_count"),
                    ExpectColumnValuesToBeBetween(
                        column="validation_count",
                        min_value=0,
                        max_value=1000000,
                    ),
                    ExpectColumnValuesToMatchRegex(
                        column="date",
                        regex=r"^\d{4}-\d{2}-\d{2}$",
                    ),
                ],
            )
        )


def validate_validations(data_dir: str = "data/raw") -> bool:
    """Valide les fichiers de validations rail contre les expectations GE."""
    context = gx.get_context(mode="file", project_root_dir="great_expectations")

    files = sorted(Path(data_dir).glob("validations_*.json"))
    if not files:
        logger.warning("Aucun fichier validations trouvé dans %s", data_dir)
        return True

    latest = files[-1]
    logger.info("Validation de %s", latest)

    import pandas as pd

    with open(latest) as f:
        records = json.load(f)
    df = pd.DataFrame(records)

    ds = context.data_sources.add_or_update_pandas(name="validations_source")
    asset = ds.add_dataframe_asset(name="validations_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    suite = get_or_create_suite(context)
    results = batch.validate(suite)

    success = results.success
    logger.info(
        "Résultat: %s (%d/%d expectations passées)",
        "PASS" if success else "FAIL",
        results.statistics["successful_expectations"],
        results.statistics["evaluated_expectations"],
    )
    return success


if __name__ == "__main__":
    ok = validate_validations()
    sys.exit(0 if ok else 1)
