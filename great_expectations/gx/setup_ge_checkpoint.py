"""
Setup Great Expectations checkpoint for raw_validations BigQuery table.
GE version: 1.x (fluent API)

Run from project root:
    python great_expectations/gx/setup_ge_checkpoint.py
"""

import os
import sys

import great_expectations as gx

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────

PROJECT_ID = "idfm-analytics-dev-488611"
DATASET = "transport_raw"
TABLE = "raw_validations"

# Script is at great_expectations/gx/setup_ge_checkpoint.py
# GX_DIR must point to great_expectations/gx/
GX_DIR = os.path.dirname(os.path.abspath(__file__))

# ─────────────────────────────────────────────────────────────
# Load context
# ─────────────────────────────────────────────────────────────

print(f"📂 Loading GE context from: {GX_DIR}")
context = gx.get_context(context_root_dir=GX_DIR)

# ─────────────────────────────────────────────────────────────
# Datasource BigQuery
# ─────────────────────────────────────────────────────────────

DATASOURCE_NAME = "bigquery_raw"

try:
    context.data_sources.delete(DATASOURCE_NAME)
    print(f"🗑️  Removed existing datasource: {DATASOURCE_NAME}")
except Exception:
    pass

print(f"🔌 Creating BigQuery datasource: {DATASOURCE_NAME}")
datasource = context.data_sources.add_or_update_sql(
    name=DATASOURCE_NAME,
    connection_string=f"bigquery://{PROJECT_ID}/{DATASET}",
)

# ─────────────────────────────────────────────────────────────
# Asset + Batch definition
# ─────────────────────────────────────────────────────────────

ASSET_NAME = "raw_validations_asset"
BATCH_DEF_NAME = "full_table_batch"

print(f"📦 Adding table asset: {ASSET_NAME}")
asset = datasource.add_table_asset(name=ASSET_NAME, table_name=TABLE)
batch_definition = asset.add_batch_definition_whole_table(name=BATCH_DEF_NAME)

# ─────────────────────────────────────────────────────────────
# Expectation Suite — load from existing JSON file
# ─────────────────────────────────────────────────────────────

SUITE_NAME = "validations_rail_quality"

print(f"📋 Loading expectation suite: {SUITE_NAME}")

# In GE 1.x, suites saved as JSON must be retrieved via context.suites
# If not found, add it (reads from expectations/ store automatically)
try:
    suite = context.suites.get(SUITE_NAME)
except Exception:
    suite = None

if suite is None:
    # Suite JSON exists on disk but not registered — add it
    print(f"   Suite not in registry, adding from expectations store...")
    suite = context.suites.add(gx.ExpectationSuite(name=SUITE_NAME))

print(f"✅ Suite loaded: {len(suite.expectations)} expectations")

# If suite has 0 expectations (freshly registered), reload from JSON
if len(suite.expectations) == 0:
    import json
    suite_path = os.path.join(GX_DIR, "expectations", f"{SUITE_NAME}.json")
    print(f"   Loading expectations from: {suite_path}")
    with open(suite_path) as f:
        suite_data = json.load(f)

    for exp_dict in suite_data["expectations"]:
        suite.add_expectation(
            gx.expectations.UnexpectedRowsExpectation
            if exp_dict["type"] == "unexpected_rows_expectation"
            else getattr(gx.expectations,
                "".join(w.capitalize() for w in exp_dict["type"].split("_")))
            (**exp_dict.get("kwargs", {}))
        )
    suite.save()
    print(f"✅ Loaded {len(suite.expectations)} expectations from JSON")

# ─────────────────────────────────────────────────────────────
# Validation Definition
# ─────────────────────────────────────────────────────────────
# Delete the regex expectation on date (incompatible with BQ DATE type)
def get_exp_type(e):
    return getattr(e, 'type', None) or getattr(e, 'expectation_type', None) or type(e).__name__

suite.expectations = [
    e for e in suite.expectations
    if get_exp_type(e) != "expect_column_values_to_match_regex"
]
suite.save()


VALIDATION_DEF_NAME = "raw_validations_quality_check"

try:
    context.validation_definitions.delete(VALIDATION_DEF_NAME)
    print(f"🗑️  Removed existing validation definition: {VALIDATION_DEF_NAME}")
except Exception:
    pass

print(f"🔍 Creating validation definition: {VALIDATION_DEF_NAME}")
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(
        name=VALIDATION_DEF_NAME,
        data=batch_definition,
        suite=suite,
    )
)

# ─────────────────────────────────────────────────────────────
# Checkpoint
# ─────────────────────────────────────────────────────────────

CHECKPOINT_NAME = "raw_validations_checkpoint"

try:
    context.checkpoints.delete(CHECKPOINT_NAME)
    print(f"🗑️  Removed existing checkpoint: {CHECKPOINT_NAME}")
except Exception:
    pass

print(f"🚦 Creating checkpoint: {CHECKPOINT_NAME}")
checkpoint = context.checkpoints.add(
    gx.Checkpoint(
        name=CHECKPOINT_NAME,
        validation_definitions=[validation_definition],
        result_format="SUMMARY",
    )
)

# ─────────────────────────────────────────────────────────────
# Run checkpoint
# ─────────────────────────────────────────────────────────────

print(f"\n🏃 Running checkpoint: {CHECKPOINT_NAME}")
result = checkpoint.run()

# ─────────────────────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("📊 CHECKPOINT RESULTS")
print("="*60)

success = result.success
print(f"Overall: {'✅ PASSED' if success else '❌ FAILED'}")

# for vr_key, vr in result.run_results.items():
#     stats = vr["validation_result"]["statistics"]
#     print(f"\n  Evaluated:  {stats['evaluated_expectations']}")
#     print(f"  Successful: {stats['successful_expectations']}")
#     print(f"  Failed:     {stats['unsuccessful_expectations']}")

#     if not vr["validation_result"]["success"]:
#         print("\n  ❌ Failed expectations:")
#         for exp_result in vr["validation_result"]["results"]:
#             if not exp_result["success"]:
#                 print(f"    - {exp_result['expectation_config']['type']}")
#                 print(f"      column: {exp_result['expectation_config']['kwargs'].get('column', 'N/A')}")

for vr_key, vr in result.run_results.items():
    stats = vr.statistics
    print(f"\n  Evaluated:  {stats['evaluated_expectations']}")
    print(f"  Successful: {stats['successful_expectations']}")
    print(f"  Failed:     {stats['unsuccessful_expectations']}")

    if not vr.success:
        print("\n  ❌ Failed expectations:")
        for exp_result in vr.results:
            if not exp_result.success:
                print(f"    - {exp_result.expectation_config.type}")
                col = exp_result.expectation_config.kwargs.get('column', 'N/A')
                print(f"      column: {col}")

print("="*60)

if not success:
    sys.exit(1)
