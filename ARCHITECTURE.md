# IDFM Analytics DataOps — Architecture Notes

Quick reference for architectural decisions that are not obvious from reading
the models individually. Intended for contributors and reviewers.

---

## Stop Identifier Systems

Two parallel identifier systems coexist in the project. Understanding which
one to use depends on what you are trying to do.

### System 1 — `stop_id` (STIF format)

- **Source**: `raw_validations.code_stif_arret`
- **Format**: numeric string, e.g. `"401"`, `"8775860"`
- **Grain**: physical stop (quai / platform level)
- **Present in**: `stg_validations_rail_daily`, `fct_validations_daily`, `dim_stop`
- **Use when**: working with validation counts at stop level

### System 2 — `station_id_zdc` (IDFM Zone de Correspondance)

- **Source**: `raw_validations.ida` → propagated through staging and facts
- **Format**: integer, e.g. `474`, `1234`
- **Grain**: station (groups multiple stops / platforms under one interchange zone)
- **Present in**: `fct_validations_daily`, `stg_ref_stations`, `mart_validations_station_daily`
- **Use when**: working with geographic data (lat/lon), Looker Studio maps,
  or any analysis requiring station-level aggregation

### Two Paths to Station Data

```
Path 1 — stop name only
fct_validations_daily.stop_name
→ direct, no join needed, but no coordinates

Path 2 — station with coordinates (for maps / geo analysis)
fct_validations_daily.station_id_zdc
  → JOIN stg_ref_stations ON station_id_zdc = station_id
  → gives latitude, longitude, transport_mode, operator
  → OR use mart_validations_station_daily directly (pre-joined)
```

### Known Limitation

`stop_id` (STIF) and `station_id_zdc` (IDFM) cannot be reliably joined
without an intermediate mapping table. A stop may belong to multiple zones
and the mapping is not always 1:1. This is tracked as a **V4 improvement**
via GTFS data (`offre-horaires-tc-gtfs-idfm` in `apis.yml`), which provides
the authoritative stop → station mapping.

---

## Operator Code System (`line_code_trns`)

`fct_validations_daily.line_code_trns` is the STIF transport operator code —
a numeric string identifying the operator, not the line.

| Code range | Operator |
|---|---|
| `100` | RATP |
| `760`–`762` | Optile (private operators) |
| `800`–`890` | SNCF Transilien |

**This is NOT the same as `dim_line.line_id`** (IDFM format, e.g. `"C01836"`).
There is no direct FK between `fct_validations_daily` and `dim_line`.
The bridge goes through `stg_ref_stop_lines` (stop_id → line_id) — used
in `mart_line_demand_vs_punctuality` (V3).

---

## Incremental Strategy

Both fact tables use `insert_overwrite` (BigQuery partition replacement):

- **`fct_validations_daily`**: partitioned by `validation_date` (DAY) — safe,
  each day's partition is fully replaced on each run
- **`fct_punctuality_monthly`**: partitioned by `month_date` (MONTH) — risk:
  SNCF may publish retroactive corrections for past months. Use
  `make dbt-refresh-prod MODEL=fct_punctuality_monthly` to recompute a
  specific past month. Do not run a full-refresh without a confirmed need.

In **dev**, both models filter to `var('dev_start_date')` → `var('dev_end_date')`
(default: 2024) to avoid scanning 4.1M rows on every `dbt build`.

---

## Dataset Naming

In production, the `generate_schema_name` macro produces:

| dbt layer | BigQuery dataset |
|---|---|
| staging | `transport_staging_staging` ⚠️ |
| core | `transport_staging_core` |
| marts | `transport_staging_analytics` |
| seeds | `transport_raw` |
| elementary | `transport_staging_elementary` |

> **Note**: `transport_staging_staging` is a known naming issue (double suffix).
> Renaming requires a maintenance window (Terraform + DAGs + Looker Studio).
> Tracked as V2 task 2b.

---

## V4 Roadmap Dependency

Several V3 analytical improvements are constrained by missing joins:

- `mart_line_demand_vs_punctuality` (3q) — requires verifying
  `stg_ref_stop_lines` coverage before committing
- `mart_station_lq_by_commune` (3k) — requires verifying
  `dim_stop.town` field quality before committing
- Full stop ↔ station reconciliation — requires GTFS (V4)
