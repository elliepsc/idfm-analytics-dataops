# MARTS Guide — How to use the analytical marts

Quick reference for each mart : business question, grain, interpretation,
known limitations, and decision use case.
Read this before querying a mart directly or building a dashboard.

---

## Business marts (`models/marts/business/`)

### `mart_validations_station_daily`
| | |
|---|---|
| **Business question** | How many validations per station per day, with geographic coordinates? |
| **Grain** | 1 station × 1 day |
| **Key columns** | `station_id_zdc`, `daily_validation_count`, `latitude`, `longitude` |
| **Interpretation** | Primary source for the Looker Studio Station Map (Page 5). Covers ~93% of validation volume via primary source + complement seed. |
| **Known limitations** | Uses `station_id_zdc` (IDFM Zone de Correspondance), not `stop_id` (STIF). See ARCHITECTURE.md for the two identifier systems. |
| **Decision use case** | Geographic demand analysis, station capacity planning, map visualisation. |

---

### `mart_network_scorecard_monthly`
| | |
|---|---|
| **Business question** | How is the network performing month over month on demand and punctuality? |
| **Grain** | 1 line × 1 month |
| **Key columns** | `line_id`, `month_date`, `total_validations`, `punctuality_rate`, `risk_category`, `mom_validation_growth_pct` |
| **Interpretation** | `risk_category` (high/medium/low) combines volume drop AND punctuality degradation. A line is `high_risk` only when both indicators degrade simultaneously. MoM comparison is noisy — prefer `mart_validations_yoy_monthly` for trend analysis. |
| **Known limitations** | Validations join is at network level (no line_id in raw_validations). Demand metrics apply to the whole network, not per line. See `mart_line_demand_vs_punctuality` for a true line-level cross. |
| **Decision use case** | Executive scorecard, weekly operations review, risk monitoring. |

---

### `mart_navigo_penetration_rate`
| | |
|---|---|
| **Business question** | Which stations have the highest share of subscription riders vs occasional? |
| **Grain** | 1 station × 1 month |
| **Key columns** | `station_id_zdc`, `navigo_penetration_rate`, `occasional_rate`, `loyalty_segment` |
| **Interpretation** | High `navigo_penetration_rate` (>80%) = captive, loyal ridership — these stations are structurally dependent on the network. Low rate = tourist / occasional traffic that is sensitive to disruptions. `loyalty_segment`: `high_loyalty` ≥ 80%, `mixed` 50-80%, `occasional_dominant` < 50%. |
| **Known limitations** | Depends on `is_subscription` flag from `stg_validations_rail_daily` (3a). Requires `dbt build` after 3a deployment. |
| **Decision use case** | Network loyalty KPI, identifying stations where service degradation will drive ridership loss. |

---

### `mart_ticket_share_over_time`
| | |
|---|---|
| **Business question** | How does the composition of ticket types evolve over time? |
| **Grain** | 1 category × 1 month |
| **Key columns** | `category`, `validation_month`, `category_share_pct`, `share_mom_change_pp`, `disruption_signal_flag` |
| **Interpretation** | ⭐ **Most original insight in the project.** During network disruptions, `Occasionnel` share drops sharply (occasional riders avoid the network) while `Abonnement` share resists (captive demand). `disruption_signal_flag = TRUE` when Occasionnel share drops > 3pp MoM. |
| **Known limitations** | Requires `ticket_type_mapping` seed. Some raw ticket codes may not match the seed (mapped to NULL → COALESCE to 'Autre'). |
| **Decision use case** | Disruption impact analysis, structural dependency assessment, demand elasticity by rider type. |

---

### `mart_validations_yoy_monthly`
| | |
|---|---|
| **Business question** | How does this month compare to the same month last year? |
| **Grain** | 1 month (network total) |
| **Key columns** | `validation_month`, `total_validations`, `same_month_last_year`, `yoy_growth_pct`, `yoy_trend` |
| **Interpretation** | LAG(12) eliminates seasonal noise present in MoM. Use this mart for trend detection instead of `mart_network_scorecard_monthly` which uses LAG(1). `yoy_trend`: strong_growth / moderate_growth / stable / moderate_decline / strong_decline. `no_baseline` = first year of data (2023). |
| **Known limitations** | Network-level only (no line breakdown). First 12 months have `same_month_last_year = NULL`. |
| **Decision use case** | Annual performance review, budget planning, post-disruption recovery measurement. |

---

### `mart_station_ranking_monthly`
| | |
|---|---|
| **Business question** | Which stations are rising or falling in the network ranking? |
| **Grain** | 1 station × 1 month |
| **Key columns** | `station_id_zdc`, `station_rank`, `prev_month_rank`, `rank_change`, `rank_trend` |
| **Interpretation** | A station dropping 5+ positions in one month signals an unresolved incident, line closure, or structural service change. `rank_trend`: rising / falling / stable / new_entry. Note: `rank_change` is negative when improving (lower rank number = better). |
| **Known limitations** | Source is `mart_validations_station_daily` — inherits its ~93% station coverage. New stations (new_entry) have no `prev_month_rank`. |
| **Decision use case** | Incident detection, service quality monitoring, station performance comparison. |

---

### `mart_punctuality_vs_target`
| | |
|---|---|
| **Business question** | Which lines are persistently missing their contractual punctuality target? |
| **Grain** | 1 line × 1 month |
| **Key columns** | `line_id`, `punctuality_rate`, `target_punctuality_rate`, `gap_to_target`, `target_status`, `structurally_fragile` |
| **Interpretation** | Replaces the fixed `quality_category` thresholds (same for all lines) with per-line contractual objectives. `target_status`: above / near / below / critical. `structurally_fragile = TRUE` when line misses target 6+ months out of the last 12. |
| **Known limitations** | Only covers lines with a defined target in `line_punctuality_target` seed (13 Transilien/RER lines). Lines without targets are excluded (WHERE target IS NOT NULL). |
| **Decision use case** | Contractual compliance reporting, identifying chronically underperforming lines, STIF reporting. |

---

## Interpretive marts (`models/marts/business/` — V3 interpretive layer)

### `mart_line_punctuality_trajectory`
| | |
|---|---|
| **Business question** | Which lines are structurally fragile? Which are resilient corridors? Which are recovering vs chronically weak? |
| **Grain** | 1 line × 1 month |
| **Key columns** | `line_id`, `trajectory_label`, `bad_months_last_3`, `bad_months_last_6`, `recovery_flag` |
| **Interpretation** | Transforms punctuality time series into behavioural profiles. `trajectory_label`: resilient_corridor / structurally_fragile / recovering / chronically_weak / volatile_under_pressure. More interpretable than a raw punctuality score. |
| **Known limitations** | Transilien/RER lines only (scope of fct_punctuality_monthly). Requires ~6 months of data for trajectory labels to be meaningful. |
| **Decision use case** | Network fragility assessment, investment prioritisation, peer review presentation. |

---

### `mart_stop_volume_profile`
| | |
|---|---|
| **Business question** | Which stations show stable vs volatile demand patterns? |
| **Grain** | 1 station × 1 month (top stations by volume) |
| **Key columns** | `station_id_zdc`, `stop_profile`, `rolling_avg_3m`, `cv` (coefficient of variation) |
| **Interpretation** | 3 profiles based on coefficient of variation (std/mean): `high_volume_stable`, `high_volume_volatile`, `low_volume_erratic`. Filtered to stations above a minimum volume threshold. |
| **Known limitations** | `seasonal` profile excluded — insufficient data history (2023-2025) to reliably distinguish seasonal from erratic patterns. |
| **Decision use case** | Station capacity planning, identifying operationally unstable stops. |

---

### `mart_line_demand_vs_punctuality`
| | |
|---|---|
| **Business question** | Do high-demand lines degrade faster? Which lines remain resilient under pressure? Are some lines structurally fragile even without high demand? |
| **Grain** | 1 line × 1 month |
| **Key columns** | `line_id`, `demand_index`, `punctuality_rate`, `demand_punctuality_profile` |
| **Interpretation** | ⚠️ **Conditional mart** — requires verifying `stg_ref_stop_lines` coverage before using. Bridges validations (stop level) to punctuality (line level) via stop→line mapping. `demand_punctuality_profile`: resilient_under_pressure / fragile_under_pressure / structurally_fragile / stable_low_demand. |
| **Known limitations** | Bridge via `stg_ref_stop_lines` may have incomplete coverage — run `SELECT COUNT(*), COUNT(DISTINCT line_id) FROM stg_ref_stop_lines` to validate. Demand is aggregated across all stops of a line (approximate). |
| **Decision use case** | Most analytically rich mart in the project. Enables hypothesis-driven analysis of network resilience. |

---

## Monitoring marts (`models/marts/monitoring/`)

### `fct_data_health_daily`
| | |
|---|---|
| **Business question** | Are the core tables fresh and within SLA? |
| **Grain** | 1 table × 1 day |
| **Key columns** | `table_name`, `sla_met`, `freshness_hours`, `sla_status_label`, `anomaly_source` |
| **Interpretation** | `sla_status_label`: OK / WARNING / CRITICAL. `anomaly_source` (V3): distinguishes `pipeline` anomalies (freshness, SLA breach, row_count) from `business` anomalies (volume_zscore). Do not mix the two in alerts. |
| **Known limitations** | Monitors only 3 tables: fct_validations_daily, fct_punctuality_monthly, mart_network_scorecard_monthly. |
| **Decision use case** | Daily data health check, SLA compliance reporting, Looker Studio data quality page. |

---

### `mart_anomaly_history`
| | |
|---|---|
| **Business question** | How many anomalies per month? Is the z-score threshold 2.5 well calibrated? |
| **Grain** | 1 anomaly event |
| **Key columns** | `anomaly_date`, `z_score`, `direction`, `severity`, `anomalies_this_month` |
| **Interpretation** | Meta-monitoring mart — analyses the monitoring system itself. Use to detect if anomalies cluster around school holidays (z-score baseline bias) or if the threshold generates too many/few alerts. |
| **Known limitations** | Reads directly from `transport_raw.dag_metrics` (outside dbt lineage until this mart). Only includes rows where `is_anomaly = TRUE`. Requires dag_metrics to exist in BigQuery. |
| **Decision use case** | Threshold calibration, alert fatigue analysis, monitoring quality review. |

---

## Commit note
All marts above were created in V3 (April 2026).
For the full analytical architecture, see `ARCHITECTURE.md`.
For deployment order, see `BACKLOG.md` (V3 items 3a→3q).
