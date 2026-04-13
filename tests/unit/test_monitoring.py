"""
Unit tests for orchestration/airflow/dags/utils/monitoring.py

Behaviors tested:
  - check_statistical_anomaly: z-score computation, anomaly detection,
    edge cases (std=0, empty baseline, insufficient data, missing today)

All tests mock the BigQuery client — no GCP credentials required.

Coverage target: check_statistical_anomaly() — the most critical
production code in the monitoring stack (z-score anomaly detection).
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

# Add dags/ so that `from utils.config import ...` resolves inside monitoring.py
sys.path.insert(
    0,
    str(
        Path(__file__).parent.parent.parent
        / "orchestration"
        / "airflow"
        / "dags"
    ),
)
# Add dags/utils/ so that `import monitoring` resolves directly
sys.path.insert(
    0,
    str(
        Path(__file__).parent.parent.parent
        / "orchestration"
        / "airflow"
        / "dags"
        / "utils"
    ),
)

from monitoring import check_statistical_anomaly

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────


def _make_bq_rows(date_counts: dict):
    """
    Build a list of mock BigQuery Row objects from {date_str: count}.
    Mimics the structure returned by client.query().result().
    Note: validation_date stored as string to match str() comparison in monitoring.py.
    """
    rows = []
    for date_str, count in date_counts.items():
        row = Mock()
        row.validation_date = (
            date_str  # str — monitoring.py compares str(row.validation_date)
        )
        row.daily_total = count
        rows.append(row)
    return rows


def _patch_bq(rows):
    """
    Context manager: patches google.cloud.bigquery.Client so that
    client.query().result() returns the given rows list.

    Note: monitoring.py imports bigquery inside the function body
    (from google.cloud import bigquery), so we must patch at the
    google.cloud.bigquery level, not monitoring.bigquery.

    Returns a list (not iterator) — monitoring.py calls list() then
    iterates again, so an iterator would be exhausted after the first pass.
    """
    mock_client = MagicMock()
    mock_client.query.return_value.result.return_value = rows  # list, not iter
    return patch("google.cloud.bigquery.Client", return_value=mock_client)


# ─────────────────────────────────────────────────────────────
# Normal cases
# ─────────────────────────────────────────────────────────────


class TestCheckStatisticalAnomalyNormal:

    def test_no_anomaly_when_today_equals_mean(self):
        """z_score = 0 when today == mean of baseline. is_anomaly must be False."""
        rows = _make_bq_rows(
            {
                "2024-01-01": 1000,
                "2024-01-02": 1000,
                "2024-01-03": 1000,
                "2024-01-04": 1000,
                "2024-01-05": 1000,
                "2024-01-06": 1000,
                "2024-01-07": 1000,  # today
            }
        )
        with _patch_bq(rows):
            result = check_statistical_anomaly(
                project_id="test-project",
                execution_date="2024-01-07",
            )

        assert result["z_score"] == 0.0
        assert result["is_anomaly"] is False
        assert result["direction"] == "normal"

    def test_anomaly_detected_low_volume(self):
        """Large drop below baseline triggers is_anomaly=True, direction='low'."""
        # Baseline must have variance for z-score to be non-zero
        # Using realistic varying values around 10000 ± ~500
        rows = _make_bq_rows(
            {
                "2024-01-01": 9800,
                "2024-01-02": 10200,
                "2024-01-03": 9700,
                "2024-01-04": 10300,
                "2024-01-05": 9900,
                "2024-01-06": 10100,
                "2024-01-07": 100,  # today — massive drop, should trigger anomaly
            }
        )
        with _patch_bq(rows):
            result = check_statistical_anomaly(
                project_id="test-project",
                execution_date="2024-01-07",
            )

        assert result["is_anomaly"] is True
        assert result["direction"] == "low"
        assert result["z_score"] < -2.5

    def test_anomaly_detected_high_volume(self):
        """Large spike above baseline triggers is_anomaly=True, direction='high'."""
        rows = _make_bq_rows(
            {
                "2024-01-01": 9800,
                "2024-01-02": 10200,
                "2024-01-03": 9700,
                "2024-01-04": 10300,
                "2024-01-05": 9900,
                "2024-01-06": 10100,
                "2024-01-07": 100000,  # today — massive spike
            }
        )
        with _patch_bq(rows):
            result = check_statistical_anomaly(
                project_id="test-project",
                execution_date="2024-01-07",
            )

        assert result["is_anomaly"] is True
        assert result["direction"] == "high"
        assert result["z_score"] > 2.5

    def test_result_contains_all_expected_keys(self):
        """Result dict always contains all documented keys."""
        rows = _make_bq_rows(
            {
                "2024-01-01": 1000,
                "2024-01-02": 1000,
                "2024-01-03": 1000,
                "2024-01-04": 1000,
                "2024-01-05": 1000,
                "2024-01-06": 1000,
                "2024-01-07": 1000,
            }
        )
        with _patch_bq(rows):
            result = check_statistical_anomaly(
                project_id="test-project",
                execution_date="2024-01-07",
            )

        expected_keys = {
            "today_count",
            "mean_7d",
            "std_7d",
            "z_score",
            "is_anomaly",
            "direction",
            "execution_date",
        }
        assert expected_keys == set(result.keys())

    def test_execution_date_preserved_in_result(self):
        """execution_date in result matches the input parameter."""
        rows = _make_bq_rows(
            {
                "2024-03-15": 500,
                "2024-03-16": 500,
                "2024-03-17": 500,
                "2024-03-18": 500,
                "2024-03-19": 500,
                "2024-03-20": 500,
                "2024-03-21": 500,
            }
        )
        with _patch_bq(rows):
            result = check_statistical_anomaly(
                project_id="test-project",
                execution_date="2024-03-21",
            )

        assert result["execution_date"] == "2024-03-21"

    def test_custom_threshold_respected(self):
        """A custom z_score_threshold changes the anomaly boundary."""
        # With threshold=10.0, a moderate spike should NOT be flagged
        rows = _make_bq_rows(
            {
                "2024-01-01": 1000,
                "2024-01-02": 1000,
                "2024-01-03": 1000,
                "2024-01-04": 1000,
                "2024-01-05": 1000,
                "2024-01-06": 1000,
                "2024-01-07": 4000,  # spike but below threshold=10
            }
        )
        with _patch_bq(rows):
            result = check_statistical_anomaly(
                project_id="test-project",
                execution_date="2024-01-07",
                z_score_threshold=10.0,
            )

        assert result["is_anomaly"] is False


# ─────────────────────────────────────────────────────────────
# Edge cases — std = 0
# ─────────────────────────────────────────────────────────────


class TestCheckStatisticalAnomalyStdZero:

    def test_std_zero_does_not_crash(self):
        """When all baseline values are identical, std=0 — must not raise ZeroDivisionError."""
        rows = _make_bq_rows(
            {
                "2024-01-01": 500,
                "2024-01-02": 500,
                "2024-01-03": 500,
                "2024-01-04": 500,
                "2024-01-05": 500,
                "2024-01-06": 500,
                "2024-01-07": 500,  # today == baseline
            }
        )
        with _patch_bq(rows):
            result = check_statistical_anomaly(
                project_id="test-project",
                execution_date="2024-01-07",
            )

        # Should not raise — z_score forced to 0.0 when std=0
        assert result["z_score"] == 0.0
        assert result["is_anomaly"] is False
        assert result["std_7d"] == 0.0

    def test_std_zero_with_different_today_does_not_crash(self):
        """std=0 on baseline + today different — must not crash, z_score=0."""
        rows = _make_bq_rows(
            {
                "2024-01-01": 500,
                "2024-01-02": 500,
                "2024-01-03": 500,
                "2024-01-04": 500,
                "2024-01-05": 500,
                "2024-01-06": 500,
                "2024-01-07": 9999,  # today very different but std=0
            }
        )
        with _patch_bq(rows):
            result = check_statistical_anomaly(
                project_id="test-project",
                execution_date="2024-01-07",
            )

        # Cannot compute meaningful z-score — returns 0.0 safely
        assert result["z_score"] == 0.0
        assert result["is_anomaly"] is False


# ─────────────────────────────────────────────────────────────
# Edge cases — insufficient data
# ─────────────────────────────────────────────────────────────


class TestCheckStatisticalAnomalyInsufficientData:

    def test_fewer_than_3_rows_returns_default_result(self):
        """Less than 3 rows total — function returns safely with None values."""
        rows = _make_bq_rows(
            {
                "2024-01-06": 1000,
                "2024-01-07": 1000,  # only 2 rows
            }
        )
        with _patch_bq(rows):
            result = check_statistical_anomaly(
                project_id="test-project",
                execution_date="2024-01-07",
            )

        assert result["z_score"] is None
        assert result["is_anomaly"] is False
        assert result["today_count"] is None

    def test_empty_result_returns_default(self):
        """No rows at all — function returns default dict without crashing."""
        with _patch_bq([]):
            result = check_statistical_anomaly(
                project_id="test-project",
                execution_date="2024-01-07",
            )

        assert result["z_score"] is None
        assert result["is_anomaly"] is False

    def test_baseline_fewer_than_2_days_returns_default(self):
        """Only today's row in results — baseline < 2 days, returns safely."""
        rows = _make_bq_rows(
            {
                "2024-01-05": 1000,
                "2024-01-06": 1000,
                "2024-01-07": 1000,  # 2 baseline days + today = 3 rows total
            }
        )
        # Override lookback so baseline ends up with only 1 day
        with _patch_bq(rows):
            result = check_statistical_anomaly(
                project_id="test-project",
                execution_date="2024-01-07",
                lookback_days=2,  # fetches 3 rows, baseline = 2 — just passes
            )

        # Should compute normally with 2-day baseline
        assert result["is_anomaly"] is False

    def test_missing_today_in_results_returns_default(self):
        """BQ returns rows but execution_date is not among them — returns safely."""
        rows = _make_bq_rows(
            {
                "2024-01-01": 1000,
                "2024-01-02": 1000,
                "2024-01-03": 1000,
                "2024-01-04": 1000,
                "2024-01-05": 1000,
                "2024-01-06": 1000,
                # 2024-01-07 (execution_date) absent from results
            }
        )
        with _patch_bq(rows):
            result = check_statistical_anomaly(
                project_id="test-project",
                execution_date="2024-01-07",
            )

        assert result["today_count"] is None
        assert result["is_anomaly"] is False


# ─────────────────────────────────────────────────────────────
# Edge cases — BQ failure
# ─────────────────────────────────────────────────────────────


class TestCheckStatisticalAnomalyBQFailure:

    def test_bq_exception_returns_default_without_raising(self):
        """If BigQuery raises, function catches and returns default dict."""
        with patch(
            "google.cloud.bigquery.Client", side_effect=Exception("BQ unavailable")
        ):
            result = check_statistical_anomaly(
                project_id="test-project",
                execution_date="2024-01-07",
            )

        assert result["is_anomaly"] is False
        assert result["z_score"] is None
