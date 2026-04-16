"""
Extract daily disruption messages from IDFM.

Priority order:
1. PRIM API, when PRIM_API_KEY and PRIM_API_URL are configured.
2. ODS fallback ("cartes-des-travaux") for planned works only.
"""

import argparse
import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from google.cloud import storage
from odsv2_client import ODSv2Client
from prim_client import PRIMClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parent.parent))

INCIDENT_TYPE_MAP = {
    "TRAVAUX": "planned_work",
    "TRAVAUX PROGRAMMES": "planned_work",
    "INCIDENT": "unplanned_disruption",
    "PERTURBATION": "unplanned_disruption",
    "PERTURBEE": "unplanned_disruption",
    "FERMETURE PARTIELLE": "partial_closure",
    "FERMETURE TOTALE": "full_closure",
    "BLOQUANTE": "full_closure",
    "ACCES": "access_issue",
    "ACCESSIBILITE": "access_issue",
    "ASCENSEUR": "access_issue",
}


def load_config():
    config_path = PROJECT_ROOT / "config" / "apis.yml"
    with open(config_path, encoding="utf-8") as file:
        return yaml.safe_load(file)


def normalise_incident_type(*parts: str | None) -> str:
    """Map raw values to the canonical 5-category taxonomy."""
    haystack = " ".join(part for part in parts if part).upper().strip()
    if not haystack:
        return "unplanned_disruption"

    for key, canonical in INCIDENT_TYPE_MAP.items():
        if key in haystack:
            return canonical

    return "unplanned_disruption"


def _get_nested_value(payload: dict[str, Any], path: str) -> Any:
    value: Any = payload
    for key in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _extract_prim_records(
    payload: dict[str, Any] | list[dict[str, Any]],
    records_path: str | None,
) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload

    if records_path:
        value = _get_nested_value(payload, records_path)
        if isinstance(value, list):
            return value

    for candidate in (
        "disruptions",
        "result.disruptions",
        "data.disruptions",
        "results",
    ):
        value = _get_nested_value(payload, candidate)
        if isinstance(value, list):
            return value

    return []


def _parse_iso_date(raw_value: str | None) -> date | None:
    if not raw_value:
        return None
    try:
        return datetime.strptime(raw_value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_prim_datetime(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None

    for fmt in ("%Y%m%dT%H%M%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(raw_value, fmt)
        except ValueError:
            continue

    return None


def _extract_application_dates(
    periods: list[dict[str, Any]] | None,
) -> tuple[str | None, str | None]:
    begins: list[datetime] = []
    ends: list[datetime] = []

    for period in periods or []:
        begin = _parse_prim_datetime(period.get("begin"))
        end = _parse_prim_datetime(period.get("end"))
        if begin:
            begins.append(begin)
        if end:
            ends.append(end)

    incident_date = min(begins).date().isoformat() if begins else None
    incident_end_date = max(ends).date().isoformat() if ends else None
    return incident_date, incident_end_date


def _get_application_periods(record: dict[str, Any]) -> list[dict[str, Any]]:
    return record.get("application_periods") or record.get("applicationPeriods") or []


def _normalise_line_id(raw_line_id: str | None) -> str | None:
    if not raw_line_id:
        return None
    if raw_line_id.startswith("line:IDFM:"):
        return raw_line_id.split(":")[-1]
    return raw_line_id


def _extract_primary_line(disruption: dict[str, Any]) -> dict[str, Any]:
    linked_line = disruption.get("_linked_line")
    if isinstance(linked_line, dict):
        return linked_line

    lines = disruption.get("lines") or []
    if lines:
        return lines[0]

    impacted_objects = (
        disruption.get("impacted_objects") or disruption.get("impactedObjects") or []
    )

    for obj in impacted_objects:
        pt_object = obj.get("pt_object") or obj
        embedded_type = pt_object.get("embedded_type") or obj.get("type")

        if embedded_type == "line":
            return pt_object.get("line") or pt_object

        line = pt_object.get("line")
        if isinstance(line, dict):
            return line

    return {}


def _extract_affected_stop_names(disruption: dict[str, Any]) -> list[str]:
    stop_names: list[str] = []

    impacted_objects = (
        disruption.get("impacted_objects") or disruption.get("impactedObjects") or []
    )

    for obj in impacted_objects:
        pt_object = obj.get("pt_object") or obj
        embedded_type = pt_object.get("embedded_type") or obj.get("type")

        if embedded_type not in {"stop_point", "stop_area"}:
            continue

        name = pt_object.get("name")
        if not name and isinstance(pt_object.get(embedded_type), dict):
            name = pt_object[embedded_type].get("name")

        if name:
            stop_names.append(name.strip())

    # Keep order, remove duplicates
    return list(dict.fromkeys(name for name in stop_names if name))


def _extract_message_texts(disruption: dict[str, Any]) -> list[str]:
    texts: list[str] = []
    for message in disruption.get("messages") or []:
        text = message.get("text")
        if text:
            texts.append(text.strip())
    return texts


def _extract_message_title(disruption: dict[str, Any]) -> str | None:
    for message in disruption.get("messages") or []:
        channel = message.get("channel") or {}
        types = channel.get("types") or []
        if "title" in types and message.get("text"):
            return message["text"].strip()

    texts = _extract_message_texts(disruption)
    return texts[0] if texts else None


def _extract_transport_mode(line: dict[str, Any]) -> str | None:
    physical_modes = line.get("physical_modes") or []
    if physical_modes and physical_modes[0].get("name"):
        return physical_modes[0]["name"].upper()

    commercial_mode = line.get("commercial_mode") or {}
    if commercial_mode.get("name"):
        return commercial_mode["name"].upper()

    mode = line.get("mode")
    return mode.upper() if isinstance(mode, str) and mode else None


def _build_line_lookup(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}

    for line_report in payload.get("line_reports") or []:
        line = line_report.get("line") or {}
        if not isinstance(line, dict) or not line:
            continue

        for link in line.get("links") or []:
            if link.get("type") != "disruption" or not link.get("id"):
                continue
            lookup.setdefault(link["id"], line)

    return lookup


def _enrich_prim_records(
    payload: dict[str, Any] | list[dict[str, Any]],
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return records

    # line_reports format: attach top-level disruptions as context for each report
    if records and "line" in records[0]:
        disruptions = payload.get("disruptions") or []
        return [{**record, "_disruptions": disruptions} for record in records]

    # disruptions format: enrich with line info from line_reports via links
    line_lookup = _build_line_lookup(payload)
    if not line_lookup:
        return records

    enriched: list[dict[str, Any]] = []
    for record in records:
        record_id = record.get("id") or record.get("uri")
        linked_line = line_lookup.get(record_id)

        if linked_line:
            enriched.append({**record, "_linked_line": linked_line})
        else:
            enriched.append(record)

    return enriched


def _record_overlaps_window(
    record: dict[str, Any],
    start_date: str,
    end_date: str,
) -> bool:
    # line_report format: check against any of the attached disruptions' periods
    if "line" in record:
        disruptions = record.get("_disruptions") or []
        if not disruptions:
            return True
        return any(
            _record_overlaps_window(d, start_date, end_date) for d in disruptions
        )

    window_start = _parse_iso_date(start_date)
    window_end = _parse_iso_date(end_date)
    if not window_start or not window_end:
        return True

    incident_start, incident_end = _extract_application_dates(
        _get_application_periods(record)
    )
    incident_start_date = _parse_iso_date(incident_start)
    incident_end_date = _parse_iso_date(incident_end)

    if not incident_start_date and not incident_end_date:
        return True

    incident_start_date = incident_start_date or incident_end_date
    incident_end_date = incident_end_date or incident_start_date

    if not incident_start_date or not incident_end_date:
        return True

    return incident_start_date <= window_end and incident_end_date >= window_start


def _is_usable_prim_record(record: dict[str, Any]) -> bool:
    # line_report format: usable when the report has a valid line id
    if "line" in record:
        return bool((record.get("line") or {}).get("id"))
    # disruption format: filter to incident category
    return (record.get("category") or "").upper() == "INCIDENTS"


def _fetch_prim_page(
    client: PRIMClient,
    prim_config: dict[str, Any],
    start_date: str,
    end_date: str,
    start_page: int = 0,
) -> tuple[dict[str, Any] | list[dict[str, Any]], list[dict[str, Any]]]:
    params = _build_prim_params(prim_config, start_date, end_date)
    params["start_page"] = start_page

    payload = client.get_json(params=params)
    records = _extract_prim_records(payload, prim_config.get("records_path"))
    return payload, _enrich_prim_records(payload, records)


def _build_prim_params(prim_config: dict[str, Any], start_date: str, end_date: str):
    params = dict(prim_config.get("params", {}))

    start_param = prim_config.get("start_date_param")
    end_param = prim_config.get("end_date_param")

    if start_param:
        params[start_param] = start_date
    if end_param:
        params[end_param] = end_date

    return params


def _extract_from_prim(
    dataset_config: dict[str, Any],
    start_date: str,
    end_date: str,
) -> list[dict[str, Any]]:
    prim_config = dataset_config.get("prim", {})
    api_key = os.getenv("PRIM_API_KEY")
    api_url = prim_config.get("api_url") or os.getenv("PRIM_API_URL")

    if not api_key or not api_url:
        return []

    client = PRIMClient(
        url=api_url,
        api_key=api_key,
        api_key_location=prim_config.get("api_key_location", "header"),
        api_key_name=prim_config.get("api_key_name", "apikey"),
        default_params=prim_config.get("default_params", {}),
    )

    payload, records = _fetch_prim_page(
        client=client,
        prim_config=prim_config,
        start_date=start_date,
        end_date=end_date,
        start_page=0,
    )

    all_records = list(records)
    pagination = payload.get("pagination") if isinstance(payload, dict) else None

    if isinstance(pagination, dict):
        total_result = pagination.get("total_result") or len(records)
        items_per_page = pagination.get("items_per_page") or len(records) or 1
        current_page = pagination.get("start_page", 0)

        while len(all_records) < total_result:
            current_page += 1
            _, page_records = _fetch_prim_page(
                client=client,
                prim_config=prim_config,
                start_date=start_date,
                end_date=end_date,
                start_page=current_page,
            )
            if not page_records:
                break

            all_records.extend(page_records)

            if len(page_records) < items_per_page:
                break

    filtered_records = [
        record
        for record in all_records
        if _is_usable_prim_record(record)
        and _record_overlaps_window(record, start_date, end_date)
    ]

    logger.info(
        "Extracted %s PRIM incidents after filtering (%s raw records)",
        len(filtered_records),
        len(all_records),
    )
    return filtered_records


def _extract_from_ods_fallback(
    idfm_config: dict[str, Any],
    dataset_config: dict[str, Any],
    start_date: str,
    end_date: str,
) -> list[dict[str, Any]]:
    client = ODSv2Client(
        base_url=idfm_config["base_url"],
        dataset_id=dataset_config["id"],
        api_key=os.getenv("IDFM_API_KEY"),
    )

    fields = dataset_config["fields"]
    select_clause = ", ".join(fields.values())
    date_begin_field = fields["incident_date"]  # date_debut
    date_end_field = fields["incident_end_date"]  # date_fin

    # Find works ACTIVE during the window, not just ones that started then.
    # date_debut <= end_date → started before or during the window
    # date_fin IS NULL OR date_fin >= start_date → still ongoing during the window
    where_clause = (
        f"{date_begin_field} <= '{end_date}'"
        f" AND ({date_end_field} IS NULL OR {date_end_field} >= '{start_date}')"
    )

    return client.get_all_records(select=select_clause, where=where_clause)


def _transform_line_report_record(
    record: dict[str, Any],
    ingestion_ts: str,
) -> dict[str, Any]:
    """Transform a Navitia line_report object into a canonical incident record.

    line_report structure:
      {"line": {...}, "pt_objects": [...], "_disruptions": [...]}
    Line info comes from record["line"]; date/cause from the first attached disruption.
    """
    line = record.get("line") or {}
    disruptions = record.get("_disruptions") or []
    first_disruption = disruptions[0] if disruptions else {}

    incident_date, incident_end_date = _extract_application_dates(
        _get_application_periods(first_disruption)
    )
    # Daily snapshot: fall back to today when no application period is found
    if not incident_date:
        incident_date = datetime.now().strftime("%Y-%m-%d")

    # Extract affected stop names from pt_objects
    stop_names: list[str] = []
    for pt_obj in record.get("pt_objects") or []:
        stop_area = pt_obj.get("stop_area") or {}
        name = stop_area.get("name") or pt_obj.get("name")
        if name:
            stop_names.append(name.strip())
    stop_names = list(dict.fromkeys(stop_names))

    severity_name = (first_disruption.get("severity") or {}).get("name")
    title = _extract_message_title(first_disruption)
    message_text = " ".join(_extract_message_texts(first_disruption))
    raw_type = (
        first_disruption.get("cause")
        or severity_name
        or first_disruption.get("category")
    )

    return {
        "incident_date": incident_date,
        "incident_end_date": incident_end_date,
        "line_id": _normalise_line_id(line.get("id")),
        "line_name": line.get("code") or line.get("name"),
        "incident_type_raw": raw_type,
        "incident_type": normalise_incident_type(
            first_disruption.get("cause"),
            severity_name,
            title,
            message_text,
        ),
        "cause": first_disruption.get("cause"),
        "affected_stops": ", ".join(stop_names),
        "transport_mode": _extract_transport_mode(line),
        "affected_stop_count": len(stop_names),
        "ingestion_ts": ingestion_ts,
        "source": "idfm_incidents_prim_navitia",
    }


def _transform_prim_record(
    record: dict[str, Any],
    ingestion_ts: str,
) -> dict[str, Any]:
    # Dispatch on format: line_report vs disruption
    if "line" in record:
        return _transform_line_report_record(record, ingestion_ts)

    line = _extract_primary_line(record)
    incident_date, incident_end_date = _extract_application_dates(
        _get_application_periods(record)
    )
    affected_stops = _extract_affected_stop_names(record)
    severity_name = (record.get("severity") or {}).get("name")
    title = _extract_message_title(record)
    message_text = " ".join(_extract_message_texts(record))

    raw_type = record.get("cause") or severity_name or record.get("category")

    return {
        "incident_date": incident_date,
        "incident_end_date": incident_end_date,
        "line_id": _normalise_line_id(line.get("id")),
        "line_name": line.get("code") or line.get("name") or line.get("shortName"),
        "incident_type_raw": raw_type,
        "incident_type": normalise_incident_type(
            record.get("cause"),
            severity_name,
            title,
            message_text,
        ),
        "cause": record.get("cause"),
        "affected_stops": ", ".join(affected_stops),
        "transport_mode": _extract_transport_mode(line),
        "affected_stop_count": len(affected_stops),
        "ingestion_ts": ingestion_ts,
        "source": "idfm_incidents_prim_navitia",
    }


def _transform_ods_record(
    record: dict[str, Any],
    fields: dict[str, str],
    ingestion_ts: str,
    extraction_date: str,
) -> dict[str, Any]:
    extracted_record = {target: record.get(source) for target, source in fields.items()}
    extracted_record["incident_type_raw"] = extracted_record.get("incident_type")
    extracted_record["incident_type"] = normalise_incident_type(
        extracted_record.get("incident_type")
    )

    affected_raw = extracted_record.get("affected_stops") or ""
    extracted_record["affected_stop_count"] = len(
        [stop for stop in affected_raw.split(",") if stop.strip()]
    )

    # Override incident_date with the extraction date (start_date) so the
    # staging grain (incident_date, incident_type) represents "active works on day X"
    # rather than the original work start date (which may be weeks in the past).
    extracted_record["incident_date"] = extraction_date

    extracted_record["ingestion_ts"] = ingestion_ts
    extracted_record["source"] = "idfm_incidents_ods_fallback"
    return extracted_record


def extract_incidents_daily(
    start_date: str = None,
    end_date: str = None,
    gcs_bucket: str = None,
    output_dir: Path = None,
):
    """Extract incident messages from PRIM first, then ODS fallback."""
    today = datetime.now().strftime("%Y-%m-%d")
    start_date = start_date or today
    end_date = end_date or today

    config = load_config()
    idfm_config = config["idfm"]
    dataset_config = idfm_config["datasets"]["incidents"]

    logger.info("Extracting incidents from %s to %s", start_date, end_date)

    ingestion_ts = datetime.now(timezone.utc).isoformat()

    records: list[dict[str, Any]] = []
    source_name = "none"

    try:
        records = _extract_from_prim(dataset_config, start_date, end_date)
        if records:
            source_name = "prim"
    except Exception as exc:
        logger.warning("PRIM extraction failed, fallback to ODS: %s", exc)

    if not records:
        try:
            records = _extract_from_ods_fallback(
                idfm_config=idfm_config,
                dataset_config=dataset_config,
                start_date=start_date,
                end_date=end_date,
            )
            if records:
                source_name = "ods_fallback"
        except Exception as exc:
            logger.warning("ODS fallback extraction skipped: %s", exc)
            return

    if not records:
        logger.warning("No incident records found for %s to %s", start_date, end_date)
        return

    if source_name == "prim":
        extracted = [_transform_prim_record(record, ingestion_ts) for record in records]
    else:
        fields = dataset_config["fields"]
        extracted = [
            _transform_ods_record(
                record, fields, ingestion_ts, extraction_date=start_date
            )
            for record in records
        ]

    extracted = [
        record
        for record in extracted
        if record.get("incident_date") and record.get("incident_type")
    ]

    if not extracted:
        logger.warning("All incident records were filtered out after transformation")
        return

    filename = f"incidents_{start_date}_{end_date}.json"
    ndjson = "\n".join(json.dumps(record, ensure_ascii=False) for record in extracted)

    bucket_name = gcs_bucket or os.getenv("GCS_BUCKET_RAW")
    if bucket_name:
        blob_path = f"incidents/{filename}"
        storage.Client().bucket(bucket_name).blob(blob_path).upload_from_string(
            ndjson, content_type="application/json"
        )
        logger.info(
            "Uploaded %s incidents from %s to gs://%s/%s",
            len(extracted),
            source_name,
            bucket_name,
            blob_path,
        )
    else:
        local_dir = (
            Path(output_dir) if output_dir else PROJECT_ROOT / "data/bronze/incidents"
        )
        local_dir.mkdir(parents=True, exist_ok=True)
        filepath = local_dir / filename
        with open(filepath, "w", encoding="utf-8") as file:
            file.write(ndjson)
        logger.info(
            "Saved %s incidents from %s to %s",
            len(extracted),
            source_name,
            filepath,
        )


def main():
    parser = argparse.ArgumentParser(description="Extract IDFM daily incident messages")
    parser.add_argument(
        "--start-date",
        default=None,
        help="Start date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="End date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--bucket", default=None, help="GCS bucket (default: GCS_BUCKET_RAW env var)"
    )
    parser.add_argument(
        "--output", default=None, help="Local output dir (fallback when no GCS)"
    )
    args = parser.parse_args()
    extract_incidents_daily(
        start_date=args.start_date,
        end_date=args.end_date,
        gcs_bucket=args.bucket,
        output_dir=args.output,
    )


if __name__ == "__main__":
    main()
