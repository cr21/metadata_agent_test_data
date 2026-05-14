from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

import google.auth
import google.auth.transport.requests
import requests as _requests
from fastmcp import FastMCP
from google.cloud import dataplex_v1
from google.protobuf import field_mask_pb2, struct_pb2
from google.protobuf.json_format import ParseDict

mcp = FastMCP("bq-dataplex")

_PROJECT = "project-5c016d48-80d5-4534-b69"
_LOCATION = "us"
_ASPECT_TYPE_ID = "column-lineage"
_ASPECT_TYPE = f"projects/{_PROJECT}/locations/{_LOCATION}/aspectTypes/{_ASPECT_TYPE_ID}"
_DATAPLEX_BASE = "https://dataplex.googleapis.com/v1"


@lru_cache(maxsize=1)
def _client() -> dataplex_v1.CatalogServiceClient:
    return dataplex_v1.CatalogServiceClient()


def _entry_name(table_fqn: str) -> str:
    project, dataset, table = table_fqn.split(".")
    return (
        f"projects/{_PROJECT}/locations/{_LOCATION}/entryGroups/@bigquery/entries"
        f"/bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}"
    )


def _make_aspect(col: dict) -> dataplex_v1.Aspect:
    pii = "TRUE" if str(col.get("pii", "false")).lower() == "true" else "FALSE"
    col_name = col["column_name"]
    payload = {
        "column_name": col_name,
        "column_description": col.get("column_description", ""),
        "column_datatype": col.get("column datatype", col.get("column_datatype", "")),
        "source_columns": list(col.get("source_columns", [])),
        "source_datatypes": list(
            col.get("source datatypes", col.get("source_datatypes", []))
        ),
        "transformation": col.get("transformation", ""),
        "pii": pii,
    }
    return dataplex_v1.Aspect(
        aspect_type=_ASPECT_TYPE,
        path=f"Schema.{col_name}",
        data=ParseDict(payload, struct_pb2.Struct()),
    )


def _get_entry_json(entry_name: str, view: str = "ALL") -> dict:
    """Fetch a Dataplex entry via REST. The Python gRPC client silently drops
    Struct data fields for custom aspects, so we call the REST API directly."""
    creds, _ = google.auth.default()
    creds.refresh(google.auth.transport.requests.Request())
    url = f"{_DATAPLEX_BASE}/{entry_name}?view={view}"
    resp = _requests.get(url, headers={"Authorization": f"Bearer {creds.token}"})
    resp.raise_for_status()
    return resp.json()


@mcp.tool()
def apply_lineage(stm_path: str) -> list[dict[str, Any]]:
    """
    Apply column-lineage aspects to BigQuery table entries in Dataplex Catalog
    from an STM mapping JSON file.

    Args:
        stm_path: Path to the STM mapping JSON file
                  (e.g. outputs/etl_kpi_customer_orders.json).

    Returns:
        List of results per table, each with:
        - `table`: fully-qualified table name
        - `aspects_applied`: number of column aspects written
        - `entry`: full Dataplex entry resource name
    """
    with open(stm_path) as f:
        stm = json.load(f)

    results = []
    for table_mapping in stm["stm_mapping"]:
        table_fqn = table_mapping["target_table_name"]
        aspects = {
            f"{_PROJECT}.{_LOCATION}.{_ASPECT_TYPE_ID}@Schema.{col['column_name']}": _make_aspect(col)
            for col in table_mapping["columns"]
        }
        updated = _client().update_entry(
            request=dataplex_v1.UpdateEntryRequest(
                entry=dataplex_v1.Entry(name=_entry_name(table_fqn), aspects=aspects),
                update_mask=field_mask_pb2.FieldMask(paths=["aspects"]),
                aspect_keys=list(aspects.keys()),
            )
        )
        results.append(
            {
                "table": table_fqn,
                "aspects_applied": len(aspects),
                "entry": updated.name,
            }
        )
    return results


@mcp.tool()
def get_column_lineage(table_fqn: str) -> list[dict[str, Any]]:
    """
    Fetch all column-lineage aspects applied to a BigQuery table in Dataplex Catalog.

    Args:
        table_fqn: Fully-qualified BigQuery table name as `project.dataset.table`.

    Returns:
        List of column lineage records sorted by column name, each with:
        - `column_name`, `column_description`, `column_datatype`
        - `source_columns` (list), `source_datatypes` (list)
        - `transformation`, `pii`
        Returns an empty list if no column-lineage aspects are found.
    """
    entry = _get_entry_json(_entry_name(table_fqn))
    aspects = entry.get("aspects", {})

    columns = []
    for key, val in aspects.items():
        if _ASPECT_TYPE_ID not in key:
            continue
        data = val.get("data", {})
        columns.append(
            {
                "column_name": data.get("column_name", ""),
                "column_description": data.get("column_description", ""),
                "column_datatype": data.get("column_datatype", ""),
                "source_columns": data.get("source_columns", []),
                "source_datatypes": data.get("source_datatypes", []),
                "transformation": data.get("transformation", ""),
                "pii": data.get("pii", "FALSE"),
            }
        )
    return sorted(columns, key=lambda c: c["column_name"])


if __name__ == "__main__":
    mcp.run()
