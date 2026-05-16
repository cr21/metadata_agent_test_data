from __future__ import annotations

from functools import lru_cache
from typing import Any

from fastmcp import FastMCP
from google.cloud import bigquery

mcp = FastMCP("bq-tools")


@lru_cache(maxsize=8)
def _get_bigquery_client(project_id: str) -> bigquery.Client:
    """Internal helper. NOT exposed as a tool — clients can't be JSON-serialized."""
    return bigquery.Client(project=project_id)


@mcp.tool()
def table_fqn(project_id: str, dataset: str, table: str) -> str:
    """
    Build a fully-qualified BigQuery table id.

    Args:
        project_id: BigQuery project id.
        dataset: Dataset id.
        table: Table name (unqualified).

    Returns:
        Fully-qualified table id as `project.dataset.table` (no backticks).
    """
    return f"{project_id}.{dataset}.{table}"


@mcp.tool()
def table_exists(project_id: str, fqn: str) -> bool:
    """
    Check whether a BigQuery table exists.

    Args:
        project_id: BigQuery project id to initialize the client with.
        fqn: Fully-qualified table id as `project.dataset.table` (no backticks).

    Returns:
        True if the table can be fetched via `get_table`, else False.

    Notes:
        This returns False for "not found" and also for auth/permission/config errors,
        because errors are intentionally swallowed to keep the tool simple for MCP use.
    """
    client = _get_bigquery_client(project_id)
    try:
        client.get_table(fqn)
        return True
    except Exception:
        return False


@mcp.tool()
def table_schema(project_id: str, fqn: str) -> dict[str, Any]:
    """
    Fetch a BigQuery table schema as JSON.

    Args:
        project_id: BigQuery project id to initialize the client with.
        fqn: Fully-qualified table id as `project.dataset.table` (no backticks).

    Returns:
        Dict with:
        - `table`: the input `fqn`
        - `schema`: list of fields with `name`, `type`, `mode`, `description`

    Raises:
        Any BigQuery error raised by `get_table` (e.g. not found, permission denied).
    """
    client = _get_bigquery_client(project_id)
    table = client.get_table(fqn)
    return {
        "table": fqn,
        "schema": [
            {
                "name": f.name,
                "type": f.field_type,
                "mode": f.mode,
                "description": f.description,
            }
            for f in table.schema
        ],
    }


@mcp.tool()
def get_sample_data(
    project_id: str, fqn: str, limit: int = 100
) -> list[dict[str, Any]]:
    """
    Return sample rows from a BigQuery table.

    Args:
        project_id: BigQuery project id to initialize the client with.
        fqn: Fully-qualified table id as `project.dataset.table` (no backticks).
        limit: Max number of rows to return (default 100).

    Returns:
        List of rows as JSON-serializable dicts (column -> value).

    Notes:
        This runs: `SELECT * FROM \`fqn\` LIMIT <limit>`. For large/wide tables, prefer a
        smaller limit and/or a narrower query on the caller side.
    """
    client = _get_bigquery_client(project_id)
    query = f"SELECT * FROM `{fqn}` LIMIT {int(limit)}"
    rows = client.query(query).result()
    return [dict(row.items()) for row in rows]

@mcp.tool()
def get_all_datasets(project_id: str) -> list[str]:
    """
    List datasets in a BigQuery project.

    Args:
        project_id: BigQuery project id.

    Returns:
        List of dataset ids (e.g. `analytics`, `raw`, ...), not full resource names.
    """
    client = _get_bigquery_client(project_id)
    return [dataset.dataset_id for dataset in client.list_datasets()]

@mcp.tool()
def get_all_tables(project_id: str, dataset: str) -> list[str]:
    """
    List tables in a BigQuery dataset.

    Args:
        project_id: BigQuery project id.
        dataset: Dataset id to list tables from (unqualified).

    Returns:
        List of table ids (unqualified names only).
    """
    client = _get_bigquery_client(project_id)
    return [table.table_id for table in client.list_tables(dataset)]

@mcp.tool()
def check_usage_of_tables(project_id: str, dataset: str, table: str) -> dict[str, Any]:
    """
    Find dataset-local objects that reference a given table.

    This tool performs a **best-effort** dependency lookup by scanning SQL text in:
    - `INFORMATION_SCHEMA.ROUTINES` (procedures / functions)
    - `INFORMATION_SCHEMA.VIEWS` (views)

    It searches for the exact quoted reference `` `project.dataset.table` `` inside
    routine/view definitions.

    Args:
        project_id: BigQuery project id that owns the dataset.
        dataset: Dataset id that contains the table and the objects to scan.
        table: Table name (unqualified).

    Returns:
        JSON-serializable dict with keys:
        - `table`: `project.dataset.table`
        - `needle`: the exact string searched for (quoted FQN)
        - `routines`: list of matching routines (name/type/language)
        - `views`: list of matching views (name)
        - `notes`: limitations / caveats
        - `*_error` (optional): stringified BigQuery error if a query fails

    Limitations:
        - Only scans **within the provided dataset** (not cross-dataset/project).
        - Only finds **explicit, quoted** references like `` `p.d.t` ``.
        - May miss indirect usage (e.g. via intermediate views), and may include false positives.
    """
    client = _get_bigquery_client(project_id)
    fqn = f"{project_id}.{dataset}.{table}"

    # Heuristic-only approach: INFORMATION_SCHEMA doesn't provide a perfect dependency graph
    # for arbitrary SQL. We search routine/view definitions for references to the table.
    target_needle = f"`{fqn}`"

    routines_query = f"""
    SELECT
      routine_catalog AS project_id,
      routine_schema AS dataset,
      routine_name,
      routine_type,
      language,
      routine_definition
    FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.ROUTINES`
    WHERE routine_definition IS NOT NULL
      AND STRPOS(routine_definition, @needle) > 0
    ORDER BY routine_name
    """

    views_query = f"""
    SELECT
      table_catalog AS project_id,
      table_schema AS dataset,
      table_name AS view_name,
      view_definition
    FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.VIEWS`
    WHERE view_definition IS NOT NULL
      AND STRPOS(view_definition, @needle) > 0
    ORDER BY view_name
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("needle", "STRING", target_needle)]
    )

    result: dict[str, Any] = {
        "table": fqn,
        "needle": target_needle,
        "routines": [],
        "views": [],
        "notes": [
            "This is a best-effort string search over routine/view definitions.",
            "It will miss indirect usage and unquoted references, and may include false positives.",
        ],
    }

    try:
        routines_rows = client.query(routines_query, job_config=job_config).result()
        result["routines"] = [
            {
                "project_id": row["project_id"],
                "dataset": row["dataset"],
                "routine_name": row["routine_name"],
                "routine_type": row["routine_type"],
                "language": row["language"],
            }
            for row in routines_rows
        ]
    except Exception as e:
        result["routines_error"] = str(e)

    try:
        views_rows = client.query(views_query, job_config=job_config).result()
        result["views"] = [
            {
                "project_id": row["project_id"],
                "dataset": row["dataset"],
                "view_name": row["view_name"],
            }
            for row in views_rows
        ]
    except Exception as e:
        result["views_error"] = str(e)

    return result

@mcp.tool()
def get_code(project_id: str, dataset: str, entity_name: str) -> str:
    """
    Fetch the SQL text for a dataset entity (view or routine).

    The tool looks up, in order:
    - A view named `entity_name` from `INFORMATION_SCHEMA.VIEWS.view_definition`
    - A routine named `entity_name` from `INFORMATION_SCHEMA.ROUTINES.routine_definition`

    Args:
        project_id: BigQuery project id that owns the dataset.
        dataset: Dataset id to search within.
        entity_name: Unqualified view name or routine name.

    Returns:
        The SQL / definition string for the entity (plain text).

    Raises:
        ValueError: if no view or routine with that name exists in the dataset.

    """
    client = _get_bigquery_client(project_id)

    # 1) Views
    view_query = f"""
    SELECT view_definition
    FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.VIEWS`
    WHERE table_name = @name
    LIMIT 1
    """

    # 2) Routines (procedures/functions)
    routine_query = f"""
    SELECT routine_definition, routine_type, language
    FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.ROUTINES`
    WHERE routine_name = @name
    LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("name", "STRING", entity_name)]
    )

    view_rows = list(client.query(view_query, job_config=job_config).result())
    if view_rows and view_rows[0].get("view_definition") is not None:
        return str(view_rows[0]["view_definition"])

    routine_rows = list(client.query(routine_query, job_config=job_config).result())
    if routine_rows and routine_rows[0].get("routine_definition") is not None:
        return str(routine_rows[0]["routine_definition"])

    raise ValueError(
        f"Entity not found in `{project_id}.{dataset}` as view or routine: {entity_name}"
    )


if __name__ == "__main__":
    mcp.run()
