
import json

from rich.console import Console
from rich.table import Table

console = Console()


# ============================================================
# SPAN IDENTIFICATION
# ============================================================

def get_span_id(hit):

    # Use the actual OpenTelemetry span ID.
    # Never use the Elasticsearch document _id for matching.
    return hit.get("_source", {}).get("span_id")


# ============================================================
# ATTRIBUTE COMPARISON
# ============================================================

def flatten_document(value, prefix=""):

    result = {}

    if isinstance(value, dict):

        for key, item in value.items():

            path = (
                f"{prefix}.{key}"
                if prefix
                else key
            )

            result.update(
                flatten_document(item, path)
            )

    elif isinstance(value, list):

        result[prefix] = json.dumps(
            value,
            sort_keys=True,
            default=str,
        )

    else:

        result[prefix] = value

    return result


def compare_span_attributes(before_hit, after_hit):

    before = flatten_document(
        before_hit["_source"]
    )

    after = flatten_document(
        after_hit["_source"]
    )

    # Ignore exporter/ingestion metadata and schema URLs.
    ignored_prefixes = (
        "event.ingested",
        "data_stream.",
        "agent.",
        "observer.",
        "resource.schema_url",
        "scope.schema_url",
    )

    missing = object()

    differences = []

    all_keys = set(before) | set(after)

    for key in sorted(all_keys):

        if key.startswith(ignored_prefixes):
            continue

        before_value = before.get(key, missing)
        after_value = after.get(key, missing)

        if before_value != after_value:

            differences.append({
                "attribute": key,

                "before": (
                    "<MISSING>"
                    if before_value is missing
                    else before_value
                ),

                "after": (
                    "<MISSING>"
                    if after_value is missing
                    else after_value
                ),
            })

    return differences


# ============================================================
# BUILD SPAN DIFF
# ============================================================

def build_span_diff(before_spans, after_spans):

    """
    Match before/after spans by span_id.

    Preserve before-index ordering.
    Spans found only after sampling are appended afterwards.
    """

    before_map = {
        get_span_id(hit): hit
        for hit in before_spans
        if get_span_id(hit)
    }

    after_map = {
        get_span_id(hit): hit
        for hit in after_spans
        if get_span_id(hit)
    }

    ordered_ids = list(before_map.keys())

    ordered_ids.extend(
        span_id
        for span_id in after_map
        if span_id not in before_map
    )

    results = []

    for span_id in ordered_ids:

        before_hit = before_map.get(span_id)
        after_hit = after_map.get(span_id)

        if before_hit and not after_hit:

            status = "MISSING AFTER"
            differences = []

        elif after_hit and not before_hit:

            status = "ONLY AFTER"
            differences = []

        else:

            differences = compare_span_attributes(
                before_hit,
                after_hit,
            )

            status = (
                "MODIFIED"
                if differences
                else "UNCHANGED"
            )

        source = (
            before_hit["_source"]
            if before_hit
            else after_hit["_source"]
        )

        results.append({

            "span_id": span_id,

            "name": source.get(
                "name",
                "Unknown span",
            ),

            "parent_span_id": source.get(
                "parent_span_id"
            ),

            "service": (
                source.get("resource", {})
                .get("attributes", {})
                .get("service.name", "Unknown")
            ),

            "status": status,

            "before": before_hit,

            "after": after_hit,

            "differences": differences,
        })

    return results


# ============================================================
# DISPLAY SELECTED SPAN DETAILS
# ============================================================

def display_span_diff_detail(item):

    """Display details for the one selected span."""

    console.print(
        f"\n[bold cyan]{item['name']}[/bold cyan] "
        f"({item['span_id']})"
    )

    console.print(
        f"Status: [bold]{item['status']}[/bold]"
    )

    # Display attribute differences.
    if item["differences"]:

        table = Table(
            title="Attribute Differences",
            show_lines=True,
        )

        table.add_column("Attribute")
        table.add_column("Before")
        table.add_column("After")

        for difference in item["differences"]:

            table.add_row(
                difference["attribute"],
                str(difference["before"]),
                str(difference["after"]),
            )

        console.print(table)

    elif item["status"] == "UNCHANGED":

        console.print(
            "[green]No semantic attribute differences found.[/green]"
        )

    elif item["status"] == "MISSING AFTER":

        console.print(
            "[red]This span is missing from the after-sampling dataset.[/red]"
        )

    elif item["status"] == "ONLY AFTER":

        console.print(
            "[blue]This span exists only in the after-sampling dataset.[/blue]"
        )

    # Display the before document if available.
    if item["before"]:

        console.print(
            "\n[bold]BEFORE SAMPLING DOCUMENT[/bold]"
        )

        console.print_json(
            json.dumps(
                item["before"]["_source"],
                indent=2,
                default=str,
            )
        )

    # Display the after document if available.
    if item["after"]:

        console.print(
            "\n[bold]AFTER SAMPLING DOCUMENT[/bold]"
        )

        console.print_json(
            json.dumps(
                item["after"]["_source"],
                indent=2,
                default=str,
            )
        )

