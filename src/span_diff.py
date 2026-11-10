import json

from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from rich.text import Text

console = Console()


def get_span_id(hit):
    source = hit["_source"]
    return source.get("span_id") or hit.get("_id")


def flatten_document(value, prefix=""):
    """
    Flatten nested dictionaries so individual attributes
    can be compared.
    """

    result = {}

    if isinstance(value, dict):
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else key
            result.update(flatten_document(item, path))

    elif isinstance(value, list):
        result[prefix] = json.dumps(
            value,
            sort_keys=True,
            default=str
        )

    else:
        result[prefix] = value

    return result


def compare_span_attributes(before_hit, after_hit):
    before = flatten_document(before_hit["_source"])
    after = flatten_document(after_hit["_source"])

    # Ignore ingestion-specific metadata.
    ignored_prefixes = (
        "event.ingested",
        "data_stream.",
        "agent.",
        "observer.",
        "resource.schema_url",
        "scope.schema_url",
    )

    all_keys = set(before) | set(after)
    differences = []

    for key in sorted(all_keys):
        if key.startswith(ignored_prefixes):
            continue

        before_value = before.get(key)
        after_value = after.get(key)

        if before_value != after_value:
            differences.append({
                "attribute": key,
                "before": before_value,
                "after": after_value,
            })

    return differences


def build_span_diff(before_spans, after_spans):
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

    all_span_ids = set(before_map) | set(after_map)

    results = []

    for span_id in all_span_ids:
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
                after_hit
            )

            status = "MODIFIED" if differences else "UNCHANGED"

        source = (
            before_hit["_source"]
            if before_hit
            else after_hit["_source"]
        )

        results.append({
            "span_id": span_id,
            "name": source.get("name", "Unknown span"),
            "parent_span_id": source.get("parent_span_id"),
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


def display_span_tree(results):
    """
    Render the trace's parent-child hierarchy.
    """

    root = Tree("[bold cyan]Trace span hierarchy[/bold cyan]")

    nodes = {
        item["span_id"]: item
        for item in results
    }

    children = {}

    for item in results:
        parent_id = item["parent_span_id"]

        if parent_id in nodes:
            children.setdefault(parent_id, []).append(item["span_id"])

    status_styles = {
        "UNCHANGED": "green",
        "MODIFIED": "yellow",
        "MISSING AFTER": "red",
        "ONLY AFTER": "blue",
    }

    visited = set()

    def add_node(parent, span_id):
        if span_id in visited:
            return

        visited.add(span_id)
        item = nodes[span_id]

        style = status_styles[item["status"]]

        label = Text(
            f"{item['name']} | {item['service']} | {item['status']}",
            style=style
        )

        branch = parent.add(label)

        for child_id in children.get(span_id, []):
            add_node(branch, child_id)

    root_ids = [
        item["span_id"]
        for item in results
        if (
            not item["parent_span_id"]
            or item["parent_span_id"] not in nodes
        )
    ]

    for span_id in root_ids:
        add_node(root, span_id)

    # Include any disconnected or cyclic spans rather than hiding them.
    for span_id in nodes:
        if span_id not in visited:
            add_node(root, span_id)

    console.print(root)


def display_span_diff(before_spans, after_spans):
    results = build_span_diff(before_spans, after_spans)

    if not results:
        console.print("[yellow]No spans available for comparison.[/yellow]")
        return

    unchanged = sum(
        item["status"] == "UNCHANGED" for item in results
    )
    modified = sum(
        item["status"] == "MODIFIED" for item in results
    )
    missing = sum(
        item["status"] == "MISSING AFTER" for item in results
    )
    only_after = sum(
        item["status"] == "ONLY AFTER" for item in results
    )

    before_count = len(before_spans)
    after_count = len(after_spans)

    retention = (
        after_count / before_count * 100
        if before_count
        else 0
    )

    reduction = (
        100 - retention
        if before_count
        else 0
    )

    console.print("\n[bold cyan]SPAN-LEVEL DIFF[/bold cyan]")

    summary = Table(title="Span Comparison Summary")

    summary.add_column("Metric")
    summary.add_column("Value", justify="right")

    summary.add_row("Before spans", str(before_count))
    summary.add_row("After spans", str(after_count))
    summary.add_row("Unchanged spans", str(unchanged))
    summary.add_row("Modified spans", str(modified))
    summary.add_row("Missing after", str(missing))
    summary.add_row("Only after", str(only_after))
    summary.add_row("Span retention", f"{retention:.2f}%")
    summary.add_row("Span reduction", f"{reduction:.2f}%")

    console.print(summary)

    # Individual span comparison.
    table = Table(
        title="Individual Span Differences",
        show_lines=True
    )

    table.add_column("Span ID", style="cyan")
    table.add_column("Span Name")
    table.add_column("Service")
    table.add_column("Status")

    for item in results:
        status = item["status"]

        styles = {
            "UNCHANGED": "[green]UNCHANGED[/green]",
            "MODIFIED": "[yellow]MODIFIED[/yellow]",
            "MISSING AFTER": "[red]MISSING AFTER[/red]",
            "ONLY AFTER": "[blue]ONLY AFTER[/blue]",
        }

        table.add_row(
            str(item["span_id"]),
            str(item["name"]),
            str(item["service"]),
            styles[status],
        )

    console.print(table)

    # Show changed attributes.
    changed_items = [
        item for item in results
        if item["differences"]
    ]

    if changed_items:
        console.print("\n[bold yellow]ATTRIBUTE DIFFERENCES[/bold yellow]")

        for item in changed_items:
            console.print(
                f"\n[bold]{item['name']} "
                f"({item['span_id']})[/bold]"
            )

            diff_table = Table(show_lines=True)

            diff_table.add_column("Attribute")
            diff_table.add_column("Before")
            diff_table.add_column("After")

            for difference in item["differences"]:
                diff_table.add_row(
                    difference["attribute"],
                    str(difference["before"]),
                    str(difference["after"]),
                )

            console.print(diff_table)

    # Show parent-child structure.
    console.print("\n")
    display_span_tree(results)
