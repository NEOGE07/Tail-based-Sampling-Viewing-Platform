
import json

from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

from elasticsearch_client import (
    ELASTICSEARCH_INDEX,
    ELASTICSEARCH_INDEX_BEFORE,
    ELASTICSEARCH_INDEX_AFTER,
)

from span_diff import (
    build_span_diff,
    display_span_diff_detail,
)

console = Console()

TRACE_LIMIT = 50
SPAN_LIMIT = 1000


# ============================================================
# ELASTICSEARCH QUERIES
# ============================================================

def get_trace_summaries(client, index, limit=TRACE_LIMIT):
    """Return the most frequent traces from an index."""

    response = client.search(
        index=index,
        size=0,
        aggs={
            "traces": {
                "terms": {
                    "field": "trace_id",
                    "size": limit,
                    "order": {"_count": "desc"},
                }
            }
        },
    )

    buckets = response["aggregations"]["traces"]["buckets"]

    return [
        {
            "trace_id": bucket["key"],
            "span_count": bucket["doc_count"],
        }
        for bucket in buckets
    ]


def get_span_counts_for_ids(client, index, trace_ids):
    """Count the same baseline trace IDs in another index."""

    if not trace_ids:
        return {}

    response = client.search(
        index=index,
        size=0,
        query={
            "terms": {
                "trace_id": trace_ids
            }
        },
        aggs={
            "traces": {
                "terms": {
                    "field": "trace_id",
                    "size": len(trace_ids),
                }
            }
        },
    )

    buckets = response["aggregations"]["traces"]["buckets"]

    return {
        bucket["key"]: bucket["doc_count"]
        for bucket in buckets
    }


def build_trace_catalog(client):
    """
    Select the baseline 50 traces from BEFORE sampling.

    All viewing options reuse these exact trace IDs
    and their original ordering.
    """

    baseline = get_trace_summaries(
        client,
        ELASTICSEARCH_INDEX_BEFORE,
        TRACE_LIMIT,
    )

    trace_ids = [
        item["trace_id"]
        for item in baseline
    ]

    if not trace_ids:
        return []

    before_counts = {
        item["trace_id"]: item["span_count"]
        for item in baseline
    }

    generic_counts = get_span_counts_for_ids(
        client,
        ELASTICSEARCH_INDEX,
        trace_ids,
    )

    after_counts = get_span_counts_for_ids(
        client,
        ELASTICSEARCH_INDEX_AFTER,
        trace_ids,
    )

    catalog = []

    for trace_id in trace_ids:

        before_count = before_counts.get(trace_id, 0)
        generic_count = generic_counts.get(trace_id, 0)
        after_count = after_counts.get(trace_id, 0)

        if after_count == 0:
            status = "DROPPED"

        elif after_count < before_count:
            status = "PARTIAL"

        elif after_count == before_count:
            status = "RETAINED"

        else:
            status = "AFTER MORE"

        catalog.append({
            "trace_id": trace_id,
            "before_count": before_count,
            "generic_count": generic_count,
            "after_count": after_count,
            "sampling_status": status,
        })

    return catalog


def get_spans(client, index, trace_id, limit=SPAN_LIMIT):

    response = client.search(
        index=index,
        query={
            "term": {
                "trace_id": trace_id
            }
        },
        size=limit,
        sort=[
            {
                "@timestamp": {
                    "order": "asc",
                    "unmapped_type": "date",
                }
            }
        ],
    )

    return response["hits"]["hits"]


# ============================================================
# DISPLAY HELPERS
# ============================================================

def get_service_name(source):

    return (
        source.get("resource", {})
        .get("attributes", {})
        .get("service.name", "N/A")
    )


def display_trace_catalog(catalog, view):

    table = Table(
        title=f"Trace Explorer - {view}",
        show_lines=True,
    )

    table.add_column("#", justify="right")
    table.add_column("Trace ID", style="cyan")

    if view == "Before Sampling":

        table.add_column("Before Spans", justify="right")
        table.add_column("Status")

    elif view == "After Sampling":

        table.add_column("After Spans", justify="right")
        table.add_column("Status")

    elif view == "Generic Traces":

        table.add_column("Generic Spans", justify="right")
        table.add_column("Status")

    else:

        table.add_column("Before", justify="right")
        table.add_column("After", justify="right")
        table.add_column("Sampling Status")

    for number, trace in enumerate(catalog, start=1):

        if view == "Before Sampling":

            count = trace["before_count"]

            table.add_row(
                str(number),
                trace["trace_id"],
                str(count),
                "AVAILABLE" if count else "MISSING",
            )

        elif view == "After Sampling":

            count = trace["after_count"]

            status = (
                "[red]DROPPED[/red]"
                if count == 0
                else "[green]AVAILABLE[/green]"
            )

            table.add_row(
                str(number),
                trace["trace_id"],
                str(count),
                status,
            )

        elif view == "Generic Traces":

            count = trace["generic_count"]

            status = (
                "[red]DROPPED[/red]"
                if count == 0
                else "[green]AVAILABLE[/green]"
            )

            table.add_row(
                str(number),
                trace["trace_id"],
                str(count),
                status,
            )

        else:

            status = trace["sampling_status"]

            styles = {
                "DROPPED": "[red]DROPPED[/red]",
                "PARTIAL": "[yellow]PARTIAL[/yellow]",
                "RETAINED": "[green]RETAINED[/green]",
                "AFTER MORE": "[magenta]AFTER MORE[/magenta]",
            }

            table.add_row(
                str(number),
                trace["trace_id"],
                str(trace["before_count"]),
                str(trace["after_count"]),
                styles[status],
            )

    console.print(table)


def display_spans(spans, dataset_name, comparison=False):

    table = Table(
        title=f"Spans - {dataset_name}",
        show_lines=True,
    )

    table.add_column("#", justify="right")
    table.add_column("Span ID", style="cyan")
    table.add_column("Span Name")
    table.add_column("Parent Span ID")
    table.add_column("Service")

    table.add_column(
        "Comparison Status" if comparison else "Duration"
    )

    for number, item in enumerate(spans, start=1):

        if comparison:

            source = (
                item["before"]["_source"]
                if item["before"]
                else item["after"]["_source"]
            )

            span_id = item["span_id"]

            styles = {
                "UNCHANGED": "[green]UNCHANGED[/green]",
                "MODIFIED": "[yellow]MODIFIED[/yellow]",
                "MISSING AFTER": "[red]MISSING AFTER[/red]",
                "ONLY AFTER": "[blue]ONLY AFTER[/blue]",
            }

            final_column = styles[item["status"]]

        else:

            source = item["_source"]

            span_id = source.get("span_id", "N/A")

            final_column = str(
                source.get("duration", "N/A")
            )

        table.add_row(
            str(number),
            str(span_id),
            str(source.get("name", "N/A")),
            str(source.get("parent_span_id", "N/A")),
            str(get_service_name(source)),
            final_column,
        )

    console.print(table)


def display_span_document(hit):

    console.print_json(
        json.dumps(
            hit["_source"],
            indent=2,
            default=str,
        )
    )


def print_back_option():

    console.print("b. Back")


def wait_for_back():

    while input().strip().lower() != "b":
        pass


# ============================================================
# SPAN NAVIGATION
# ============================================================

def browse_span_list(spans, dataset_name):

    if not spans:

        console.print("[yellow]No spans found.[/yellow]")
        return

    # Display the span list only once.
    display_spans(spans, dataset_name)

    print_back_option()

    while True:

        choice = Prompt.ask("Select a span number")

        if choice.lower() == "b":
            return

        if not choice.isdigit():

            console.print("[red]Invalid selection.[/red]")
            continue

        number = int(choice)

        if not 1 <= number <= len(spans):

            console.print("[red]Invalid span number.[/red]")
            continue

        # Display only the selected span.
        display_span_document(spans[number - 1])

        print_back_option()

        wait_for_back()


def browse_comparison_span_list(before_spans, after_spans):

    results = build_span_diff(
        before_spans,
        after_spans,
    )

    if not results:

        console.print(
            "[yellow]No spans available for comparison.[/yellow]"
        )
        return

    # Display the comparison list only once.
    display_spans(
        results,
        "Before vs After Tail Sampling",
        comparison=True,
    )

    print_back_option()

    while True:

        choice = Prompt.ask("Select a span number")

        if choice.lower() == "b":
            return

        if not choice.isdigit():

            console.print("[red]Invalid selection.[/red]")
            continue

        number = int(choice)

        if not 1 <= number <= len(results):

            console.print("[red]Invalid span number.[/red]")
            continue

        # Display only the selected span's comparison.
        display_span_diff_detail(results[number - 1])

        print_back_option()

        wait_for_back()


# ============================================================
# TRACE NAVIGATION
# ============================================================

def browse_dataset(
    client,
    catalog,
    index,
    dataset_name,
    count_key,
):

    if not catalog:

        console.print("[yellow]No baseline traces found.[/yellow]")
        return

    # Display the fixed 50-record list only once.
    display_trace_catalog(catalog, dataset_name)

    print_back_option()

    while True:

        choice = Prompt.ask("Select a trace number")

        if choice.lower() == "b":
            return

        if not choice.isdigit():

            console.print("[red]Invalid selection.[/red]")
            continue

        number = int(choice)

        if not 1 <= number <= len(catalog):

            console.print("[red]Invalid trace number.[/red]")
            continue

        trace = catalog[number - 1]

        trace_id = trace["trace_id"]

        if trace[count_key] == 0:

            console.print(
                f"[red]Trace {trace_id} is DROPPED or absent "
                f"from {dataset_name}.[/red]"
            )

            continue

        spans = get_spans(
            client,
            index,
            trace_id,
        )

        browse_span_list(
            spans,
            dataset_name,
        )


# ============================================================
# BEFORE / AFTER COMPARISON
# ============================================================

def browse_comparison(client, catalog):

    if not catalog:

        console.print("[yellow]No baseline traces found.[/yellow]")
        return

    # Use the same baseline trace IDs.
    display_trace_catalog(
        catalog,
        "Before vs After Tail Sampling",
    )

    print_back_option()

    while True:

        choice = Prompt.ask("Select a trace number")

        if choice.lower() == "b":
            return

        if not choice.isdigit():

            console.print("[red]Invalid selection.[/red]")
            continue

        number = int(choice)

        if not 1 <= number <= len(catalog):

            console.print("[red]Invalid trace number.[/red]")
            continue

        trace = catalog[number - 1]

        trace_id = trace["trace_id"]

        before_spans = get_spans(
            client,
            ELASTICSEARCH_INDEX_BEFORE,
            trace_id,
        )

        after_spans = get_spans(
            client,
            ELASTICSEARCH_INDEX_AFTER,
            trace_id,
        )

        console.print(
            f"\n[bold cyan]Trace ID:[/bold cyan] {trace_id}"
        )

        console.print(
            f"Sampling status: [bold]{trace['sampling_status']}[/bold]"
        )

        browse_comparison_span_list(
            before_spans,
            after_spans,
        )


# ============================================================
# MAIN MENU
# ============================================================

def run_trace_explorer(client):

    console.print(
        "\n[bold cyan]Building the fixed baseline of 50 traces "
        "from the before-sampling index...[/bold cyan]"
    )

    catalog = build_trace_catalog(client)

    if not catalog:

        console.print(
            "[yellow]No traces found in the before-sampling index.[/yellow]"
        )

        return

    console.print(
        f"[green]Loaded {len(catalog)} baseline trace IDs. "
        "The same IDs will be used in every viewing option.[/green]"
    )

    while True:

        console.print("\n[bold cyan]OTel Trace Explorer[/bold cyan]")

        console.print("1. Generic traces")
        console.print("2. Before tail sampling")
        console.print("3. After tail sampling")
        console.print("4. Compare before vs after")
        console.print("5. Clear output")
        console.print("6. Exit")

        choice = Prompt.ask("\nSelect an option")

        if choice == "1":

            browse_dataset(
                client,
                catalog,
                ELASTICSEARCH_INDEX,
                "Generic Traces",
                "generic_count",
            )

        elif choice == "2":

            browse_dataset(
                client,
                catalog,
                ELASTICSEARCH_INDEX_BEFORE,
                "Before Sampling",
                "before_count",
            )

        elif choice == "3":

            browse_dataset(
                client,
                catalog,
                ELASTICSEARCH_INDEX_AFTER,
                "After Sampling",
                "after_count",
            )

        elif choice == "4":

            browse_comparison(
                client,
                catalog,
            )

        elif choice == "5":

            # Clear the terminal without rebuilding the trace catalog.
            console.clear()

        elif choice == "6":

            console.print("[green]Exiting Trace Explorer.[/green]")
            break

        else:

            console.print("[red]Invalid option.[/red]")
