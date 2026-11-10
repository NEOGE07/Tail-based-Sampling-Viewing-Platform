import json

from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

from span_diff import display_span_diff

from elasticsearch_client import (
    ELASTICSEARCH_INDEX,
    ELASTICSEARCH_INDEX_BEFORE,
    ELASTICSEARCH_INDEX_AFTER,
)

console = Console()


# ============================================================
# ELASTICSEARCH QUERIES
# ============================================================

def get_trace_summaries(client, index, limit=50):
    response = client.search(
        index=index,
        size=0,
        aggs={
            "traces": {
                "terms": {
                    "field": "trace_id",
                    "size": limit,
                    "order": {"_count": "desc"}
                }
            }
        }
    )

    buckets = response["aggregations"]["traces"]["buckets"]

    return [
        {
            "trace_id": bucket["key"],
            "span_count": bucket["doc_count"]
        }
        for bucket in buckets
    ]


def get_spans(client, index, trace_id, limit=1000):
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
                    "unmapped_type": "date"
                }
            }
        ]
    )

    return response["hits"]["hits"]


# ============================================================
# BEFORE / AFTER COMPARISON
# ============================================================

def get_comparison_summaries(client, limit=50):
    before_traces = get_trace_summaries(
        client,
        ELASTICSEARCH_INDEX_BEFORE,
        limit
    )

    after_traces = get_trace_summaries(
        client,
        ELASTICSEARCH_INDEX_AFTER,
        limit
    )

    before_map = {
        item["trace_id"]: item["span_count"]
        for item in before_traces
    }

    after_map = {
        item["trace_id"]: item["span_count"]
        for item in after_traces
    }

    all_trace_ids = set(before_map) | set(after_map)

    results = []

    for trace_id in all_trace_ids:
        before_count = before_map.get(trace_id, 0)
        after_count = after_map.get(trace_id, 0)

        if before_count > 0 and after_count == 0:
            status = "DROPPED"
        elif after_count < before_count:
            status = "PARTIAL"
        elif after_count == before_count:
            status = "RETAINED"
        else:
            status = "AFTER MORE"

        results.append({
            "trace_id": trace_id,
            "before_count": before_count,
            "after_count": after_count,
            "status": status
        })

    results.sort(
        key=lambda item: item["before_count"],
        reverse=True
    )

    return results[:limit]


# ============================================================
# DISPLAY FUNCTIONS
# ============================================================

def display_traces(traces, dataset_name):
    table = Table(
        title=f"Traces - {dataset_name}",
        show_lines=True
    )

    table.add_column("#", justify="right")
    table.add_column("Trace ID", style="cyan")
    table.add_column("Span Count", justify="right")

    for i, trace in enumerate(traces, start=1):
        table.add_row(
            str(i),
            trace["trace_id"],
            str(trace["span_count"])
        )

    console.print(table)


def display_comparison(traces):
    table = Table(
        title="Before vs After Tail Sampling",
        show_lines=True
    )

    table.add_column("#", justify="right")
    table.add_column("Trace ID", style="cyan")
    table.add_column("Before", justify="right")
    table.add_column("After", justify="right")
    table.add_column("Status")

    for i, trace in enumerate(traces, start=1):
        status = trace["status"]

        if status == "DROPPED":
            status_display = "[red]DROPPED[/red]"
        elif status == "PARTIAL":
            status_display = "[yellow]PARTIAL[/yellow]"
        elif status == "RETAINED":
            status_display = "[green]RETAINED[/green]"
        else:
            status_display = "[magenta]AFTER MORE[/magenta]"

        table.add_row(
            str(i),
            trace["trace_id"],
            str(trace["before_count"]),
            str(trace["after_count"]),
            status_display
        )

    console.print(table)


def display_spans(spans, dataset_name):
    table = Table(
        title=f"Spans - {dataset_name}",
        show_lines=True
    )

    table.add_column("#", justify="right")
    table.add_column("Span ID", style="cyan")
    table.add_column("Span Name")
    table.add_column("Parent Span ID")
    table.add_column("Service")
    table.add_column("Duration")

    for i, hit in enumerate(spans, start=1):
        source = hit["_source"]

        service = (
            source.get("resource", {})
            .get("attributes", {})
            .get("service.name", "N/A")
        )

        table.add_row(
            str(i),
            str(source.get("span_id", "N/A")),
            str(source.get("name", "N/A")),
            str(source.get("parent_span_id", "N/A")),
            str(service),
            str(source.get("duration", "N/A"))
        )

    console.print(table)


def display_span_document(hit):
    console.print_json(
        json.dumps(hit["_source"], indent=2, default=str)
    )


# ============================================================
# DATASET BROWSING
# ============================================================

def browse_dataset(client, index, dataset_name):
    while True:
        traces = get_trace_summaries(client, index)

        if not traces:
            console.print("[yellow]No traces found.[/yellow]")
            return

        display_traces(traces, dataset_name)

        choice = Prompt.ask(
            "\nSelect a trace number, or 'b' to return"
        )

        if choice.lower() == "b":
            return

        if not choice.isdigit():
            console.print("[red]Invalid selection.[/red]")
            continue

        trace_number = int(choice)

        if not 1 <= trace_number <= len(traces):
            console.print("[red]Invalid trace number.[/red]")
            continue

        selected_trace = traces[trace_number - 1]
        trace_id = selected_trace["trace_id"]

        spans = get_spans(client, index, trace_id)

        if not spans:
            console.print("[yellow]No spans found.[/yellow]")
            continue

        while True:
            display_spans(spans, dataset_name)

            span_choice = Prompt.ask(
                "\nSelect a span number to inspect, or 'b' to return"
            )

            if span_choice.lower() == "b":
                break

            if not span_choice.isdigit():
                console.print("[red]Invalid selection.[/red]")
                continue

            span_number = int(span_choice)

            if not 1 <= span_number <= len(spans):
                console.print("[red]Invalid span number.[/red]")
                continue

            display_span_document(spans[span_number - 1])


# ============================================================
# COMPARISON BROWSING
# ============================================================

def browse_comparison(client):
    while True:
        traces = get_comparison_summaries(client)

        if not traces:
            console.print("[yellow]No traces found.[/yellow]")
            return

        display_comparison(traces)

        choice = Prompt.ask(
            "\nSelect a trace number, or 'b' to return"
        )

        if choice.lower() == "b":
            return

        if not choice.isdigit():
            console.print("[red]Invalid selection.[/red]")
            continue

        trace_number = int(choice)

        if not 1 <= trace_number <= len(traces):
            console.print("[red]Invalid trace number.[/red]")
            continue

        selected_trace = traces[trace_number - 1]
        trace_id = selected_trace["trace_id"]

        console.print(f"\n[bold cyan]Trace ID:[/bold cyan] {trace_id}")
        console.print(
            f"Sampling status: [bold]{selected_trace['status']}[/bold]"
        )

        before_spans = get_spans(
            client,
            ELASTICSEARCH_INDEX_BEFORE,
            trace_id
        )

        after_spans = get_spans(
            client,
            ELASTICSEARCH_INDEX_AFTER,
            trace_id
        )

        if before_spans or after_spans:
            display_span_diff(before_spans, after_spans)
        else:
            console.print("[yellow]No spans found.[/yellow]")


# ============================================================
# MAIN MENU
# ============================================================

def run_trace_explorer(client):
    while True:
        console.print("\n[bold cyan]OTel Trace Explorer[/bold cyan]")

        console.print("1. Generic traces")
        console.print("2. Before tail sampling")
        console.print("3. After tail sampling")
        console.print("4. Compare before vs after")
        console.print("5. Exit")

        choice = Prompt.ask("\nSelect an option")

        if choice == "1":
            browse_dataset(
                client,
                ELASTICSEARCH_INDEX,
                "Generic Traces"
            )

        elif choice == "2":
            browse_dataset(
                client,
                ELASTICSEARCH_INDEX_BEFORE,
                "Before Sampling"
            )

        elif choice == "3":
            browse_dataset(
                client,
                ELASTICSEARCH_INDEX_AFTER,
                "After Sampling"
            )

        elif choice == "4":
            browse_comparison(client)

        elif choice == "5":
            console.print("[green]Exiting Trace Explorer.[/green]")
            break

        else:
            console.print("[red]Invalid option.[/red]")
