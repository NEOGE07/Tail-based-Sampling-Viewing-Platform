import json
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone

from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table
from rich.tree import Tree

from elasticsearch_client import (
    ELASTICSEARCH_INDEX,
    ELASTICSEARCH_INDEX_BEFORE,
    ELASTICSEARCH_INDEX_AFTER,
)

from span_diff import (
    build_span_diff,
    display_span_diff_detail,
)


# ============================================================
# CONFIGURATION
# ============================================================

console = Console()

TRACE_LIMIT = 50
SPAN_LIMIT = 1000

LATENCY_THRESHOLD_MS = Decimal("1000")
PROBABILISTIC_SAMPLING_PERCENTAGE = 10

SHOW_SPAN_HIERARCHY = True


# ============================================================
# ELASTICSEARCH QUERIES
# ============================================================

def get_trace_summaries(client, index, limit=TRACE_LIMIT):
    """
    Retrieve the most recently active traces from an index.
    """

    response = client.search(
        index=index,
        size=0,
        aggs={
            "traces": {
                "terms": {
                    "field": "trace_id",
                    "size": limit,
                    "order": {
                        "latest_timestamp": "desc"
                    },
                },
                "aggs": {
                    "latest_timestamp": {
                        "max": {
                            "field": "@timestamp"
                        }
                    }
                },
            }
        },
    )

    buckets = (
        response.get("aggregations", {})
        .get("traces", {})
        .get("buckets", [])
    )

    summaries = []

    for bucket in buckets:
        summaries.append({
            "trace_id": bucket["key"],
            "span_count": bucket["doc_count"],
            "latest_timestamp": (
                bucket.get("latest_timestamp", {}).get("value")
            ),
        })

    return summaries


def get_span_counts_for_ids(client, index, trace_ids):
    """
    Retrieve span counts for a supplied list of trace IDs.
    """

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

    buckets = (
        response.get("aggregations", {})
        .get("traces", {})
        .get("buckets", [])
    )

    counts = {
        bucket["key"]: bucket["doc_count"]
        for bucket in buckets
    }

    for trace_id in trace_ids:
        counts.setdefault(trace_id, 0)

    return counts


def get_spans(client, index, trace_id, limit=SPAN_LIMIT):
    """
    Retrieve spans for one trace ID.

    Returns complete Elasticsearch hits.
    """

    response = client.search(
        index=index,
        size=limit,
        query={
            "term": {
                "trace_id": trace_id
            }
        },
        sort=[
            {
                "@timestamp": {
                    "order": "asc"
                }
            }
        ],
    )

    return response.get("hits", {}).get("hits", [])


# ============================================================
# TRACE CATALOGUE
# ============================================================

def get_sampling_status(before_count, after_count):
    """
    Describe the observed span-count relationship.

    These labels do not claim that tail sampling selectively
    removed or created spans.
    """

    if before_count == 0:
        return "NO BEFORE DATA"

    if after_count == 0:
        return "NO AFTER DATA"

    if after_count < before_count:
        return "FEWER SPANS"

    if after_count > before_count:
        return "MORE SPANS"

    return "SAME SPAN COUNT"


def build_trace_catalog(client):
    """
    Build a catalogue using the latest traces in the Before index.

    The same trace IDs are used to retrieve counts from all
    three datasets.
    """

    console.print(
        "\n[cyan]Building trace catalogue...[/cyan]"
    )

    baseline = get_trace_summaries(
        client,
        ELASTICSEARCH_INDEX_BEFORE,
        TRACE_LIMIT,
    )

    if not baseline:
        console.print(
            "[red]No traces found in the Before Sampling index.[/red]"
        )
        return []

    trace_ids = [
        item["trace_id"]
        for item in baseline
    ]

    generic_counts = get_span_counts_for_ids(
        client,
        ELASTICSEARCH_INDEX,
        trace_ids,
    )

    before_counts = get_span_counts_for_ids(
        client,
        ELASTICSEARCH_INDEX_BEFORE,
        trace_ids,
    )

    after_counts = get_span_counts_for_ids(
        client,
        ELASTICSEARCH_INDEX_AFTER,
        trace_ids,
    )

    catalog = []

    for item in baseline:

        trace_id = item["trace_id"]

        before_count = before_counts.get(trace_id, 0)
        generic_count = generic_counts.get(trace_id, 0)
        after_count = after_counts.get(trace_id, 0)

        catalog.append({
            "trace_id": trace_id,
            "before_count": before_count,
            "generic_count": generic_count,
            "after_count": after_count,
            "sampling_status": get_sampling_status(
                before_count,
                after_count,
            ),
        })

    console.print(
        f"[green]Catalogue ready: {len(catalog)} traces.[/green]"
    )

    return catalog


# ============================================================
# DOCUMENT HELPERS
# ============================================================

def get_source(hit):
    """
    Safely retrieve an Elasticsearch document's _source.
    """

    if isinstance(hit, dict):
        return hit.get("_source", hit)

    return {}


def get_service_name(hit):

    source = get_source(hit)

    resource = source.get("resource", {})
    attributes = resource.get("attributes", {})

    return attributes.get(
        "service.name",
        "unknown",
    )


def get_span_name(hit):

    source = get_source(hit)

    return source.get(
        "name",
        "unknown",
    )


def get_span_id(hit):

    source = get_source(hit)

    return source.get(
        "span_id",
        "unknown",
    )


def get_parent_span_id(hit):

    source = get_source(hit)

    return source.get(
        "parent_span_id"
    )


# ============================================================
# TIMESTAMP AND LATENCY HELPERS
# ============================================================

def timestamp_to_nanoseconds(timestamp):
    """
    Convert Elasticsearch @timestamp to nanoseconds.

    Numeric timestamps are interpreted as epoch milliseconds.
    ISO timestamps are also supported.
    """

    if timestamp is None:
        return None

    try:
        numeric_timestamp = Decimal(str(timestamp))

        return int(
            numeric_timestamp * Decimal("1000000")
        )

    except (InvalidOperation, ValueError):
        pass

    try:
        parsed = datetime.fromisoformat(
            str(timestamp).replace("Z", "+00:00")
        )

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return int(
            parsed.timestamp() * 1_000_000_000
        )

    except (ValueError, TypeError):
        return None


def get_trace_latency_ms(spans):
    """
    Calculate trace duration using the earliest span start
    and latest span end.
    """

    if not spans:
        return None

    earliest_start = None
    latest_end = None

    for hit in spans:

        source = get_source(hit)

        timestamp = timestamp_to_nanoseconds(
            source.get("@timestamp")
        )

        duration = source.get("duration")

        if timestamp is None or duration is None:
            return None

        try:
            duration_ns = int(duration)
        except (ValueError, TypeError):
            return None

        span_end = timestamp + duration_ns

        if earliest_start is None or timestamp < earliest_start:
            earliest_start = timestamp

        if latest_end is None or span_end > latest_end:
            latest_end = span_end

    if earliest_start is None or latest_end is None:
        return None

    return Decimal(
        latest_end - earliest_start
    ) / Decimal("1000000")


# ============================================================
# ERROR DETECTION
# ============================================================

def is_error_status(hit):
    """
    Detect common OpenTelemetry error status representations.
    """

    source = get_source(hit)

    status = source.get("status", {})

    if not isinstance(status, dict):
        status = {}

    attributes = source.get("attributes", {})

    if not isinstance(attributes, dict):
        attributes = {}

    possible_status_values = [
        status.get("code"),
        source.get("status_code"),
        attributes.get("otel.status_code"),
    ]

    for value in possible_status_values:

        if value is None:
            continue

        normalized = str(value).strip().upper()

        if normalized in (
            "ERROR",
            "STATUSCODEERROR",
            "2",
        ):
            return True

    return False


# ============================================================
# SAMPLING EXPLANATION
# ============================================================

def explain_sampling_decision(before_spans, after_spans):
    """
    Explain which configured sampling conditions appear to match.

    Deterministic policies:
        - ERROR status
        - Trace latency > 1000 ms

    Probabilistic policy:
        - 10% sampling

    The actual tail-sampling decision is not automatically
    stored in these Elasticsearch documents. Probabilistic
    explanations are inferred, not confirmed.
    """

    if not before_spans:

        return {
            "title": "UNKNOWN",
            "details": [
                "No Before Sampling spans were found.",
                "The sampling decision cannot be evaluated.",
            ],
            "confirmed": False,
        }

    error_spans = [
        hit
        for hit in before_spans
        if is_error_status(hit)
    ]

    latency_ms = get_trace_latency_ms(before_spans)

    latency_match = (
        latency_ms is not None
        and latency_ms > LATENCY_THRESHOLD_MS
    )

    error_match = len(error_spans) > 0
    after_exists = len(after_spans) > 0

    deterministic_reasons = []

    if error_match:
        deterministic_reasons.append(
            f"ERROR policy: {len(error_spans)} error span(s) detected."
        )

    if latency_match:
        deterministic_reasons.append(
            f"Latency policy: trace latency is "
            f"{latency_ms:.2f} ms, exceeding "
            f"{LATENCY_THRESHOLD_MS} ms."
        )

    # Deterministic policy matched and trace exists after.

    if deterministic_reasons and after_exists:

        return {
            "title": "DETERMINISTIC POLICY MATCH",
            "details": [
                *deterministic_reasons,
                "The trace exists in the After Sampling index.",
                "At least one deterministic keep condition matches.",
                "This is consistent with the configured keep policies.",
            ],
            "confirmed": True,
        }

    # Deterministic policy matched but trace is absent after.

    if deterministic_reasons and not after_exists:

        return {
            "title": "EXPECTED KEEP - DATA MISSING",
            "details": [
                *deterministic_reasons,
                "The trace is absent from the After Sampling index.",
                "The configured deterministic policy should retain it.",
                "Investigate collector routing, exporter failures, "
                "and Elasticsearch indexing.",
            ],
            "confirmed": False,
        }

    # No deterministic match and latency unavailable.

    if latency_ms is None:

        return {
            "title": "UNKNOWN - INSUFFICIENT TIMING DATA",
            "details": [
                "No ERROR status was detected.",
                "Trace latency could not be calculated reliably.",
                "The probabilistic decision cannot be established.",
            ],
            "confirmed": False,
        }

    # No deterministic match, but trace exists after.

    if after_exists:

        return {
            "title": "LIKELY PROBABILISTIC KEEP",
            "details": [
                "No ERROR status was detected.",
                f"Trace latency: {latency_ms:.2f} ms.",
                f"Latency does not exceed {LATENCY_THRESHOLD_MS} ms.",
                "The trace exists in the After Sampling index.",
                f"The {PROBABILISTIC_SAMPLING_PERCENTAGE}% "
                "probabilistic policy is a possible explanation.",
                "This explanation is INFERRED, not confirmed.",
            ],
            "confirmed": False,
        }

    # No deterministic match and trace absent after.

    return {
        "title": "LIKELY PROBABILISTIC DROP",
        "details": [
            "No ERROR status was detected.",
            f"Trace latency: {latency_ms:.2f} ms.",
            f"Latency does not exceed {LATENCY_THRESHOLD_MS} ms.",
            "The trace is absent from the After Sampling index.",
            f"The {PROBABILISTIC_SAMPLING_PERCENTAGE}% "
            "probabilistic policy may not have selected it.",
            "This explanation is INFERRED, not confirmed.",
            "An export, indexing, or routing failure is also possible.",
        ],
        "confirmed": False,
    }


def display_sampling_explanation(before_spans, after_spans):

    result = explain_sampling_decision(
        before_spans,
        after_spans,
    )

    console.print()

    console.rule(
        "[bold cyan]Tail Sampling Explanation[/bold cyan]"
    )

    title = result["title"]

    if result["confirmed"]:
        console.print(
            f"[bold green]{title}[/bold green]"
        )

    elif "UNKNOWN" in title:
        console.print(
            f"[bold yellow]{title}[/bold yellow]"
        )

    else:
        console.print(
            f"[bold cyan]{title}[/bold cyan]"
        )

    for detail in result["details"]:
        console.print(f"  • {detail}")

    if len(before_spans) >= SPAN_LIMIT:
        console.print(
            "\n[yellow]Warning: Before Sampling span retrieval "
            "reached the configured limit. The explanation "
            "may be incomplete.[/yellow]"
        )

    if len(after_spans) >= SPAN_LIMIT:
        console.print(
            "[yellow]Warning: After Sampling span retrieval "
            "reached the configured limit.[/yellow]"
        )

    if (
        after_spans
        and len(after_spans) < len(before_spans)
    ):
        console.print(
            "[yellow]Notice: The After dataset contains fewer "
            "spans. Tail sampling normally retains the complete "
            "trace, so investigate possible missing spans, "
            "export failures, or indexing differences.[/yellow]"
        )

    if len(after_spans) > len(before_spans):
        console.print(
            "[yellow]Notice: The After dataset contains more spans "
            "than the Before dataset. These additional spans will "
            "be identified as ONLY AFTER in the comparison.[/yellow]"
        )

    console.print()


# ============================================================
# SPAN HIERARCHY
# ============================================================

def display_span_hierarchy(spans, comparison=False):
    """
    Display a parent-child span hierarchy.
    """

    if not spans:
        return

    root = Tree(
        "[bold cyan]Trace Span Hierarchy[/bold cyan]"
    )

    span_lookup = {}
    children = {}

    for hit in spans:

        source = get_source(hit)
        span_id = source.get("span_id")

        if not span_id:
            continue

        span_lookup[span_id] = hit

    for span_id, hit in span_lookup.items():

        parent_id = get_parent_span_id(hit)

        children.setdefault(parent_id, []).append(span_id)

    def add_children(parent_node, parent_id, visited):

        child_ids = children.get(parent_id, [])

        for child_id in child_ids:

            if child_id in visited:
                continue

            visited.add(child_id)

            hit = span_lookup[child_id]

            source = get_source(hit)

            name = source.get("name", "unknown")
            service = get_service_name(hit)

            label = (
                f"[bold]{name}[/bold] "
                f"[dim]({service})[/dim] "
                f"[dim]ID: {child_id}[/dim]"
            )

            child_node = parent_node.add(label)

            add_children(
                child_node,
                child_id,
                visited,
            )

    visited = set()

    root_ids = [
        span_id
        for span_id, hit in span_lookup.items()
        if not get_parent_span_id(hit)
    ]

    for root_id in root_ids:

        hit = span_lookup[root_id]
        source = get_source(hit)

        name = source.get("name", "unknown")
        service = get_service_name(hit)

        node = root.add(
            f"[bold green]{name}[/bold green] "
            f"[dim]({service})[/dim] "
            f"[dim]ID: {root_id}[/dim]"
        )

        visited.add(root_id)

        add_children(
            node,
            root_id,
            visited,
        )

    # Handle spans whose parent is not present.
    # Also catches disconnected or cyclic structures.

    remaining = [
        span_id
        for span_id in span_lookup
        if span_id not in visited
    ]

    if remaining:

        orphan_node = root.add(
            "[yellow]Unattached / Orphan Spans[/yellow]"
        )

        for span_id in remaining:

            if span_id in visited:
                continue

            hit = span_lookup[span_id]
            source = get_source(hit)

            name = source.get("name", "unknown")
            service = get_service_name(hit)

            node = orphan_node.add(
                f"{name} ({service}) [ID: {span_id}]"
            )

            visited.add(span_id)

            add_children(
                node,
                span_id,
                visited,
            )

    console.print(root)


# ============================================================
# SPAN DISPLAY
# ============================================================

def display_spans(
    spans,
    dataset_name,
    comparison=False,
):

    if not spans:

        console.print(
            f"[yellow]No spans found in {dataset_name}.[/yellow]"
        )

        return

    console.print()

    console.rule(
        f"[bold cyan]{dataset_name}[/bold cyan]"
    )

    if SHOW_SPAN_HIERARCHY:

        display_span_hierarchy(
            spans,
            comparison,
        )

    table = Table(
        title=f"{dataset_name} - Span List",
        show_lines=True,
    )

    table.add_column("#", justify="right", style="dim")
    table.add_column("Service", style="cyan")
    table.add_column("Span Name", style="bold")
    table.add_column("Span ID", style="dim")
    table.add_column("Parent Span ID", style="dim")
    table.add_column("Kind")
    table.add_column("Duration (ns)", justify="right")
    table.add_column("Status")

    for number, hit in enumerate(spans, start=1):

        source = get_source(hit)

        status = source.get("status", {})

        if not isinstance(status, dict):
            status = {}

        status_value = status.get("code", "Unset")

        if status_value in ("ERROR", 2, "2"):
            status_text = "[red]ERROR[/red]"
        else:
            status_text = str(status_value)

        table.add_row(
            str(number),
            get_service_name(hit),
            str(source.get("name", "unknown")),
            str(source.get("span_id", "unknown")),
            str(source.get("parent_span_id", "-")),
            str(source.get("kind", "unknown")),
            str(source.get("duration", "-")),
            status_text,
        )

    console.print(table)

    if len(spans) >= SPAN_LIMIT:
        console.print(
            f"[yellow]Warning: Display limited to "
            f"{SPAN_LIMIT} spans. This trace may contain more.[/yellow]"
        )


def display_span_document(hit):

    source = get_source(hit)

    console.print()

    console.rule(
        "[bold cyan]Span Document[/bold cyan]"
    )

    console.print_json(
        json.dumps(
            source,
            indent=2,
            default=str,
        )
    )


# ============================================================
# TRACE CATALOGUE DISPLAY
# ============================================================

def display_trace_catalog(catalog, view):

    if not catalog:
        console.print(
            "[yellow]No traces available.[/yellow]"
        )
        return None

    table = Table(
        title=view,
        show_lines=True,
    )

    table.add_column("#", justify="right", style="dim")
    table.add_column("Trace ID", style="cyan")
    table.add_column("Generic", justify="right")
    table.add_column("Before", justify="right")
    table.add_column("After", justify="right")
    table.add_column("Span Count Status")

    for number, item in enumerate(catalog, start=1):

        status = item["sampling_status"]

        if status == "NO AFTER DATA":
            status_display = "[red]NO AFTER DATA[/red]"

        elif status == "FEWER SPANS":
            status_display = "[yellow]FEWER SPANS[/yellow]"

        elif status == "MORE SPANS":
            status_display = "[blue]MORE SPANS[/blue]"

        elif status == "SAME SPAN COUNT":
            status_display = "[green]SAME SPAN COUNT[/green]"

        else:
            status_display = status

        table.add_row(
            str(number),
            item["trace_id"],
            str(item["generic_count"]),
            str(item["before_count"]),
            str(item["after_count"]),
            status_display,
        )

    console.print(table)

    choice = Prompt.ask(
        "\nSelect a trace number, or B to go back",
        default="B",
    )

    if choice.lower() == "b":
        return None

    try:
        selected_number = int(choice)

        if 1 <= selected_number <= len(catalog):
            return catalog[selected_number - 1]

    except ValueError:
        pass

    console.print(
        "[red]Invalid selection.[/red]"
    )

    return None


# ============================================================
# SPAN NAVIGATION
# ============================================================

def browse_span_list(
    spans,
    dataset_name,
    comparison=False,
):

    if not spans:
        console.print(
            "[yellow]No spans to display.[/yellow]"
        )
        return

    while True:

        display_spans(
            spans,
            dataset_name,
            comparison,
        )

        choice = Prompt.ask(
            "\nEnter span number to inspect, or B to go back",
            default="B",
        )

        if choice.lower() == "b":
            return

        try:
            number = int(choice)

            if 1 <= number <= len(spans):

                display_span_document(
                    spans[number - 1]
                )

                Prompt.ask(
                    "\nPress Enter to return to the span list",
                    default="",
                )

            else:
                console.print(
                    "[red]Invalid span number.[/red]"
                )

        except ValueError:

            console.print(
                "[red]Please enter a valid number or B.[/red]"
            )


# ============================================================
# BEFORE VS AFTER SPAN COMPARISON
# ============================================================

def browse_comparison_span_list(before_spans, after_spans):
    """
    Compare spans and provide separate filters for:

        1. All spans
        2. ONLY AFTER
        3. MISSING AFTER
        4. MODIFIED
        5. UNCHANGED

    Selecting a span opens its detailed comparison.
    """

    if not before_spans and not after_spans:

        console.print(
            "[yellow]No spans available for comparison.[/yellow]"
        )

        return

    results = build_span_diff(
        before_spans,
        after_spans,
    )

    if not results:

        console.print(
            "[yellow]No span comparison results.[/yellow]"
        )

        return

    # --------------------------------------------------------
    # COUNT COMPARISON CATEGORIES
    # --------------------------------------------------------

    status_order = [
        "ONLY AFTER",
        "MISSING AFTER",
        "MODIFIED",
        "UNCHANGED",
    ]

    status_counts = {
        status: sum(
            1
            for item in results
            if item["status"] == status
        )
        for status in status_order
    }

    # --------------------------------------------------------
    # FILTER DEFINITIONS
    # --------------------------------------------------------

    filters = {
        "1": ("ALL SPANS", None),
        "2": ("ONLY AFTER", "ONLY AFTER"),
        "3": ("MISSING AFTER", "MISSING AFTER"),
        "4": ("MODIFIED", "MODIFIED"),
        "5": ("UNCHANGED", "UNCHANGED"),
    }

    while True:

        console.print()

        console.rule(
            "[bold cyan]Span Comparison Summary[/bold cyan]"
        )

        # ----------------------------------------------------
        # SUMMARY TABLE
        # ----------------------------------------------------

        summary = Table(
            title="Before vs After - Difference Summary",
            show_lines=True,
        )

        summary.add_column("Category")
        summary.add_column("Description")
        summary.add_column("Count", justify="right")

        summary.add_row(
            "[blue]ONLY AFTER[/blue]",
            "Spans found only in the After dataset",
            str(status_counts["ONLY AFTER"]),
        )

        summary.add_row(
            "[red]MISSING AFTER[/red]",
            "Spans found only in the Before dataset",
            str(status_counts["MISSING AFTER"]),
        )

        summary.add_row(
            "[yellow]MODIFIED[/yellow]",
            "Spans present in both datasets with differences",
            str(status_counts["MODIFIED"]),
        )

        summary.add_row(
            "[green]UNCHANGED[/green]",
            "Spans present in both datasets without differences",
            str(status_counts["UNCHANGED"]),
        )

        summary.add_section()

        summary.add_row(
            "[bold]TOTAL[/bold]",
            "Unique span IDs compared",
            f"[bold]{len(results)}[/bold]",
        )

        console.print(summary)

        # ----------------------------------------------------
        # FILTER MENU
        # ----------------------------------------------------

        console.print("\n[bold]Comparison Filters[/bold]")

        console.print(
            "[cyan]1[/cyan] - All spans"
        )

        console.print(
            "[cyan]2[/cyan] - ONLY AFTER (new/additional spans)"
        )

        console.print(
            "[cyan]3[/cyan] - MISSING AFTER"
        )

        console.print(
            "[cyan]4[/cyan] - MODIFIED"
        )

        console.print(
            "[cyan]5[/cyan] - UNCHANGED"
        )

        console.print(
            "[dim]B - Back to trace comparison[/dim]"
        )

        filter_choice = Prompt.ask(
            "\nChoose a comparison filter",
            choices=["1", "2", "3", "4", "5", "B"],
            default="1",
        )

        if filter_choice.upper() == "B":
            return

        filter_name, filter_status = filters[filter_choice]

        if filter_status is None:

            filtered_results = results

        else:

            filtered_results = [
                item
                for item in results
                if item["status"] == filter_status
            ]

        # ----------------------------------------------------
        # EMPTY FILTER
        # ----------------------------------------------------

        if not filtered_results:

            console.print(
                f"\n[yellow]No spans found under "
                f"{filter_name}.[/yellow]"
            )

            Prompt.ask(
                "\nPress Enter to return to filters",
                default="",
            )

            continue

        # ----------------------------------------------------
        # FILTERED SPAN TABLE
        # ----------------------------------------------------

        while True:

            table = Table(
                title=f"Span Comparison - {filter_name}",
                show_lines=True,
            )

            table.add_column("#", justify="right")
            table.add_column("Span Name", style="bold")
            table.add_column("Service", style="cyan")
            table.add_column("Span ID", style="dim")
            table.add_column("Comparison Status")

            for number, item in enumerate(
                filtered_results,
                start=1,
            ):

                status = item.get(
                    "status",
                    "UNKNOWN",
                )

                if status == "UNCHANGED":
                    status_display = "[green]UNCHANGED[/green]"

                elif status == "MODIFIED":
                    status_display = "[yellow]MODIFIED[/yellow]"

                elif status == "MISSING AFTER":
                    status_display = "[red]MISSING AFTER[/red]"

                elif status == "ONLY AFTER":
                    status_display = "[bold blue]ONLY AFTER[/bold blue]"

                else:
                    status_display = status

                table.add_row(
                    str(number),
                    str(item.get("name", "unknown")),
                    str(item.get("service", "unknown")),
                    str(item.get("span_id", "unknown")),
                    status_display,
                )

            console.print(table)

            # ------------------------------------------------
            # SELECT SPAN
            # ------------------------------------------------

            choice = Prompt.ask(
                "\nEnter span number to inspect, "
                "or B to change filter",
                default="B",
            )

            if choice.lower() == "b":
                break

            try:

                number = int(choice)

                if 1 <= number <= len(filtered_results):

                    selected_item = filtered_results[number - 1]

                    display_span_diff_detail(
                        selected_item
                    )

                    Prompt.ask(
                        "\nPress Enter to return to span list",
                        default="",
                    )

                else:

                    console.print(
                        "[red]Invalid span number.[/red]"
                    )

            except ValueError:

                console.print(
                    "[red]Please enter a valid number or B.[/red]"
                )


# ============================================================
# DATASET BROWSING
# ============================================================

def browse_dataset(
    client,
    catalog,
    index,
    dataset_name,
    count_key,
):

    while True:

        selected = display_trace_catalog(
            catalog,
            dataset_name,
        )

        if selected is None:
            return

        trace_id = selected["trace_id"]

        console.print(
            f"\n[bold cyan]Trace ID:[/bold cyan] {trace_id}"
        )

        console.print(
            f"[dim]Expected span count in this dataset: "
            f"{selected[count_key]}[/dim]"
        )

        spans = get_spans(
            client,
            index,
            trace_id,
        )

        if not spans:

            console.print(
                f"[yellow]This trace does not exist in "
                f"{dataset_name}.[/yellow]"
            )

            Prompt.ask(
                "\nPress Enter to return",
                default="",
            )

            continue

        browse_span_list(
            spans,
            dataset_name,
        )


# ============================================================
# BEFORE VS AFTER TRACE COMPARISON
# ============================================================

def browse_comparison(client, catalog):

    while True:

        selected = display_trace_catalog(
            catalog,
            "Before vs After Tail Sampling",
        )

        if selected is None:
            return

        trace_id = selected["trace_id"]

        console.print()

        console.rule(
            "[bold cyan]Trace Sampling Comparison[/bold cyan]"
        )

        console.print(
            f"[bold]Trace ID:[/bold] {trace_id}"
        )

        console.print(
            f"[bold]Before spans:[/bold] "
            f"{selected['before_count']}"
        )

        console.print(
            f"[bold]After spans:[/bold] "
            f"{selected['after_count']}"
        )

        console.print(
            f"[bold]Span count status:[/bold] "
            f"{selected['sampling_status']}"
        )

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

        # Explain the possible sampling decision.

        display_sampling_explanation(
            before_spans,
            after_spans,
        )

        # Compare the actual spans.

        browse_comparison_span_list(
            before_spans,
            after_spans,
        )


# ============================================================
# MAIN TRACE EXPLORER
# ============================================================

def run_trace_explorer(client):

    global SHOW_SPAN_HIERARCHY

    console.clear()

    console.print(
        "\n[bold cyan]OpenTelemetry Trace Explorer[/bold cyan]"
    )

    console.print(
        "[dim]Elasticsearch-backed trace exploration "
        "and tail-sampling comparison[/dim]\n"
    )

    # Build one fixed catalogue from the Before index.

    catalog = build_trace_catalog(client)

    if not catalog:
        return

    while True:

        hierarchy_status = (
            "[green]ON[/green]"
            if SHOW_SPAN_HIERARCHY
            else "[red]OFF[/red]"
        )

        console.print()

        menu = Table(
            title="Main Menu",
            show_header=False,
            box=None,
        )

        menu.add_row(
            "[cyan]1[/cyan]",
            "Generic traces",
        )

        menu.add_row(
            "[cyan]2[/cyan]",
            "Before tail sampling",
        )

        menu.add_row(
            "[cyan]3[/cyan]",
            "After tail sampling",
        )

        menu.add_row(
            "[cyan]4[/cyan]",
            "Compare before vs after",
        )

        menu.add_row(
            "[cyan]5[/cyan]",
            f"Toggle span hierarchy ({hierarchy_status})",
        )

        menu.add_row(
            "[cyan]6[/cyan]",
            "Clear output",
        )

        menu.add_row(
            "[cyan]7[/cyan]",
            "Exit",
        )

        console.print(menu)

        choice = Prompt.ask(
            "\nSelect an option",
            choices=[
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
            ],
        )

        # ----------------------------------------------------
        # 1. GENERIC TRACES
        # ----------------------------------------------------

        if choice == "1":

            browse_dataset(
                client,
                catalog,
                ELASTICSEARCH_INDEX,
                "Generic Traces",
                "generic_count",
            )

        # ----------------------------------------------------
        # 2. BEFORE SAMPLING
        # ----------------------------------------------------

        elif choice == "2":

            browse_dataset(
                client,
                catalog,
                ELASTICSEARCH_INDEX_BEFORE,
                "Before Sampling",
                "before_count",
            )

        # ----------------------------------------------------
        # 3. AFTER SAMPLING
        # ----------------------------------------------------

        elif choice == "3":

            browse_dataset(
                client,
                catalog,
                ELASTICSEARCH_INDEX_AFTER,
                "After Sampling",
                "after_count",
            )

        # ----------------------------------------------------
        # 4. BEFORE VS AFTER COMPARISON
        # ----------------------------------------------------

        elif choice == "4":

            browse_comparison(
                client,
                catalog,
            )

        # ----------------------------------------------------
        # 5. TOGGLE SPAN HIERARCHY
        # ----------------------------------------------------

        elif choice == "5":

            SHOW_SPAN_HIERARCHY = not SHOW_SPAN_HIERARCHY

            if SHOW_SPAN_HIERARCHY:

                console.print(
                    "\n[green]Span hierarchy enabled.[/green]"
                )

            else:

                console.print(
                    "\n[yellow]Span hierarchy disabled.[/yellow]"
                )

        # ----------------------------------------------------
        # 6. CLEAR OUTPUT
        # ----------------------------------------------------

        elif choice == "6":

            console.clear()

            console.print(
                "[bold cyan]OpenTelemetry Trace Explorer[/bold cyan]"
            )

            console.print(
                "[dim]Output cleared.[/dim]"
            )

        # ----------------------------------------------------
        # 7. EXIT
        # ----------------------------------------------------

        elif choice == "7":

            console.print(
                "\n[bold green]Exiting Trace Explorer.[/bold green]"
            )

            break
