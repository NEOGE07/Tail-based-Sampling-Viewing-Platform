import math
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from elasticsearch_client import (
    test_connection,
    ELASTICSEARCH_INDEX_BEFORE,
    ELASTICSEARCH_INDEX_AFTER,
)

from span_diff import build_span_diff


# ============================================================
# CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="OpenTelemetry Trace Explorer",
    page_icon="🔍",
    layout="wide",
)

# Elasticsearch aggregation page size.
AGGREGATION_PAGE_SIZE = 5000

# Maximum spans retrieved for an individual trace.
SPAN_LIMIT = 1000

# Elasticsearch request timeout.
REQUEST_TIMEOUT = 120

# Trace catalogue cache duration.
CACHE_TTL = 3600

# Tail sampling configuration.
ERROR_STATUS = "ERROR"
LATENCY_THRESHOLD_MS = 1000
PROBABILISTIC_PERCENTAGE = 10.0

# IMPORTANT:
# The probabilistic decision is deliberately not reconstructed
# in this version. Confirm the exact Collector version,
# hash_salt, and hash implementation before enabling it.


# ============================================================
# APPLICATION HEADER
# ============================================================

st.title("OpenTelemetry Trace Explorer")

st.caption(
    "Explore traces before and after tail sampling, "
    "compare spans, and investigate sampling decisions."
)


# ============================================================
# ELASTICSEARCH CONNECTION
# ============================================================

@st.cache_resource
def get_es_client():

    client = test_connection()

    if client is None:
        raise ConnectionError(
            "Unable to connect to Elasticsearch."
        )

    return client


try:

    client = get_es_client()

except Exception as e:

    st.error(f"Elasticsearch connection failed: {e}")
    st.stop()


# ============================================================
# GENERAL HELPERS
# ============================================================

def get_document_source(document):

    if not isinstance(document, dict):
        return {}

    return document.get("_source", document)


def get_nested_value(data, path):
    """
    Retrieve a nested field using dot notation.

    Supports both nested dictionaries and literal dotted keys.
    """

    if not isinstance(data, dict):
        return None

    if path in data:
        return data[path]

    current = data

    for part in path.split("."):

        if not isinstance(current, dict):
            return None

        if part not in current:
            return None

        current = current[part]

    return current


def get_first_value(data, paths):

    for path in paths:

        value = get_nested_value(data, path)

        if value is not None:
            return value

    return None


def format_timestamp(timestamp):

    if timestamp is None:
        return "N/A"

    try:

        if isinstance(timestamp, (int, float)):

            timestamp = parse_timestamp(timestamp)

        elif isinstance(timestamp, str):

            timestamp = parse_timestamp(timestamp)

        if timestamp is None:
            return "N/A"

        if timestamp.tzinfo is None:

            timestamp = timestamp.replace(
                tzinfo=timezone.utc
            )

        return timestamp.astimezone().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    except Exception:

        return str(timestamp)


# ============================================================
# TIMESTAMP PARSING
# ============================================================

def parse_timestamp(value):
    """
    Convert timestamps into timezone-aware datetime objects.

    Supports:
      - ISO 8601 strings
      - Unix seconds
      - Unix milliseconds
      - Unix microseconds
      - Unix nanoseconds

    Elasticsearch stores this project's @timestamp value as a
    numeric string in epoch milliseconds, so numeric strings
    must be handled as well as numeric Python values.
    """

    if value is None:
        return None

    if isinstance(value, datetime):

        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)

        return value

    if isinstance(value, str):

        value = value.strip()

        if not value:
            return None

        # First check whether the string is a numeric Unix timestamp.
        try:

            numeric_value = float(value)

        except (TypeError, ValueError):

            numeric_value = None

        if numeric_value is not None:
            return parse_timestamp(numeric_value)

        # Otherwise, parse it as ISO 8601.
        try:

            parsed = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )

            if parsed.tzinfo is None:

                parsed = parsed.replace(
                    tzinfo=timezone.utc
                )

            return parsed

        except (TypeError, ValueError):

            return None

    if isinstance(value, (int, float)):

        number = float(value)

        if number > 1e17:
            # Nanoseconds.
            number = number / 1_000_000_000

        elif number > 1e14:
            # Microseconds.
            number = number / 1_000_000

        elif number > 1e11:
            # Milliseconds.
            number = number / 1_000

        try:

            return datetime.fromtimestamp(
                number,
                tz=timezone.utc,
            )

        except (OverflowError, OSError, ValueError):

            return None

    return None


# ============================================================
# SPAN START AND END TIMES
# ============================================================

# Confirmed Elasticsearch document schema:
#
#   @timestamp : epoch milliseconds
#   duration   : nanoseconds
#
# The Elasticsearch OTel exporter stores the span start in
# @timestamp. There is no separate end_time field in these docs.
#
# Therefore:
#
#   Span End = @timestamp + duration


def get_span_start(span):

    source = get_document_source(span)

    return parse_timestamp(
        source.get("@timestamp")
    )


def get_span_duration_ms(span):
    """
    Return span duration in milliseconds.

    The duration field in this index is confirmed to be in
    nanoseconds (field-capabilities metadata: unit=nanos).
    """

    source = get_document_source(span)

    duration_ns = source.get("duration")

    if duration_ns is None:
        return None

    try:

        return float(duration_ns) / 1_000_000

    except (TypeError, ValueError):

        return None


def get_span_end(span):
    """
    Calculate span end from @timestamp + duration.
    """

    start = get_span_start(span)

    duration_ms = get_span_duration_ms(span)

    if start is None or duration_ms is None:
        return None

    return start + pd.Timedelta(
        milliseconds=duration_ms
    )


# ============================================================
# SPAN STATUS DETECTION
# ============================================================

def normalise_status(value):
    """
    Normalise OpenTelemetry span status values.

    OpenTelemetry StatusCode:
        0 = UNSET
        1 = OK
        2 = ERROR

    HTTP response codes are not treated as OTel span status.
    """

    if isinstance(value, dict):

        value = value.get(
            "code",
            value.get("status_code"),
        )

    if value is None:
        return None

    if isinstance(value, (int, float)):

        if int(value) == 2:
            return "ERROR"

        if int(value) == 1:
            return "OK"

        if int(value) == 0:
            return "UNSET"

    text = str(value).strip().upper()

    if text in [
        "ERROR",
        "STATUS_CODE_ERROR",
    ]:
        return "ERROR"

    if text in [
        "OK",
        "STATUS_CODE_OK",
    ]:
        return "OK"

    if text in [
        "UNSET",
        "STATUS_CODE_UNSET",
    ]:
        return "UNSET"

    return text


def get_span_status(span):

    source = get_document_source(span)

    value = get_first_value(
        source,
        [
            "status.code",
            "status_code",
            "status",
        ],
    )

    if value is None:

        attributes = source.get(
            "attributes",
            {},
        )

        value = get_first_value(
            attributes,
            [
                "otel.status_code",
                "status.code",
            ],
        )

    return normalise_status(value)


# ============================================================
# TRACE POLICY EVALUATION
# ============================================================

def evaluate_trace_policies(
    before_spans,
    before_expected_count,
    after_spans,
):
    """
    Evaluate the configured tail-sampling policies
    against the Before trace documents.

    Policies:
      1. Error: retain if any span has OTel ERROR status.
      2. Latency: retain if complete trace duration > 1000ms.
      3. Probabilistic: 10% sampling.

    The probabilistic decision is not reconstructed here.
    """

    results = []

    # --------------------------------------------------------
    # Determine whether the Before trace is complete.
    # --------------------------------------------------------

    retrieved_count = len(before_spans)

    before_truncated = (
        before_expected_count > retrieved_count
    )

    # --------------------------------------------------------
    # ERROR POLICY
    # --------------------------------------------------------

    span_statuses = [
        get_span_status(span)
        for span in before_spans
    ]

    error_spans = [
        index
        for index, status in enumerate(span_statuses)
        if status == ERROR_STATUS
    ]

    if error_spans:

        error_result = True

        error_explanation = (
            f"{len(error_spans)} span(s) have "
            "OpenTelemetry status ERROR."
        )

    elif before_truncated:

        error_result = None

        error_explanation = (
            "The Before trace exceeds the retrieval limit. "
            "An ERROR span may exist outside the retrieved "
            "documents."
        )

    elif any(status is None for status in span_statuses):

        error_result = None

        error_explanation = (
            "Some spans do not contain a recognised "
            "OpenTelemetry status field. The error "
            "condition cannot be fully verified."
        )

    else:

        error_result = False

        error_explanation = (
            "No span with OpenTelemetry status ERROR "
            "was found."
        )

    results.append(
        {
            "policy": "Error policy",
            "result": error_result,
            "explanation": error_explanation,
        }
    )

    # --------------------------------------------------------
    # LATENCY POLICY
    # --------------------------------------------------------

    span_times = []
    incomplete_time_spans = []

    for span in before_spans:

        start = get_span_start(span)
        end = get_span_end(span)

        if start is None or end is None:

            incomplete_time_spans.append(
                get_document_source(span).get(
                    "span_id",
                    "unknown",
                )
            )

            continue

        span_times.append(
            (start, end)
        )

    # A trace duration based on only some spans can be misleading.
    # Require valid start and duration values for every retrieved
    # span before treating the calculated duration as complete.

    if (
        span_times
        and not incomplete_time_spans
        and not before_truncated
    ):

        earliest_start = min(
            start for start, end in span_times
        )

        latest_end = max(
            end for start, end in span_times
        )

        trace_duration_ms = (
            latest_end - earliest_start
        ).total_seconds() * 1000

    else:

        earliest_start = None
        latest_end = None
        trace_duration_ms = None

    known_span_duration_ms = max(
        (
            (end - start).total_seconds() * 1000
            for start, end in span_times
        ),
        default=None,
    )

    if (
        trace_duration_ms is not None
        and trace_duration_ms > LATENCY_THRESHOLD_MS
    ):

        latency_result = True

        latency_explanation = (
            f"Trace duration is {trace_duration_ms:,.2f} ms. "
            f"This exceeds the {LATENCY_THRESHOLD_MS} ms "
            "threshold."
        )

    elif (
        known_span_duration_ms is not None
        and known_span_duration_ms > LATENCY_THRESHOLD_MS
    ):

        # Even if the trace is truncated or other spans have
        # incomplete timing, one span longer than the threshold
        # proves that the overall trace duration exceeds it.

        latency_result = True

        latency_explanation = (
            f"At least one span has a duration of "
            f"{known_span_duration_ms:,.2f} ms. The overall "
            f"trace duration must therefore exceed the "
            f"{LATENCY_THRESHOLD_MS} ms threshold."
        )

    elif before_truncated:

        latency_result = None

        latency_explanation = (
            "The trace exceeds the retrieval limit. "
            "The complete trace duration cannot be "
            "confirmed from the retrieved spans."
        )

    elif trace_duration_ms is None:

        latency_result = None

        if incomplete_time_spans:

            latency_explanation = (
                f"Could not calculate a complete trace duration: "
                f"{len(incomplete_time_spans)} retrieved span(s) "
                "are missing a valid @timestamp or duration value."
            )

        else:

            latency_explanation = (
                "The available documents do not contain "
                "enough recognised @timestamp and duration "
                "information to calculate the complete trace duration."
            )

    else:

        latency_result = False

        latency_explanation = (
            f"Trace duration is {trace_duration_ms:,.2f} ms. "
            f"This does not exceed the {LATENCY_THRESHOLD_MS} "
            "ms threshold."
        )

    results.append(
        {
            "policy": "Latency policy",
            "result": latency_result,
            "explanation": latency_explanation,
        }
    )

    # --------------------------------------------------------
    # PROBABILISTIC POLICY
    # --------------------------------------------------------

    probabilistic_explanation = (
        f"The configured sampling percentage is "
        f"{PROBABILISTIC_PERCENTAGE}%. "
        "The individual trace decision has not been "
        "reconstructed."
    )

    results.append(
        {
            "policy": "Probabilistic policy",
            "result": None,
            "explanation": probabilistic_explanation,
        }
    )

    # --------------------------------------------------------
    # OVERALL EXPECTED DECISION
    # --------------------------------------------------------

    # The policies are OR-combined.
    # Any matching retention policy is sufficient.

    if (
        error_result is True
        or latency_result is True
    ):

        expected_decision = "KEEP"

        expected_explanation = (
            "At least one deterministic retention policy "
            "matched. The trace qualifies for retention "
            "regardless of the probabilistic policy."
        )

    elif (
        error_result is False
        and latency_result is False
    ):

        expected_decision = "INDETERMINATE"

        expected_explanation = (
            "Neither the error nor latency policy matched. "
            "The 10% probabilistic policy may still select "
            "this trace. Its individual decision has not "
            "been reconstructed."
        )

    else:

        expected_decision = "INDETERMINATE"

        expected_explanation = (
            "At least one deterministic policy could not "
            "be fully evaluated. The expected sampling "
            "decision cannot be established."
        )

    return {
        "policies": results,
        "expected_decision": expected_decision,
        "expected_explanation": expected_explanation,
        "trace_duration_ms": trace_duration_ms,
        "error_count": len(error_spans),
        "before_truncated": before_truncated,
    }


# ============================================================
# ACTUAL TAIL-SAMPLING POLICY METADATA
# ============================================================

def find_sampling_metadata(spans):
    """
    Look for policy metadata added by the Collector's
    recordpolicy feature gate.

    These attributes are only expected when that feature
    gate is enabled and the relevant decision metadata
    was attached to the exported spans.
    """

    metadata = {
        "policy": set(),
        "composite_policy": set(),
        "cached_decision": set(),
    }

    for span in spans:

        source = get_document_source(span)

        attribute_maps = [
            source,
            source.get("attributes", {}),
            source.get("resource", {}).get("attributes", {}),
            source.get("resource_attributes", {}),
        ]

        for attribute_map in attribute_maps:

            if not isinstance(attribute_map, dict):
                continue

            for key in metadata:

                value = get_first_value(
                    attribute_map,
                    [
                        f"tailsampling.{key}",
                        key,
                    ],
                )

                if value is not None:

                    metadata[key].add(
                        str(value)
                    )

    return {
        key: sorted(values)
        for key, values in metadata.items()
    }


# ============================================================
# TRACE CLASSIFICATION
# ============================================================

def classify_trace(
    before_spans,
    after_spans,
    expected_before_count,
    expected_after_count,
):
    """
    Classify a selected trace using span IDs when available.

    This is more reliable than comparing document counts
    alone because it identifies which baseline spans are
    absent from After.
    """

    before_ids = set()
    after_ids = set()

    for span in before_spans:

        source = get_document_source(span)

        span_id = source.get("span_id")

        if span_id:
            before_ids.add(str(span_id))

    for span in after_spans:

        source = get_document_source(span)

        span_id = source.get("span_id")

        if span_id:
            after_ids.add(str(span_id))

    missing_ids = sorted(
        before_ids - after_ids
    )

    extra_ids = sorted(
        after_ids - before_ids
    )

    if expected_after_count == 0:

        result = "DROPPED"

    elif before_ids and after_ids:

        if missing_ids:

            result = "PARTIALLY PRESENT"

        elif expected_before_count > len(before_spans):

            result = "INCOMPLETE RETRIEVAL"

        else:

            result = "RETAINED"

    elif expected_after_count < expected_before_count:

        result = "PARTIALLY PRESENT"

    else:

        result = "RETAINED"

    return {
        "result": result,
        "before_ids": before_ids,
        "after_ids": after_ids,
        "missing_ids": missing_ids,
        "extra_ids": extra_ids,
    }


# ============================================================
# RETRIEVE ALL TRACE SUMMARIES
# ============================================================

def get_all_trace_summaries(es_client, index):
    """
    Retrieve every unique trace ID using composite
    aggregation pagination.
    """

    all_traces = []

    after_key = None

    while True:

        composite_aggregation = {
            "size": AGGREGATION_PAGE_SIZE,

            "sources": [
                {
                    "trace_id": {
                        "terms": {
                            "field": "trace_id"
                        }
                    }
                }
            ],
        }

        if after_key:

            composite_aggregation["after"] = after_key

        response = es_client.options(
            request_timeout=REQUEST_TIMEOUT
        ).search(
            index=index,
            size=0,
            aggs={
                "traces": {
                    "composite": composite_aggregation,

                    "aggs": {
                        "last_seen": {
                            "max": {
                                "field": "@timestamp"
                            }
                        }
                    },
                }
            },
        )

        aggregation = response["aggregations"]["traces"]

        buckets = aggregation.get(
            "buckets",
            [],
        )

        if not buckets:
            break

        for bucket in buckets:

            last_seen = bucket["last_seen"].get(
                "value_as_string"
            )

            if last_seen is None:

                last_seen = bucket["last_seen"].get(
                    "value"
                )

            all_traces.append(
                {
                    "trace_id": bucket["key"]["trace_id"],
                    "span_count": bucket["doc_count"],
                    "last_seen": format_timestamp(last_seen),
                }
            )

        after_key = aggregation.get("after_key")

        if not after_key:
            break

    return all_traces


# ============================================================
# BUILD COMPLETE TRACE CATALOGUE
# ============================================================

@st.cache_data(ttl=CACHE_TTL)
def load_trace_catalogue(_es_client):

    before_traces = get_all_trace_summaries(
        _es_client,
        ELASTICSEARCH_INDEX_BEFORE,
    )

    after_traces = get_all_trace_summaries(
        _es_client,
        ELASTICSEARCH_INDEX_AFTER,
    )

    before_map = {
        trace["trace_id"]: trace
        for trace in before_traces
    }

    after_map = {
        trace["trace_id"]: trace
        for trace in after_traces
    }

    catalogue = []

    # Before is the baseline.
    for trace_id, before in before_map.items():

        before_count = before["span_count"]

        after = after_map.get(trace_id)

        after_count = (
            after["span_count"]
            if after
            else 0
        )

        if after_count == 0:

            result = "DROPPED"

        elif after_count < before_count:

            result = "PARTIALLY PRESENT"

        else:

            result = "RETAINED"

        catalogue.append(
            {
                "trace_id": trace_id,
                "last_seen": before["last_seen"],
                "before_spans": before_count,
                "after_spans": after_count,
                "sampling_result": result,
            }
        )

    catalogue.sort(
        key=lambda item: item["last_seen"],
        reverse=True,
    )

    return catalogue


# ============================================================
# RETRIEVE SELECTED TRACE SPANS
# ============================================================

@st.cache_data(ttl=CACHE_TTL)
def get_spans(_es_client, index, trace_id):

    response = _es_client.options(
        request_timeout=REQUEST_TIMEOUT
    ).search(
        index=index,
        size=SPAN_LIMIT,
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

    return response["hits"]["hits"]


# ============================================================
# REFRESH CONTROLS
# ============================================================

refresh_col, info_col = st.columns([1, 5])

with refresh_col:

    if st.button(
        "Refresh traces",
        width="stretch",
    ):

        st.cache_data.clear()

        st.session_state.trace_page = 0
        st.session_state.selected_trace_id = None

        st.rerun()

with info_col:

    st.caption(
        "Trace catalogue is cached for one hour. "
        "Use Refresh traces to retrieve the latest data."
    )


# ============================================================
# LOAD TRACE CATALOGUE
# ============================================================

try:

    with st.spinner(
        "Loading trace catalogue from Elasticsearch..."
    ):

        trace_catalogue = load_trace_catalogue(client)

except Exception as e:

    st.error(
        f"Unable to retrieve trace catalogue: {e}"
    )

    st.stop()


if not trace_catalogue:

    st.warning(
        "No traces were found in the Before index."
    )

    st.stop()


trace_df = pd.DataFrame(trace_catalogue)


# ============================================================
# TRACE OVERVIEW
# ============================================================

total_traces = len(trace_df)

retained_count = len(
    trace_df[
        trace_df["sampling_result"] == "RETAINED"
    ]
)

dropped_count = len(
    trace_df[
        trace_df["sampling_result"] == "DROPPED"
    ]
)

partial_count = len(
    trace_df[
        trace_df["sampling_result"] == "PARTIALLY PRESENT"
    ]
)

st.subheader("Trace Overview")

metric1, metric2, metric3, metric4 = st.columns(4)

metric1.metric(
    "Total traces",
    f"{total_traces:,}",
)

metric2.metric(
    "Retained",
    f"{retained_count:,}",
)

metric3.metric(
    "Dropped",
    f"{dropped_count:,}",
)

metric4.metric(
    "Partially present",
    f"{partial_count:,}",
)


# ============================================================
# TRACE CATALOGUE PAGINATION
# ============================================================

st.divider()

st.subheader("Trace Catalogue")

if "trace_page" not in st.session_state:

    st.session_state.trace_page = 0

if "selected_trace_id" not in st.session_state:

    st.session_state.selected_trace_id = None


page_size = st.selectbox(
    "Records per page",
    options=[500, 1000],
    index=0,
)

total_pages = max(
    1,
    math.ceil(total_traces / page_size),
)

st.session_state.trace_page = min(
    st.session_state.trace_page,
    total_pages - 1,
)

current_page = st.session_state.trace_page

start_index = current_page * page_size

end_index = min(
    start_index + page_size,
    total_traces,
)

page_df = trace_df.iloc[
    start_index:end_index
].reset_index(drop=True)


# ============================================================
# PAGE NAVIGATION
# ============================================================

previous_col, page_info_col, next_col = st.columns(
    [1, 3, 1]
)

with previous_col:

    if st.button(
        "← Previous",
        disabled=current_page == 0,
        width="stretch",
    ):

        st.session_state.trace_page -= 1

        st.session_state.selected_trace_id = None

        st.rerun()


with page_info_col:

    st.markdown(
        f"<div style='text-align:center'>"
        f"<b>Page {current_page + 1:,} of {total_pages:,}</b>"
        f"<br>"
        f"Showing {start_index + 1:,}–{end_index:,} "
        f"of {total_traces:,} traces"
        f"</div>",
        unsafe_allow_html=True,
    )


with next_col:

    if st.button(
        "Next →",
        disabled=current_page >= total_pages - 1,
        width="stretch",
    ):

        st.session_state.trace_page += 1

        st.session_state.selected_trace_id = None

        st.rerun()


# ============================================================
# TRACE TABLE
# ============================================================

trace_selection = st.dataframe(
    page_df,
    width="stretch",
    height=600,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    key=f"trace_table_{current_page}_{page_size}",
)


# ============================================================
# SELECTED TRACE
# ============================================================

selected_rows = trace_selection.selection.rows

if selected_rows:

    selected_row = selected_rows[0]

    selected_trace = page_df.iloc[selected_row]

    selected_trace_id = selected_trace["trace_id"]

    st.session_state.selected_trace_id = selected_trace_id

else:

    selected_trace_id = st.session_state.selected_trace_id


# ============================================================
# SELECTED TRACE DETAIL
# ============================================================

if selected_trace_id:

    st.divider()

    st.subheader("Selected Trace")

    st.text("Trace ID")

    st.code(
        selected_trace_id,
        language=None,
    )

    try:

        with st.spinner(
            "Loading Before and After spans..."
        ):

            before_spans = get_spans(
                client,
                ELASTICSEARCH_INDEX_BEFORE,
                selected_trace_id,
            )

            after_spans = get_spans(
                client,
                ELASTICSEARCH_INDEX_AFTER,
                selected_trace_id,
            )

    except Exception as e:

        st.error(
            f"Unable to retrieve trace spans: {e}"
        )

        st.stop()


    # Get the expected document counts from the catalogue.
    selected_catalogue_row = trace_df[
        trace_df["trace_id"] == selected_trace_id
    ].iloc[0]

    expected_before_count = int(
        selected_catalogue_row["before_spans"]
    )

    expected_after_count = int(
        selected_catalogue_row["after_spans"]
    )


    # ========================================================
    # CLASSIFY SELECTED TRACE
    # ========================================================

    classification = classify_trace(
        before_spans,
        after_spans,
        expected_before_count,
        expected_after_count,
    )

    sampling_result = classification["result"]

    before_count = expected_before_count
    after_count = expected_after_count


    # ========================================================
    # TRACE SUMMARY
    # ========================================================

    st.subheader("Observed Trace Outcome")

    detail_col1, detail_col2, detail_col3 = st.columns(3)

    detail_col1.metric(
        "Before spans",
        before_count,
    )

    detail_col2.metric(
        "After spans",
        after_count,
    )

    detail_col3.metric(
        "Observed result",
        sampling_result,
    )


    if sampling_result == "DROPPED":

        st.warning(
            "This trace exists in Before but has no "
            "matching documents in After."
        )

    elif sampling_result == "PARTIALLY PRESENT":

        st.warning(
            "The After index contains only part of the "
            "baseline span set. This is a data discrepancy, "
            "not proof that the tail sampler intentionally "
            "dropped individual spans."
        )

    elif sampling_result == "INCOMPLETE RETRIEVAL":

        st.warning(
            "The retrieved documents are incomplete. "
            "Increase SPAN_LIMIT to inspect the full trace."
        )

    else:

        st.success(
            "All retrieved baseline span IDs are present "
            "in the After index."
        )


    # ========================================================
    # SAMPLING POLICY ANALYSIS
    # ========================================================

    st.divider()

    st.subheader("Sampling Decision Analysis")

    st.caption(
        "The policies below are evaluated against the "
        "Before trace documents. The observed outcome is "
        "calculated independently from the After index."
    )


    evaluation = evaluate_trace_policies(
        before_spans,
        expected_before_count,
        after_spans,
    )


    # Display each policy.
    for policy in evaluation["policies"]:

        policy_name = policy["policy"]

        policy_result = policy["result"]

        with st.container(border=True):

            policy_col1, policy_col2 = st.columns(
                [2, 1]
            )

            with policy_col1:

                st.markdown(
                    f"**{policy_name}**"
                )

            with policy_col2:

                if policy_result is True:

                    st.success("MATCHED")

                elif policy_result is False:

                    st.info("NOT MATCHED")

                else:

                    st.warning("NOT CONFIRMED")

            st.write(
                policy["explanation"]
            )


    # ========================================================
    # OVERALL EXPECTED DECISION
    # ========================================================

    st.markdown("#### Overall Policy Evaluation")

    expected_decision = evaluation[
        "expected_decision"
    ]

    if expected_decision == "KEEP":

        st.success(
            "EXPECTED: KEEP"
        )

    else:

        st.warning(
            "EXPECTED: INDETERMINATE"
        )

    st.write(
        evaluation["expected_explanation"]
    )


    if evaluation["trace_duration_ms"] is not None:

        st.metric(
            "Calculated trace duration",
            f"{evaluation['trace_duration_ms']:,.2f} ms",
        )


    # ========================================================
    # POLICY / OBSERVED OUTCOME CONSISTENCY
    # ========================================================

    st.markdown("#### Policy / Observed Outcome Consistency")

    if sampling_result == "PARTIALLY PRESENT":

        st.warning(
            "PARTIAL DATA PRESENCE: Tail sampling makes "
            "trace-level decisions. Missing individual spans "
            "suggest an ingestion, export, retrieval, or "
            "pipeline discrepancy."
        )

    elif sampling_result == "INCOMPLETE RETRIEVAL":

        st.warning(
            "The trace could not be fully inspected because "
            "the span retrieval limit was reached."
        )

    elif expected_decision == "KEEP" and sampling_result == "DROPPED":

        st.error(
            "POLICY / OBSERVED OUTCOME MISMATCH: "
            "At least one deterministic retention policy "
            "matched, but no trace documents were found in After."
        )

        st.write(
            "Investigate trace delivery, tail-sampler capacity, "
            "decision timing, collector routing, and export errors."
        )

    elif expected_decision == "KEEP" and sampling_result == "RETAINED":

        st.success(
            "CONSISTENT: A deterministic retention policy "
            "matched and the trace is present in After."
        )

    elif expected_decision == "INDETERMINATE" and sampling_result == "DROPPED":

        st.info(
            "The observed outcome is consistent with a dropped "
            "trace, but the individual probabilistic decision "
            "has not been independently confirmed."
        )

    elif expected_decision == "INDETERMINATE" and sampling_result == "RETAINED":

        st.info(
            "The trace is present in After. It may have been "
            "selected by the probabilistic policy, but the "
            "individual hash decision has not been reconstructed."
        )


    # ========================================================
    # ACTUAL SAMPLING POLICY METADATA
    # ========================================================

    st.divider()

    st.subheader("Recorded Tail-Sampling Metadata")

    sampling_metadata = find_sampling_metadata(
        after_spans
    )

    if any(sampling_metadata.values()):

        if sampling_metadata["policy"]:

            st.write(
                "**Sampling policy:**",
                ", ".join(sampling_metadata["policy"]),
            )

        if sampling_metadata["composite_policy"]:

            st.write(
                "**Composite policy:**",
                ", ".join(
                    sampling_metadata["composite_policy"]
                ),
            )

        if sampling_metadata["cached_decision"]:

            st.write(
                "**Cached decision:**",
                ", ".join(
                    sampling_metadata["cached_decision"]
                ),
            )

    else:

        st.info(
            "No tailsampling.policy metadata was found "
            "in the After documents. The Collector may not "
            "have the recordpolicy feature gate enabled, "
            "or the metadata may not have been exported."
        )


    # ========================================================
    # MISSING SPANS
    # ========================================================

    st.divider()

    st.subheader("Missing Span Analysis")

    missing_ids = classification["missing_ids"]

    extra_ids = classification["extra_ids"]

    if missing_ids:

        st.warning(
            f"{len(missing_ids)} baseline span ID(s) "
            "were not found in After."
        )

        missing_df = pd.DataFrame(
            {
                "Missing Span ID": missing_ids
            }
        )

        st.dataframe(
            missing_df,
            width="stretch",
            hide_index=True,
        )

    else:

        st.success(
            "No missing baseline span IDs were identified "
            "among the retrieved documents."
        )

    if extra_ids:

        st.info(
            f"{len(extra_ids)} span ID(s) were found in After "
            "but not in the retrieved Before documents."
        )


    # ========================================================
    # SPAN COMPARISON
    # ========================================================

    st.divider()

    st.subheader("Span Comparison")

    span_differences = build_span_diff(
        before_spans,
        after_spans,
    )

    if not span_differences:

        st.info(
            "No span comparison results are available."
        )

    else:

        span_table_data = []

        for item in span_differences:

            span_table_data.append(
                {
                    "Name": item.get("name", "N/A"),
                    "Service": item.get("service", "N/A"),
                    "Span ID": item.get("span_id", "N/A"),
                    "Comparison": item.get("status", "N/A"),
                }
            )

        span_df = pd.DataFrame(
            span_table_data
        )


        # ----------------------------------------------------
        # SPAN TABLE
        # ----------------------------------------------------

        st.caption(
            "Select a span to inspect its attributes "
            "and Before/After documents."
        )

        span_selection = st.dataframe(
            span_df,
            width="stretch",
            height=400,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key=f"span_table_{selected_trace_id}",
        )


        # ----------------------------------------------------
        # SELECTED SPAN
        # ----------------------------------------------------

        selected_span_rows = span_selection.selection.rows

        if selected_span_rows:

            selected_span_index = selected_span_rows[0]

            selected_span = span_differences[
                selected_span_index
            ]

            st.divider()

            st.subheader("Span Details")

            st.write(
                f"**Span name:** "
                f"{selected_span.get('name', 'N/A')}"
            )

            st.write(
                f"**Span ID:** "
                f"{selected_span.get('span_id', 'N/A')}"
            )

            st.write(
                f"**Service:** "
                f"{selected_span.get('service', 'N/A')}"
            )

            st.write(
                f"**Comparison result:** "
                f"{selected_span.get('status', 'N/A')}"
            )


            # ------------------------------------------------
            # ATTRIBUTE DIFFERENCES
            # ------------------------------------------------

            differences = selected_span.get(
                "differences",
                [],
            )

            st.markdown("#### Attribute Differences")

            if differences:

                for difference in differences:

                    st.write(difference)

            else:

                st.caption(
                    "No attribute differences were identified."
                )


            # ------------------------------------------------
            # DOCUMENTS
            # ------------------------------------------------

            before_document = selected_span.get(
                "before"
            )

            after_document = selected_span.get(
                "after"
            )

            before_source = (
                get_document_source(before_document)
                if before_document
                else None
            )

            after_source = (
                get_document_source(after_document)
                if after_document
                else None
            )

            before_tab, after_tab = st.tabs(
                [
                    "Before document",
                    "After document",
                ]
            )

            with before_tab:

                if before_source:

                    st.json(before_source)

                else:

                    st.info(
                        "This span does not exist in Before."
                    )

            with after_tab:

                if after_source:

                    st.json(after_source)

                else:

                    st.info(
                        "This span does not exist in After."
                    )


    # ========================================================
    # RETRIEVAL LIMIT WARNING
    # ========================================================

    if (
        expected_before_count >= SPAN_LIMIT
        or expected_after_count >= SPAN_LIMIT
    ):

        st.warning(
            f"Span retrieval reached the configured limit "
            f"of {SPAN_LIMIT}. This trace may contain more "
            "spans than are currently displayed. Increase "
            "SPAN_LIMIT if necessary."
        )

else:

    st.divider()

    st.info(
        "Select a trace from the catalogue above to inspect "
        "its sampling policies, missing spans, and "
        "Before/After comparison."
    )
