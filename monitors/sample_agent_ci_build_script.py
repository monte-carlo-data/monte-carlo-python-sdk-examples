#!/usr/bin/env python3
"""
Sample script for gating CI builds using Monte Carlo agent monitor evals.

Prerequisites:
1. Pre-prod agent monitor configured with dynamic runtime variable (e.g., {{ci_build_id}})
2. Agent instrumented to tag invocations with CI build ID
3. Agent invoked with golden dataset, traces tagged with current CI build ID

Usage:
    python sample_agent_ci_build_script.py \
        --monitor-uuid "3b54b254-c2d6-4efa-9226-dd2379b89217" \
        --ci-build-id "build-12345" \
        --expected-trace-count 10

The script automatically retrieves monitor configuration (MCON, agentSpanFilters,
runtime variable names) from the monitor UUID.

Exit codes:
    0: Monitor passed (no breach)
    1: Monitor failed (breach detected)
    2: Error during execution
"""

import argparse
import sys
import time

from pycarlo.core import Client
from pycarlo.features.agent import AgentService, AgentSpanFilter, SpanAttributeFilter
from pycarlo.features.monitor import AgentMonitorConfig, MonitorService


def run_ci_build_gate(
    monitor_uuid: str,
    ci_build_id: str,
    expected_trace_count: int,
    trace_timeout_minutes: int,
    monitor_timeout_minutes: int,
) -> int:
    """Run CI build gate with agent monitor eval."""
    client = Client()
    agent_service = AgentService(mc_client=client)
    monitor_service = MonitorService(mc_client=client, print_func=print)

    # Step 0: Fetch monitor configuration
    print(f"Fetching monitor configuration for {monitor_uuid}...")
    try:
        monitor_config = monitor_service.get_agent_monitor_config(monitor_uuid)
    except ValueError as e:
        print(f"ERROR: {e}")
        return 2

    print(f"Monitor: {monitor_config.name} ({monitor_config.description})")
    print(f"MCON: {monitor_config.mcon}")
    print(f"Agent span filters: {len(monitor_config.agent_span_filters)}")
    print(f"Runtime variables: {monitor_config.runtime_variable_names}")
    print(f"Attribute key: {monitor_config.attribute_key}")

    if not monitor_config.attribute_key:
        print("ERROR: Monitor does not have an attribute key configured in filters.")
        return 2

    # Step 1: Wait for expected traces to be visible
    traces_ready = wait_for_traces(
        agent_service=agent_service,
        monitor_config=monitor_config,
        attribute_value=ci_build_id,
        expected_count=expected_trace_count,
        timeout_minutes=trace_timeout_minutes,
    )

    if not traces_ready:
        print("ERROR: Traces not visible within timeout. Failing build.")
        return 2

    # Step 2: Build runtime variables from monitor config
    runtime_variables: dict[str, str] = {}
    for var_name in monitor_config.runtime_variable_names:
        runtime_variables[var_name] = ci_build_id

    # Step 3: Run monitor with runtime variables and poll until complete
    print(f"\nRunning monitor {monitor_uuid}...")
    print(f"Runtime variables: {runtime_variables}")
    try:
        breached = monitor_service.run_and_poll(
            monitor_uuid=monitor_uuid,
            runtime_variables=runtime_variables if runtime_variables else None,
            timeout_in_minutes=monitor_timeout_minutes,
        )
    except (ValueError, TimeoutError) as e:
        print(f"ERROR: Monitor execution failed: {e}")
        return 2

    # Step 4: Report result
    if breached:
        print("\n❌ MONITOR BREACHED - Failing CI build")
        return 1
    else:
        print("\n✅ MONITOR PASSED - CI build may proceed")
        return 0


def wait_for_traces(
    agent_service: AgentService,
    monitor_config: AgentMonitorConfig,
    attribute_value: str,
    expected_count: int,
    timeout_minutes: int = 10,
    poll_interval_seconds: int = 10,
) -> bool:
    """Poll until expected trace count is visible."""
    attribute_key = monitor_config.attribute_key
    print(f"Waiting for {expected_count} traces with {attribute_key}={attribute_value}...")

    if not monitor_config.mcon:
        print("ERROR: Monitor does not have an MCON configured.")
        return False

    if not attribute_key:
        print("ERROR: Monitor does not have an attribute key configured.")
        return False

    # Convert monitor's agentSpanFilters to AgentSpanFilter objects
    agent_span_filters: list[AgentSpanFilter] = []
    for f in monitor_config.agent_span_filters:
        agent_span_filters.append(
            AgentSpanFilter(
                agent=f.agent,
                workflow=f.workflow,
                task=f.task,
                span_name=f.span_name,
            )
        )

    timeout_start = time.time()
    timeout_seconds = timeout_minutes * 60
    count = 0

    while time.time() < timeout_start + timeout_seconds:
        count = agent_service.get_agent_span_count(
            mcon=monitor_config.mcon,
            agent_span_filters=agent_span_filters if agent_span_filters else None,
            attribute_filters=[SpanAttributeFilter(key=attribute_key, value=attribute_value)],
        )
        print(f"Current trace count: {count}, expected: {expected_count}")

        if count >= expected_count:
            print("Expected trace count reached.")
            return True

        print(f"Polling again in {poll_interval_seconds}s...")
        time.sleep(poll_interval_seconds)

    print(f"Timeout: Only found {count} traces after {timeout_minutes} minutes.")
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Gate CI builds using agent monitor evals")
    parser.add_argument("--monitor-uuid", required=True, help="UUID of the agent monitor")
    parser.add_argument("--ci-build-id", required=True, help="CI build ID value")
    parser.add_argument(
        "--expected-trace-count",
        type=int,
        required=True,
        help="Expected number of traces before running monitor",
    )
    parser.add_argument(
        "--trace-timeout-minutes",
        type=int,
        default=10,
        help="Timeout waiting for traces (default: 10)",
    )
    parser.add_argument(
        "--monitor-timeout-minutes",
        type=int,
        default=10,
        help="Timeout waiting for monitor execution (default: 10)",
    )
    args = parser.parse_args()

    return run_ci_build_gate(
        monitor_uuid=args.monitor_uuid,
        ci_build_id=args.ci_build_id,
        expected_trace_count=args.expected_trace_count,
        trace_timeout_minutes=args.trace_timeout_minutes,
        monitor_timeout_minutes=args.monitor_timeout_minutes,
    )


if __name__ == "__main__":
    sys.exit(main())
