import json
import time
from typing import Callable, List, Optional, Sequence, Union, cast
from uuid import UUID

from box import Box

from pycarlo.common import get_logger
from pycarlo.common.utils import boxify
from pycarlo.core import Client, Mutation, Query
from pycarlo.features.circuit_breakers.exceptions import (
    CircuitBreakerPipelineException,
    CircuitBreakerPollException,
)
from pycarlo.lib.schema import CircuitBreakerState, SqlJobCheckpointStatus

logger = get_logger(__name__)


class CircuitBreakerService:
    _TERM_STATES = {"PROCESSING_COMPLETE", "HAS_ERROR"}

    def __init__(
        self,
        mc_client: Optional[Client] = None,
        print_func: Callable = logger.info,
    ):
        """
        Convenience methods to help with using circuit breaker rules.

        :param mc_client: MCD client (e.g. for creating a custom session); created otherwise.
        :param print_func: Function to use for echoing. Uses python logging by default, which
                           requires setting MCD_VERBOSE_ERRORS.
        """
        self._client = mc_client or Client()
        self._print_func = print_func

    def trigger_and_poll(
        self,
        rule_uuid: Optional[Union[str, UUID]] = None,
        namespace: Optional[str] = None,
        rule_name: Optional[str] = None,
        timeout_in_minutes: int = 5,
    ) -> Optional[bool]:
        """
        Convenience method to both trigger and poll (wait) on circuit breaker rule execution.

        :param rule_uuid: UUID of the rule (custom SQL monitor) to execute.
        :param namespace: namespace of the rule (custom SQL monitor) to execute.
        :param rule_name: name of the rule (custom SQL monitor) to execute.
        :param timeout_in_minutes: Polling timeout in minutes. See poll() for details.
        :return: True if rule execution has breach; False otherwise. See poll() for any
                 exceptions raised.
        """
        breaches = self.poll_all(
            job_execution_uuids=self.trigger_all(
                rule_uuid=rule_uuid,
                namespace=namespace,
                rule_name=rule_name,
            ),
            timeout_in_minutes=timeout_in_minutes,
        )
        return bool(breaches > 0)

    def trigger(
        self,
        rule_uuid: Optional[Union[str, UUID]] = None,
        namespace: Optional[str] = None,
        rule_name: Optional[str] = None,
    ) -> str:
        """
        Trigger a rule to start execution with circuit breaker checkpointing.

        :param rule_uuid: UUID of the rule (custom SQL monitor) to execute.
        :param namespace: namespace of the rule (custom SQL monitor) to execute.
        :param rule_name: name of the rule (custom SQL monitor) to execute.
        :return: Job execution UUID, as a string, to be used to retrieve execution state / status.
        """
        mutation = Mutation()

        if rule_uuid:
            mutation.trigger_circuit_breaker_rule(rule_uuid=str(rule_uuid)).__fields__(
                "job_execution_uuid"
            )
        elif rule_name:
            if namespace:
                mutation.trigger_circuit_breaker_rule(
                    namespace=namespace, rule_name=rule_name
                ).__fields__("job_execution_uuid")
            else:
                mutation.trigger_circuit_breaker_rule(rule_name=rule_name).__fields__(
                    "job_execution_uuid"
                )
        else:
            raise ValueError("rule UUID or namespace and rule name must be specified")

        mutation_client = self._client(mutation)
        job_execution_uuid = mutation_client.trigger_circuit_breaker_rule.job_execution_uuid
        self._print_func(
            f"Triggered rule with ID '{rule_uuid}'. "
            f"Received '{job_execution_uuid}' as execution ID."
        )
        return cast(str, job_execution_uuid)

    def trigger_all(
        self,
        rule_uuid: Optional[Union[str, UUID]] = None,
        namespace: Optional[str] = None,
        rule_name: Optional[str] = None,
    ) -> List[str]:
        """
        Trigger a rule to start execution with circuit breaker checkpointing.

        This function supports rules that create multiple executions (e.g. rules with variables
        or over multiple tables)

        :param rule_uuid: UUID of the rule (custom SQL monitor) to execute.
        :param namespace: namespace of the rule (custom SQL monitor) to execute.
        :param rule_name: name of the rule (custom SQL monitor) to execute.
        :return: Job execution UUIDs, as strings, to be used to retrieve execution state / status.
        """
        mutation = Mutation()

        if rule_uuid:
            mutation.trigger_circuit_breaker_rule_v2(rule_uuid=str(rule_uuid)).__fields__(
                "job_execution_uuids"
            )
        elif rule_name:
            if namespace:
                mutation.trigger_circuit_breaker_rule_v2(
                    namespace=namespace, rule_name=rule_name
                ).__fields__("job_execution_uuids")
            else:
                mutation.trigger_circuit_breaker_rule_v2(rule_name=rule_name).__fields__(
                    "job_execution_uuids"
                )
        else:
            raise ValueError("rule UUID or namespace and rule name must be specified")

        job_execution_uuids = [
            str(id)
            for id in self._client(mutation).trigger_circuit_breaker_rule_v2.job_execution_uuids
        ]
        self._print_func(
            f"Triggered rule with ID '{rule_uuid}'. "
            f"Received {job_execution_uuids} as execution IDs."
        )
        return job_execution_uuids

    def poll(
        self,
        job_execution_uuid: Union[str, UUID],
        timeout_in_minutes: int = 5,
    ) -> Optional[int]:
        """
        Poll status / state of an execution for a triggered rule. Polls until status is in a term
        state or timeout.

        :param job_execution_uuid: UUID for the job execution of a rule (custom SQL monitor).
        :param timeout_in_minutes: Polling timeout in minutes. Note that The Data Collector Lambda
                                   has a max timeout of 15 minutes when executing a query. Queries
                                   that take longer to execute are not supported, so we recommend
                                   filtering down the query output to improve performance (e.g limit
                                   WHERE clause). If you expect a query to take the full 15 minutes
                                   we recommend padding the timeout to 20 minutes.
        :return: Breach count across all executions. A greater than 0 value indicates a breach.
        :raise CircuitBreakerPipelineException: An error in executing the
                                                rule (e.g. error in query).
        :raise CircuitBreakerPollException: A timeout during polling or a malformed response.
        """
        return self.poll_all([job_execution_uuid], timeout_in_minutes=timeout_in_minutes)

    def poll_all(
        self,
        job_execution_uuids: Sequence[Union[str, UUID]],
        timeout_in_minutes: int = 5,
    ) -> int:
        """
        Poll status / state of executions for a triggered rule. Polls until status is in a term
        state or timeout.

        :param job_execution_uuids: UUIDs for the job executions of a rule (custom SQL monitor).
        :param timeout_in_minutes: Polling timeout in minutes. Note that The Data Collector Lambda
                                   has a max timeout of 15 minutes when executing a query. Queries
                                   that take longer to execute are not supported, so we recommend
                                   filtering down the query output to improve performance (e.g limit
                                   WHERE clause). If you expect a query to take the full 15 minutes
                                   we recommend padding the timeout to 20 minutes.
        :return: Breach count. A greater than 0 value indicates a breach.
        :raise CircuitBreakerPipelineException: An error in executing the
                                                rule (e.g. error in query).
        :raise CircuitBreakerPollException: A timeout during polling or a malformed response.
        """
        logs = cast(
            List[Box],
            self._poll(
                job_execution_uuids=job_execution_uuids,
                timeout_in_minutes=timeout_in_minutes,
            ),
        )

        if not logs:
            raise CircuitBreakerPollException

        self._print_func(
            "Completed polling. Retrieved execution with logs "
            f"{list(map(str, logs))} for IDs {job_execution_uuids}."
        )

        breaches = 0
        has_breaches = False
        if logs and len(logs) > 0:
            for log in logs:
                if log.payload.error:
                    logs_str = "\n".join(str(log) for log in logs)
                    raise CircuitBreakerPipelineException(
                        f"Execution pipeline errored out. Details:\n{logs_str}"
                    )
                if log.payload.breach_count is not None:
                    breaches += log.payload.breach_count
                    has_breaches = True

        if not has_breaches:
            raise CircuitBreakerPollException

        return breaches

    @boxify(use_snakes=True, default_box_attr=None, default_box=True)
    def _poll(
        self,
        job_execution_uuids: Sequence[Union[str, UUID]],
        timeout_in_minutes: int,
        sleep_interval_in_seconds: int = 15,
    ) -> Optional[List[Box]]:
        timeout_start = time.time()
        while time.time() < timeout_start + 60 * timeout_in_minutes:
            query = Query()
            query.get_circuit_breaker_rule_state_v2(
                job_execution_uuids=map(str, job_execution_uuids)
            ).__fields__("status", "log")
            circuit_rule_breaker_states = cast(
                List[CircuitBreakerState],
                self._client(query).get_circuit_breaker_rule_state_v2,
            )

            aggregated_status = self._get_aggregated_status(circuit_rule_breaker_states)
            self._print_func(
                f"Retrieved execution with aggregated status '{aggregated_status}' for "
                f"IDs {job_execution_uuids}."
            )

            if aggregated_status in self._TERM_STATES:
                return self._get_payloads(circuit_rule_breaker_states, aggregated_status)

            self._print_func(
                f"Aggregated state is not terminal state for IDs {job_execution_uuids}. "
                f"Polling again in '{sleep_interval_in_seconds}' seconds."
            )
            time.sleep(sleep_interval_in_seconds)

    def _get_log_payload(self, log: str):
        log_entries = json.loads(log)
        log_entries.reverse()
        for entry in log_entries:
            if "payload" in entry:
                return Box(entry, default_box_attr=None, default_box=True)
        return Box()

    def _get_payloads(
        self,
        states: List[CircuitBreakerState],
        status: SqlJobCheckpointStatus,
    ) -> List[Box]:
        payloads = []
        for state in states:
            if state.status == status:
                payloads.append(self._get_log_payload(str(state.log)))
        return payloads

    @staticmethod
    def _get_aggregated_status(states: List[CircuitBreakerState]) -> SqlJobCheckpointStatus:
        if not states:
            return SqlJobCheckpointStatus.REGISTERED  # type: ignore

        status_by_state = {}
        for state in states:
            status_by_state.setdefault(state.status, []).append(state)

        def all_in_state(s: SqlJobCheckpointStatus):
            return len(status_by_state.get(s, [])) == len(states)

        return (
            SqlJobCheckpointStatus.HAS_ERROR  # type: ignore
            if status_by_state.get(SqlJobCheckpointStatus.HAS_ERROR)  # type: ignore
            else SqlJobCheckpointStatus.PROCESSING_COMPLETE  # type: ignore
            if all_in_state(SqlJobCheckpointStatus.PROCESSING_COMPLETE)  # type: ignore
            else SqlJobCheckpointStatus.PROCESSING_START  # type: ignore
            if status_by_state.get(SqlJobCheckpointStatus.PROCESSING_COMPLETE)  # type: ignore
            or all_in_state(SqlJobCheckpointStatus.PROCESSING_START)  # type: ignore
            else SqlJobCheckpointStatus.EXECUTING_COMPLETE  # type: ignore
            if all_in_state(SqlJobCheckpointStatus.PROCESSING_COMPLETE)  # type: ignore
            else SqlJobCheckpointStatus.EXECUTING_START  # type: ignore
            if status_by_state.get(SqlJobCheckpointStatus.EXECUTING_COMPLETE)  # type: ignore
            or all_in_state(SqlJobCheckpointStatus.EXECUTING_START)  # type: ignore
            else SqlJobCheckpointStatus.REGISTERED  # type: ignore
        )
