from typing import Any

from sgqlc.operation import Operation

from pycarlo.lib import schema


class Query(Operation):
    def __init__(self, *args: Any, **kwargs: Any):
        """
        An MCD Query operation.

        Supports all Operation params and functionality.
        """
        super().__init__(typ=schema.Query, *args, **kwargs)


class Mutation(Operation):
    def __init__(self, *args: Any, **kwargs: Any):
        """
        An MCD Mutation operation.

        Supports all Operation params and functionality.
        """
        super().__init__(typ=schema.Mutation, *args, **kwargs)
