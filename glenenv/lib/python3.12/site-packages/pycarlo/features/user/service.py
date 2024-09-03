from typing import Optional, Union, cast
from uuid import UUID

from pycarlo.core import Client, Query
from pycarlo.features.user.exceptions import (
    MultipleResourcesFoundException,
    ResourceNotFoundException,
)
from pycarlo.features.user.models import Resource
from pycarlo.features.user.queries import GET_USER_WAREHOUSES


class UserService:
    def __init__(self, mc_client: Optional[Client] = None):
        self._mc_client = mc_client or Client()

    def get_resource(self, resource_id: Optional[Union[str, UUID]] = None) -> Resource:
        """
        Get a resource (e.g. lake or warehouse).

        :param resource_id: resource identifier. If not provided, and your account only has one
                            resource, it will be returned. If your account has multiple resources
                            an exception will be raised indicating a a resource id must be provided.

        :return: resource (e.g. lake or warehouse monitored by Monte Carlo)
        :raise MultipleResourcesFoundException: multiple resources
                                                exist (a `resource_id` must be provided)
        :raise ResourceNotFoundException: a resource could not be found
        """
        response = cast(Query, self._mc_client(query=GET_USER_WAREHOUSES))
        warehouses = response.get_user.account.warehouses

        # if resource id was provided, look for matching warehouse record
        if resource_id:
            for w in warehouses:
                if w.uuid == str(resource_id):
                    return Resource(
                        id=UUID(w.uuid),  # type: ignore
                        name=w.name,  # type: ignore[reportArgumentType]
                        type=w.connection_type,  # type: ignore[reportArgumentType]
                    )

            # resource not found
            raise ResourceNotFoundException(f"Resource not found with id={resource_id}")

        # if only one warehouse exists, return it
        if len(warehouses) == 1:
            return Resource(
                id=UUID(warehouses[0].uuid),  # type: ignore
                name=warehouses[0].name,  # type: ignore[reportArgumentType]
                type=warehouses[0].connection_type,  # type: ignore[reportArgumentType]
            )
        # otherwise, raise an error requesting a resource id
        else:
            raise MultipleResourcesFoundException(
                "Multiple resources found, please specify a resource id"
            )
