from dataclasses import dataclass
from uuid import UUID


@dataclass
class Resource:
    id: UUID
    name: str
    type: str
