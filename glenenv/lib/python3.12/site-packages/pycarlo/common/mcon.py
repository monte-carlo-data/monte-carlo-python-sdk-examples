from dataclasses import dataclass
from typing import Optional
from uuid import UUID


@dataclass
class ParsedMCON:
    account_id: UUID
    resource_id: UUID
    object_type: str
    object_id: str


class MCONParser:
    @staticmethod
    def parse_mcon(mcon: Optional[str]) -> Optional[ParsedMCON]:
        if not mcon or not mcon.startswith("MCON++"):
            return None

        mcon_parts = mcon.split("++", 4)
        return ParsedMCON(
            account_id=UUID(mcon_parts[1]),
            resource_id=UUID(mcon_parts[2]),
            object_type=mcon_parts[3],
            object_id=mcon_parts[4],
        )
