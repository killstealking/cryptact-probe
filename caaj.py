from decimal import Decimal
from pydantic import BaseModel
from datetime import datetime
from typing import Any, Literal, Optional
import csv

Fields = Literal[
    "executed_at",
    "platform",
    "application",
    "service",
    "transaction_id",
    "trade_uuid",
    "type",
    "amount",
    "uti",
    "caaj_from",
    "caaj_to",
    "comment"
]

class Caaj(BaseModel):
    executed_at: datetime
    platform: str
    application: str
    service:str
    transaction_id: str
    trade_uuid: Optional[str]
    type: str
    amount: Decimal
    uti: str
    caaj_from: Optional[str]
    caaj_to: Optional[str]
    comment: Optional[str]

    def __getitem__(self, item: Fields):
        return getattr(self, item)


class CaajRepository:
    def __init__(self) -> None:
        caaj_list: list[Caaj] = []
        with open('result.csv') as f:
            for row in csv.DictReader(f, skipinitialspace=True):
                caaj_list.append(Caaj.parse_obj({k:v for k, v in row.items()}))
        self.grouped_caaj: dict[str, list[Caaj]] = self._group_by_transaction_uuid(caaj_list=caaj_list)

    def _group_by_transaction_uuid(self, caaj_list: list[Caaj]) -> dict[str, list[Caaj]]:
        grouped_caaj_dict: dict[str, list[caaj]] = {}
        for caaj in caaj_list:
            if caaj["trade_uuid"] in grouped_caaj_dict:
                grouped_caaj_dict[caaj["trade_uuid"]].append(caaj)
            else:
                grouped_caaj_dict[caaj["trade_uuid"]] = [caaj]
        return grouped_caaj_dict

    def get_grouped_caaj(self) -> dict[str, list[Caaj]]:
        return self.grouped_caaj