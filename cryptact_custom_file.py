from decimal import Decimal
from platform import platform
from pydantic import BaseModel
from datetime import datetime
from typing import Literal, Optional
from caaj import Caaj

Fields = Literal[
    "TimeStamp",
    "Action",
    "Source",
    "Base",
    "Volume",
    "Price",
    "Counter",
    "Fee",
    "FeeCry"
    "Comment"
]

TYPE_TO_ACTION = {
    "borrow": "BORROW",
    "repay": "RETURN",
    "deposit": "LEND",
    "withdraw": "RECOVER"
}

class CryptactCustomFile(BaseModel):
    TimeStamp: datetime
    Action: str
    Source: str
    Base: str
    Volume: Decimal
    Price: Optional[Decimal]
    Counter: str
    Fee: Decimal
    FeeCry: str
    Comment: Optional[str]


class CryptactRepository:
    def __init__(self, grouped_caaj: dict[str, list[Caaj]]) -> None:
        self.cryptact_custom_files: list[CryptactCustomFile] = []
        self.grouped_caaj = grouped_caaj

    def _create_source_from_caaj(self, caaj: Caaj) -> str:
        """
        CryptactのSourceとしてplatform, application, serviceをまとめたものを使う
        """
        return "{platform}/{application}/{service}".format(
            platform=caaj["platform"],
            application=caaj["application"],
            service=caaj["application"]
        )

    def _detect_action_from_type(self, type: str) -> str:
        """
        Caajからcryptact形式におけるActionを推定して返す
        """
        if type in TYPE_TO_ACTION:
            return TYPE_TO_ACTION[type]
        return "BUY"

    def _convert_base_from_uti(self, uti:str) -> str:
        """
        caajのutiからcryptact形式におけるBaseを推定して返す
        """
        base = uti.upper()
        # 真面目にやる場合はCryptactの対応トークンの取得に苦労しそう
        return base

    def _resolve_single_caaj(self, caaj: Caaj) -> None:
        """
        単独で完結しているcaajをcryptact形式に変更する
        """
        cryptact_format: CryptactCustomFile = {
            "TimeStamp": caaj["executed_at"],
            "Action": self._detect_action_from_type(type=caaj["type"]),
            "Source": self._create_source_from_caaj(caaj=caaj),
            "Base": self._convert_base_from_uti(uti=caaj["uti"]),
            "Volume": caaj["amount"],
            "Price": None,
            "Counter": "JPY",
            "Fee": 0,
            "FeeCry": "JPY",
            "Comment": caaj["comment"]
            }
        self.cryptact_custom_files.append(cryptact_format)

    def _resolve_multi_caaj(self, caaj_list: list[Caaj]) -> None:
        """
        複数からなるcaajをcryptact形式にする
        """
        return None

    def create_cryptact_custom_files(self) -> None:
        """
        Cryptact形式に変換する
        """
        grouped_caaj = self.grouped_caaj
        for transactions in grouped_caaj.values():
            if len(transactions) == 1:
                self._resolve_single_caaj(caaj=transactions[0])
            else:
                self._resolve_multi_caaj(caaj_list=transactions)
