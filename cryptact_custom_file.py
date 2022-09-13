from decimal import Decimal
from pydantic import BaseModel
from datetime import datetime
from typing import Literal, Optional
from caaj import Caaj
import decimal

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
    "withdraw": "RECOVER",
    "lose_bonds": "REDUCE",
    "get_bonds": "BONUS",

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

    def _detect_action_from_type(self, caaj: Caaj) -> str:
        """
        Caajからcryptact形式におけるActionを推定して返す
        """
        if caaj["caaj_to"] == "fee":
            return "SENDFEE"
        if caaj["type"] in TYPE_TO_ACTION:
            return TYPE_TO_ACTION[caaj["type"]]
        return "BUY"

    def _convert_base_from_uti(self, uti: str) -> str:
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
            "Action": self._detect_action_from_type(type=caaj),
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
        get_caaj = []
        lose_caaj = []
        deposit_caaj = []
        get_bonds_caaj = []
        withdraw_caaj = []
        lose_bonds_caaj = []
        for caaj in caaj_list:
            if caaj["type"] == "get":
                get_caaj.append(caaj)
            elif caaj["type"] == "lose":
                lose_caaj.append(caaj)
            elif caaj["type"] == "deposit":
                deposit_caaj.append(caaj)
            elif caaj["type"] == "get_bonds":
                get_bonds_caaj.append(caaj)
            elif caaj["type"] == "withdraw":
                withdraw_caaj.append(caaj)
            elif caaj["type"] == "lose_bonds":
                lose_bonds_caaj.append(caaj)
        if len(get_caaj) == 1 and len(lose_caaj) == 1:
            get_caaj = get_caaj[0]
            lose_caaj = lose_caaj[0]
            decimal.getcontext().prec = 10
            # https://support.cryptact.com/hc/ja/articles/360002571312-%E3%82%AB%E3%82%B9%E3%82%BF%E3%83%A0%E3%83%95%E3%82%A1%E3%82%A4%E3%83%AB%E3%81%AE%E4%BD%9C%E6%88%90%E6%96%B9%E6%B3%95-%E3%82%AB%E3%82%B9%E3%82%BF%E3%83%A0%E5%8F%96%E5%BC%95-#menu216
            cryptact_format: CryptactCustomFile = {
                "TimeStamp": get_caaj["executed_at"],
                "Action": "Buy",
                "Source": self._create_source_from_caaj(caaj=get_caaj),
                "Base": self._convert_base_from_uti(uti=get_caaj["uti"]),
                "Volume": get_caaj["amount"],
                "Price": (
                    decimal.Decimal(lose_caaj["amount"])
                    / decimal.Decimal(get_caaj["amount"])),
                "Counter": self._convert_base_from_uti(uti=lose_caaj["uti"]),
                "Fee": 0,
                "FeeCry": "JPY",
                "Comment": get_caaj["comment"]
            }
            self.cryptact_custom_files.append(cryptact_format)
        elif 
            

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
