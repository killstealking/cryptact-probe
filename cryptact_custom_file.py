import decimal
import urllib
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, Optional, Union

import pandas as pd
from pydantic import BaseModel, validator

from caaj import Caaj

urllib: Any
pd: Any
Fields = Literal[
    "Timestamp",
    "Action",
    "Source",
    "Base",
    "Volume",
    "Price",
    "Counter",
    "Fee",
    "FeeCcyComment",
]

TYPE_TO_ACTION = {
    "borrow": "BORROW",
    "repay": "RETURN",
    "deposit": "LEND",
    "withdraw": "RECOVER",
    "lose_bonds": "REDUCE",
    "get_bonds": "BONUS",
    "receive": "BONUS",
}


class CryptactCustomFile(BaseModel):
    Timestamp: str
    Action: str
    Source: str
    Base: str
    Volume: Decimal
    Price: Optional[Decimal]
    Counter: str
    Fee: Decimal
    FeeCcy: str
    Comment: Optional[str]

    def __getitem__(self, item: Fields):
        return getattr(self, item)

    @validator("Timestamp", pre=True)
    def parse_timestamp(cls, value: datetime):
        return datetime.strftime(value, "%Y/%m/%d %H:%M:%S")


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
            service=caaj["service"],
        )

    def _detect_action_from_type(self, caaj: Caaj) -> str:
        """
        Caajからcryptact形式におけるActionを推定して返す
        """
        if caaj["caaj_to"] == "fee":
            return "SENDFEE"
        if caaj["type"] in TYPE_TO_ACTION:
            return TYPE_TO_ACTION[caaj["type"]]
        else:
            return ""

    def _convert_base_from_uti(self, uti: str) -> str:
        """
        caajのutiからcryptact形式におけるBaseを推定して返す
        """
        base: str = urllib.parse.unquote(uti).upper()
        # 真面目にやる場合はCryptactの対応トークンの取得に苦労しそう
        return base

    def _resolve_single_caaj(self, caaj: Caaj) -> None:
        """
        単独で完結しているcaajをcryptact形式に変更する
        """
        cryptact_format: dict[str, Union[str, Decimal, None]] = {
            "Timestamp": caaj["executed_at"],
            "Action": self._detect_action_from_type(caaj=caaj),
            "Source": self._create_source_from_caaj(caaj=caaj),
            "Base": self._convert_base_from_uti(uti=caaj["uti"]),
            "Volume": caaj["amount"],
            "Price": None,
            "Counter": "JPY",
            "Fee": Decimal(0),
            "FeeCcy": "JPY",
            "Comment": caaj["comment"],
        }
        if cryptact_format["Action"] == "":
            return None
        self.cryptact_custom_files.append(CryptactCustomFile.parse_obj(cryptact_format))

    def _resolve_multi_caaj(self, caaj_list: list[Caaj]) -> None:
        """
        複数からなるcaajをcryptact形式にする
        """
        get_caajs: list[Caaj] = []
        lose_caajs: list[Caaj] = []
        deposit_caaj: list[Caaj] = []
        get_bonds_caaj: list[Caaj] = []
        withdraw_caaj: list[Caaj] = []
        lose_bonds_caaj: list[Caaj] = []
        for caaj in caaj_list:
            if caaj["type"] == "get":
                get_caajs.append(caaj)
            elif caaj["type"] == "lose":
                lose_caajs.append(caaj)
            elif caaj["type"] == "deposit":
                deposit_caaj.append(caaj)
            elif caaj["type"] == "get_bonds":
                get_bonds_caaj.append(caaj)
            elif caaj["type"] == "withdraw":
                withdraw_caaj.append(caaj)
            elif caaj["type"] == "lose_bonds":
                lose_bonds_caaj.append(caaj)
        if len(get_caajs) == 1 and len(lose_caajs) == 1:
            get_caaj: Caaj = get_caajs[0]
            lose_caaj: Caaj = lose_caajs[0]
            decimal.getcontext().prec = 10
            # https://support.cryptact.com/hc/ja/articles/360002571312-%E3%82%AB%E3%82%B9%E3%82%BF%E3%83%A0%E3%83%95%E3%82%A1%E3%82%A4%E3%83%AB%E3%81%AE%E4%BD%9C%E6%88%90%E6%96%B9%E6%B3%95-%E3%82%AB%E3%82%B9%E3%82%BF%E3%83%A0%E5%8F%96%E5%BC%95-#menu216
            cryptact_format: dict[str, Union[str, Decimal, None]] = {
                "Timestamp": get_caaj["executed_at"],
                "Action": "BUY",
                "Source": self._create_source_from_caaj(caaj=get_caaj),
                "Base": self._convert_base_from_uti(uti=get_caaj["uti"]),
                "Volume": get_caaj["amount"],
                "Price": (
                    decimal.Decimal(lose_caaj["amount"])
                    / decimal.Decimal(get_caaj["amount"])
                ),
                "Counter": self._convert_base_from_uti(uti=lose_caaj["uti"]),
                "Fee": Decimal(0),
                "FeeCcy": "JPY",
                "Comment": get_caaj["comment"],
            }
            self.cryptact_custom_files.append(
                CryptactCustomFile.parse_obj(cryptact_format)
            )
        else:
            for caaj in caaj_list:
                self._resolve_single_caaj(caaj=caaj)

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

    def get_cryptact_custom_files(self) -> list[CryptactCustomFile]:
        """
        Cryptactの形式になったものを返す
        """
        return self.cryptact_custom_files

    def export_cryptact_custom_files(self, file_path: Union[str, None] = None) -> None:
        """
        指定のファイルパスにcsvで吐き出す。\n
        指定がなければ./custom.csvで吐き出す
        """
        df = pd.DataFrame([s.__dict__ for s in self.cryptact_custom_files]).drop(
            columns="Comment"
        )
        df.to_csv("custom.csv" if file_path is None else file_path, index=False)
