from typing import Union

from caaj import CaajRepository
from cryptact_custom_file import CryptactRepository


def create_cryptact_custom_file(path: Union[str, None] = None):
    if path is None:
        path = "result.csv"
    caaj_repo = CaajRepository(file_path=path)
    caaj_list = caaj_repo.get_grouped_caaj()
    cryptact_repo = CryptactRepository(grouped_caaj=caaj_list)
    cryptact_repo.create_cryptact_custom_files()
    result = cryptact_repo.get_cryptact_custom_files()
    print(result)
    cryptact_repo.export_cryptact_custom_files()


if __name__ == "__main__":
    create_cryptact_custom_file()
