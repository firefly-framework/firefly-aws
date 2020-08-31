from __future__ import annotations

from typing import Type

import firefly as ff
from firefly import domain as ffd

from ..data_api_storage_interface import DataApiStorageInterface


class DataApiPgStorageInterface(DataApiStorageInterface):
    def _get_table_indexes(self, entity: Type[ffd.Entity]):
        pass

    def _add_table_index(self, entity: Type[ffd.Entity], field_):
        pass

    def _drop_table_index(self, entity: Type[ffd.Entity], name: str):
        pass

    def _get_average_row_size(self, entity: Type[ff.Entity]):
        pass

    def _generate_update_list(self, entity: Type[ffd.Entity]):
        pass

    def _generate_column_list(self, entity: Type[ffd.Entity]):
        pass

    def _generate_select_list(self, entity: Type[ffd.Entity]):
        pass

    def _generate_value_list(self, entity: Type[ffd.Entity]):
        pass

    def _generate_parameters(self, entity: ffd.Entity, part: str = None):
        pass

    def _build_entity(self, entity: Type[ffd.Entity], data, raw: bool = False):
        pass

    def _generate_create_table(self, entity: Type[ffd.Entity]):
        pass

    def raw(self, entity: Type[ffd.Entity], criteria: ffd.BinaryOp = None, limit: int = None):
        pass
