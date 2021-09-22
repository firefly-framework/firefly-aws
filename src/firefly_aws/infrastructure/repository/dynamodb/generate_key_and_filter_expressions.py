from __future__ import annotations

from typing import Tuple, Optional

import firefly as ff


class GenerateKeyAndFilterExpressions(ff.Dependency):
    def __call__(self, criteria: ff.BinaryOp, indexes: dict) -> Tuple[str, dict, str, dict]:
        single_expression, single_bindings = self._find_first_expression(
            ff.BinaryOp.from_dict(criteria.to_dict()), indexes
        )
        filter_expression, filter_bindings = criteria.to_sql()

        return single_expression, single_bindings, filter_expression, filter_bindings

    def _find_first_expression(self, criteria: ff.BinaryOp, indexes: dict) -> Tuple[Optional[str], Optional[dict]]:
        if criteria.op not in ('and', 'or'):
            if isinstance(criteria.lhv, (ff.Attr, ff.AttributeString)):
                index = self._get_index(str(criteria.lhv), indexes)
                if index is not None:
                    criteria.lhv = ff.Attr(f'gsi_{index}')
                    return criteria.to_sql()

            if isinstance(criteria.rhv, (ff.Attr, ff.AttributeString)):
                index = self._get_index(str(criteria.rhv), indexes)
                if index is not None:
                    criteria.rhv = ff.Attr(f'gsi_{index}')
                    return criteria.to_sql()

        if isinstance(criteria.lhv, ff.BinaryOp):
            expression, bindings = self._find_first_expression(criteria.lhv, indexes)
            if expression is not None:
                return expression, bindings

        if isinstance(criteria.rhv, ff.BinaryOp):
            expression, bindings = self._find_first_expression(criteria.rhv, indexes)
            if expression is not None:
                return expression, bindings

        return None, None

    def _get_index(self, attribute: str, indexes: dict) -> Optional[str]:
        for index, fields in indexes.items():
            for field in fields:
                if field['field'].lower() == attribute.lower():
                    return field['index']
        return None
