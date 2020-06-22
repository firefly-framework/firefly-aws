from __future__ import annotations

import firefly as ff


class DataApi(ff.LoggerAware):
    _rds_data_client = None
    _db_arn: str = None
    _db_secret_arn: str = None
    _db_name: str = None

    def execute(self, sql: str, params: list = None):
        params = params or []
        self.debug('%s - %s', sql, str(params))
        return self._rds_data_client.execute_statement(
            resourceArn=self._db_arn,
            secretArn=self._db_secret_arn,
            database=self._db_name,
            sql=sql,
            parameters=params
        )
