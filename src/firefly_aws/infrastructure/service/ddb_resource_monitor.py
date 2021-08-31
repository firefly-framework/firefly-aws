from __future__ import annotations

import firefly_aws.domain as domain


class DdbResourceMonitor(domain.ResourceMonitor):
    _ddb_deserializer: domain.DdbDeserializer = None
    _ddb_client = None
    _ddb_table: str = None

    def record_execution(self, message: str, seconds: float, memory: float) -> dict:
        response = self._ddb_client.update_item(
            TableName=self._ddb_table,
            Key={
                'pk': {'S': message},
                'sk': {'S': 'resource-usage'}
            },
            UpdateExpression=f"""
                SET memory = list_append(if_not_exists(memory, :empty), :memory),
                    times = list_append(if_not_exists(times, :empty), :times)
            """,
            ExpressionAttributeValues={
                ':memory': {'L': [{'N': str(memory)}]},
                ':times': {'L': [{'N': str(seconds)}]},
                ':empty': {'L': []}
            },
            ReturnValues='ALL_NEW'
        )

        return self._ddb_deserializer.deserialize(response['Attributes'])

    def get_execution_metrics(self, message: str):
        response = self._ddb_client.get_item(
            TableName=self._ddb_table,
            Key={
                'pk': {'S': message},
                'sk': {'S': 'resource-usage'}
            }
        )

        return self._ddb_deserializer.deserialize(response['Item']) if 'Item' in response else None

    def set_memory_level(self, message: str, memory: int):
        self._ddb_client.update_item(
            TableName=self._ddb_table,
            Key={
                'pk': {'S': message},
                'sk': {'S': 'memory-level'}
            },
            UpdateExpression=f"""
                SET memory_level = :memory
            """,
            ExpressionAttributeValues={
                ':memory': {'N': str(memory)},
            },
        )

    def get_memory_level(self, message: str):
        response = self._ddb_client.get_item(
            TableName=self._ddb_table,
            Key={
                'pk': {'S': message},
                'sk': {'S': 'memory-level'}
            }
        )

        return self._ddb_deserializer.deserialize(response['Item'])['memory_level'] if 'Item' in response else None
