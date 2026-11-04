"""One-table storage behind a small interface.

`MemoryStore` is the fake used in tests and specs. `DynamoStore` is the real one,
tested against moto, never against AWS from this repository's tests.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

Item = Dict[str, Any]
Change = Callable[[Optional[Item]], Item]


class Store:
    """Keys are `pk` and `sk`. Items are plain dicts of strings, ints, bools, lists, dicts."""

    def put_if_absent(self, item: Item) -> bool:
        """Write `item` only if its key is free. Returns whether it wrote."""
        raise NotImplementedError

    def put(self, item: Item) -> None:
        raise NotImplementedError

    def get(self, pk: str, sk: str) -> Optional[Item]:
        raise NotImplementedError

    def delete(self, pk: str, sk: str) -> None:
        raise NotImplementedError

    def query(self, pk: str, sk_prefix: str = "") -> List[Item]:
        raise NotImplementedError

    def by_recipient(self, recipient_key: str) -> List[Item]:
        """Deliveries for one recipient, oldest first (GSI `byRecipient`)."""
        raise NotImplementedError

    def update(self, pk: str, sk: str, change: Change) -> Item:
        """Read, apply `change(current_or_None)`, write; retried if another writer won."""
        raise NotImplementedError


class MemoryStore(Store):
    def __init__(self) -> None:
        self.items: Dict[Any, Item] = {}

    @staticmethod
    def _copy(item: Item) -> Item:
        import copy

        return copy.deepcopy(item)

    def put_if_absent(self, item: Item) -> bool:
        key = (item["pk"], item["sk"])
        if key in self.items:
            return False
        self.items[key] = self._copy(item)
        return True

    def put(self, item: Item) -> None:
        self.items[(item["pk"], item["sk"])] = self._copy(item)

    def get(self, pk: str, sk: str) -> Optional[Item]:
        item = self.items.get((pk, sk))
        return self._copy(item) if item is not None else None

    def delete(self, pk: str, sk: str) -> None:
        self.items.pop((pk, sk), None)

    def query(self, pk: str, sk_prefix: str = "") -> List[Item]:
        keys = sorted(k for k in self.items if k[0] == pk and k[1].startswith(sk_prefix))
        return [self._copy(self.items[k]) for k in keys]

    def by_recipient(self, recipient_key: str) -> List[Item]:
        found = [i for i in self.items.values() if i.get("recipientKey") == recipient_key]
        return [self._copy(i) for i in sorted(found, key=lambda i: i.get("queuedAt", ""))]

    def update(self, pk: str, sk: str, change: Change) -> Item:
        item = change(self.get(pk, sk))
        item["pk"], item["sk"] = pk, sk
        self.put(item)
        return self._copy(item)


def _from_dynamo(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, list):
        return [_from_dynamo(v) for v in value]
    if isinstance(value, dict):
        return {k: _from_dynamo(v) for k, v in value.items()}
    return value


class DynamoStore(Store):
    """Items hold a `version` so `update` can detect a concurrent writer."""

    def __init__(self, table: Any):
        self._table = table

    def put_if_absent(self, item: Item) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._table.put_item(
                Item=item, ConditionExpression="attribute_not_exists(pk)"
            )
            return True
        except ClientError as error:
            if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def put(self, item: Item) -> None:
        self._table.put_item(Item=item)

    def get(self, pk: str, sk: str) -> Optional[Item]:
        found = self._table.get_item(Key={"pk": pk, "sk": sk}, ConsistentRead=True)
        item = found.get("Item")
        return _from_dynamo(item) if item else None

    def delete(self, pk: str, sk: str) -> None:
        self._table.delete_item(Key={"pk": pk, "sk": sk})

    def query(self, pk: str, sk_prefix: str = "") -> List[Item]:
        from boto3.dynamodb.conditions import Key

        condition = Key("pk").eq(pk)
        if sk_prefix:
            condition = condition & Key("sk").begins_with(sk_prefix)
        items: List[Item] = []
        kwargs: Dict[str, Any] = {"KeyConditionExpression": condition, "ConsistentRead": True}
        while True:
            page = self._table.query(**kwargs)
            items.extend(_from_dynamo(i) for i in page["Items"])
            if "LastEvaluatedKey" not in page:
                return items
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    def by_recipient(self, recipient_key: str) -> List[Item]:
        from boto3.dynamodb.conditions import Key

        items: List[Item] = []
        kwargs: Dict[str, Any] = {
            "IndexName": "byRecipient",
            "KeyConditionExpression": Key("recipientKey").eq(recipient_key),
        }
        while True:
            page = self._table.query(**kwargs)
            items.extend(_from_dynamo(i) for i in page["Items"])
            if "LastEvaluatedKey" not in page:
                return items
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    def update(self, pk: str, sk: str, change: Change) -> Item:
        from botocore.exceptions import ClientError

        while True:
            current = self.get(pk, sk)
            item = change(current)
            item["pk"], item["sk"] = pk, sk
            seen = current.get("version") if current else None
            item["version"] = (seen or 0) + 1
            try:
                if current is None:
                    self._table.put_item(
                        Item=item, ConditionExpression="attribute_not_exists(pk)"
                    )
                elif seen is None:
                    self._table.put_item(
                        Item=item, ConditionExpression="attribute_not_exists(version)"
                    )
                else:
                    self._table.put_item(
                        Item=item,
                        ConditionExpression="version = :seen",
                        ExpressionAttributeValues={":seen": seen},
                    )
                return item
            except ClientError as error:
                if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise
