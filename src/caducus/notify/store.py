"""One-table storage behind a small interface.

`MemoryStore` is the fake used in tests and specs. `DynamoStore` is the real one,
tested against moto, never against AWS from this repository's tests.
"""

from __future__ import annotations

from decimal import Decimal
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

Item = Dict[str, Any]
Change = Callable[[Optional[Item]], Item]
Page = Tuple[List[Item], Optional[str]]

# DynamoDB's own limit on the actions in one transaction.
TRANSACTION_LIMIT = 100


@dataclass(frozen=True)
class Write:
    """One action in `Store.transact`: put `item`, or delete the key `(pk, sk)`.

    `expect_version` makes a put conditional on the stored item's `version`;
    `absent` makes it conditional on no item being there. A conditional put
    writes `version` = `expect_version + 1` (or 1 when absent).
    """

    item: Optional[Item] = None
    delete: Optional[Tuple[str, str]] = None
    expect_version: Optional[int] = None
    absent: bool = False


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

    def increment(
        self, pk: str, sk: str, deltas: Dict[str, int], expires_at: Optional[int] = None
    ) -> Item:
        """Atomically add `deltas` to number attributes (missing ones start at 0); returns the new item."""
        raise NotImplementedError

    def transact(self, writes: Sequence[Write]) -> bool:
        """Apply every write or none. False when a condition failed (nothing written)."""
        raise NotImplementedError

    def index_page(self, index: str, attribute: str, value: str, cursor: Optional[str]) -> Page:
        """One page of a keys-only index by its partition attribute, and the cursor of the next page."""
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

    def increment(self, pk, sk, deltas, expires_at=None):  # type: ignore[no-untyped-def]
        item = self.items.setdefault((pk, sk), {"pk": pk, "sk": sk})
        for name, delta in deltas.items():
            item[name] = item.get(name, 0) + delta
        if expires_at is not None and "expiresAt" not in item:
            item["expiresAt"] = expires_at
        return self._copy(item)

    def transact(self, writes: Sequence[Write]) -> bool:
        if len(writes) > TRANSACTION_LIMIT:
            raise ValueError("a transaction holds at most %d writes" % TRANSACTION_LIMIT)
        for write in writes:
            if write.item is None:
                continue
            current = self.items.get((write.item["pk"], write.item["sk"]))
            if write.absent and current is not None:
                return False
            if write.expect_version is not None and (
                current is None or current.get("version") != write.expect_version
            ):
                return False
        for write in writes:
            if write.delete is not None:
                self.items.pop(write.delete, None)
                continue
            item = self._copy(write.item)
            if write.absent:
                item["version"] = 1
            elif write.expect_version is not None:
                item["version"] = write.expect_version + 1
            self.items[(item["pk"], item["sk"])] = item
        return True

    page_size = 0  # 0: one page holds everything, like a small DynamoDB page

    def index_page(self, index, attribute, value, cursor):  # type: ignore[no-untyped-def]
        found = sorted(
            (k for k, i in self.items.items() if i.get(attribute) == value),
        )
        start = int(cursor) if cursor else 0
        end = start + self.page_size if self.page_size else len(found)
        page = [self._copy(self.items[k]) for k in found[start:end]]
        return page, (str(end) if end < len(found) else None)


def _from_dynamo(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, list):
        return [_from_dynamo(v) for v in value]
    if isinstance(value, dict):
        return {k: _from_dynamo(v) for k, v in value.items()}
    return value


def _to_dynamo(value: Any) -> Any:
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_to_dynamo(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_dynamo(v) for k, v in value.items()}
    return value


class DynamoStore(Store):
    """Items hold a `version` so `update` can detect a concurrent writer."""

    def __init__(self, table: Any):
        self._table = table

    def increment(self, pk, sk, deltas, expires_at=None):  # type: ignore[no-untyped-def]
        names = {"#a%d" % n: name for n, name in enumerate(deltas)}
        values: Dict[str, Any] = {
            ":a%d" % n: Decimal(delta) for n, delta in enumerate(deltas.values())
        }
        expression = "ADD " + ", ".join("#a%d :a%d" % (n, n) for n in range(len(deltas)))
        if expires_at is not None:
            names["#ttl"] = "expiresAt"
            values[":ttl"] = Decimal(expires_at)
            expression += " SET #ttl = if_not_exists(#ttl, :ttl)"
        found = self._table.update_item(
            Key={"pk": pk, "sk": sk},
            UpdateExpression=expression,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
            ReturnValues="ALL_NEW",
        )
        return _from_dynamo(found["Attributes"])

    def transact(self, writes: Sequence[Write]) -> bool:
        from botocore.exceptions import ClientError

        if len(writes) > TRANSACTION_LIMIT:
            raise ValueError("a transaction holds at most %d writes" % TRANSACTION_LIMIT)
        name = self._table.name

        def wire(item: Item) -> Dict[str, Any]:
            # The resource's client serializes plain values itself, as it does for put_item.
            return {k: _to_dynamo(v) for k, v in item.items()}

        actions: List[Dict[str, Any]] = []
        for write in writes:
            if write.delete is not None:
                pk, sk = write.delete
                actions.append({"Delete": {"TableName": name, "Key": wire({"pk": pk, "sk": sk})}})
                continue
            item = dict(write.item or {})
            put: Dict[str, Any] = {"TableName": name}
            if write.absent:
                item["version"] = 1
                put["ConditionExpression"] = "attribute_not_exists(pk)"
            elif write.expect_version is not None:
                item["version"] = write.expect_version + 1
                put["ConditionExpression"] = "version = :seen"
                put["ExpressionAttributeValues"] = {":seen": Decimal(write.expect_version)}
            put["Item"] = wire(item)
            actions.append({"Put": put})
        try:
            self._table.meta.client.transact_write_items(TransactItems=actions)
            return True
        except ClientError as error:
            if error.response["Error"]["Code"] == "TransactionCanceledException":
                reasons = error.response.get("CancellationReasons") or []
                if any(r.get("Code") == "ConditionalCheckFailed" for r in reasons):
                    return False
            raise

    def index_page(self, index, attribute, value, cursor):  # type: ignore[no-untyped-def]
        import json

        from boto3.dynamodb.conditions import Key

        kwargs: Dict[str, Any] = {
            "IndexName": index,
            "KeyConditionExpression": Key(attribute).eq(value),
        }
        if cursor:
            kwargs["ExclusiveStartKey"] = json.loads(cursor)
        page = self._table.query(**kwargs)
        items = [_from_dynamo(i) for i in page["Items"]]
        last = page.get("LastEvaluatedKey")
        return items, (json.dumps(_from_dynamo(last), sort_keys=True) if last else None)

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
