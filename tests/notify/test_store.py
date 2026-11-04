from __future__ import annotations

import pytest

from caducus.notify.store import DynamoStore, MemoryStore


@pytest.fixture(params=["memory", "dynamo"])
def store(request):
    if request.param == "memory":
        yield MemoryStore()
        return
    moto = pytest.importorskip("moto")
    import boto3

    with moto.mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName="t",
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "recipientKey", "AttributeType": "S"},
                {"AttributeName": "queuedAt", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "byRecipient",
                    "KeySchema": [
                        {"AttributeName": "recipientKey", "KeyType": "HASH"},
                        {"AttributeName": "queuedAt", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
        yield DynamoStore(boto3.resource("dynamodb", region_name="us-east-1").Table("t"))


def test_put_if_absent_writes_once(store):
    assert store.put_if_absent({"pk": "a", "sk": "1", "n": 1})
    assert not store.put_if_absent({"pk": "a", "sk": "1", "n": 2})
    assert store.get("a", "1")["n"] == 1


def test_get_put_delete_and_numbers_come_back_as_ints(store):
    assert store.get("a", "x") is None
    store.put({"pk": "a", "sk": "x", "count": 3, "items": [{"k": 1}]})
    assert store.get("a", "x")["count"] == 3
    assert store.get("a", "x")["items"] == [{"k": 1}]
    store.delete("a", "x")
    assert store.get("a", "x") is None


def test_query_by_prefix(store):
    for sk in ("DELIVERY#2", "DELIVERY#1", "CONTENT#1"):
        store.put({"pk": "EVENT#e", "sk": sk})
    store.put({"pk": "EVENT#other", "sk": "DELIVERY#9"})
    assert [i["sk"] for i in store.query("EVENT#e", "DELIVERY#")] == [
        "DELIVERY#1", "DELIVERY#2",
    ]
    assert len(store.query("EVENT#e")) == 3


def test_by_recipient(store):
    store.put({"pk": "EVENT#1", "sk": "DELIVERY#a", "recipientKey": "r", "queuedAt": "2026-01-02"})
    store.put({"pk": "EVENT#2", "sk": "DELIVERY#b", "recipientKey": "r", "queuedAt": "2026-01-01"})
    store.put({"pk": "EVENT#3", "sk": "DELIVERY#c", "recipientKey": "x", "queuedAt": "2026-01-01"})
    assert [i["sk"] for i in store.by_recipient("r")] == ["DELIVERY#b", "DELIVERY#a"]


def test_update_creates_then_changes(store):
    store.update("p", "s", lambda row: {"n": 1} if row is None else {"n": row["n"] + 1})
    store.update("p", "s", lambda row: {"n": row["n"] + 1})
    assert store.get("p", "s")["n"] == 2


def test_update_retries_when_another_writer_wins():
    pytest.importorskip("moto")
    moto = __import__("moto")
    import boto3

    with moto.mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName="t", BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
        )
        store = DynamoStore(boto3.resource("dynamodb", region_name="us-east-1").Table("t"))
        store.update("p", "s", lambda row: {"n": 0})
        calls = []

        def change(row):
            calls.append(row["n"])
            if len(calls) == 1:  # a rival writes between our read and our write
                store.update("p", "s", lambda r: {"n": r["n"] + 10})
            return {"n": row["n"] + 1}

        store.update("p", "s", change)
        assert store.get("p", "s")["n"] == 11 and calls == [0, 10]
