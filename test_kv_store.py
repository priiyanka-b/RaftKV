from kv_store import KVStore

def test_put_get():
    obj = KVStore()

    obj.apply({
        "op": "put",
        "key": "name",
        "value": "Priyanka"
    })

    result = obj.apply({
        "op": "get",
        "key": "name"
    })

    assert result == "Priyanka"
def test_delete():
    obj = KVStore()

    obj.apply({
        "op": "put",
        "key": "name",
        "value": "Priyanka"
    })

    obj.apply({
        "op": "delete",
        "key": "name"
    })

    result = obj.apply({
        "op": "get",
        "key": "name"
    })

    assert result == "Key Not Found"    