import json
import numbers
import os
from src.models.mongo.db_manager import DBManager
from pymongo import ASCENDING, DESCENDING

if __name__ == "__main__":
    indexes_file = "indexes.json"
    with open(indexes_file, "r") as f:
        indexes_data = json.load(f)
    client_mongo = DBManager.get_instance().db
    lst_index = []
    for x in indexes_data:
        is_text_index = False
        lst_fields = []
        weights = None
        name = x.get("name")
        table = x.get("table")
        unique = x.get("unique") or False
        ttl = x.get("ttl")
        background = x.get("background") or True
        partial = x.get("partial")
        if not table:
            raise Exception("table: {} is null or empty !".format(table))
        if not name:
            raise Exception("index name: {} is null or empty !".format(name))
        for field in x.get("fields"):
            if (
                isinstance(field.get("index_type"), numbers.Number)
                and field.get("index_type") in [ASCENDING, DESCENDING]
            ) or field.get("index_type") == "text":
                lst_fields.append((field.get("field_name"), field.get("index_type")))
                if field.get("index_type") == "text":
                    is_text_index = True
            else:
                raise Exception(
                    "index type: {} is not valid !".format(field.get("index_type"))
                )
        if is_text_index:
            if "weights" in x:
                weights = {}
                for key, value in x.get("weights").items():
                    if key not in [x[0] for x in lst_fields]:
                        raise Exception(
                            "weight key: {} is not registered !".format(key)
                        )
                    elif not isinstance(value, numbers.Number):
                        raise Exception(
                            "weight value: {} is not instance of number !".format(value)
                        )
                    weights[key] = value
        index_value = {
            "table": table,
            "name": name,
            "fields": lst_fields,
            "unique": unique,
            "background": background,
            "partial": partial,
        }
        if ttl:
            index_value["expireAfterSeconds"] = ttl
        if weights:
            index_value["weights"] = weights
        if is_text_index:
            print("index_value: {}".format(index_value))
        lst_index.append(index_value)
    for mongo_index in lst_index:
        print("\n====================================================================")
        try:
            key_index = {"name": mongo_index.get("name")}
            if mongo_index.get("unique"):
                key_index["unique"] = mongo_index.get("unique")
            if mongo_index.get("background"):
                key_index["background"] = mongo_index.get("background")
            if mongo_index.get("weights"):
                key_index["weights"] = mongo_index.get("weights")
            if mongo_index.get("expireAfterSeconds"):
                key_index["expireAfterSeconds"] = mongo_index.get("expireAfterSeconds")
            if mongo_index.get("partial"):
                key_index["partialFilterExpression"] = mongo_index.get("partial")
            result = (
                client_mongo.get_database(os.getenv("PROFILING_MONGODB_DB_NAME"))
                .get_collection(mongo_index.get("table"))
                .create_index(mongo_index.get("fields"), **key_index)
            )
            print(
                "SUCCESS:: table: {} create index with field: {} result: {}".format(
                    mongo_index.get("table"), mongo_index.get("fields"), result
                )
            )
        except Exception as ex:
            print("ERROR:: row: {} create index error: {}".format(mongo_index, ex))
        print("====================================================================\n")
