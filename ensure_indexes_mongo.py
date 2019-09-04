import json
import numbers
from src.common import WORKING_DIR
from src.models.mongo.db_manager import DBManager

if __name__ == '__main__':
    indexes_file = WORKING_DIR + "indexes.json"
    with open(indexes_file, 'r') as f:
        indexes_data = json.load(f)
    client_mongo = DBManager.get_instance().db
    lst_index = []
    for x in indexes_data:
        is_text_index = False
        lst_fields = []
        weights = None
        name = x.get('name')
        table = x.get('table')
        unique = x.get('unique') or False
        background = x.get('background') or True
        if not table:
            raise Exception("table: {} is null or empty !".format(table))
        if not name:
            raise Exception("index name: {} is null or empty !".format(name))
        for field in x.get('fields'):
            if isinstance(field.get('index_type'), numbers.Number) or field.get('index_type') == 'text':
                lst_fields.append((field.get('field_name'), field.get('index_type')))
                is_text_index = True if field.get('index_type') == 'text' else False
            else:
                raise Exception("index type: {} is not valid !".format(field.get('index_type')))
        if is_text_index:
            if 'weights' in x:
                weights = {}
                for key, value in x.get('weights').items():
                    if key not in [x[0] for x in lst_fields]:
                        raise Exception("weight key: {} is not registered !".format(key))
                    elif not isinstance(value, numbers.Number):
                        raise Exception("weight value: {} is not instance of number !".format(value))
                    weights[key] = value
        lst_index.append(
            {
                "table": table,
                "name": name,
                "fields": lst_fields,
                "unique": unique,
                "background": background,
                "weights": weights
            }
        )
    for mongo_index in lst_index:
        result = client_mongo.get_database("profiling").get_collection(mongo_index.get('table')).create_index(mongo_index.get('fields'), name=mongo_index.get('name'), unique=mongo_index.get('unique'), background=mongo_index.get('background'), weights=mongo_index.get('weights'))
        print('result: {}'.format(result))
