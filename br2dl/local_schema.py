import os
import yaml


class LocalSchema():
    def __init__(self, schema_file):
        self.schema_file = schema_file

        # Create local schema dir
        schema_dir = os.path.dirname(schema_file)
        if not os.path.exists(schema_dir):
            os.makedirs(schema_dir)

        # Create local schema file
        if not os.path.exists(schema_file):
            with open(schema_file, 'w') as db:
                yaml.dump({}, db)

    def load_all_schemas(self):
        with open(self.schema_file, 'r') as db:
            local_schemas = yaml.load(db)
        return local_schemas

    def store_all_schemas(self, schemas):
        with open(self.schema_file, 'w') as db:
            yaml.dump(schemas, db)
