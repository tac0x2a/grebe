import yaml
from pathlib import Path


class MetaStoreYAML():
    def __init__(self, meta_store_file_path: Path, logger):
        self.meta_store_file_path = meta_store_file_path

        # Create dir if not exist
        parent = meta_store_file_path.parent
        if not parent.exists():
            parent.mkdir(parents=True)
            logger.info(f"Create meta_store directory {parent}")

        # Create local meta_store file
        if not meta_store_file_path.exists():
            with meta_store_file_path.open(mode='w') as file:
                yaml.dump({}, file)
                logger.info(f"Create empty meta_store file as {meta_store_file_path}")

    def load_all_meta_data(self):
        with open(self.meta_store_file_path, 'r') as file:
            return yaml.load(file)

    def store_source_meta_data(self, source_id, source_metadata):
        metadata = self.load_all_schemas()
        metadata.update({source_id: source_metadata})
        with open(self.meta_store_file_path, 'w') as file:
            yaml.dump(metadata, file)
