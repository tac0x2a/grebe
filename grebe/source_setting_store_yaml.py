import yaml
from pathlib import Path


class SourceSettingStoreYAML():
    def __init__(self, ss_file_path: Path, logger):
        self.ss_file_path = ss_file_path

        # Create dir if not exist
        parent = ss_file_path.parent
        if not parent.exists():
            parent.mkdir(parents=True)
            logger.info(f"Create source_settings directory {parent}")

        # Create local source_setting store file
        if not ss_file_path.exists():
            with ss_file_path.open(mode='w') as file:
                yaml.dump({}, file)
                logger.info(f"Create empty source_settings file as {ss_file_path}")

    def load_all_source_settings(self):
        with open(self.ss_file_path, 'r') as file:
            return yaml.load(file)

    def store_source_setting(self, source_id, source_setting):
        source_settings = self.load_all_schemas()
        source_settings.update({source_id: source_setting})
        with open(self.ss_file_path, 'w') as file:
            yaml.dump(source_settings, file)
