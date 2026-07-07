"""
Module: config_loader.py

Purpose:
Loads project configuration from YAML files.
"""

import yaml

from framework.models.source_config import SourceConfig


class ConfigLoader:

    def __init__(self, config_path: str):

        self.config_path = config_path

        self.config = self._load_yaml()

    
    def _load_yaml(self) -> dict:
        """
        Load YAML configuration.
        """

        try:
            with open(self.config_path, "r", encoding="utf-8") as file:
                return yaml.safe_load(file)
            
        except FileNotFoundError as ex:
            raise FileNotFoundError(
                f"Configuration file not found:{self.config_path}
                ) from ex

        
    
    def get_source(self, source_name: str) -> SourceConfig:
                        
        """
        Returns the configuration for the requested source.
        """

        sources = self.config.get("sources", {})

        if source_name not in sources:

            available_sources = ", ".join(sources.keys())

            raise ValueError(
                f"Source '{source_name}' not found in configuration. "
                f"Available sources: {available_sources}"
            )

        source = sources[source_name]

        return SourceConfig(
            source_name=source_name,
            **source
        )