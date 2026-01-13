"""Warehouse mapping utilities for cross-environment migrations.

This module provides utilities to load and manage warehouse name mappings
for migrating Monte Carlo configurations between environments where
warehouse names differ.

Mapping Priority:
    1. CLI argument (--warehouse_map)
    2. Configuration file (warehouse_mapping.json)
    3. No fallback - unmapped warehouses are skipped

Example warehouse_mapping.json:
    {
        "warehouse_mapping": {
            "Source Warehouse Name": "Destination Warehouse Name",
            "Dev Snowflake": "Prod Snowflake"
        }
    }

Example CLI usage:
    --warehouse_map "Source DW=Dest DW,Dev BQ=Prod BQ"
"""

import json
import logging
from pathlib import Path
from typing import Optional

LOGGER = logging.getLogger(__name__)


class WarehouseMappingLoader:
    """Load and validate warehouse mappings from CLI or config file."""

    MAPPING_FILENAME = "warehouse_mapping.json"
    TEMPLATE_FILENAME = "warehouse_mapping_template.json"

    @staticmethod
    def parse_cli_mapping(mapping_str: str) -> dict:
        """Parse CLI mapping string to dictionary.

        Args:
            mapping_str: Comma-separated mappings in format 'source1=dest1,source2=dest2'

        Returns:
            dict: Source warehouse name -> destination warehouse name mapping
        """
        if not mapping_str:
            return {}

        mapping = {}
        pairs = mapping_str.split(',')
        for pair in pairs:
            pair = pair.strip()
            if '=' in pair:
                source, dest = pair.split('=', 1)
                source = source.strip()
                dest = dest.strip()
                if source and dest:
                    mapping[source] = dest
                else:
                    LOGGER.warning(f"Skipping invalid mapping pair: '{pair}'")
            elif pair:
                LOGGER.warning(f"Skipping invalid mapping (missing '='): '{pair}'")

        return mapping

    @staticmethod
    def load_from_file(directory: str) -> dict:
        """Load mapping from warehouse_mapping.json in directory.

        Args:
            directory: Directory path to look for warehouse_mapping.json

        Returns:
            dict: Source warehouse name -> destination warehouse name mapping
        """
        mapping_file = Path(directory) / WarehouseMappingLoader.MAPPING_FILENAME

        if not mapping_file.exists():
            return {}

        try:
            with open(mapping_file, 'r') as f:
                data = json.load(f)
                mapping = data.get('warehouse_mapping', {})
                if mapping:
                    LOGGER.debug(f"Loaded {len(mapping)} mapping(s) from {mapping_file}")
                return mapping
        except json.JSONDecodeError as e:
            LOGGER.error(f"Invalid JSON in {mapping_file}: {e}")
            return {}
        except Exception as e:
            LOGGER.warning(f"Failed to load {mapping_file}: {e}")
            return {}

    @staticmethod
    def get_mapping(cli_arg: str = None, directory: str = None) -> dict:
        """Get warehouse mapping with priority: CLI > file.

        Args:
            cli_arg: CLI mapping string (e.g., 'source1=dest1,source2=dest2')
            directory: Directory to look for warehouse_mapping.json

        Returns:
            dict: Source warehouse name -> destination warehouse name mapping
        """
        # Priority 1: CLI argument
        if cli_arg:
            mapping = WarehouseMappingLoader.parse_cli_mapping(cli_arg)
            if mapping:
                LOGGER.info(f"Using warehouse mapping from CLI ({len(mapping)} mapping(s))")
                return mapping

        # Priority 2: Config file
        if directory:
            mapping = WarehouseMappingLoader.load_from_file(directory)
            if mapping:
                LOGGER.info(f"Using warehouse mapping from {WarehouseMappingLoader.MAPPING_FILENAME} ({len(mapping)} mapping(s))")
                return mapping

        # No mapping found
        LOGGER.info("No warehouse mapping provided (CLI or file)")
        return {}

    @staticmethod
    def generate_template(source_warehouses: dict, output_dir: str) -> str:
        """Generate warehouse_mapping_template.json with source warehouses.

        Creates a template file that users can edit to define their mappings.

        Args:
            source_warehouses: Dict of warehouse_id -> warehouse_name from export
            output_dir: Directory to write the template file

        Returns:
            str: Path to the generated template file
        """
        # Get unique warehouse names
        unique_names = sorted(set(name for name in source_warehouses.values() if name))

        if not unique_names:
            LOGGER.debug("No source warehouses to generate template for")
            return ""

        template = {
            "_instructions": (
                "Replace <ENTER_DESTINATION_WAREHOUSE> with the actual warehouse name "
                "in your target environment. Then rename this file to 'warehouse_mapping.json'. "
                "Remove any warehouses you don't want to migrate."
            ),
            "warehouse_mapping": {
                name: "<ENTER_DESTINATION_WAREHOUSE>"
                for name in unique_names
            }
        }

        template_file = Path(output_dir) / WarehouseMappingLoader.TEMPLATE_FILENAME
        try:
            with open(template_file, 'w') as f:
                json.dump(template, f, indent=2)
            LOGGER.info(f"Generated mapping template: {template_file}")
            return str(template_file)
        except Exception as e:
            LOGGER.error(f"Failed to generate mapping template: {e}")
            return ""

    @staticmethod
    def validate_mapping(
        mapping: dict,
        source_warehouses: set,
        dest_warehouses: set
    ) -> dict:
        """Validate that mapping covers source warehouses and destinations exist.

        Args:
            mapping: Source name -> destination name mapping
            source_warehouses: Set of source warehouse names from CSV
            dest_warehouses: Set of available destination warehouse names

        Returns:
            dict: Validation result with 'valid', 'mapped', 'unmapped', 'invalid_dest'
        """
        mapped = []
        unmapped = []
        invalid_dest = []

        # Normalize destination names for comparison
        dest_warehouses_lower = {name.lower().strip(): name for name in dest_warehouses}

        for src_name in source_warehouses:
            if not src_name:
                continue

            dest_name = mapping.get(src_name)

            if not dest_name:
                unmapped.append(src_name)
            elif dest_name.lower().strip() not in dest_warehouses_lower:
                invalid_dest.append(f"{src_name} → {dest_name}")
            else:
                mapped.append(f"{src_name} → {dest_name}")

        return {
            'valid': len(unmapped) == 0 and len(invalid_dest) == 0,
            'mapped': mapped,
            'unmapped': unmapped,
            'invalid_dest': invalid_dest
        }

