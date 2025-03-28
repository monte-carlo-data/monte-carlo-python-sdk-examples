import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from tables import *
from lib.helpers import sdk_helpers

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class LinkTableViewUtility(Tables):

    def __init__(self, profile, config_file: str = None, progress: Progress = None):
        """Creates an instance of LinkTableViewUtility.

        Args:
            profile(str): Profile to use stored in montecarlo test.
            config_file (str): Path to the Configuration File.
            progress(Progress): Progress bar.
        """

        super().__init__(profile,  config_file, progress)
        self.progress_bar = progress

    def retrieve_assets(self, source):
        """Helper function to retrieve assets for a given source.

        Args:
            source(str): db:schema
        """

        assets = None
        if ':' not in source:
            LOGGER.error("source must be in db:schema format")
        else:
            db, schema = source.split(':')
            LOGGER.info(f"retrieving assets under {source}...")
            assets, _ = self.get_tables_in_db_schema(db, schema)
            if not assets:
                LOGGER.error(f"no assets found in {source}")

        return assets

    def map_assets(self, source_a: str, source_b: str) -> dict:
        """Maps assets from a schema to another

        Args:
            source_a(str): db:schema
            source_b(str): db:schema
        """

        asset_map = {}

        source_a_assets = self.retrieve_assets(source_a)
        if source_a_assets is None:
            return asset_map

        source_b_assets = self.retrieve_assets(source_b)
        if source_b_assets is None:
            return asset_map

        for mcon_a in source_a_assets:
            asset_a = mcon_a.split('++')[-1].split('.')[-1]
            for mcon_b in source_b_assets:
                asset_b = mcon_b.split('++')[-1].split('.')[-1]
                if asset_a == asset_b:
                    LOGGER.debug(f"asset with name {asset_a} matched")
                    asset_map[mcon_a] = mcon_b
                    break

        LOGGER.info(f"{len(asset_map)} assets mapped")

        return asset_map

    def set_table_descriptions(self, assets: dict):
        """ """

        LOGGER.info(f"setting descriptions for the {len(assets)} matches")
        for k, v in assets.items():
            description = f"### Mapping: https://getmontecarlo.com/assets/{v}"
            LOGGER.debug(f"updating asset - {k.split('++')[-1].split('.')[-1]}")
            response = self.auth.client(self.update_asset_description(k, description)).create_or_update_catalog_object_metadata
            if not response:
                LOGGER.error(f"unable to set description on {k}")
            description = f"### Mapping: https://getmontecarlo.com/assets/{k}"
            LOGGER.debug(f"updating asset - {v.split('++')[-1].split('.')[-1]}")
            response = self.auth.client(self.update_asset_description(v, description)).create_or_update_catalog_object_metadata
            if not response:
                LOGGER.error(f"unable to set description on {k}")


def main(*args, **kwargs):

    # Capture Command Line Arguments
    parser = sdk_helpers.generate_arg_parser(os.path.basename(os.path.dirname(os.path.abspath(__file__))),
                                             os.path.basename(__file__))

    if not args:
        args = parser.parse_args(*args, **kwargs)
    else:
        sdk_helpers.dump_help(parser, main, *args)
        args = parser.parse_args(*args, **kwargs)

    @sdk_helpers.ensure_progress
    def run_utility(progress, util, args):
        util.progress_bar = progress
        util.set_table_descriptions(util.map_assets(args.a_source, args.b_source))

    util = LinkTableViewUtility(args.profile)
    run_utility(util, args)


if __name__ == '__main__':
    main()
