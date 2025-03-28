import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import csv
from tables import *
from lib.helpers import sdk_helpers

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))


class BulkTableViewTagImporter(Tables):

    def __init__(self, profile, config_file: str = None, progress: Progress = None):
        """Creates an instance of BulkTableTagImporter.

        Args:
            profile(str): Profile to use stored in montecarlo test.
            config_file (str): Path to the Configuration File.
            progress(Progress): Progress bar.
        """

        super().__init__(profile,  config_file, progress)
        self.progress_bar = progress

    @staticmethod
    def validate_input_file(input_file: str) -> any:
        """Ensure path given exists.

        Args:
            input_file(str): Input file.

        Returns:
            Path: Full path to input file.
        """

        file = Path(input_file)

        if file.is_file():
            asset_ids = []
            tag_set = []
            with open(input_file, "r") as input_csv:
                reader = csv.reader(input_csv, delimiter=",")
                for row in reader:
                    if len(row) == 2:
                        tag_set.append(row[1])
                    elif len(row) > 2:
                        LOGGER.error(f"1 or 2 column(s) expected in input file, received {len(row)}")
                        sys.exit(1)
                    asset_ids.append(row[0])
            if len(asset_ids) == 0:
                LOGGER.error("No rows present in input file")
                sys.exit(1)
            return asset_ids, tag_set
        else:
            LOGGER.error("invalid input file")
            sys.exit(1)

    def generate_mcons(self, asset_ids: list, warehouse_name: str, asset_type: str):
        """Running one call per asset to obtain the MCON can be expensive, instead, the MCON can be predicted
        and this method will use the asset type from the input file to generate it.

        Args:
            asset_ids(list): list of asset ids
            warehouse_name(str): name of warehouse as it appears in MC
            asset_type(str): table or view
        """

        _, raw = self.get_warehouses()
        account, warehouse = None, None
        for acct in raw:
            account = raw[acct].uuid
            for wh in raw[acct].warehouses:
                if wh.name == warehouse_name:
                    warehouse = wh.uuid
                    break

        if None in (warehouse, account):
            LOGGER.error("unable to locate account/warehouse. Ensure the warehouse provided is spelled correctly")
            sys.exit(1)

        return [f"MCON++{account}++{warehouse}++{asset_type}++{asset}" for asset in asset_ids]

    @staticmethod
    def process_tags(mcon, tag_string, properties):
        """Helper function to process a tag string and append to properties list."""
        try:
            for tag in tag_string.split(','):
                k, v = tag.split(':', 1)  # Avoids ValueError for unexpected input
                properties.append({
                    'mcon_id': mcon,
                    'property_name': k.strip(),
                    'property_value': v.strip()
                })
        except ValueError:
            LOGGER.debug(f"Skipping invalid tag format: {tag_string}")

    def import_tags(self, assets: list, tags: str):
        """ """

        properties = []

        LOGGER.debug(f"generating payload for {len(assets)} assets")
        for index, mcon in enumerate(assets):
            if isinstance(tags, list) and index < len(tags):
                self.process_tags(mcon, tags[index], properties)
            elif isinstance(tags, str):
                self.process_tags(mcon, tags, properties)

        batches = [properties[i:i + 100] for i in range(0, len(properties), 100)]
        LOGGER.info(f"splitting {len(properties)} properties in batches of 100")
        for batch in batches:
            response = self.auth.client(self.bulk_create_or_update_object_properties(batch)).bulk_create_or_update_object_properties
            if not response:
                LOGGER.error(f"unable to set tags")
            else:
                LOGGER.info(f"tag(s) set successfully")


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
        assets, tags = util.validate_input_file(args.input_file)
        if args.tag:
            tags = args.tag
        util.import_tags(util.generate_mcons(assets, args.warehouse, args.asset_type), tags)

    util = BulkTableViewTagImporter(args.profile)
    run_utility(util, args)


if __name__ == '__main__':
    main()
