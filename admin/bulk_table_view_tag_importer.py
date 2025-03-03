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
            profile(str): Profile to use stored in montecarlo cli.
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
            with open(input_file, "r") as input_csv:
                reader = csv.reader(input_csv, delimiter=",")
                for row in reader:
                    if len(row) > 1:
                        LOGGER.error(f"1 column expected, received {len(row)}")
                        sys.exit(1)
                    asset_ids.append(row[0])
            return asset_ids
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

    def import_tags(self, assets: list, tag: str):
        """ """

        k, v = tag.split(':')
        properties = []

        LOGGER.debug(f"generating payload for {len(assets)} assets")
        for mcon in assets:
            properties.append({
                'mcon_id': mcon,
                'property_name': k,
                'property_value': v
            })

        batches = [properties[i:i + 100] for i in range(0, len(properties), 100)]
        LOGGER.info("splitting assets in batches of 100")
        for batch in batches:
            LOGGER.info(f"uploading tag for {len(batch)} assets")
            response = self.auth.client(self.bulk_create_or_update_object_properties(batch)).bulk_create_or_update_object_properties
            if not response:
                LOGGER.error(f"unable to set tags")
            else:
                LOGGER.info(f"tag set successfully")


def main(*args, **kwargs):

    # Capture Command Line Arguments
    parser = sdk_helpers.generate_arg_parser(os.path.basename(os.path.dirname(os.path.abspath(__file__))),
                                             os.path.basename(__file__))

    if not args:
        args = parser.parse_args(*args, **kwargs)
    else:
        sdk_helpers.dump_help(parser, main, *args)
        args = parser.parse_args(*args, **kwargs)

    # Initialize variables
    if ':' not in args.tag:
        print(f"[red]tag must be of key:value format")
        sys.exit(1)
    profile = args.profile

    # Initialize Util and run in given mode
    with (Progress() as progress):
        try:
            task = progress.add_task("[yellow][RUNNING]...", total=100)
            LogRotater.rotate_logs(retention_period=7)
            progress.update(task, advance=25)

            LOGGER.info(f"running utility using '{profile}' profile")
            util = BulkTableViewTagImporter(profile, progress=progress)
            util.import_tags(util.generate_mcons(util.validate_input_file(args.input_file),
                                                 args.warehouse, args.asset_type), args.tag)

            progress.update(task, description="[dodger_blue2][COMPLETE]", advance=100)
        except Exception as e:
            LOGGER.error(e, exc_info=False)
            print(traceback.format_exc())
        finally:
            progress.update(task, description="[dodger_blue2 bold][COMPLETE]", advance=100)


if __name__ == '__main__':
    main()
