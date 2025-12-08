import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from lib.util import Util
from lib.helpers import sdk_helpers
from lib.helpers.logs import LoggingConfigs, LOGGER, LOGS_DIR
import logging.config
import datetime
from rich.progress import Progress

# Initialize logger
util_name = os.path.basename(__file__).split('.')[0]
logging.config.dictConfig(LoggingConfigs.logging_configs(util_name))

# Store the workspace_migrator log file path
WORKSPACE_MIGRATOR_LOG = LOGS_DIR / f"{util_name}-{datetime.date.today()}.log"


class WorkspaceMigrator(Util):

    def __init__(self, profile, config_file: str = None, progress: Progress = None):
        """Creates an instance of WorkspaceMigrator.

        Args:
            profile(str): Profile to use stored in montecarlo test.
            config_file (str): Path to the Configuration File.
            progress(Progress): Progress bar.
        """
        super().__init__(profile, config_file, progress)
        self.progress_bar = progress
        self.objects_to_migrate = ['DOMAINS', 'TAGS', 'EXCLUSION WINDOWS', 'DATA PRODUCTS', 'BLOCK LISTS']
        self.DOMAINS_FILE = "domains.csv"
    
    def _ensure_workspace_migrator_log_handler(self):
        """Ensure that workspace_migrator.log file handler is present in root logger.
        
        This method adds a file handler to the root logger that writes to workspace_migrator.log.
        It should be called after importing submodules that might reconfigure logging.
        """
        root_logger = logging.getLogger()
        workspace_migrator_log_path = str(WORKSPACE_MIGRATOR_LOG.absolute())
        
        # Check if workspace_migrator file handler already exists
        handler_exists = False
        for handler in root_logger.handlers:
            if isinstance(handler, logging.FileHandler):
                # Compare absolute paths to handle different path formats
                handler_path = os.path.abspath(handler.baseFilename) if hasattr(handler, 'baseFilename') else None
                if handler_path == workspace_migrator_log_path:
                    handler_exists = True
                    break
        
        # Add handler if it doesn't exist
        if not handler_exists:
            file_handler = logging.FileHandler(
                WORKSPACE_MIGRATOR_LOG,
                encoding='utf-8'
            )
            file_handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            LOGGER.debug(f"Added workspace_migrator log handler: {workspace_migrator_log_path}")

    def _export_object(self, object_type: str, profile: str):
        """Export a specific object type.
        
        Args:
            object_type (str): The type of object to export (e.g., 'DOMAINS', 'TAGS').
            profile (str): Profile to use for the export.
        """
        object_type_upper = object_type.upper()
        
        if object_type_upper == 'DOMAINS':
            # TODO: Add domain export when BulkDomainExporter is available
            # from admin.bulk_domain_exporter import BulkDomainExporter
            # exporter = BulkDomainExporter(profile, progress=self.progress_bar, validate=False)
            # exporter.export_domains()
            pass
        elif object_type_upper == 'TAGS':
            # TODO: Add tag export when available
            pass
        elif object_type_upper == 'EXCLUSION WINDOWS':
            # TODO: Add exclusion windows export when available
            pass
        elif object_type_upper == 'DATA PRODUCTS':
            # TODO: Add data products export when available
            pass
        elif object_type_upper == 'BLOCK LISTS':
            # TODO: Add block lists export when available
            pass
    
    def run_export_process(self, profile: str):
        """Run all export activities.

        Args:
            profile (str): Profile to use for the export.
        """
        # Ensure workspace_migrator log handler is present after importing submodule
        # (submodule may have reconfigured logging)
        self._ensure_workspace_migrator_log_handler()

        LOGGER.info("Running export process...")
        
        # Initialize progress bar if available
        if self.progress_bar and self.progress_bar.tasks:
            task_id = self.progress_bar.tasks[0].id
            progress_per_object = 100 / len(self.objects_to_migrate)
        else:
            task_id = None
            progress_per_object = 0

        # Export each object type
        for object_type in self.objects_to_migrate:
            try:
                LOGGER.info(f"Starting '[{object_type}]' export...")
                self._export_object(object_type, profile)
                LOGGER.info(f"'[{object_type}]' export completed successfully")
            except Exception as e:
                LOGGER.error(f"Failed to export '{object_type}': {e}")
                raise
            finally:
                # Update progress bar
                if task_id is not None:
                    self.progress_bar.update(task_id, advance=progress_per_object)

        LOGGER.info("Export process completed successfully")

    def _import_object(self, object_type: str, profile: str):
        """Import a specific object type.
        
        Args:
            object_type (str): The type of object to import (e.g., 'DOMAINS', 'TAGS').
            profile (str): Profile to use for the import.
        """
        object_type_upper = object_type.upper()
        
        if object_type_upper == 'DOMAINS':
            from admin.bulk_domain_importer import BulkDomainImporter
            
            importer = BulkDomainImporter(
                profile, progress=self.progress_bar, validate=False
            )
            rows = importer.validate_input_file(self.DOMAINS_FILE)
            importer.import_domains(rows)
        elif object_type_upper == 'TAGS':
            # TODO: Add tag import when available
            pass
        elif object_type_upper == 'EXCLUSION WINDOWS':
            # TODO: Add exclusion windows import when available
            pass
        elif object_type_upper == 'DATA PRODUCTS':
            # TODO: Add data products import when available
            pass
        elif object_type_upper == 'BLOCK LISTS':
            # TODO: Add block lists import when available
            pass
    
    def run_import_process(self, profile: str):
        """Run all import activities.

        Args:
            profile (str): Profile to use for the import.
        """
        # Ensure workspace_migrator log handler is present after importing submodule
        # (submodule may have reconfigured logging)
        self._ensure_workspace_migrator_log_handler()

        LOGGER.info("Running import process...")
        
        # Initialize progress bar if available
        if self.progress_bar and self.progress_bar.tasks:
            task_id = self.progress_bar.tasks[0].id
            progress_per_object = 100 / len(self.objects_to_migrate)
        else:
            task_id = None
            progress_per_object = 0

        # Import each object type
        for object_type in self.objects_to_migrate:
            try:
                LOGGER.info(f"Starting '[{object_type}]' import...")
                self._import_object(object_type, profile)
                LOGGER.info(f"'[{object_type}]' import completed successfully")
            except Exception as e:
                LOGGER.error(f"Failed to import '{object_type}': {e}")
                raise
            finally:
                # Update progress bar
                if task_id is not None:
                    self.progress_bar.update(task_id, advance=progress_per_object)

        LOGGER.info("Import process completed successfully")


def main(*args, **kwargs):

    # Capture Command Line Arguments
    parser, subparsers = sdk_helpers.generate_arg_parser(os.path.basename(os.path.dirname(os.path.abspath(__file__))),
                                                         os.path.basename(__file__))

    if not args:
        args = parser.parse_args(*args, **kwargs)
    else:
        sdk_helpers.dump_help(parser, main, *args)
        args = parser.parse_args(*args, **kwargs)

    @sdk_helpers.ensure_progress
    def run_utility(progress, util, args):
        util.progress_bar = progress
        
        if args.commands.lower() == 'export':
            util.run_export_process(args.profile)
        elif args.commands.lower() == 'import':
            util.run_import_process(args.profile)

    util = WorkspaceMigrator(args.profile)
    run_utility(util, args)


if __name__ == '__main__':
    main()
