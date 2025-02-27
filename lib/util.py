import configparser
import io
import re
import sys
import os
import lib.helpers.constants as const
from lib.helpers.encryption import ConfigEncryption
from pathlib import Path
from lib.auth import mc_auth
from typing import Optional
from pycarlo.core import Query, Mutation
from rich.progress import Progress
from lib.helpers.logs import LOGGER


class Util(object):
    """Base Model for Utilities/Scripts."""

    def __init__(self, profile: str = None, config_file: str = None, progress: Progress = None):
        """Creates an instance of Util.

        Args:
            profile (str): Name of the mc cli profile.
            config_file (str): Path to the Configuration File.
            progress (Progress): Progress object used to
        """

        if not config_file:
            config_file = str(Path(__file__).parent.parent) + "/configs/configs.ini"
        self.configs = None
        if Path(config_file).is_file():
            LOGGER.debug(f"reading configuration settings from {config_file}")
            self.configs = configparser.ConfigParser(allow_no_value=True)
            try:
                self.configs.read(config_file)
            except (UnicodeDecodeError, configparser.MissingSectionHeaderError):
                self.configs.read_file(io.StringIO(ConfigEncryption('rsa', 'keys').decrypt_file(config_file)))
        else:
            LOGGER.error(f"config File '{config_file}' specified does not exist")
            sys.exit(1)

        self.auth = mc_auth.MCAuth(self.configs, profile, progress)
        self.profile = profile
        self.OUTPUT_DIR = Path(os.path.abspath(__file__)).parent.parent / "output"
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        self.BATCH = int(self.configs['global'].get('BATCH', '1000'))

    def get_warehouses(self) -> tuple:
        """Returns a list of warehouse uuids"""

        query = Query()
        get_user = query.get_user()
        get_user.account.__fields__("uuid")
        get_user.account.warehouses.__fields__("name", "uuid")
        res = self.auth.client(query).get_user
        warehouses = [warehouse.uuid for warehouse in res.account.warehouses]

        return warehouses, res

    def get_domains(self):

        query = Query()
        query.get_all_domains().__fields__("name", "uuid")

        domains = self.auth.client(query).get_all_domains

        return domains

    def get_data_products(self, batch_size: Optional[int] = None):

        batch_size = self.BATCH if batch_size is None else batch_size

        query = Query()
        get_data_products = query.get_data_products()
        get_data_products.__fields__("description", "name", "uuid")
        get_data_products.audiences.__fields__("uuid")
        get_data_products.assets(first=batch_size).edges.node.__fields__("mcon")
        get_data_products.assets.page_info.__fields__("has_next_page", end_cursor=True)

        return query


class Admin(Util):

    def __init__(self, profile: str = None, config_file: str = None, progress: Progress = None):
        super().__init__(profile, config_file, progress)

    @staticmethod
    def enable_schema_usage(dw_id: str, project: str, dataset: str, rules: list) -> Mutation:
        """Enable tables under a schema to be monitored.

            Args:
                dw_id(str): Warehouse UUID from MC.
                dataset(str): Target for tables in the project/database.
                project(str): Target for tables in the dataset/schema.
                rules(list(dict)): List of rules for deciding which tables are monitored.

            Returns:
                Mutation: Formed MC Mutation object.

        """

        not_none_params = {k: v for k, v in locals().items() if v is not None}
        mutation = Mutation()
        update_monitored_table_rule_list = mutation.update_monitored_table_rule_list(**not_none_params)
        update_monitored_table_rule_list.__fields__("id")

        return mutation

    @staticmethod
    def enable_row_count():
        """Mutation not available in pycarlo. Return mutation to enable/disable RC monitoring"""

        mutation = f"""
            mutation updateToggleSizeCollection($mcon: String!, $enabled: Boolean!) {{
                toggleSizeCollection(
                    mcon: $mcon
                    enabled: $enabled	
                ) {{
                    enabled
                }}
            }}
        """

        return mutation


class Tables(Util):

    def __init__(self, profile: str = None, config_file: str = None, progress: Progress = None):
        super().__init__(profile, config_file, progress)

    def get_tables(self, dw_id: str = None, domain_id: str = None, search: str = "", is_monitored: bool = True,
                   project_name: str = "", dataset: str = "", batch_size: Optional[int] = None,
                   after: Optional[str] = None) -> Query:
        """Retrieve table information based on warehouse id and search parameter.

            Args:
                dw_id(str): Warehouse UUID from MC.
                domain_id(str): Domain UUID from MC.
                search(str): Database/Schema combination to apply in search filter.
                is_monitored(bool): Status of table.
                project_name(str): Database name.
                dataset(str): Schema name.
                batch_size(int): Limit of results returned by the response.
                after(str): Cursor value for next batch.

            Returns:
                Query: Formed MC Query object.

        """

        not_none_params = {k: v for k, v in locals().items() if v is not None}
        if not_none_params.get('self'):
            del not_none_params['self']
        batch_size = self.BATCH if batch_size is None else batch_size

        query = Query()
        get_tables = query.get_tables(first=batch_size, **not_none_params, is_deleted=False)
        get_tables.edges.node.__fields__("full_table_id", "mcon", "table_type")
        get_tables.edges.node.table_capabilities.__fields__("has_non_metadata_size_collection")
        get_tables.page_info.__fields__(end_cursor=True)
        get_tables.page_info.__fields__("has_next_page")

        return query
    
    def get_monitored_tables(self, batch_size: Optional[int] = None,
                   after: Optional[str] = None) -> Query:
        """Retrieve table information based on warehouse id and search parameter.

            Returns:
                Query: Tables that have monitoring enabled, but not query-based volume monitoring.

        """

        batch_size = self.BATCH if batch_size is None else batch_size

        query = Query()
        get_tables = query.get_tables(first=batch_size, is_deleted=False,
                                      is_monitored=True,
                                      **(dict(after=after) if after else {}))
        get_tables.edges.node.__fields__("mcon")
        get_tables.edges.node.table_capabilities.__fields__("has_non_metadata_size_collection")
        get_tables.page_info.__fields__(end_cursor=True)
        get_tables.page_info.__fields__("has_next_page")

        return query

    def get_tables_in_db_schema(self, db: str, schema: str):

        raw_items = []
        mcons = []
        cursor = None
        while True:
            response = self.auth.client(self.get_tables(project_name=db, dataset=schema, after=cursor)).get_tables
            for table in response.edges:
                mcons.append(table.node.mcon)
            if response.page_info.has_next_page:
                cursor = response.page_info.end_cursor
            else:
                break

        return mcons, raw_items

    def get_domain_tables(self, domain_name):

        all_domains = self.get_domains()
        domain_uuid = None
        for domain in all_domains:
            if domain.name == domain_name:
                domain_uuid = domain.uuid
                break

        raw_items = []
        mcons = []
        cursor = None
        while True:
            response = self.auth.client(self.get_tables(domain_id=domain_uuid, after=cursor)).get_tables
            for table in response.edges:
                mcons.append(table.node.mcon)
            if response.page_info.has_next_page:
                cursor = response.page_info.end_cursor
            else:
                break

        return mcons, raw_items

    def get_mcons(self, dw_id: str, search: str = "") -> tuple:
        """Get tables' mcon values

            Args:
                dw_id(str): Warehouse UUID from MC.
                search(str): Database/Schema combination to apply in search filter.

            Returns:
                tuple: List of table mcons and extended raw response.
        """

        raw_items = []
        mcons = []
        cursor = None
        while True:
            response = self.auth.client(self.get_tables(dw_id=dw_id, search=search, after=cursor)).get_tables
            for table in response.edges:
                mcons.append(table.node.mcon)
            if response.page_info.has_next_page:
                cursor = response.page_info.end_cursor
            else:
                break

        return mcons, raw_items

    def get_mcons_by_fulltableid(self, warehouse_id: str, full_table_ids: list[str]):
        """ """

        raw_items = []
        mcons = []
        cursor = None
        while True:
            response = self.auth.client(self.get_tables(dw_id=warehouse_id, search="", after=cursor)).get_tables
            if len(response.edges) > 0:
                raw_items.extend(response.edges)
                for table in response.edges:
                    if table.node.full_table_id in full_table_ids:
                        mcons.append(table.node.mcon)
                    else:
                        LOGGER.debug(f"{table.node.full_table_id} not found")

            if response.page_info.has_next_page:
                cursor = response.page_info.end_cursor
            else:
                break

        return mcons, raw_items

    @staticmethod
    def update_asset_description(mcon: str, description: str) -> Mutation:

        not_none_params = {k: v for k, v in locals().items() if v is not None}
        mutation = Mutation()
        create_or_update_catalog_object_metadata = mutation.create_or_update_catalog_object_metadata(**not_none_params)
        create_or_update_catalog_object_metadata.catalog_object_metadata.__fields__("description")

        return mutation

    def bulk_create_or_update_object_properties(self, props: list) -> Mutation:
        """Create or update a list of properties (tags) for objects (e.g. tables, fields, etc.)

            Returns:
                Mutation: Mutation object to execute by client.

        """

        mutation = Mutation()
        bulk_create_or_update_object_properties = mutation.bulk_create_or_update_object_properties(
            input_object_properties=props)
        bulk_create_or_update_object_properties.object_properties.__fields__("mcon_id")

        return mutation


class Monitors(Util):

    def __init__(self, profile: str = None, config_file: str = None, progress: Progress = None):
        super().__init__(profile, config_file, progress)

    @staticmethod
    def summarize_apply_results(cli_output: str):
        """Displays # of change

        Args:
            cli_output(str): Message from montecarlodata cli
        """

        deletes = re.findall(r" -.*DELETE", cli_output).__len__()
        updates = re.findall(r" -.*UPDATE", cli_output).__len__()
        creates = re.findall(r" -.*CREATE", cli_output).__len__()
        LOGGER.info("----------- SUMMARY -----------")
        LOGGER.info(f"- DELETES: {deletes}") if deletes else None
        LOGGER.info(f"- UPDATES: {updates}") if updates else None
        LOGGER.info(f"- CREATES: {creates}") if creates else None
        LOGGER.info("-------------------------------")

    def get_custom_rules(self, warehouse_id: str, batch_size: Optional[int] = None,
                         after: Optional[str] = None) -> Query:
        """Retrieve custom rule monitors for a particular warehouse.

            Args:
                warehouse_id(str): Warehouse UUID from MC.
                batch_size(int): Limit of results returned by the response.
                after(str): Cursor value for next batch.

            Returns:
                Query: Formed MC Query object.

        """

        batch_size = self.BATCH if batch_size is None else batch_size

        query = Query()
        get_custom_rules = query.get_custom_rules(first=batch_size, warehouse_uuid=warehouse_id,
                                                  **(dict(after=after) if after else {}))
        get_custom_rules.edges.node.__fields__("uuid", "rule_type", "is_paused")
        get_custom_rules.edges.node.queries(first=batch_size).edges.node.__fields__("uuid", "entities", "sql_query")
        get_custom_rules.page_info.__fields__(end_cursor=True)
        get_custom_rules.page_info.__fields__("has_next_page")

        return query

    def get_custom_rules_with_assets(self, dw_id: str, asset: str) -> tuple:
        """ Identify custom rules that contain an asset.

            Args:
                dw_id(str): Warehouse UUID from MC.
                asset(str): Project/Dataset/Table name.

            Returns:
                tuple: List of custom rules referencing an asset and extended raw response.
        """

        raw_items = []
        monitors = []
        cursor = None
        while True:
            response = self.auth.client(
                self.get_custom_rules(warehouse_id=dw_id, after=cursor)).get_custom_rules
            if len(response.edges) > 0:
                raw_items.extend(response.edges)
                for edge in response.edges:
                    if not edge.node.is_paused:
                        if len(edge.node.queries.edges) > 0:
                            for node in edge.node.queries.edges:
                                LOGGER.debug(
                                    f"monitor of type {edge.node.rule_type} found in {node.node.entities}"
                                    f" - {edge.node.uuid} - getCustomRules")
                                if node.node.entities:
                                    if any(asset in item for item in [ent for ent in node.node.entities]):
                                        LOGGER.debug(
                                            f"monitor of type {edge.node.rule_type} found in {node.node.entities}"
                                            f" - {edge.node.uuid} - getCustomRules")
                                        monitors.append(edge.node.uuid)
            if response.page_info.has_next_page:
                cursor = response.page_info.end_cursor
            else:
                break

        return monitors, raw_items

    def get_monitors_by_entities(self, dw_id: str, asset: str, batch_size: Optional[int] = None) -> tuple:
        """Retrieve all monitors based on search criteria. This method only accounts for UNPAUSED and UI monitors.

                Args:
                    dw_id(str): Warehouse UUID from MC.
                    asset(str): Project/Dataset/Table to apply in search filter.
                    batch_size(int): Limit of results returned by the response.

                Returns:
                    tuple: List of custom monitors matching search and extended raw response.

        """

        batch_size = self.BATCH if batch_size is None else batch_size

        raw_items = []
        monitors = []
        skip_records = 0
        while True:
            query = Query()
            get_monitors = query.get_monitors(search=[asset], limit=batch_size, offset=skip_records,
                                              search_fields=["ENTITIES"])
            get_monitors.__fields__("uuid", "entities", "monitor_type", "monitor_status", "resource_id", "name",
                                    "namespace")
            response = self.auth.client(query).get_monitors
            if len(response) > 0:
                raw_items.extend(response)
                for monitor in response:
                    LOGGER.debug(
                        f"monitor of type {monitor.monitor_type} found in {monitor.entities} - "
                        f"{monitor.uuid} - getMonitors")
                    if monitor.monitor_status != "PAUSED" and monitor.namespace == 'ui':
                        if monitor.resource_id == dw_id:
                            monitors.append(monitor.uuid)
                            LOGGER.debug(
                                f"monitor of type {monitor.monitor_type} found in {monitor.entities} - "
                                f"{monitor.uuid} - getMonitors")

            skip_records += self.BATCH
            if len(response) < self.BATCH:
                break

        return monitors, raw_items

    def get_monitors_by_audience(self, audiences: list, batch_size: Optional[int] = None, skip_records: Optional[int] = 0) -> tuple:

        batch_size = self.BATCH if batch_size is None else batch_size

        raw_items = []
        monitors = []
        skip_records = 0
        while True:
            query = Query()
            get_monitors = query.get_monitors(limit=batch_size, offset=skip_records, labels=audiences)
            get_monitors.__fields__("uuid", "monitor_type", "resource_id")
            response = self.auth.client(query).get_monitors
            if len(response) > 0:
                raw_items.extend(response)
                for monitor in response:
                    monitors.append(monitor.uuid)

            skip_records += self.BATCH
            if len(response) < self.BATCH:
                break

        return monitors, raw_items

    def get_monitors_by_type(self, dw_id: str, types: list[const.MonitorTypes], is_ootb_replacement: bool = False,
                             mcons: list[str] = None, batch_size: Optional[int] = None) -> tuple:
        """Retrieve all monitors under monitor type(s).

                Args:
                    dw_id(str): Warehouse UUID from MC.
                    types(list[MonitorTypes]): Supported monitor type.
                    is_ootb_replacement(bool): Replacement of OOTB Freshness or Volume monitors.
                    mcons(list[str]): Filter by associated entities.
                    batch_size(int): Limit of results returned by the response.

                Returns:
                    tuple: List of custom monitors matching search and extended raw response.

        """

        batch_size = self.BATCH if batch_size is None else batch_size
        mcons = [] if mcons is None else mcons

        raw_items = []
        monitors = []
        skip_records = 0
        while True:
            query = Query()
            get_monitors = query.get_monitors(monitor_types=types, mcons=mcons, is_ootb_replacement=is_ootb_replacement,
                                              limit=batch_size, offset=skip_records)
            get_monitors.__fields__("uuid", "description", "monitor_type", "monitor_status", "resource_id", "name",
                                    "rule_comparisons")
            get_monitors.schedule_config.__fields__("interval_crontab", "interval_minutes",
                                                    "schedule_type", "start_time", "timezone")
            response = self.auth.client(query).get_monitors
            if len(response) > 0:
                raw_items.extend(response)
                for monitor in response:
                    if monitor.resource_id == dw_id:
                        monitors.append(monitor.uuid)
                        LOGGER.debug(
                            f"monitor of type {monitor.monitor_type} found - "
                            f"{monitor.uuid} - getMonitors")

            skip_records += self.BATCH
            if len(response) < self.BATCH:
                break

        return monitors, raw_items

    def get_ui_monitors(self, batch_size: Optional[int] = None, skip_records: Optional[int] = 0) -> tuple:

        batch_size = self.BATCH if batch_size is None else batch_size

        raw_items = []
        monitors = []
        while True:
            query = Query()
            get_monitors = query.get_monitors(limit=batch_size, offset=skip_records, namespaces=["ui"])
            get_monitors.__fields__("uuid", "monitor_type", "resource_id", "is_paused", "next_execution_time",
                                    "monitor_run_status", "connection_id")
            get_monitors.schedule_config.__fields__("interval_crontab", "interval_minutes",
                                                    "schedule_type", "start_time", "timezone")
            response = self.auth.client(query).get_monitors
            if len(response) > 0:
                raw_items.extend(response)
                for monitor in response:
                    monitors.append(monitor.uuid)

            skip_records += self.BATCH
            if len(response) < self.BATCH:
                break

        return monitors, raw_items

    def get_mac_monitors(self, batch_size: Optional[int] = None, skip_records: Optional[int] = 0) -> tuple:

        batch_size = self.BATCH if batch_size is None else batch_size

        raw_items = []
        monitors = []
        while True:
            query = Query()
            get_monitors = query.get_monitors(limit=batch_size, offset=skip_records, is_template_managed=True)
            get_monitors.__fields__("uuid", "monitor_type", "resource_id", "is_paused", "next_execution_time",
                                    "monitor_run_status", "connection_id")
            get_monitors.schedule_config.__fields__("interval_crontab", "interval_minutes",
                                                    "schedule_type", "start_time", "timezone")
            response = self.auth.client(query).get_monitors
            if len(response) > 0:
                raw_items.extend(response)
                for monitor in response:
                    monitors.append(monitor.uuid)

            skip_records += self.BATCH
            if len(response) < self.BATCH:
                break

        return monitors, raw_items

    def export_yaml_template(self, monitor_uuids: list[str], export_name) -> dict:
        """Export MaC configuration.

        Args:
            monitor_uuids(list(str)): List of monitor uuids.
            export_name(str): Name to include in export.

        Returns:
            dict: Yaml export.

        """

        query = Query()
        get_yaml = query.export_monte_carlo_config_templates(monitor_uuids=monitor_uuids, export_name=export_name)
        get_yaml.__fields__("config_template_as_yaml")
        yaml_template = self.auth.client(query).export_monte_carlo_config_templates

        return yaml_template

    @staticmethod
    def delete_monitor(monitor_uuid: str) -> Mutation:
        mutation = Mutation()
        mutation.delete_monitor(monitor_id=monitor_uuid).__fields__('success')
        return mutation

    @staticmethod
    def delete_custom_rule(rule_uuid: str) -> Mutation:
        mutation = Mutation()
        mutation.delete_custom_rule(uuid=rule_uuid).__fields__('uuid')
        return mutation

    def delete_custom_monitor(self, monitor_uuid):

        try:
            _ = self.auth.client(self.delete_monitor(monitor_uuid)).delete_monitor
            LOGGER.debug(f"monitor [{monitor_uuid}] deleted successfully - deleteMonitor")
        except:
            try:
                _ = self.auth.client(self.delete_custom_rule(monitor_uuid)).delete_custom_rule
                LOGGER.debug(f"monitor [{monitor_uuid}] deleted successfully - deleteCustomRule")
            except:
                LOGGER.error(f"Unable to delete monitor [{monitor_uuid}]")

    @staticmethod
    def toggle_monitor_state():
        """Return mutation to enable/disable monitor (metric)"""

        mutation = f"""
                mutation toggleMonitorState($ruleId: UUID!, $pause: Boolean!) {{
                  pauseMonitor(pause: $pause, uuid: $ruleId) {{
                    monitor {{
                      id
                      uuid
                      isPaused
                    }}
                  }}
                }}
                """

        return mutation

    @staticmethod
    def toggle_rule_state():
        """Return mutation to enable/disable rule (sql rules)"""

        mutation = f"""
                mutation toggleRuleState($ruleId: UUID!, $pause: Boolean!) {{
                  pauseRule(pause: $pause, uuid: $ruleId) {{
                    rule {{
                      id
                      uuid
                      isPaused
                    }}
                  }}
                }}
                """

        return mutation

    def pause_monitor(self, monitor_uuid: str, pause: bool) -> bool:

        try:
            _ = self.auth.client(self.toggle_monitor_state(),
                                 variables={"ruleId": monitor_uuid, "pause": pause}).pauseMonitor
            LOGGER.debug(f"monitor [{monitor_uuid}] toggled successfully - pauseMonitor")
            return True
        except:
            try:
                _ = self.auth.client(self.toggle_rule_state(),
                                     variables={"ruleId": monitor_uuid, "pause": pause}).pauseRule
                LOGGER.debug(f"monitor [{monitor_uuid}] toggled successfully - pauseRule")
                return True
            except:
                LOGGER.error(f"Unable to pause monitor - {monitor_uuid}")
                return False
    
    def toggle_size_collection(self,mcon: str, enabled: True) -> Mutation:
        mutation=Mutation()
        mutation.toggle_size_collection(mcon=mcon, enabled=enabled).__fields__("enabled")
        
        return mutation

