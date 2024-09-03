from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from dataclasses_json import dataclass_json

from pycarlo.features.metadata import AllowBlockList, FilterEffectType, FilterType, MetadataFilter


@dataclass_json
@dataclass
class MetadataFiltersContainer:
    """
    More documentation and samples in the link below:
    https://www.notion.so/montecarlodata/Catalog-Schema-Filtering-59edd6eff7f74c94ab6bfca75d2e3ff1

    MetadataFiltersContainer class that includes a metadata_filters list that works
    in the following way:
    A list of filters where:
    - each filter can be a block or allow filter
    - each filter can optionally filter: project, dataset, table and table_type
    - each filter matches using of the following types: exact match, regular expression, prefix
    - there's a default effect (allow/block) configured in the list, those elements with no matching
      filter will be resolved with the default effect in the list.
    - This class supports filtering objects in memory or generating SQL conditions for filtering,
      for SQL generation an encoder function is required that maps the different filter types to
      SQL, also a dictionary mapping from property name to column name is required (for example to
      map 'project' to 'database' or 'dataset' to 'schema'.
    - The order in which elements are added to the list is not relevant, priority is assigned based
      on the effect, rules with the default effect have the higher priority and stop the iteration.

    Filtering works by prioritizing "explicit" filters, a filter is considered explicit when it is
    configured with the same effect used as the default in the list.
    Let's suppose we have the following sample data:
    - prj_1: ds_1, ds_2
    - prj_2: ds_1, ds_2, ds_3
    - project_3: dataset_1, ds_2
    _ project_4: dataset_4

    These are examples using the sample data above:
    - list(default=allow): block(prj_*), allow(prj_1)
        - will allow prj_1 and all those projects not matching prj_*
        - allowed: (prj_1, all datasets), (project_3, all datasets), (project_4, all datasets)
    - list(default=block): allow(prj_*), block(prj_1)
        - will allow only prj_* except prj_1 that is explicitly blocked
        - allowed: (prj_2, all datasets)
    - list(default=allow): allow(*), block(prj_1)
        - will allow everything, allow(*) is considered a explicit rule and has the highest priority
        - allowed: everything
    - list(default=allow): block(*), allow(prj_1)
        - will allow only prj_1
        - allowed: (prj_1, all datasets)
    - list(default=allow): block(*, ds_*)
        - will block all datasets named ds_*
        - allowed: (project_3, dataset_1), (project_4, all datasets)
    - list(default=allow): allow(prj_2, ds_3), block(*, ds_*)
        - will block all datasets named ds_3 except for ds_3 in prj_2 that is explicitly allowed.
          Please note order is not relevant and the result would be exactly the same for:
            block(*, ds_*), allow(prj_2, ds_3)
        - allowed: (prj_2, ds_3), (project_3, dataset_1), (project_4, all datasets)
    - list(default=block): allow(*, dataset_*):
        - only datasets named dataset_* will be allowed
        - allowed: (project_3, dataset_1), (project_4, dataset_4)
    """

    metadata_filters: AllowBlockList = field(default_factory=AllowBlockList)

    @property
    def is_metadata_filtered(self) -> bool:
        return bool(self.metadata_filters.filters)

    @property
    def is_metadata_blocked(self):
        """
        Helper method for detecting an edge case where everything is blocked because the default
        effect is block and all filters are also blocking, in this case it doesn't make sense to
        run queries or filter data.
        """
        return self.metadata_filters.default_effect == FilterEffectType.BLOCK and all(
            f.effect == FilterEffectType.BLOCK for f in self.metadata_filters.filters
        )

    def is_project_with_datasets_filtered(self, project: str) -> bool:
        """
        Returns True if there's at least one filter configured for the specified project filtering
        on datasets, this can be used to check if get_sql_conditions for the project is going to
        return an empty query or not.
        """
        return self.is_metadata_filtered and any(
            f.matches(project=project) and f.dataset is not None
            for f in self.metadata_filters.filters
        )

    def is_whole_project_blocked(self, project: str) -> bool:
        """
        Helper method to be used when projects are iterated first, returns True if the project is
        fully blocked (blocked by a filter with no dataset or blocked by default) False otherwise.
        For example for a list like: list(default=allow): block(prj_1, ds_1) this method will return
        False as prj_1 is not fully blocked, there might be a (prj_1, ds_2) that is allowed, so
        prj_1 still needs to be iterated.
        """
        # the condition parameter below is to include blocking filters only if applied to the
        # whole project, we don't want to exclude a project just because a dataset is excluded
        effect = self._get_effect(
            metadata_filters=self.metadata_filters,
            force_regexp=False,
            condition=lambda f: not f.dataset or f.effect == FilterEffectType.ALLOW,
            project=project,
        )
        return effect == FilterEffectType.BLOCK

    def is_metadata_element_allowed(self, **kwargs: Any) -> bool:
        """
        Metadata elements filtering, iterates all filters looking for a match, if there's
        a match in a filter with the default effect, it's considered an explicit filter
        and search stops with the result effect being the default one.
        If there's a match in a filter not configured with the default effect, search continues.
        When the search completes, if there was a match then the result effect will be the one in
        the matched filter (must be the non-default effect).
        If there was no match the result effect will be the default one.
        Result for this method is True only if the result effect is ALLOW.

        Data is matched using properties specified in kwargs, the following keys are supported
        in kwargs: 'project', 'dataset', 'table', 'table_type'.
        """
        effect = self._get_effect(
            metadata_filters=self.metadata_filters, force_regexp=False, **kwargs
        )
        return effect == FilterEffectType.ALLOW

    @staticmethod
    def _get_effect(
        metadata_filters: AllowBlockList,
        force_regexp: bool,
        condition: Optional[Callable[[MetadataFilter], bool]] = None,
        **kwargs: Any,
    ) -> FilterEffectType:
        """
        Returns the effect for a metadata element with the properties specified by kwargs
        (project, dataset, table, table_type).
        If there's an explicit filter matching (a filter is explicit if the effect is the default
        one) then default effect is returned.
        If there's a match in the "other effect" list then the "other effect" is returned, we're
        calling "other effect" to the effect that is not the default one.
        If no matching filter, the default effect is returned.
        """
        if not metadata_filters.filters or any(
            f.matches(force_regexp, **kwargs)
            for f in metadata_filters.get_default_effect_filters(condition=condition)
        ):
            return metadata_filters.default_effect

        if any(
            f.matches(force_regexp, **kwargs)
            for f in metadata_filters.get_other_effect_filters(condition=condition)
        ):
            return metadata_filters.other_effect

        return metadata_filters.default_effect

    def is_dataset_allowed(self, project: Optional[str], dataset: str) -> bool:
        """
        Helper method intended to be used when projects and datasets are iterated in memory.
        It returns True if the dataset in the given project is allowed (not blocked), this is
        equivalent to call is_metadata_element_allowed(project=project, dataset=dataset)
        """
        return self.is_metadata_element_allowed(project=project, dataset=dataset)

    def get_sql_conditions(
        self,
        column_mapping: Dict,  # maps project and dataset to the column name to use
        encoder: Callable[[str, str, FilterType], str],
        project: Optional[str] = None,
    ) -> Optional[str]:
        """
        Helper method that returns a SQL query fragment with conditions for the current filters.
        If project is specified this will return conditions only for the specified project and this
        is supposed to be called after checking that is_project_with_datasets_filtered and
        is_project_allowed returned True.
        column_mapping is used to map filter fields (like project and dataset) to the actual
        database columns (like database and schema).
        encoder is used to encode a filter in the SQL dialect, it needs to encode to expressions
        like "database = 'db_1'" or "database REGEXP 'db_.*'".
        Examples:
            - default=block, filters=allow(project=x_*), block(project=x_1), allow(project=z)
                SQL: NOT(project='x_1') AND ((project REGEXP 'x_*') OR (project='z')
            - default=allow, filters=block(project=x_*), allow(project=x_1), block(project=z)
                SQL: project='x_1' OR (NOT(project REGEXP 'x_*') AND NOT(project='z'))
        Basically we first put all filters matching the default condition joined by
        AND/OR (block/allow), and then all filters with the other effect joined by OR/AND.
        """
        if not self.metadata_filters.filters:
            return None
        if project and not self.is_project_with_datasets_filtered(project):
            return None

        def project_condition(f: MetadataFilter):
            return not project or f.matches(project=project)

        default_effect = self.metadata_filters.default_effect
        default_effect_filters = self.metadata_filters.get_default_effect_filters(
            condition=project_condition
        )
        other_effect_filters = self.metadata_filters.get_other_effect_filters(
            condition=project_condition
        )
        default_effect_op = " OR " if default_effect == FilterEffectType.ALLOW else " AND "
        other_effect_op = " AND " if default_effect == FilterEffectType.ALLOW else " OR "

        default_effect_conditions = default_effect_op.join(
            [
                f"({self._get_sql_field_condition(f, column_mapping, encoder)})"
                for f in default_effect_filters
            ]
        )
        other_effect_conditions = other_effect_op.join(
            [
                self._get_sql_field_condition(f, column_mapping, encoder)
                for f in other_effect_filters
            ]
        )
        conditions = default_effect_conditions
        if conditions and other_effect_conditions:
            conditions += default_effect_op
            conditions += "(" + other_effect_conditions + ")"
        elif not conditions:
            conditions = other_effect_conditions
        return f"({conditions})" if conditions else ""

    @staticmethod
    def _get_sql_field_condition(
        mf: MetadataFilter,
        column_mapping: Dict,
        encoder: Callable[[str, str, FilterType], str],
    ):
        # The comparison is performed case-insensitive (check MetadataFilter._safe_match)
        # We can use LOWER here since it is part of standard SQL (like AND/OR/NOT), so including it
        # here is a way to make sure that all comparisons are case-insensitive in the SQL sentences
        # for all engines
        conditions = " AND ".join(
            [
                encoder(
                    f"LOWER({column})",
                    getattr(mf, field).lower(),
                    mf.type if field == mf.filter_type_target_field() else FilterType.EXACT_MATCH,
                )
                for (field, column) in column_mapping.items()
                if getattr(mf, field) is not None
            ]
        )
        return f"NOT({conditions})" if mf.effect == FilterEffectType.BLOCK else f"({conditions})"
