# Instructions:
# 1. Run this script: python admin/bulk_blocklist_importer.py
# 2. Input your API Key ID and Token (generated in Settings -> API within MC UI)
# 3. Input the path to the CSV file from bulk_blocklist_exporter.py
#
# Input CSV format (with header):
#   resource_id,target_object_type,match_type,dataset,project,effect
#   abc123-uuid,dataset,exact_match,my_dataset,my_project,block
#
# Notes:
# - resource_id: The warehouse/connection UUID
# - target_object_type: dataset, project, schema, table (lowercase)
# - match_type: exact_match or wildcard
# - dataset: Dataset/database name
# - project: Project name (required for dataset blocks as parent)
# - effect: block or allow

from pycarlo.core import Client, Session
import csv
from collections import defaultdict


def get_existing_blocklist_entries(client, batch_size=500):
    """Get all existing blocklist entries with pagination."""
    entries = []
    cursor = None
    
    while True:
        after_clause = f', after: "{cursor}"' if cursor else ''
        
        query = f"""
        query GetCollectionBlockList {{
            getCollectionBlockList(
                first: {batch_size}{after_clause}
            ) {{
                edges {{
                    cursor
                    node {{
                        id
                        matchType
                        project
                        targetObjectType
                        effect
                        dataset
                        resourceId
                    }}
                }}
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
            }}
        }}"""
        
        response = client(query).get_collection_block_list
        
        if response and response.edges:
            for edge in response.edges:
                node = edge.node
                entries.append({
                    'id': node.id,
                    'resource_id': node.resource_id,
                    'target_object_type': node.target_object_type,
                    'match_type': node.match_type,
                    'dataset': node.dataset or '',
                    'project': node.project or '',
                    'effect': node.effect or ''
                })
            
            if response.page_info.has_next_page:
                cursor = response.page_info.end_cursor
            else:
                break
        else:
            break
    
    return entries


def modify_blocklist_entries(client, resource_id, target_object_type, entries):
    """Add blocklist entries using the modifyCollectionBlockList mutation.
    
    The API requires:
    - parentScope: identifies the parent hierarchy (resourceId, and optionally project/dataset)
    - targetObjectType: the type of objects being blocked (project, dataset, etc.)
    - collectionBlocks: array of block entries
    
    parentScope hierarchy:
    - project blocks: { resourceId }
    - dataset blocks: { resourceId, project }
    - schema blocks: { resourceId, project, dataset }
    - table blocks: { resourceId, project, dataset, schema }
    """
    mutation = """
    mutation modifyCollectionBlockList(
        $collectionBlocks: [ModifyCollectionBlockListInput]!,
        $parentScope: CollectionBlockListParentScopeInput!,
        $targetObjectType: CollectionPreferenceTargetObjectType!
    ) {
        modifyCollectionBlockList(
            collectionBlocks: $collectionBlocks,
            parentScope: $parentScope,
            targetObjectType: $targetObjectType
        ) {
            id
            resourceId
            targetObjectType
            matchType
            effect
        }
    }
    """
    
    # Build the collection blocks array and determine parent scope
    collection_blocks = []
    parent_scope = {"resourceId": resource_id}
    
    for entry in entries:
        block_input = {
            "resourceId": resource_id,
            "matchType": entry['match_type'],
            "effect": entry['effect'] or 'block'
        }
        
        # Always include project and dataset in block_input if available
        if entry.get('project'):
            block_input["project"] = entry['project']
        if entry.get('dataset'):
            block_input["dataset"] = entry['dataset']
        
        # Build parentScope based on targetObjectType
        if target_object_type == 'dataset' and entry.get('project'):
            parent_scope["project"] = entry['project']
        elif target_object_type == 'schema':
            if entry.get('project'):
                parent_scope["project"] = entry['project']
            if entry.get('dataset'):
                parent_scope["dataset"] = entry['dataset']
        elif target_object_type == 'table':
            if entry.get('project'):
                parent_scope["project"] = entry['project']
            if entry.get('dataset'):
                parent_scope["dataset"] = entry['dataset']
        
        collection_blocks.append(block_input)
    
    variables = {
        "collectionBlocks": collection_blocks,
        "parentScope": parent_scope,
        "targetObjectType": target_object_type
    }
    
    response = client(mutation, variables=variables)
    return response.modify_collection_block_list


def import_blocklist(client, input_file):
    """Import blocklist entries from CSV."""
    # Get existing blocklist entries for reference
    existing_entries = get_existing_blocklist_entries(client)
    print(f"Found {len(existing_entries)} existing blocklist entries")
    
    # Create a set of existing entries for duplicate detection
    existing_keys = set()
    for entry in existing_entries:
        key = (
            entry['resource_id'],
            entry['target_object_type'],
            entry['match_type'],
            entry['dataset'],
            entry['project']
        )
        existing_keys.add(key)
    
    # Read CSV data and group by (resource_id, target_object_type)
    # Each API call can only handle one target_object_type at a time
    entries_by_resource_and_type = defaultdict(list)
    with open(input_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            entry = {
                'resource_id': row['resource_id'].strip(),
                'target_object_type': row['target_object_type'].strip(),
                'match_type': row['match_type'].strip(),
                'dataset': row.get('dataset', '').strip() or None,
                'project': row.get('project', '').strip() or None,
                'effect': row.get('effect', '').strip() or None
            }
            # Group by both resource_id and target_object_type
            key = (entry['resource_id'], entry['target_object_type'])
            entries_by_resource_and_type[key].append(entry)
    
    total_entries = sum(len(entries) for entries in entries_by_resource_and_type.values())
    print(f"Processing {total_entries} entries from CSV")
    print(f"Grouped into {len(entries_by_resource_and_type)} (resource, type) group(s)")
    
    success_count = 0
    skip_count = 0
    fail_count = 0
    
    for (resource_id, target_object_type), entries in entries_by_resource_and_type.items():
        # Filter entries - skip duplicates
        entries_to_process = []
        for entry in entries:
            entry_key = (
                entry['resource_id'],
                entry['target_object_type'],
                entry['match_type'],
                entry['dataset'] or '',
                entry['project'] or ''
            )
            
            if entry_key in existing_keys:
                print(f"  - SKIP (already exists): {entry['project'] or entry['dataset']} ({entry['target_object_type']})")
                skip_count += 1
                continue
            
            entries_to_process.append(entry)
        
        if not entries_to_process:
            continue
        
        try:
            response = modify_blocklist_entries(
                client,
                resource_id=resource_id,
                target_object_type=target_object_type,
                entries=entries_to_process
            )
            
            if response:
                for entry in entries_to_process:
                    print(f"  - ADDED: {entry['project'] or entry['dataset']} ({entry['target_object_type']}) - SUCCESS")
                    success_count += 1
                    entry_key = (
                        entry['resource_id'],
                        entry['target_object_type'],
                        entry['match_type'],
                        entry['dataset'] or '',
                        entry['project'] or ''
                    )
                    existing_keys.add(entry_key)
            else:
                for entry in entries_to_process:
                    print(f"  - FAILED: {entry['project'] or entry['dataset']} ({entry['target_object_type']}) - no response")
                    fail_count += 1
                
        except Exception as e:
            for entry in entries_to_process:
                print(f"  - FAILED: {entry['project'] or entry['dataset']} ({entry['target_object_type']}) - {e}")
                fail_count += 1
    
    print(f"\nImport complete:")
    print(f"  - Successful: {success_count}")
    print(f"  - Skipped: {skip_count}")
    print(f"  - Failed: {fail_count}")


if __name__ == '__main__':
    mcd_id = input("MCD ID: ")
    mcd_token = input("MCD Token: ")
    input_file = input("Input CSV filename: ")
    
    if not input_file:
        print("Input CSV filename is required.")
    else:
        client = Client(session=Session(mcd_id=mcd_id, mcd_token=mcd_token))
        import_blocklist(client, input_file)

