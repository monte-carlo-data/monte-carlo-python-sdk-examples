# Instructions:
# 1. Run this script: python admin/bulk_blocklist_exporter.py
# 2. Input your API Key ID and Token (generated in Settings -> API within MC UI)
# 3. Input the name of the CSV file you would like to create
#
# Output CSV format:
#   resource_id,target_object_type,match_type,dataset,project,effect
#   abc123-uuid,TABLE,EXACT,my_dataset,my_project,BLOCK
#
# Note: The output CSV can be used with a bulk_blocklist_importer.py to migrate blocklist entries to another workspace.

from pycarlo.core import Client, Session
import csv


def get_blocklist_entries(client, batch_size=500):
    """Get all blocklist entries with pagination."""
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


def export_blocklist(client, output_file):
    """Export all blocklist entries to CSV."""
    print("Fetching blocklist entries...")
    entries = get_blocklist_entries(client)
    print(f"Found {len(entries)} blocklist entries")
    
    if not entries:
        print("No blocklist entries found.")
        return
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['resource_id', 'target_object_type', 'match_type', 'dataset', 'project', 'effect'])
        
        for entry in entries:
            writer.writerow([
                entry['resource_id'],
                entry['target_object_type'],
                entry['match_type'],
                entry['dataset'],
                entry['project'],
                entry['effect']
            ])
    
    print(f"Export complete: {output_file} ({len(entries)} rows)")


if __name__ == '__main__':
    mcd_id = input("MCD ID: ")
    mcd_token = input("MCD Token: ")
    output_file = input("Output CSV filename: ")
    
    if not output_file:
        print("Output CSV filename is required.")
    else:
        client = Client(session=Session(mcd_id=mcd_id, mcd_token=mcd_token))
        export_blocklist(client, output_file)

