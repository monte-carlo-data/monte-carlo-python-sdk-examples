# Instructions:
# 1. Run this script: python admin/bulk_domain_exporter.py
# 2. Input your API Key ID and Token (generated in Settings -> API within MC UI)
# 3. Input the name of the CSV file you would like to create
# 
# Output CSV format (no header):
#   Finance,MCON++123++456++table++transactions
#   Analytics,MCON++123++456++view++sessions
#
# Note: The output CSV can be used with bulk_domain_importer.py to migrate domains to another workspace.

from pycarlo.core import Client, Query, Session
import csv

def get_domains(client):
    """Get all domains with their assets."""
    query = Query()
    query.get_all_domains().__fields__("name", "uuid", "description")
    return client(query).get_all_domains

def get_domain_assets(client, domain_uuid, batch_size=1000):
    """Get all asset MCONs for a domain."""
    mcons = []
    cursor = None
    while True:
        query = Query()
        get_tables = query.get_tables(first=batch_size, domain_id=domain_uuid, is_deleted=False,
                                       **(dict(after=cursor) if cursor else {}))
        get_tables.edges.node.__fields__("mcon")
        get_tables.page_info.__fields__("has_next_page", end_cursor=True)
        
        response = client(query).get_tables
        for table in response.edges:
            mcons.append(table.node.mcon)
        
        if response.page_info.has_next_page:
            cursor = response.page_info.end_cursor
        else:
            break
    return mcons

def export_domains(client, output_file):
    """Export all domains and their assets to CSV."""
    domains = get_domains(client)
    print(f"Found {len(domains)} domains")
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        for domain in domains:
            print(f"Processing: {domain.name}")
            assets = get_domain_assets(client, domain.uuid)
            
            if assets:
                for mcon in assets:
                    writer.writerow([domain.name, mcon])
            else:
                # Include empty domains
                writer.writerow([domain.name, ''])
    
    print(f"Export complete: {output_file}")

if __name__ == '__main__':
    mcd_id = input("MCD ID: ")
    mcd_token = input("MCD Token: ")
    output_file = input("Output CSV filename: ")
    
    client = Client(session=Session(mcd_id=mcd_id, mcd_token=mcd_token))
    export_domains(client, output_file)
