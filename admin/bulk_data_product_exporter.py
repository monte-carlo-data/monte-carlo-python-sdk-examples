# Instructions:
# 1. Run this script: python admin/bulk_data_product_exporter.py
# 2. Input your API Key ID and Token (generated in Settings -> API within MC UI)
# 3. Input the name of the CSV file you would like to create
#
# Output CSV format:
#   data_product_name,data_product_description,asset_mcon
#   Customer Analytics,Analytics for customer data,MCON++123++456++table++customers
#   Revenue Dashboard,Revenue KPIs,MCON++123++456++view++orders
#
# Note: The output CSV can be used with a bulk_data_product_importer.py to migrate data products to another workspace.

from pycarlo.core import Client, Query, Session
import csv


def get_data_products(client):
    """Get all data products with basic info."""
    query = Query()
    get_data_products = query.get_data_products()
    get_data_products.__fields__("name", "uuid", "description", "is_deleted")
    return client(query).get_data_products


def get_data_product_assets(client, data_product_uuid, batch_size=1000):
    """Get all asset MCONs for a data product with pagination using getDataProductV2."""
    mcons = []
    cursor = None
    
    while True:
        after_clause = f', after: "{cursor}"' if cursor else ''
        query = f"""
        query getDataProductAssets {{
            getDataProductV2(dataProductId: "{data_product_uuid}") {{
                uuid
                assets(first: {batch_size}{after_clause}) {{
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                    edges {{
                        node {{
                            mcon
                        }}
                    }}
                }}
            }}
        }}"""
        
        response = client(query).get_data_product_v2
        
        if response and response.assets:
            for edge in response.assets.edges:
                mcons.append(edge.node.mcon)
            
            if response.assets.page_info.has_next_page:
                cursor = response.assets.page_info.end_cursor
            else:
                break
        else:
            break
    
    return mcons


def export_data_products(client, output_file):
    """Export all data products and their assets to CSV."""
    data_products = get_data_products(client)
    active_dps = [dp for dp in data_products if not dp.is_deleted]
    print(f"Found {len(active_dps)} active data products")
    
    rows_to_write = []
    
    for dp in active_dps:
        assets = get_data_product_assets(client, dp.uuid)
        print(f"  - {dp.name}: {len(assets)} assets")
        
        if assets:
            for mcon in assets:
                rows_to_write.append([dp.name, dp.description or '', mcon])
        else:
            rows_to_write.append([dp.name, dp.description or '', ''])
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['data_product_name', 'data_product_description', 'asset_mcon'])
        for row in rows_to_write:
            writer.writerow(row)
    
    print(f"Export complete: {output_file} ({len(rows_to_write)} rows)")


if __name__ == '__main__':
    mcd_id = input("MCD ID: ")
    mcd_token = input("MCD Token: ")
    output_file = input("Output CSV filename: ")
    
    client = Client(session=Session(mcd_id=mcd_id, mcd_token=mcd_token))
    export_data_products(client, output_file)
