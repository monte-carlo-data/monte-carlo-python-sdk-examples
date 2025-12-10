# Instructions:
# 1. Run this script: python admin/bulk_data_product_importer.py
# 2. Input your API Key ID and Token (generated in Settings -> API within MC UI)
# 3. Input the path to the CSV file from bulk_data_product_exporter.py
#
# Input CSV format (with header):
#   data_product_name,data_product_description,asset_mcon
#   Customer Analytics,Analytics for customer data,MCON++123++456++table++customers
#   Revenue Dashboard,Revenue KPIs,MCON++123++456++view++orders

from pycarlo.core import Client, Query, Session
import csv


def get_existing_data_products(client):
    """Get all existing data products."""
    query = Query()
    get_data_products = query.get_data_products()
    get_data_products.__fields__("name", "uuid", "description", "is_deleted")
    return client(query).get_data_products


def create_or_update_data_product(client, name, description=None, uuid=None):
    """Create or update a data product (without assets)."""
    mutation = """
    mutation createOrUpdateDataProduct($name: String!, $description: String, $uuid: UUID) {
        createOrUpdateDataProduct(name: $name, description: $description, uuid: $uuid) {
            dataProduct {
                uuid
                name
            }
        }
    }
    """
    
    variables = {"name": name}
    if description:
        variables["description"] = description
    if uuid:
        variables["uuid"] = uuid
    
    response = client(mutation, variables=variables)
    return response.create_or_update_data_product


def set_data_product_assets(client, data_product_id, mcons):
    """Set assets for a data product using setDataProductAssets mutation."""
    if not mcons:
        return None
    
    mutation = """
    mutation setDataProductAssets($dataProductId: UUID!, $mcons: [String!]!) {
        setDataProductAssets(dataProductId: $dataProductId, mcons: $mcons) {
            dataProduct {
                uuid
                name
            }
        }
    }
    """
    
    variables = {
        "dataProductId": data_product_id,
        "mcons": mcons
    }
    
    response = client(mutation, variables=variables)
    return response.set_data_product_assets


def import_data_products(client, input_file):
    """Import data products from CSV."""
    # Get existing data products for matching
    existing_dps = get_existing_data_products(client)
    dp_map = {dp.name: dp.uuid for dp in existing_dps if not dp.is_deleted}
    print(f"Found {len(dp_map)} existing data products")
    
    # Read and group CSV data by data product
    data_products = {}
    with open(input_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            name = row['data_product_name'].strip()
            desc = row.get('data_product_description', '').strip()
            mcon = row['asset_mcon'].strip()
            
            if name not in data_products:
                data_products[name] = {'description': desc, 'mcons': []}
            if mcon:
                data_products[name]['mcons'].append(mcon)
    
    print(f"Processing {len(data_products)} data products from CSV")
    
    # Create/update each data product
    for name, info in data_products.items():
        existing_uuid = dp_map.get(name)
        
        # Step 1: Create or update the data product
        response = create_or_update_data_product(
            client, 
            name, 
            info['description'] or None,
            existing_uuid
        )
        
        if response and response.data_product:
            dp = response.data_product
            action = 'updated' if existing_uuid else 'created'
            print(f"  - {dp.name}: {action} ({dp.uuid})")
            
            # Step 2: Set assets for the data product
            if info['mcons']:
                try:
                    set_data_product_assets(client, dp.uuid, info['mcons'])
                    print(f"    Assigned {len(info['mcons'])} assets")
                except Exception as e:
                    print(f"    Failed to assign assets: {e}")
        else:
            print(f"  - {name}: FAILED to create/update")
    
    print("Import complete")


if __name__ == '__main__':
    mcd_id = input("MCD ID: ")
    mcd_token = input("MCD Token: ")
    input_file = input("Input CSV filename: ")
    
    client = Client(session=Session(mcd_id=mcd_id, mcd_token=mcd_token))
    import_data_products(client, input_file)
