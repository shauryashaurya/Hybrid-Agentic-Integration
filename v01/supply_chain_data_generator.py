# supply_chain_data_generator.py
# Generates complex multi-echelon supply chain data with realistic inconsistencies
# Simulates data from manufacturers, distributors, retailers, and logistics providers

import csv
import json
import random
import os
from datetime import datetime, timedelta
from faker import Faker
import uuid

# Initialize generators
fake = Faker()
Faker.seed(43)
random.seed(43)

# Create supply chain data directory
os.makedirs('supply_chain_data', exist_ok=True)


def generate_manufacturers_data():
    # Manufacturing plant data with production records
    # Different plants use different naming conventions

    # Plant A - Uses technical codes
    headers_a = ['PLT_ID', 'PRD_CD', 'BTCH_NO',
                 'MFG_DT', 'QTY', 'QC_STS', 'COST_PER_UNIT']
    rows_a = []

    for i in range(200):
        rows_a.append([
            'PLA001',
            f'SKU{random.randint(1000, 1099)}',
            f'BTH{datetime.now().strftime("%Y%m")}{i:04d}',
            fake.date_between(start_date='-60d',
                              end_date='today').strftime('%Y%m%d'),
            random.randint(100, 5000),
            random.choice(['PASS', 'FAIL', 'REWORK']),
            round(random.uniform(10.0, 150.0), 2)
        ])

    with open('supply_chain_data/plant_a_production.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers_a)
        writer.writerows(rows_a)

    # Plant B - Uses business-friendly names
    headers_b = ['facility_name', 'product_name', 'batch_id',
                 'production_date', 'units_produced', 'quality_check', 'unit_cost']
    rows_b = []

    for i in range(200):
        rows_b.append([
            'Shanghai Manufacturing Center',
            f'Product_{random.randint(1000, 1099)}',
            f'{uuid.uuid4().hex[:8]}',
            fake.date_between(start_date='-60d',
                              end_date='today').strftime('%m/%d/%Y'),
            random.randint(100, 5000),
            random.choice(['Approved', 'Rejected', 'Pending']),
            f'${round(random.uniform(10.0, 150.0), 2)}'
        ])

    with open('supply_chain_data/plant_b_production.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers_b)
        writer.writerows(rows_b)

    return ['supply_chain_data/plant_a_production.csv', 'supply_chain_data/plant_b_production.csv']


def generate_inventory_data():
    # Warehouse inventory from different systems

    # WMS System 1 - Legacy format
    headers_1 = ['WH', 'LOC', 'ITEM', 'OH_QTY',
                 'ALLOC_QTY', 'AVAIL_QTY', 'LST_CNT_DT']
    rows_1 = []

    for i in range(500):
        oh_qty = random.randint(0, 10000)
        alloc_qty = random.randint(0, min(oh_qty, 5000))
        rows_1.append([
            f'WH{random.randint(1, 5):02d}',
            f'{random.choice(["A", "B", "C", "D"])}{random.randint(1, 99):02d}-{random.randint(1, 20):02d}',
            f'SKU{random.randint(1000, 1099)}',
            oh_qty,
            alloc_qty,
            oh_qty - alloc_qty,
            fake.date_between(start_date='-30d',
                              end_date='today').strftime('%y%m%d')
        ])

    with open('supply_chain_data/wms_inventory.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers_1)
        writer.writerows(rows_1)

    # Modern WMS - JSON format from API
    inventory_json = {
        'timestamp': datetime.now().isoformat(),
        'warehouses': []
    }

    for wh_id in range(1, 6):
        warehouse = {
            'warehouse_id': f'DC-{wh_id:03d}',
            'name': f'{fake.city()} Distribution Center',
            'inventory': []
        }

        for i in range(100):
            item = {
                'sku': f'Product_{random.randint(1000, 1099)}',
                'location': {
                    'zone': random.choice(['A', 'B', 'C', 'D']),
                    'rack': random.randint(1, 99),
                    'shelf': random.randint(1, 20)
                },
                'quantities': {
                    'on_hand': random.randint(0, 10000),
                    'available': random.randint(0, 8000),
                    'reserved': random.randint(0, 2000),
                    'in_transit': random.randint(0, 1000)
                },
                'last_cycle_count': fake.date_time_between(start_date='-30d', end_date='now').isoformat()
            }
            warehouse['inventory'].append(item)

        inventory_json['warehouses'].append(warehouse)

    with open('supply_chain_data/modern_wms_inventory.json', 'w') as f:
        json.dump(inventory_json, f, indent=2)

    return ['supply_chain_data/wms_inventory.csv', 'supply_chain_data/modern_wms_inventory.json']


def generate_logistics_data():
    # Shipment and transportation data from different carriers

    # Carrier 1 - Tab-delimited format
    headers = [
        'SHIPMENT_ID\tORIGIN\tDESTINATION\tCARRIER\tMODE\tSTATUS\tETD\tETA\tACTUAL_DELIVERY']
    rows = []

    for i in range(300):
        etd = fake.date_between(start_date='-30d', end_date='+30d')
        eta = etd + timedelta(days=random.randint(1, 7))
        actual = eta + timedelta(days=random.randint(-2, 3)
                                 ) if random.random() > 0.3 else None

        row = '\t'.join([
            f'SHP{8000 + i}',
            f'WH{random.randint(1, 5):02d}',
            fake.city(),
            random.choice(['FedEx', 'UPS', 'DHL', 'CARRIER_X']),
            random.choice(['AIR', 'GROUND', 'OCEAN', 'RAIL']),
            random.choice(['IN_TRANSIT', 'DELIVERED', 'DELAYED', 'PENDING']),
            etd.strftime('%Y-%m-%d'),
            eta.strftime('%Y-%m-%d'),
            actual.strftime('%Y-%m-%d') if actual else 'NULL'
        ])
        rows.append(row)

    with open('supply_chain_data/carrier_shipments.tsv', 'w') as f:
        f.write(headers[0] + '\n')
        for row in rows:
            f.write(row + '\n')

    # Carrier 2 - XML format (EDI simulation)
    xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n<SHIPMENTS>\n'

    for i in range(100):
        xml_content += f'''  <SHIPMENT>
    <ID>{uuid.uuid4().hex[:12].upper()}</ID>
    <FROM>{f"DC-{random.randint(1, 5):03d}"}</FROM>
    <TO>{fake.address().replace(chr(10), ", ")}</TO>
    <ITEMS>
      <ITEM>
        <SKU>SKU{random.randint(1000, 1099)}</SKU>
        <QTY>{random.randint(1, 100)}</QTY>
      </ITEM>
    </ITEMS>
    <TRACKING>{random.choice(["1Z", ""])}{fake.numerify("###########")}</TRACKING>
    <STATUS>{random.randint(1, 5)}</STATUS>
  </SHIPMENT>
'''

    xml_content += '</SHIPMENTS>'

    with open('supply_chain_data/edi_shipments.xml', 'w') as f:
        f.write(xml_content)

    return ['supply_chain_data/carrier_shipments.tsv', 'supply_chain_data/edi_shipments.xml']


def generate_supplier_data():
    # Supplier master data and purchase orders

    # Supplier master - inconsistent formats
    headers = ['SUPP_ID', 'SUPP_NAME', 'CONTACT',
               'LEAD_TIME_DAYS', 'MIN_ORDER_QTY', 'PAYMENT_TERMS']
    rows = []

    for i in range(50):
        rows.append([
            f'SUP{4000 + i}',
            fake.company(),
            fake.email(),
            random.randint(1, 30),
            # Inconsistent MOQ format
            random.choice([100, 500, 1000, 'MOQ:250', 'Min: 50 units']),
            random.choice(
                ['NET30', 'Net 30', '30 days', 'NET45', '2/10 NET30'])
        ])

    with open('supply_chain_data/supplier_master.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    # Purchase orders with varying formats
    po_data = {
        'purchase_orders': []
    }

    for i in range(200):
        po = {
            'po_number': f'PO{datetime.now().year}{i:05d}',
            'supplier': f'SUP{4000 + random.randint(0, 49)}',
            'order_date': fake.date_between(start_date='-60d', end_date='today').isoformat(),
            'items': [],
            'total_value': 0
        }

        num_items = random.randint(1, 5)
        for j in range(num_items):
            item_value = round(random.uniform(100, 5000), 2)
            po['items'].append({
                'line': j + 1,
                'material': f'SKU{random.randint(1000, 1099)}',
                'quantity': random.randint(10, 1000),
                'unit_price': round(item_value / random.randint(10, 100), 2),
                'total': item_value
            })
            po['total_value'] += item_value

        po['total_value'] = round(po['total_value'], 2)
        po_data['purchase_orders'].append(po)

    with open('supply_chain_data/purchase_orders.json', 'w') as f:
        json.dump(po_data, f, indent=2)

    return ['supply_chain_data/supplier_master.csv', 'supply_chain_data/purchase_orders.json']


def generate_demand_forecast():
    # Demand planning data with different time granularities

    # Weekly forecast
    headers = ['WEEK_START', 'SKU', 'FORECAST_QTY',
               'ACTUAL_QTY', 'ACCURACY', 'MODEL_VERSION']
    rows = []

    start_date = datetime.now() - timedelta(weeks=12)

    for week in range(12):
        week_start = start_date + timedelta(weeks=week)
        for sku in range(1000, 1020):
            forecast = random.randint(100, 2000)
            # No actuals for future weeks
            actual = forecast + random.randint(-200, 200) if week < 8 else None
            accuracy = abs(1 - abs(actual - forecast) /
                           forecast) * 100 if actual else None

            rows.append([
                week_start.strftime('%Y-%m-%d'),
                f'SKU{sku}',
                forecast,
                actual if actual else '',
                f'{accuracy:.1f}%' if accuracy else '',
                f'v{random.choice(["2.1", "2.2", "3.0"])}'
            ])

    with open('supply_chain_data/demand_forecast_weekly.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    # Monthly aggregated forecast (different format)
    monthly_data = []

    for month in range(6):
        month_date = datetime.now() - timedelta(days=30 * (6 - month))
        for sku in range(1000, 1020):
            monthly_data.append({
                'period': month_date.strftime('%b %Y'),
                # Different naming convention
                'product_code': f'Product_{sku}',
                'predicted_demand': random.randint(400, 8000),
                'confidence_interval': {
                    'lower': random.randint(300, 3000),
                    'upper': random.randint(5000, 10000)
                },
                'seasonality_factor': round(random.uniform(0.8, 1.3), 2)
            })

    with open('supply_chain_data/demand_forecast_monthly.json', 'w') as f:
        json.dump({'forecasts': monthly_data}, f, indent=2)

    return ['supply_chain_data/demand_forecast_weekly.csv', 'supply_chain_data/demand_forecast_monthly.json']


def generate_quality_metrics():
    # Quality control data from different inspection systems

    # Inspection system 1 - Detailed defects
    headers = ['INSPECTION_ID', 'BATCH_NO', 'INSPECTOR', 'DATE',
               'DEFECT_CODE', 'SEVERITY', 'QTY_INSPECTED', 'QTY_DEFECTIVE']
    rows = []

    defect_codes = ['SCR001', 'DNT002', 'CLR003', 'DIM004', 'PKG005', 'LBL006']

    for i in range(300):
        rows.append([
            f'INSP{10000 + i}',
            f'BTH{datetime.now().strftime("%Y%m")}{random.randint(0, 200):04d}',
            f'QC{random.randint(100, 199)}',
            fake.date_between(start_date='-30d',
                              end_date='today').strftime('%Y-%m-%d'),
            random.choice(defect_codes),
            random.choice(['CRITICAL', 'MAJOR', 'MINOR']),
            random.randint(100, 1000),
            random.randint(0, 50)
        ])

    with open('supply_chain_data/quality_inspections.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    return ['supply_chain_data/quality_inspections.csv']


def generate_supply_chain_kpis():
    # Aggregated KPI data from different systems
    kpi_data = {
        'reporting_period': datetime.now().strftime('%Y-%m'),
        'kpis': {
            'inventory_metrics': {
                'inventory_turnover': round(random.uniform(4.0, 12.0), 2),
                'days_inventory_outstanding': random.randint(20, 60),
                'stockout_rate': round(random.uniform(0.01, 0.05), 3),
                'excess_inventory_percentage': round(random.uniform(0.05, 0.15), 3)
            },
            'fulfillment_metrics': {
                'order_fill_rate': round(random.uniform(0.92, 0.99), 3),
                'on_time_delivery': round(random.uniform(0.85, 0.98), 3),
                'perfect_order_rate': round(random.uniform(0.80, 0.95), 3),
                'average_fulfillment_time_hours': round(random.uniform(24, 72), 1)
            },
            'supplier_metrics': {
                'supplier_defect_rate': round(random.uniform(0.001, 0.02), 4),
                'supplier_on_time_delivery': round(random.uniform(0.88, 0.99), 3),
                'supplier_lead_time_variance': round(random.uniform(0.05, 0.20), 3)
            },
            'cost_metrics': {
                'logistics_cost_per_unit': round(random.uniform(2.0, 8.0), 2),
                'warehousing_cost_per_unit': round(random.uniform(0.5, 2.0), 2),
                'total_supply_chain_cost_percentage': round(random.uniform(0.08, 0.15), 3)
            }
        }
    }

    with open('supply_chain_data/supply_chain_kpis.json', 'w') as f:
        json.dump(kpi_data, f, indent=2)

    return ['supply_chain_data/supply_chain_kpis.json']


def generate_all_supply_chain_data():
    # Generate all supply chain data files
    print("\nGenerating supply chain data...")

    all_files = []

    # Manufacturing data
    files = generate_manufacturers_data()
    all_files.extend(files)
    print(f"Created manufacturing data: {len(files)} files")

    # Inventory data
    files = generate_inventory_data()
    all_files.extend(files)
    print(f"Created inventory data: {len(files)} files")

    # Logistics data
    files = generate_logistics_data()
    all_files.extend(files)
    print(f"Created logistics data: {len(files)} files")

    # Supplier data
    files = generate_supplier_data()
    all_files.extend(files)
    print(f"Created supplier data: {len(files)} files")

    # Demand forecast data
    files = generate_demand_forecast()
    all_files.extend(files)
    print(f"Created demand forecast data: {len(files)} files")

    # Quality metrics
    files = generate_quality_metrics()
    all_files.extend(files)
    print(f"Created quality metrics data: {len(files)} files")

    # KPI data
    files = generate_supply_chain_kpis()
    all_files.extend(files)
    print(f"Created KPI data: {len(files)} files")

    print(f"\nTotal supply chain files created: {len(all_files)}")

    # Generate a mapping reference file
    reference = {
        'data_sources': {
            'manufacturing': {
                'plant_a': 'Technical codes, compact format',
                'plant_b': 'Business names, verbose format'
            },
            'inventory': {
                'legacy_wms': 'CSV with abbreviated columns',
                'modern_wms': 'JSON with nested structure'
            },
            'logistics': {
                'carrier_1': 'Tab-delimited shipment data',
                'carrier_2': 'XML EDI format'
            },
            'suppliers': {
                'master_data': 'CSV with inconsistent formats',
                'purchase_orders': 'JSON with line items'
            },
            'demand': {
                'weekly': 'CSV with accuracy metrics',
                'monthly': 'JSON with confidence intervals'
            }
        },
        'common_issues': [
            'SKU vs Product_ID vs product_code naming',
            'Date formats: YYYYMMDD vs MM/DD/YYYY vs YYYY-MM-DD',
            'Quantity fields: QTY vs units_produced vs quantity',
            'Status codes vs full text status',
            'Nested JSON vs flat CSV structures',
            'Different granularities (weekly vs monthly)',
            'Missing values represented differently'
        ]
    }

    with open('supply_chain_data/data_reference.json', 'w') as f:
        json.dump(reference, f, indent=2)

    print("Created data reference file")

    return all_files


# Main execution
if __name__ == "__main__":
    generate_all_supply_chain_data()
