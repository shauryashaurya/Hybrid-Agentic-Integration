# fake_data_generator.py
# This module generates all fake data needed for the workshop
# Includes CSVs with inconsistent headers, sample JSON responses, and mock database schemas

import csv
import json
import random
import os
from datetime import datetime, timedelta
from faker import Faker

# Initialize Faker for realistic data generation
fake = Faker()
Faker.seed(42)  # Reproducible results
random.seed(42)

# Create data directory if it doesn't exist
os.makedirs('workshop_data', exist_ok=True)


def generate_customer_csv_v1():
    # First version of customer data with specific column naming convention
    headers = ['Customer_ID', 'Full_Name', 'Email_Address',
               'Registration_Date', 'Account_Status']

    rows = []
    for i in range(100):
        rows.append([
            f'CUST{1000 + i}',
            fake.name(),
            fake.email(),
            fake.date_between(start_date='-2y',
                              end_date='today').strftime('%Y-%m-%d'),
            random.choice(['Active', 'Inactive', 'Pending'])
        ])

    with open('workshop_data/customers_v1.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    return 'workshop_data/customers_v1.csv'


def generate_customer_csv_v2():
    # Second version with different naming conventions and date format
    headers = ['CustID', 'CustomerName',
               'ContactEmail', 'SignupDate', 'Status']

    rows = []
    for i in range(100):
        rows.append([
            f'{2000 + i}',  # Different ID format
            fake.name(),
            fake.email(),
            # US date format
            fake.date_between(start_date='-2y',
                              end_date='today').strftime('%m/%d/%Y'),
            random.choice(['A', 'I', 'P'])  # Abbreviated status codes
        ])

    with open('workshop_data/customers_v2.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    return 'workshop_data/customers_v2.csv'


def generate_customer_csv_v3():
    # Third version with yet another convention, including nulls
    headers = ['client_number', 'name', 'email_addr', 'created_at', 'active']

    rows = []
    for i in range(100):
        # Introduce some null values
        email = fake.email() if random.random() > 0.1 else ''
        rows.append([
            f'CLT-{3000 + i}',
            fake.name(),
            email,
            # European format
            fake.date_between(start_date='-2y',
                              end_date='today').strftime('%d-%m-%Y'),
            # Boolean as string with some nulls
            random.choice(['true', 'false', ''])
        ])

    with open('workshop_data/customers_v3.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    return 'workshop_data/customers_v3.csv'


def generate_orders_csv():
    # Orders data with foreign key references to customers
    headers = ['OrderID', 'CustomerRef', 'OrderDate',
               'TotalAmount', 'Currency', 'Items']

    rows = []
    for i in range(500):
        # Mix different customer ID formats to simulate real-world mess
        customer_ref_formats = [
            f'CUST{1000 + random.randint(0, 99)}',
            f'{2000 + random.randint(0, 99)}',
            f'CLT-{3000 + random.randint(0, 99)}'
        ]

        rows.append([
            f'ORD{5000 + i}',
            random.choice(customer_ref_formats),
            fake.date_between(start_date='-1y',
                              end_date='today').strftime('%Y-%m-%d'),
            round(random.uniform(10.50, 999.99), 2),
            # Mixed currency representations
            random.choice(['USD', 'EUR', 'GBP', '$', '€', '£']),
            random.randint(1, 10)
        ])

    with open('workshop_data/orders.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    return 'workshop_data/orders.csv'


def generate_products_json():
    # Generate product data in JSON format (simulating API response)
    products = []

    for i in range(50):
        product = {
            'id': f'PRD{4000 + i}',
            'name': fake.catch_phrase(),
            'description': fake.text(max_nb_chars=200),
            'price': {
                'amount': round(random.uniform(5.0, 500.0), 2),
                'currency': random.choice(['USD', 'EUR', 'GBP'])
            },
            'category': random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports']),
            # Include null values
            'in_stock': random.choice([True, False, None]),
            'last_updated': fake.date_time_between(start_date='-30d', end_date='now').isoformat()
        }
        products.append(product)

    with open('workshop_data/products.json', 'w') as f:
        json.dump({'products': products, 'total': len(
            products), 'page': 1}, f, indent=2)

    return 'workshop_data/products.json'


def generate_legacy_xml_data():
    # Generate XML data simulating legacy system export
    xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
<DATA_EXPORT>
    <METADATA>
        <EXPORT_DATE>{}</EXPORT_DATE>
        <SYSTEM>LEGACY_ERP_V2.3</SYSTEM>
    </METADATA>
    <RECORDS>'''.format(datetime.now().strftime('%Y%m%d'))

    for i in range(30):
        xml_content += f'''
        <REC>
            <FLD01>{6000 + i}</FLD01>
            <FLD02>{fake.company()}</FLD02>
            <FLD03>{fake.address().replace('', ', ')}</FLD03>
            <FLD04>{random.choice(['Y', 'N', ''])}</FLD04>
            <FLD05>{round(random.uniform(1000, 100000), 2)}</FLD05>
            <FLD06>{fake.date_between(start_date='-5y', end_date='today').strftime('%Y%m%d')}</FLD06>
        </REC>'''

    xml_content += '''
    </RECORDS>
</DATA_EXPORT>'''

    with open('workshop_data/legacy_data.xml', 'w') as f:
        f.write(xml_content)

    return 'workshop_data/legacy_data.xml'


def generate_schema_mapping_examples():
    # Generate example mappings for training/testing
    mappings = {
        'field_mappings': [
            {
                'source_fields': ['Customer_ID', 'CustID', 'client_number', 'customer_id', 'cust_num'],
                'target_field': 'customer_id',
                'confidence': 0.95,
                'reasoning': 'All fields represent unique customer identifiers'
            },
            {
                'source_fields': ['Full_Name', 'CustomerName', 'name', 'client_name', 'cust_name'],
                'target_field': 'customer_name',
                'confidence': 0.98,
                'reasoning': 'All fields contain customer name information'
            },
            {
                'source_fields': ['Email_Address', 'ContactEmail', 'email_addr', 'email', 'contact'],
                'target_field': 'email',
                'confidence': 0.92,
                'reasoning': 'Fields contain email contact information'
            },
            {
                'source_fields': ['Registration_Date', 'SignupDate', 'created_at', 'date_created', 'reg_dt'],
                'target_field': 'created_date',
                'confidence': 0.88,
                'reasoning': 'Timestamps indicating when record was created'
            }
        ],
        'type_mappings': {
            'date_formats': ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%Y%m%d'],
            'boolean_representations': [
                ['true', 'false'],
                ['Y', 'N'],
                ['1', '0'],
                ['Active', 'Inactive'],
                ['A', 'I']
            ],
            'null_values': ['', 'NULL', 'null', 'None', 'N/A', '-', '#N/A']
        }
    }

    with open('workshop_data/schema_mappings.json', 'w') as f:
        json.dump(mappings, f, indent=2)

    return 'workshop_data/schema_mappings.json'


def generate_all_data():
    # Generate all sample data for the workshop
    print("Generating workshop data...")

    files_created = []

    # Generate different versions of customer CSVs
    files_created.append(generate_customer_csv_v1())
    print(f"Created: {files_created[-1]}")

    files_created.append(generate_customer_csv_v2())
    print(f"Created: {files_created[-1]}")

    files_created.append(generate_customer_csv_v3())
    print(f"Created: {files_created[-1]}")

    # Generate orders data
    files_created.append(generate_orders_csv())
    print(f"Created: {files_created[-1]}")

    # Generate product JSON (API simulation)
    files_created.append(generate_products_json())
    print(f"Created: {files_created[-1]}")

    # Generate legacy XML
    files_created.append(generate_legacy_xml_data())
    print(f"Created: {files_created[-1]}")

    # Generate schema mapping examples
    files_created.append(generate_schema_mapping_examples())
    print(f"Created: {files_created[-1]}")

    print("\nAll workshop data generated successfully!")
    print(f"Total files created: {len(files_created)}")

    return files_created


# Main execution
if __name__ == "__main__":
    generate_all_data()
