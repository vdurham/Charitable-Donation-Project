"""
This script:
1) Reads the index file from the Giving Tuesday Data Lake S3 bucket to get the URLs of the latest XML files for private foundations.
2) Extracts donor and recipient information from the XML files.
3) Inserts the extracted data into BigQuery tables.
"""

import pandas as pd
import boto3
import requests
import xml.etree.ElementTree as ET
from google.cloud import bigquery
from google.oauth2 import service_account

# Replace with your BigQuery credentials
from bigquery_constants import BQ_PROJECT, BQ_DATASET, BQ_TABLE_DONORS, BQ_TABLE_RECIPIENTS, BQ_KEYFILE

# Initialize AWS S3 client
session = boto3.Session(profile_name='990-project')
s3 = session.client('s3')

def read_s3_index_file_for_priv_fndn_xmls(bucket, key):

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body']
        data = pd.read_json(body)
    except s3.exceptions.NoSuchKey:
        print(f"Error: The object with key '{key}' does not exist in the bucket '{bucket}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

    private_fndns = data[data['FormType'] == '990PF']
    xml_urls = private_fndns['URL'].tolist()
    print(f"Found {len(xml_urls)} XML URLs for private foundations.")
    return xml_urls

def extract_data_from_xml(xml_content):
    root = ET.fromstring(xml_content)

    namespace = {'irs': 'http://www.irs.gov/efile'}

    recipients = []
    donors = {}

    # Extract Filer/Donor information
    filer = root.find('.//irs:ReturnHeader/irs:Filer', namespace)

    if filer is not None:
        filer_ein = filer.findtext('irs:EIN', default=None, namespaces=namespace)
        
        business_name = filer.find('irs:BusinessName', namespace)
        if business_name is not None:
            business_name_line1 = business_name.findtext('irs:BusinessNameLine1Txt', default=None, namespaces=namespace)
            business_name_line2 = business_name.findtext('irs:BusinessNameLine2Txt', default=None, namespaces=namespace)
            filer_name = f"{business_name_line1} {business_name_line2}" if business_name_line2 else business_name_line1
        else:
            filer_name = None
        
        filer_control_text = filer.findtext('irs:BusinessNameControlTxt', default=None, namespaces=namespace)
        filer_phone = filer.findtext('irs:PhoneNum', default=None, namespaces=namespace)
        
        us_address = filer.find('irs:USAddress', namespace)
        if us_address is not None:
            filer_address_line1 = us_address.findtext('irs:AddressLine1Txt', default=None, namespaces=namespace)
            filer_address_line2 = us_address.findtext('irs:AddressLine2Txt', default=None, namespaces=namespace)
            filer_city = us_address.findtext('irs:CityNm', default=None, namespaces=namespace)
            filer_state_or_province = us_address.findtext('irs:StateAbbreviationCd', default=None, namespaces=namespace)
            filer_zip = us_address.findtext('irs:ZIPCd', default=None, namespaces=namespace)
        else:
            filer_address_line1 = filer_address_line2 = filer_city = filer_state_or_province = filer_zip = None

        filer_total_assets_eoy_amt = root.findtext('.//irs:TotalAssetsEOYAmt', default=None, namespaces=namespace)
        filer_total_corpus_amt = root.findtext('.//irs:TotalCorpusAmt', default=None, namespaces=namespace)
        filer_cash_eoy_amt = root.findtext('.//irs:CashEOYAmt', default=None, namespaces=namespace)

        filer_data = {
            'FilerEIN': filer_ein,
            'FilerName': filer_name,
            'FilerControlText': filer_control_text,
            'FilerPhone': filer_phone,
            'FilerAddressLine1': filer_address_line1,
            'FilerAddressLine2': filer_address_line2,
            'FilerCity': filer_city,
            'FilerStateOrProvince': filer_state_or_province,
            'FilerZIP': filer_zip,
            'FilerTotalAssetsEOYAmt': filer_total_assets_eoy_amt,
            'FilerTotalCorpusAmt': filer_total_corpus_amt,
            'FilerCashEOYAmt': filer_cash_eoy_amt
        }

        print(f"Extracted data for Filer EIN: {filer_ein}")

        # Add Filer data to the donors dictionary
        donors[filer_ein] = filer_data
    else:
        filer_data = None
        filer_ein = None

    # Extract Grant/Contribution Recipient information
    for group in root.findall('.//irs:GrantOrContributionPdDurYrGrp', namespace):
        recipient_business_name = group.find('irs:RecipientBusinessName', namespace)
        recipient_person_name = group.findtext('irs:RecipientPersonNm', default=None, namespaces=namespace)

        if recipient_business_name is not None:
            business_name_line1 = recipient_business_name.findtext('irs:BusinessNameLine1Txt', default=None, namespaces=namespace)
            business_name_line2 = recipient_business_name.findtext('irs:BusinessNameLine2Txt', default=None, namespaces=namespace)
            recipient_name = f"{business_name_line1} {business_name_line2}" if business_name_line2 else business_name_line1
        else:
            recipient_name = recipient_person_name

        recipient_data = {
            'RecipientName': recipient_name,
            'RecipientAddressLine1': None,
            'RecipientAddressLine2': None,
            'RecipientCity': None,
            'RecipientStateOrProvince': None,
            'RecipientZIP': None,
            'RecipientCountry': None,
            'RecipientRelationship': group.findtext('irs:RecipientRelationshipTxt', default=None, namespaces=namespace),
            'Purpose': group.findtext('irs:GrantOrContributionPurposeTxt', default=None, namespaces=namespace),
            'Amount': group.findtext('irs:Amt', default=None, namespaces=namespace),
            'DonorEIN': filer_ein
        }

        us_address = group.find('irs:RecipientUSAddress', namespace)
        if us_address is not None:
            recipient_data['RecipientAddressLine1'] = us_address.findtext('irs:AddressLine1Txt', default=None, namespaces=namespace)
            recipient_data['RecipientAddressLine2'] = us_address.findtext('irs:AddressLine2Txt', default=None, namespaces=namespace)
            recipient_data['RecipientCity'] = us_address.findtext('irs:CityNm', default=None, namespaces=namespace)
            recipient_data['RecipientStateOrProvince'] = us_address.findtext('irs:StateAbbreviationCd', default=None, namespaces=namespace)
            recipient_data['RecipientZIP'] = us_address.findtext('irs:ZIPCd', default=None, namespaces=namespace)
            recipient_data['RecipientCountry'] = 'US'

        foreign_address = group.find('irs:RecipientForeignAddress', namespace)
        if foreign_address is not None:
            recipient_data['RecipientAddressLine1'] = foreign_address.findtext('irs:AddressLine1Txt', default=None, namespaces=namespace)
            recipient_data['RecipientAddressLine2'] = foreign_address.findtext('irs:AddressLine2Txt', default=None, namespaces=namespace)
            recipient_data['RecipientCity'] = foreign_address.findtext('irs:CityNm', default=None, namespaces=namespace)
            recipient_data['RecipientStateOrProvince'] = foreign_address.findtext('irs:ProvinceOrStateNm', default=None, namespaces=namespace)
            recipient_data['RecipientCountry'] = foreign_address.findtext('irs:CountryCd', default=None, namespaces=namespace)

        recipients.append(recipient_data)
    
    print(f"Extracted data for {len(recipients)} Grant/Contribution Recipients.")
    
    return donors, recipients

def insert_data_to_bigquery(donors, recipients):
    try:
        # Initialize BigQuery client
        client = bigquery.Client.from_service_account_json(BQ_KEYFILE)
        
        # Define dataset
        dataset_id = f"{BQ_PROJECT}.{BQ_DATASET}"
        donors_table_id = f"{dataset_id}.{BQ_TABLE_DONORS}"
        recipients_table_id = f"{dataset_id}.{BQ_TABLE_RECIPIENTS}"
        
        # Define donors table
        donors_table_id = f"{dataset_id}.{BQ_TABLE_DONORS}"
        donors_table_schema = [
            bigquery.SchemaField('FilerEIN', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('FilerName', 'STRING'),
            bigquery.SchemaField('FilerControlText', 'STRING'),
            bigquery.SchemaField('FilerPhone', 'STRING'),
            bigquery.SchemaField('FilerAddressLine1', 'STRING'),
            bigquery.SchemaField('FilerAddressLine2', 'STRING'),
            bigquery.SchemaField('FilerCity', 'STRING'),
            bigquery.SchemaField('FilerStateOrProvince', 'STRING'),
            bigquery.SchemaField('FilerZIP', 'STRING'),
            bigquery.SchemaField('FilerTotalAssetsEOYAmt', 'STRING'),
            bigquery.SchemaField('FilerTotalCorpusAmt', 'STRING'),
            bigquery.SchemaField('FilerCashEOYAmt', 'STRING')
        ]

        # Define recipients table
        recipients_table_id = f"{dataset_id}.{BQ_TABLE_RECIPIENTS}"
        recipients_table_schema = [
            bigquery.SchemaField('RecipientName', 'STRING'),
            bigquery.SchemaField('RecipientAddressLine1', 'STRING'),
            bigquery.SchemaField('RecipientAddressLine2', 'STRING'),
            bigquery.SchemaField('RecipientCity', 'STRING'),
            bigquery.SchemaField('RecipientStateOrProvince', 'STRING'),
            bigquery.SchemaField('RecipientZIP', 'STRING'),
            bigquery.SchemaField('RecipientCountry', 'STRING'),
            bigquery.SchemaField('RecipientRelationship', 'STRING'),
            bigquery.SchemaField('Purpose', 'STRING'),
            bigquery.SchemaField('Amount', 'INTEGER'),
            bigquery.SchemaField('DonorEIN', 'STRING')
        ]
        
        # Create tables if they don't exist
        for table_id, schema in [(donors_table_id, donors_table_schema), (recipients_table_id, recipients_table_schema)]:
            try:
                client.get_table(table_id)  # Check if table exists
            except NotFound:
                schema = bigquery.SchemaField.from_api_repr(schema)
                table = bigquery.Table(table_id, schema=schema)
                client.create_table(table)
        
        # Insert data into donors table
        donors_rows = [list(data.values()) for data in donors.values() if data]
        donors_rows = [{field.name: value for field, value in zip(donors_table_schema, row)} for row in donors_rows]
        errors = client.insert_rows_json(donors_table_id, donors_rows)
        if errors:
            print(f"Errors while inserting into donors table: {errors}")

        print(f"Inserted data for {len(donors_rows)} donors.")

        # Insert data into recipients table
        recipients_rows = [list(data.values()) for data in recipients]
        recipients_rows = [{field.name: value for field, value in zip(recipients_table_schema, row)} for row in recipients_rows]
        errors = client.insert_rows_json(recipients_table_id, recipients_rows)
        if errors:
            print(f"Errors while inserting into recipients table: {errors}")
        
        print(f"Inserted data for {len(recipients_rows)} recipients.")

    except Exception as e:
        print(f"An error occurred: {e}")

def __main__():
    
    # Define S3 bucket and object key
    bucket_name = 'gt990datalake-rawdata'
    object_key = 'Indices/990xmls/index_latest_only_efiledata_xmls_created_on_2024-07-23.json'
    
    print(f"Reading XML URLs from S3 index file: s3://{bucket_name}/{object_key}")

    # Read the index file and get XML URLs
    xml_urls = read_s3_index_file_for_priv_fndn_xmls(bucket_name, object_key)
    
    donors = {}
    recipients = []
    count = 0

    # Process each XML URL
    for url in xml_urls:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                # Extract data from XML content
                xml_content = response.text
                new_donors, new_recipients = extract_data_from_xml(xml_content)

                # Merge new data into existing data
                donors.update(new_donors)
                recipients.extend(new_recipients)
                count += 1
            else:
                print(f"Failed to retrieve XML data from URL: {url} (Status Code: {response.status_code})")
            if count == 5:
                break
        except Exception as e:
            print(f"An error occurred while processing {url}: {e}")

    # Print summary of extracted data
    print(f"Extracted data for {len(donors)} donors and {len(recipients)} recipients.")

    # Insert the extracted data into BigQuery
    insert_data_to_bigquery(donors, recipients)

if __name__ == '__main__':
    __main__()