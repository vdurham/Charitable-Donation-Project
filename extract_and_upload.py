import pandas as pd
import boto3
import requests
import xml.etree.ElementTree as ET
from google.cloud import bigquery
from google.oauth2 import service_account

# Replace with your BigQuery credentials
from bigquery_constants import BQ_PROJECT, BQ_DATASET, BQ_TABLE_DONORS, BQ_TABLE_RECIPIENTS, BQ_KEYFILE


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
    return xml_urls

def extract_data_from_xml(xml_content):
    root = ET.fromstring(xml_content)
    recipients = []
    donors = {}

    # Extract Filer/Donor information
    filer = root.find('.//Filer')
    if filer is not None:
        filer_ein = filer.findtext('EIN')

        business_name_line1 = filer.findtext('BusinessName/BusinessNameLine1Txt')
        business_name_line2 = filer.findtext('BusinessName/BusinessNameLine2Txt')
        filer_name = f"{business_name_line1} {business_name_line2}" if business_name_line2 else business_name_line1
        
        filer_control_text = filer.findtext('BusinessNameControlTxt')
        filer_phone = filer.findtext('PhoneNum')
        filer_address_line1 = filer.findtext('USAddress/AddressLine1Txt')
        filer_address_line2 = filer.findtext('USAddress/AddressLine2Txt') if filer.findtext('USAddress/AddressLine2Txt') else None
        filer_city = filer.findtext('USAddress/CityNm')
        filer_state_or_province = filer.findtext('USAddress/StateAbbreviationCd')
        filer_zip = filer.findtext('USAddress/ZIPCd')
        filer_total_assets_eoy_amt = root.findtext('.//TotalAssetsEOYAmt')
        filer_total_corpus_amt = root.findtext('.//TotalCorpusAmt')
        filer_cash_eoy_amt = root.findtext('.//CashEOYAmt')

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
    else:
        filer_data = None

    # Add Filer data to the donors dictionary
    if donors.get(filer_ein) is None:
        donors[filer_ein] = filer_data

    # Extract Grant/Contribution Recipient information
    for group in root.findall('.//GrantOrContributionPdDurYrGrp'):

        business_name_line1 = filer.findtext('RecipientBusinessName/BusinessNameLine1Txt')
        business_name_line2 = filer.findtext('RecipientBusinessName/BusinessNameLine2Txt')
        recipient_name = f"{business_name_line1} {business_name_line2}" if business_name_line2 else business_name_line1

        recipient_data = {
            'RecipientName': recipient_name,
            'RecipientAddressLine1': None,
            'RecipientAddressLine2': None,
            'RecipientCity': None,
            'RecipientStateOrProvince': None,
            'RecipientZIP': None,
            'RecipientCountry': None,
            'RecipientRelationship': group.findtext('RecipientRelationshipTxt'),
            'Purpose': group.findtext('GrantOrContributionPurposeTxt'),
            'Amount': group.findtext('Amt'),
            'DonorEIN': filer_ein
        }

        us_address = group.find('RecipientUSAddress')
        if us_address is not None:
            recipient_data['RecipientAddressLine1'] = us_address.findtext('AddressLine1Txt')
            recipient_data['RecipientAddressLine2'] = us_address.findtext('AddressLine2Txt') if us_address.findtext('AddressLine2Txt') else None
            recipient_data['RecipientCity'] = us_address.findtext('CityNm')
            recipient_data['RecipientStateOrProvince'] = us_address.findtext('StateAbbreviationCd')
            recipient_data['RecipientZIP'] = us_address.findtext('ZIPCd')
            recipient_data['RecipientCountry'] = 'US'

        foreign_address = group.find('.//RecipientForeignAddress')
        if foreign_address is not None:
            recipient_data['RecipientAddressLine1'] = foreign_address.findtext('AddressLine1Txt')
            recipient_data['RecipientAddressLine2'] = foreign_address.findtext('AddressLine2Txt') if foreign_address.findtext('AddressLine2Txt') else None
            recipient_data['RecipientCity'] = foreign_address.findtext('CityNm')
            recipient_data['RecipientStateOrProvince'] = foreign_address.findtext('ProvinceOrStateNm')
            recipient_data['RecipientCountry'] = foreign_address.findtext('CountryCd')

        recipients.append(recipient_data)
    
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

        # Insert data into recipients table
        recipients_rows = [list(data.values()) for data in recipients]
        recipients_rows = [{field.name: value for field, value in zip(recipients_table_schema, row)} for row in recipients_rows]
        errors = client.insert_rows_json(recipients_table_id, recipients_rows)
        if errors:
            print(f"Errors while inserting into recipients table: {errors}")

    except Exception as e:
        print(f"An error occurred: {e}")

def __main__():

    session = boto3.Session(profile_name='990-project')
    s3 = session.client('s3')

    # Giving Tuesday Data Lake S3 bucket and object key
    bucket_name = 'gt990datalake-rawdata'
    object_key = 'Indices/990xmls/index_latest_only_efiledata_xmls_created_on_2024-07-23.json'

    xml_urls = read_s3_index_file_for_priv_fndn_xmls(bucket_name, object_key)
    
    donors = {}
    recipients = []

    for url in xml_urls:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                # Extract data from the XML content
                xml_content = response.text
                new_donors, new_recipients = extract_data_from_xml(xml_content)

                # Merge new data into existing data
                donors.update(new_donors)
                recipients.extend(new_recipients)
            else:
                print(f"Failed to retrieve XML data from URL: {url}")
        except Exception as e:
            print(f"An error occurred while processing {url}: {e}")

    # Insert the extracted data into BigQuery
    insert_data_to_bigquery(donors, recipients)


if __name__ == '__main__':
    __main__()



