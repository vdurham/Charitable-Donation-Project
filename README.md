# Giving Tuesday Data Extraction to BigQuery

## Overview

This project is designed to extract donor and recipient information from XML files related to private foundations and store the extracted data in Google BigQuery for data analysis. The process involves reading an index file from an S3 bucket in the Giving Tuesday Data Lake, using it to retrieve the each 990 PF xml file, and parsing the xml contents to obtain relevant information.

## Project Structure

The project includes a Python script that performs the following key tasks:

1. **Read the Index File**: The script reads an index file from the S3 bucket to gather URLs of the latest XML files for private foundations.
2. **Extract Data**: It extracts donor and recipient information from the retrieved XML files.
3. **Insert Data into BigQuery**: Finally, the extracted data is inserted into specified tables in Google BigQuery.

## Prerequisites

Before running the script, ensure you have the following:

- Python 3.x installed on your machine.
- The following Python libraries:
  - `pandas`
  - `boto3`
  - `requests`
  - `xml.etree.ElementTree`
  - `google-cloud-bigquery`
  - `google-auth`

You can install the required libraries using pip:

```bash
pip install pandas boto3 requests google-cloud-bigquery google-auth
```

You'll also need to create a `bigquery_constants.py` file that should contain your BigQuery credentials:

```python
BQ_PROJECT = 'your_project_id'
BQ_DATASET = 'your_dataset_name'
BQ_TABLE_DONORS = 'your_donors_table_name'
BQ_TABLE_RECIPIENTS = 'your_recipients_table_name'
BQ_KEYFILE = 'path_to_your_service_account_key_file.json'
```

Once you've set everything up, run the script via:
```bash
python your_script_name.py
```

## Acknowledgments
The Giving Tuesday Data Commons is the reason why a project like this is possible. Please check out the published 990 data that they curate and maintain here: https://990data.givingtuesday.org/

This project was motivated by Mimi Brown, who wanted to facilitate transparency for private foundation charitable giving in her local area.
