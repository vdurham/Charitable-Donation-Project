import boto3
import sys

s3 = boto3.client('s3')

bucket_name = 'gt990datalake-rawdata'
object_key = 'Indices/990xmls/index_latest_only_efiledata_xmls_created_on_2024-07-23.json'

def read_s3_file(bucket, key, num_lines=50):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body']
        
        # Read the file in chunks
        chunk_size = 1024  # Read in chunks of 1KB
        buffer = ''
        count = 0
        
        while True:
            chunk = body.read(chunk_size).decode('utf-8')
            if not chunk:
                break
            buffer += chunk
            
            # Split buffer into lines and print
            lines = buffer.splitlines(True)
            while count < num_lines and lines:
                print(lines.pop(0).strip())
                count += 1
            
            # Retain remaining lines in buffer
            buffer = ''.join(lines)
            
            if count >= num_lines:
                break

    except s3.exceptions.NoSuchKey:
        print(f"Error: The object with key '{key}' does not exist in the bucket '{bucket}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    read_s3_file(bucket_name, object_key)