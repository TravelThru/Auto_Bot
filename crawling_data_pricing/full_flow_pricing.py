import requests
import os
import time
import logging
from processing_data import process_data

# Set up logging
logging.basicConfig(filename='fetch_pricing_data.log',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DEFAULT_TENANT_ID = 'a3f88450-77ef-4df3-89ea-c69cbc9bc410'
DEFAULT_CLIENT_ID = 'ad6b066a-d749-4f0b-bfbb-bad8de0af5d1'
DEFAULT_CLIENT_SECRET = 'YwZ8Q~N6dAwc~sTcMAQsDQXwCKDfPBk81miLVbL4'
DEFAULT_DOMAIN = 'mytravelthru.sharepoint.com'

SHAREPOINT_CONFIG = {
    'tenant_id': DEFAULT_TENANT_ID,
    'client_id': DEFAULT_CLIENT_ID,
    'client_secret': DEFAULT_CLIENT_SECRET,
    'domain': DEFAULT_DOMAIN
}

def get_access_token(config):
    logging.info("Getting access token")
    token_url = f'https://login.microsoftonline.com/{config["tenant_id"]}/oauth2/v2.0/token'
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': config['client_id'],
        'client_secret': config['client_secret'],
        'scope': 'https://graph.microsoft.com/.default'
    }
    token_r = requests.post(token_url, data=token_data)
    return token_r.json()['access_token']

def get_site_and_drive_id(site_name, config):
    access_token = get_access_token(config)
    
    # Get Site ID
    site_url = f"https://graph.microsoft.com/v1.0/sites/{config['domain']}:/sites/{site_name}"
    headers = {
        'Authorization': 'Bearer ' + access_token
    }
    site_response = requests.get(site_url, headers=headers)
    
    logging.info(f"Site Response Status Code: {site_response.status_code}")
    
    if site_response.status_code == 200:
        site_id = site_response.json()['id']
    else:
        logging.error(f"Failed to get site ID: {site_response.status_code} {site_response.content}")
        raise Exception(f"Failed to get site ID: {site_response.status_code} {site_response.content}")
    
    # Get Drive ID
    drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    drive_response = requests.get(drive_url, headers=headers)
    
    logging.info(f"Drive Response Status Code: {drive_response.status_code}")
    
    if drive_response.status_code == 200:
        drives = drive_response.json()['value']
        # Default Document Library
        drive_id = next(drive['id'] for drive in drives if drive['name'] == 'Documents')
    else:
        logging.error(f"Failed to get drive ID: {drive_response.status_code} {drive_response.content}")
        raise Exception(f"Failed to get drive ID: {drive_response.status_code} {drive_response.content}")
    
    return site_id, drive_id

def find_file(filename):
    logging.info(f"Searching for file: {filename}")
    for root, dirs, files in os.walk("/"):
        if filename in files:
            return os.path.join(root, filename)
    return None

def download_data_file(site_id, drive_id, data_file_path, config):
    access_token = get_access_token(config)
    local_file = find_file('data_input_pricing_file.xlsx')
    
    if local_file is None:
        local_file = 'data_input_pricing_file.xlsx'
    
    download_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:{data_file_path}:/content"
    headers = {
        'Authorization': 'Bearer ' + access_token
    }
    response = requests.get(download_url, headers=headers)
    
    if response.status_code == 200:
        with open(local_file, 'wb') as f:
            f.write(response.content)
        logging.info(f"File downloaded and saved as '{local_file}'")
    else:
        logging.error(f"Failed to download file: {response.status_code} {response.text}")
        if local_file == 'data_input_pricing_file.xlsx':
            raise FileNotFoundError("Backup file 'data_input_pricing_file.xlsx' not found.")

def is_file_locked(site_id, drive_id, file_path, access_token):
    check_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:{file_path}"
    headers = {
        'Authorization': 'Bearer ' + access_token
    }
    response = requests.get(check_url, headers=headers)
    if response.status_code == 200:
        return False
    elif response.status_code == 423:
        return True
    else:
        logging.error(f"Failed to check file status: {response.status_code} {response.text}")
        return False

def wait_until_unlocked(site_id, drive_id, file_path, access_token, max_retries=10, wait_time=5):
    for attempt in range(max_retries):
        if not is_file_locked(site_id, drive_id, file_path, access_token):
            return True
        logging.warning(f"File is locked. Waiting for {wait_time} seconds before retrying... (Attempt {attempt + 1}/{max_retries})")
        time.sleep(wait_time)
    return False

def upload_log_file(site_id, drive_id, log_file_path, config):
    access_token = get_access_token(config)
    local_file = find_file('Pricing_Data.xlsx')
    
    if local_file is None:
        local_file = 'Pricing_Data.xlsx'
    
    if not wait_until_unlocked(site_id, drive_id, log_file_path, access_token):
        logging.error("File is still locked after multiple attempts. Exiting.")
        return
    
    upload_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:{log_file_path}:/content?@microsoft.graph.conflictBehavior=replace"
    headers = {
        'Authorization': 'Bearer ' + access_token,
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    }
    
    max_retries = 10
    for attempt in range(max_retries):
        with open(local_file, 'rb') as f:
            response = requests.put(upload_url, headers=headers, data=f)
        
        if response.status_code == 201 or response.status_code == 200:
            logging.info("Log file successfully uploaded to SharePoint!")
            break
        elif response.status_code == 423:
            logging.warning(f"Attempt {attempt + 1} failed: Resource locked. Retrying in 5 seconds...")
            time.sleep(5)
        else:
            logging.error(f"Failed to upload log file: {response.status_code} {response.text}")
            break
    else:
        logging.error("Max retries reached. Failed to upload log file.")

# Usage
site_name = 'PricingDataAutomate'
data_file_path = '/AM_Input/location_input.xlsx'
log_file_path = '/AM - Pricing Analysis/Pricing_Data.xlsx'
config = SHAREPOINT_CONFIG

# Define a function to be executed
def main():
    try:
        site_id, drive_id = get_site_and_drive_id(site_name, config)
        download_data_file(site_id, drive_id, data_file_path, config)
        process_data()
        upload_log_file(site_id, drive_id, log_file_path, config)
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == '__main__':
    main()

