import requests
import os
import time
import logging
from processing_data import process_data
import requests
import pandas as pd
import random
from concurrent.futures import ProcessPoolExecutor, as_completed
import datetime
import os
import logging
import multiprocessing
import io

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

# def find_file(filename):
#     logging.info(f"Searching for file: {filename}")
#     for root, dirs, files in os.walk("/"):
#         if filename in files:
#             return os.path.join(root, filename)
#     return None

logging.basicConfig(
    filename='fetch_pricing_data.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def convert_timestamp_to_utc(timestamp):
    if timestamp:
        timestamp_seconds = timestamp / 1000.0
        return datetime.datetime.utcfromtimestamp(timestamp_seconds).isoformat()
    return None

def fetch_data(session, pickup, dropoff, pickup_datetime, passenger):
    USER_AGENTS = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/116.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0',
        'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/116.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.188',
    ]

    base_url = "https://taxi.booking.com/search-results-mfe/rates"
    user_agent = random.choice(USER_AGENTS)
    headers = {
        "User-Agent": user_agent
    }

    params = {
        "format": "envelope",
        "passenger": passenger,
        "pickup": pickup,
        "pickupDateTime": pickup_datetime,
        "dropoff": dropoff,
        "affiliate": "booking-taxi",
        "populateSupplierName": "true",
        "language": "en-gb",
        "currency": "USD",
        "enablePTSearch": "true",
        "isExpandable": "true",
        "displayLocalSupplierText": "true",
        "preSelectedResultReference": 1
    }

    try:
        response = session.get(base_url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        requested_url = response.url
        logging.info(f"Data fetched successfully from URL: {requested_url}")
        return response.json(), requested_url
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None, None

def download_process_data_file(site_id, drive_id, data_file_path, config):
    access_token = get_access_token(config)
    
    download_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:{data_file_path}:/content"
    headers = {
        'Authorization': 'Bearer ' + access_token
    }
    response = requests.get(download_url, headers=headers)
    response.raise_for_status()
    
    start_time = datetime.datetime.now()
    logging.info("Process started at: %s", start_time)
    # Read Excel file content
    df = pd.read_excel(io.BytesIO(response.content))
    logging.info("data input loaded successfully.")

    # Extract columns from DataFrame
    pick_locationid = df[['From locationId', 'To locationId']]

    all_results = []
    times = ["03:00:00", "09:00:00", "23:00:00"]
    passengers = [1, 2, 3]
    current_date = datetime.datetime.utcnow()
    target_date = current_date + datetime.timedelta(days=5)
    formatted_date = target_date.strftime('%Y-%m-%d')

    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        with requests.Session() as session:
            futures = [
                executor.submit(fetch_data, session, row['From locationId'], row['To locationId'], f"{formatted_date}T{time}", passenger)
                for index, row in pick_locationid.iterrows()
                for time in times
                for passenger in passengers
            ]

            for future in as_completed(futures):
                try:
                    data, requested_url = future.result()
                    if data is None:
                        continue
                    
                    journeys = data.get('journeys', [])
                    for journey in journeys:
                        legs = journey.get('legs', [])
                        for leg in legs:
                            results = leg.get('results', [])
                            for result in results:
                                all_results.append({
                                        'refresh_time': datetime.datetime.utcnow().strftime('%Y-%m-%d'),
                                        'requested_url': requested_url,
                                        'pickup_location_id': leg.get('pickupLocation', {}).get('locationId', ''),
                                        'pickup_name': leg.get('pickupLocation', {}).get('name', ''),
                                        'pickup_location_postcode': int(leg.get('pickupLocation', {}).get('postcode', '')),
                                        'pickup_location_city': leg.get('pickupLocation', {}).get('city', ''),
                                        'pickup_location_country': leg.get('pickupLocation', {}).get('country', ''),
                                        'pickup_location_timezone': leg.get('pickupLocation', {}).get('timezone', ''),
                                        'pickup_location_latitude': leg.get('pickupLocation', {}).get('latLng', '').get('latitude', ''),
                                        'pickup_location_longitude': leg.get('pickupLocation', {}).get('latLng', '').get('longitude', ''),
                                        'pickup_location_airportcode': leg.get('pickupLocation', {}).get('airportCode', ''),
                                        'pickup_location_establishment': leg.get('pickupLocation', {}).get('establishment', ''),
                                        'pickup_location_locationtype': leg.get('pickupLocation', {}).get('locationType', ''),
                                        'pickup_location_description': leg.get('pickupLocation', {}).get('description', ''),
                                        'dropoff_location_id': leg.get('dropoffLocation', {}).get('locationId', ''),
                                        'dropoff_location_name': leg.get('dropoffLocation', {}).get('name', ''),
                                        'dropoff_location_postcode': int(leg.get('dropoffLocation', {}).get('postcode', '')),
                                        'dropoff_location_city': leg.get('dropoffLocation', {}).get('city', ''),
                                        'dropoff_location_country': leg.get('dropoffLocation', {}).get('country', ''),
                                        'dropoff_location_timezone': leg.get('dropoffLocation', {}).get('timezone', ''),
                                        'dropoff_location_latitude': leg.get('dropoffLocation', {}).get('latLng', '').get('latitude', ''),
                                        'dropoff_location_longitude': leg.get('dropoffLocation', {}).get('latLng', '').get('longitude', ''),
                                        'dropoff_location_airportcode': leg.get('dropoffLocation', {}).get('airportCode', ''),
                                        'dropoff_location_establishment': leg.get('dropoffLocation', {}).get('establishment', ''),
                                        'dropoff_location_locationtype': leg.get('dropoffLocation', {}).get('locationType', ''),
                                        'dropoff_location_description': leg.get('dropoffLocation', {}).get('description', ''),
                                        'requestedPickupDateTime': leg.get('requestedPickupDateTime', {}),
                                        'searchReference': leg.get('searchReference', {}),
                                        'searchTime': convert_timestamp_to_utc(leg.get('searchTime', None)),
                                        'resultReference': int(result.get('resultReference', '')),
                                        'supplierID': result.get('supplierID', ''),
                                        'supplierLocationID': result.get('supplierLocationID', ''),
                                        'predict_pickup_time': result.get('predictedPickupDateTime', ''),
                                        'bags': result.get('bags', ''),
                                        'meetAndGreet': result.get('meetAndGreet', ''),
                                        'publicTransport': result.get('publicTransport', ''),
                                        'imageUrl': result.get('imageUrl', ''),
                                        'drivingDistance': result.get('drivingDistance', ''),
                                        'duration': result.get('duration', ''),
                                        'maxPassenger': int(result.get('maxPassenger', '')),
                                        'originalPrice': result.get('originalPrice', ''),
                                        'price': result.get('price', ''),
                                        'currency': result.get('currency', ''),
                                        'hourFrom': result.get('hourFrom', ''),
                                        'hourUntil': result.get('hourUntil', ''),
                                        'frequencyMins': result.get('frequencyMins', ''),
                                        'twentyFourHourCancellation': result.get('twentyFourHourCancellation', ''),
                                        'twoHourCancellation': result.get('twentyFourHourCancellation', ''),
                                        'nonRefundable': result.get('twentyFourHourCancellation', ''),
                                        'type': result.get('type', ''),
                                        'car_details_model': result.get('carDetails', {}).get('model', ''),
                                        'car_details_modelDescription': result.get('carDetails', {}).get('modelDescription', ''),
                                        'car_details_description': result.get('carDetails', {}).get('description', ''),
                                        'link': result.get('link', ''),
                                        'supplierCategory': result.get('supplierCategory', ''),
                                        'supplierName': result.get('supplierName', ''),
                                        'cancellationLeadTimeMinutes': result.get('cancellationLeadTimeMinutes', ''),
                                        'priceRuleID': result.get('priceRuleID', ''),
                                        'priceDiscountPercentage': result.get('priceDiscountPercentage', ''),
                                        'estimatedDriverPickupTimeMinutes': result.get('estimatedDriverPickupTimeMinutes', ''),
                                        'passenger': leg.get('passenger', ''),
                                        'selfLink': leg.get('selfLink', '')
                                })
                except Exception as e:
                    logging.error(f"An error occurred while processing a future: {e}")

    # Chuyển đổi danh sách kết quả thành DataFrame
    results_df = pd.DataFrame(all_results)
    end_time = datetime.datetime.now()
    logging.info("Process completed at: %s", end_time)
    logging.info("Total processing time: %s", end_time - start_time)
    return results_df

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

def upload_log_file(site_id, drive_id, log_file_path, config, results_df):
    access_token = get_access_token(config)

    # Step 1: Download the existing file from SharePoint
    download_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:{log_file_path}:/content"
    headers = {
        'Authorization': 'Bearer ' + access_token
    }
    
    try:
        response = requests.get(download_url, headers=headers)
        response.raise_for_status()
        existing_df = pd.read_excel(io.BytesIO(response.content))
    except requests.exceptions.HTTPError as err:
        if err.response.status_code == 404:
            logging.warning("File not found. Creating a new one.")
            existing_df = pd.DataFrame()
        else:
            logging.error(f"Failed to download existing log file: {err}")
            return
    
    # Step 2: Append the new data to the existing DataFrame
    combined_df = pd.concat([existing_df, results_df], ignore_index=True)
    
    # Convert combined DataFrame to bytes
    combined_bytes = io.BytesIO()
    combined_df.to_excel(combined_bytes, index=False, engine='openpyxl')
    combined_bytes.seek(0)
    
    if not wait_until_unlocked(site_id, drive_id, log_file_path, access_token):
        logging.error("File is still locked after multiple attempts. Exiting.")
        return
    
    # Step 3: Upload the updated file back to SharePoint
    upload_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:{log_file_path}:/content?@microsoft.graph.conflictBehavior=replace"
    headers = {
        'Authorization': 'Bearer ' + access_token,
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    }
    
    max_retries = 10
    for attempt in range(max_retries):
        response = requests.put(upload_url, headers=headers, data=combined_bytes.getvalue())
        
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
data_file_path = '/AM_Input/test.xlsx'
log_file_path = '/AM - Pricing Analysis/Pricing_Data.xlsx'
config = SHAREPOINT_CONFIG

# Define a function to be executed
def main():
    try:
        site_id, drive_id = get_site_and_drive_id(site_name, config)
        results_df = download_process_data_file(site_id, drive_id, data_file_path, config)
        upload_log_file(site_id, drive_id, log_file_path, config, results_df)
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == '__main__':
    main()
