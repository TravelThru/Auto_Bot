import requests
import pandas as pd
import random
from concurrent.futures import ProcessPoolExecutor, as_completed
import datetime
import os
import logging
import multiprocessing

# Configure logging
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

def process_data():
    start_time = datetime.datetime.now()
    logging.info("Process started at: %s", start_time)

    # def find_file(filename, search_path="/"):
    #     for root, dirs, files in os.walk(search_path):
    #         if filename in files:
    #             return os.path.join(root, filename)
    #     return None

    def read_data_file():
        file_path = 'data_input_pricing_file.xlsx'
        if file_path is None:
            raise FileNotFoundError("File 'data_input_pricing_file.xlsx' not found.")
        return pd.read_excel(file_path)

    # Read data from Excel file
    df = read_data_file()
    logging.info("data_input_pricing_file.xlsx loaded successfully.")

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

    # Lưu DataFrame vào file Excel
    output_file = 'Pricing_Data.xlsx'
    try:
        # Kiểm tra xem file đã tồn tại chưa
        if os.path.exists(output_file):
            existing_data = pd.read_excel(output_file)
            combined_data = pd.concat([existing_data, results_df], ignore_index=True)
            combined_data.to_excel(output_file, index=False)
            logging.info(f"Data successfully appended to {output_file}")
        else:
            results_df.to_excel(output_file, index=False)
            logging.info(f"Data successfully written to new {output_file}")

    except Exception as e:
        logging.error(f"Failed to save data to Excel: {e}")

    end_time = datetime.datetime.now()
    logging.info("Process completed at: %s", end_time)
    logging.info("Total processing time: %s", end_time - start_time)
