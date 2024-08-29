import json
import requests
import hashlib
import hmac
import time
from datetime import datetime, timedelta, date
import math
from dotenv import load_dotenv

load_dotenv()

""" Utility Functions """
def save_to_json(data, filename):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)

def save_dataframe_to_csv(dataframe, file_path):
    try:
        dataframe.to_csv(file_path, index=False)
        print(f"DataFrame successfully saved to {file_path}")
    except Exception as e:
        print(f"An error occurred while saving the DataFrame to CSV: {e}")

def convert_to_unix(date_input):
    
    if isinstance(date_input, str):
        date_obj = datetime.strptime(date_input, '%Y-%m-%d')
    elif isinstance(date_input, datetime):
        date_obj = date_input
    else:
        raise ValueError("Input should be a date string or a datetime object")
    
    # Convert the datetime object to a Unix timestamp (in seconds) and then to milliseconds
    timestamp_ms = date_obj.timestamp() * 1000
    no_dec_timestamp = math.trunc(timestamp_ms)
    return no_dec_timestamp

def convert_timestamp_to_date(timestamp_ms_str):
    # Convert the string timestamp to an integer
    timestamp_ms = int(timestamp_ms_str)
    
    # Convert the timestamp from milliseconds to seconds
    timestamp_sec = timestamp_ms / 1000
    
    # Convert the timestamp to a datetime object
    date_time = datetime.fromtimestamp(timestamp_sec)
    
    # Format the datetime object to a date-only string
    date_only = date_time.strftime('%Y-%m-%d')
    
    return date_only

def get_binance_symbols():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)
    data = response.json()

    symbols = data.get('symbols')
    all_symbols = []

    for item in symbols:

        status = item.get('status')

        if status == 'TRADING':

            symbol_list = {
                'symbol': item.get('symbol')
            }

            all_symbols.append(symbol_list)
    
    return all_symbols

""" Core Feature """
def get_binance_trade_history(bin_api_key, bin_secret_key, start_time, end_time, symbol):

    base_url = 'https://api.binance.com'
    limit = 1000

    timestamp = int(time.time() * 1000)
    params = f'timestamp={timestamp}&limit={limit}&startTime={start_time}&endTime={end_time}&symbol={symbol}'
    signature = hmac.new(bin_secret_key.encode('utf-8'), params.encode('utf-8'), hashlib.sha256).hexdigest()

    headers = {
        'X-MBX-APIKEY': bin_api_key
    }

    url = f"{base_url}/api/v3/myTrades?{params}&signature={signature}"
        
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        if data is None:
            data = []  
        return data
    else:
        print(f"Error: Received status code {response.status_code} for symbol {symbol} with start_time {start_time} and end_time {end_time}")
        print(response.text)
        return []  

def loop_get_binance_history(bin_api_key, bin_secret_key, start_date, end_date, binance_symbols):
    
    # Handling dates 
    unix_start = convert_to_unix(start_date)
    unix_end = convert_to_unix(end_date)

    print(f"Binance: Full unix range {unix_start} to {unix_end}")

    # Another Loop to collect more than 7 days data
    current_start_time = start_date # Current start time for the loop
    trade_history_full = []

    while current_start_time < end_date:
    
        current_end_time = min(current_start_time + timedelta(days=1), end_date)

        unix_start = convert_to_unix(current_start_time)
        unix_end = convert_to_unix(current_end_time)
        trade_history_in_range = []

        print(f"Starting at {unix_start}, ending at {unix_end}")

        # Define a makeshift binance_symbols list for debugging
        binance_symbols = [
            {"symbol": "MEMEUSDT"},
            {"symbol": "RONINUSDT"},
        ]

        for symbol_item in binance_symbols:
            symbol = symbol_item.get('symbol')
            
            print(f"Current Symbol: {symbol}")
            raw_history = get_binance_trade_history(bin_api_key, bin_secret_key, unix_start, unix_end, symbol)
            print(raw_history)
            trade_history_in_range.extend(raw_history)

            # Spot Limit - 6000, Limit 300 Calls Per Minute
            time.sleep(0.3)  
        
        # Save info and update time
        trade_history_full.extend(trade_history_in_range)
        current_start_time = current_end_time 

    return trade_history_full

def get_bin_history(mode, bin_api_key, bin_secret_key):
    current_time_exact = datetime.now()

    # Convert the date back to a datetime object at midnight (00:00:00)
    current_date = datetime.combine(current_time_exact, datetime.min.time())

    end_date = current_date 

    # 2 Modes
    if mode == 'Full': 
        print('Getting data from current date to 2 years ago')
        start_date = end_date - timedelta(days=730)  # 2 years = 730 days
        
    elif mode == 'Weekly':
        print('Getting data from current date to 1 week ago')
        #start_date = end_date - timedelta(days=100) # Debug check RON and MEME
        start_date = end_date - timedelta(weeks=1)

    binance_symbols = get_binance_symbols()
    raw_result = loop_get_binance_history(bin_api_key, bin_secret_key, start_date, end_date, binance_symbols)

    return raw_result

