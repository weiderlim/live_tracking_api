import json
import pandas as pd 
import requests
import os 
import hashlib
import hmac
import time
from datetime import datetime, timedelta, date
import math
import urllib.parse
from requests.exceptions import ConnectTimeout
from dotenv import load_dotenv
from binance_spot_history import get_bin_history

load_dotenv()

# Dictionary to cache prices - Shared for binance and bybit for perfomance reasons
price_cache = {}

def spot_pnl():

    # Owners and mapping to PIC
    #acc_owners = ['A', 'TEST', 'J', 'VKEE'] 
    acc_owners = ['J', 'JM2', 'VKEE', 'KS']

    pic = {
        "A": "Test",
        "TEST": "WD",
        "J": "Jansen",
        "VKEE": "Vkee",
        "JM": "Joshua Moh",
        "JM2": "Joshua Moh",
        "KS": "KS",
    }

    df_all_entries = pd.DataFrame()

    for owner in acc_owners:
        print(f'Checking Owner: {owner}')
        
        bb_api_key = os.getenv(f'{owner}_BYBIT_API_KEY', 'none')
        bb_secret_key = os.getenv(f'{owner}_BYBIT_SECRET_KEY', 'none')
        bin_api_key = os.getenv(f'{owner}_BIN_API_KEY', 'none')
        bin_secret_key = os.getenv(f'{owner}_BIN_SECRET_KEY', 'none')

        # Initialize time, 2 years ago -> Today
        start_date, end_date = assign_time()

        """ Bybit Section """
        if bb_api_key != 'none':
            raw_results = loop_get_bybit_history(bb_api_key, bb_secret_key, 'spot', start_date, end_date)
            df_bybit_trades = parse_bybit_trades(raw_results, pic.get(owner))

            # Withdrawals
            bybit_raw_withdrawals = get_loop_bybit_withdraw(bb_api_key, bb_secret_key, 2, start_date, end_date, '')
            df_bybit_withdrawals = parse_bybit_withdrawals(bybit_raw_withdrawals, pic.get(owner))

            # Deposits
            bybit_raw_deposits = get_loop_bybit_deposit(bb_api_key, bb_secret_key, start_date, end_date, '')
            df_bybit_deposits = parse_bybit_deposits(bybit_raw_deposits, pic.get(owner))

            # Debugs Save JSON
            #save_to_json(bybit_raw_withdrawals, f'trades/bybit_withdraws_{owner}.json')
            #save_to_json(bybit_raw_deposits, f'trades/bybit_deposits_{owner}.json')
            
            # Concat all
            df_all_entries = pd.concat([df_all_entries, df_bybit_trades, df_bybit_withdrawals, df_bybit_deposits], ignore_index=True)

        """ Binance Section """
        if bin_api_key != 'none':
            raw_results = get_bin_history('Weekly',bin_api_key, bin_secret_key)
            df_binance_trades = parse_binance_trades(raw_results, pic.get(owner))

            df_all_entries = pd.concat([df_all_entries, df_binance_trades], ignore_index=True)

            # Withdrawals
            bin_raw_withdrawals = get_loop_bin_withdraw(bin_api_key, bin_secret_key, start_date, end_date)
            df_bin_withdrawals = parse_bin_withdrawals(bin_raw_withdrawals, pic.get(owner))

            # Deposits
            bin_raw_deposits = get_loop_bin_deposit(bin_api_key, bin_secret_key, start_date, end_date)
            df_bin_deposits = parse_bin_deposits(bin_raw_deposits, pic.get(owner))

            # Debugs
            #save_to_json(bin_raw_withdrawals, f'trades/bin_withdraws_{owner}.json') # Debug
            #save_to_json(bin_raw_deposits, f'trades/bin_deposits_{owner}.json') # Debug

            # Concat all
            df_all_entries = pd.concat([df_all_entries, df_binance_trades, df_bin_withdrawals, df_bin_deposits], ignore_index=True)

    json_all_spot_pnl = df_all_entries.to_dict(orient='records')
    #save_to_json(json_all_spot_pnl, "trades/spot_pnl.json") # Debug

    return {
        "statusCode": 200,
        "body":  json.dumps(json_all_spot_pnl)
    }

""" Bybit Code Section """
# Trading History
def assign_time():
    current_time_exact = datetime.now()

    # Convert the date back to a datetime object at midnight (00:00:00)
    current_date = datetime.combine(current_time_exact, datetime.min.time())

    end_date = current_date 

    print('Getting data from current date to 2 years ago')
    start_date = end_date - timedelta(days=730)  # 2 years = 730 days

    return start_date, end_date

def get_bybit_trade_history(bb_api_key, bb_secret_key, category, start_time, end_time, cursor):
    url = "https://api.bybit.com/v5/execution/list"
    parse_cursor = urllib.parse.quote(cursor)

    parameters = {
        "category": category, # spot/linear/inverse/option
        "startTime" : start_time,
        "endTime": end_time,
        "cursor": cursor
    }
    
    try:
        timestamp = str(int(time.time() * 1000))
        queryString = f"category={category}&startTime={start_time}&endTime={end_time}&cursor={parse_cursor}"
        param_str = f'{timestamp}{bb_api_key}{queryString}'
        signature = hmac.new(bb_secret_key.encode('utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()

        headers = {
            "accept": "application/json",
            'X-BAPI-SIGN': signature,
            'X-BAPI-API-KEY': bb_api_key,
            'X-BAPI-TIMESTAMP': timestamp,
        }
        
        response = requests.get(url, headers=headers, params=parameters)
        
        if response.status_code == 200:
            data = response.json()

            return {
                "statusCode": 200,
                "body": data
            }
        
    except ConnectTimeout:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Request timed out while fetching Bybit unified balance"
            })
        }

def loop_get_bybit_history(bb_api_key, bb_secret_key, category, start_date, end_date):

    unix_start = convert_to_unix(start_date)
    unix_end = convert_to_unix(end_date)
    print(f"Bybit: Full unix range {unix_start}, {unix_end}")

    # Another Loop to collect more than 7 days data
    current_start_time = start_date # Current start time for the loop
    trade_history_full = []

    while current_start_time < end_date:
        current_end_time = min(current_start_time + timedelta(days=1), end_date)

        unix_start = convert_to_unix(current_start_time)
        unix_end = convert_to_unix(current_end_time)

        #print(f"Starting at {unix_start}, ending at {unix_end}") # Debug

        # Loop Cursor
        cursor = ""
        trade_history_in_range = []

        while True:
            raw_history = get_bybit_trade_history(bb_api_key, bb_secret_key, category, unix_start, unix_end, cursor)

            # Parsing only to entries
            result = raw_history.get('body').get('result')
            cursor = result.get('nextPageCursor')

            if not cursor:
                break
                
            history_list = result.get('list')
            trade_history_in_range.extend(history_list)
        
        # Save info and update time
        trade_history_full.extend(trade_history_in_range)
        current_start_time = current_end_time 

    return trade_history_full

def parse_bybit_trades(bybit_trade_history, owner):
    
    bybit_orders = []

    for trade in bybit_trade_history:
        
        execValue = trade.get('execPrice') # Reconfirm if execPrice or execValue will give in USD terms
        execQty = trade.get('execQty')
        usd_value = float(execValue) * float(execQty)

        order = {
            'date': convert_timestamp_to_date(trade.get('execTime')),
            'position': trade.get('symbol'),
            'action': trade.get('side'),
            'PIC': owner,
            'exchange': 'bybit-unified-spot',
            'exec_qty': execQty,
            'exec_price': execValue,
            'usd_value': usd_value
        }
    
        bybit_orders.append(order)
    
    df_bybit_orders = pd.DataFrame(bybit_orders)
    return df_bybit_orders

# Withdrawal
def get_bybit_withdraw(bb_api_key, bb_secret_key, withdraw_type, start_time, end_time, cursor):
    url = "https://api.bybit.com/v5/asset/withdraw/query-record"
    parse_cursor = urllib.parse.quote(cursor)
    withdrawType = withdraw_type # Withdraw type. 0(default): on chain. 1: off chain. 2: all

    parameters = {
        "withdrawType" : withdrawType,
        "startTime" : start_time,
        "endTime": end_time,
        "cursor": cursor
    }
    
    try:
        timestamp = str(int(time.time() * 1000))
        queryString = f"withdrawType={withdrawType}&startTime={start_time}&endTime={end_time}&cursor={parse_cursor}"
        param_str = f'{timestamp}{bb_api_key}{queryString}'
        signature = hmac.new(bb_secret_key.encode('utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()

        headers = {
            "accept": "application/json",
            'X-BAPI-SIGN': signature,
            'X-BAPI-API-KEY': bb_api_key,
            'X-BAPI-TIMESTAMP': timestamp,
        }
        
        response = requests.get(url, headers=headers, params=parameters)
        
        if response.status_code == 200:
            data = response.json()
            return {
                "statusCode": 200,
                "body": data
            }
        
    except ConnectTimeout:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Request timed out while fetching Bybit Withdrawal Records"
            })
        }

def get_loop_bybit_withdraw(bb_api_key, bb_secret_key, withdraw_type, start_date, end_date, cursor):

    unix_start = convert_to_unix(start_date)
    unix_end = convert_to_unix(end_date)
    print(f"Bybit Withdraw: Full unix range {unix_start}, {unix_end}")

    # Another Loop to collect more than 7 days data
    current_start_time = start_date # Current start time for the loop
    record_history_full = []

    while current_start_time < end_date:
        current_end_time = min(current_start_time + timedelta(days=30), end_date)

        unix_start = convert_to_unix(current_start_time)
        unix_end = convert_to_unix(current_end_time)

        #print(f"Starting at {unix_start}, ending at {unix_end}") # Debug

        # Loop Cursor
        cursor = ""
        record_in_range = []

        while True:
            raw_record = get_bybit_withdraw(bb_api_key, bb_secret_key, withdraw_type, unix_start, unix_end, cursor)
            # Parsing only to entries
            result = raw_record.get('body').get('result')
            cursor = result.get('nextPageCursor')

            if not cursor:
                break
                
            record_list = result.get('rows')
            record_in_range.extend(record_list)
        
        # Save info and update time
        record_history_full.extend(record_in_range)
        current_start_time = current_end_time 

    return record_history_full

def parse_bybit_withdrawals(bybit_withdrawals, owner): 
    bybit_orders = []

    for trade in bybit_withdrawals:
        
        symbol = trade.get('coin') 
        price = float(get_bybit_price(symbol))
        amount = float(trade.get('amount'))

        usd_value = price * amount

        order = {
            'date': convert_timestamp_to_date(trade.get('createTime')),
            'position': symbol, 
            'action': 'Withdraw',
            'PIC': owner,
            'exchange': 'bybit',
            'exec_qty': amount,
            'exec_price': price,
            'usd_value': usd_value
        }
    
        bybit_orders.append(order)
    
    df_bybit_orders = pd.DataFrame(bybit_orders)
    return df_bybit_orders

# Deposit
def get_bybit_deposit(bb_api_key, bb_secret_key, start_time, end_time, cursor):

    url = "https://api.bybit.com/v5/asset/deposit/query-record"
    parse_cursor = urllib.parse.quote(cursor)

    parameters = {
        "startTime" : start_time,
        "endTime": end_time,
        "cursor": cursor
    }
    
    try:
        timestamp = str(int(time.time() * 1000))
        queryString = f"startTime={start_time}&endTime={end_time}&cursor={parse_cursor}"
        param_str = f'{timestamp}{bb_api_key}{queryString}'
        signature = hmac.new(bb_secret_key.encode('utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()

        headers = {
            "accept": "application/json",
            'X-BAPI-SIGN': signature,
            'X-BAPI-API-KEY': bb_api_key,
            'X-BAPI-TIMESTAMP': timestamp,
        }
        
        response = requests.get(url, headers=headers, params=parameters)
        
        if response.status_code == 200:
            data = response.json()
            return {
                "statusCode": 200,
                "body": data
            }
        
    except ConnectTimeout:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Request timed out while fetching Bybit Deposit Records"
            })
        }

def get_loop_bybit_deposit(bb_api_key, bb_secret_key, start_date, end_date, cursor):

    unix_start = convert_to_unix(start_date)
    unix_end = convert_to_unix(end_date)
    print(f"Bybit Deposit: Full unix range {unix_start}, {unix_end}")

    # Another Loop to collect more than 7 days data
    current_start_time = start_date # Current start time for the loop
    record_history_full = []

    while current_start_time < end_date:
        current_end_time = min(current_start_time + timedelta(days=30), end_date)

        unix_start = convert_to_unix(current_start_time)
        unix_end = convert_to_unix(current_end_time)

        #print(f"Starting at {unix_start}, ending at {unix_end}") # Debug

        # Loop Cursor
        cursor = ""
        record_in_range = []

        while True:
            raw_record = get_bybit_deposit(bb_api_key, bb_secret_key, unix_start, unix_end, cursor)
            # Parsing only to entries
            result = raw_record.get('body').get('result')
            cursor = result.get('nextPageCursor')

            if not cursor:
                break
                
            record_list = result.get('rows')
            record_in_range.extend(record_list)
        
        # Save info and update time
        record_history_full.extend(record_in_range)
        current_start_time = current_end_time 

    return record_history_full

def parse_bybit_deposits(bybit_deposits, owner): 
    bybit_orders = []

    for trade in bybit_deposits:
        
        symbol = trade.get('coin') 
        price = float(get_bybit_price(symbol))
        amount = float(trade.get('amount'))

        usd_value = price * amount

        order = {
            'date': convert_timestamp_to_date(trade.get('successAt')),
            'position': symbol, 
            'action': 'Deposit',
            'PIC': owner,
            'exchange': 'bybit',
            'exec_qty': amount,
            'exec_price': price,
            'usd_value': usd_value
        }
    
        bybit_orders.append(order)
    
    df_bybit_orders = pd.DataFrame(bybit_orders)
    return df_bybit_orders


""" Binance Code Section """
# Trade History
def parse_binance_trades(binance_trade_history, owner):
    
    binance_orders = []

    for trade in binance_trade_history:
        
        price = trade.get('price') # Reconfirm if execPrice or execValue will give in USD terms
        quantity = trade.get('qty')
        usd_value = float(price) * float(quantity)
        isBuyer = trade.get('isBuyer')
        action = ''

        # Decide buy or sell
        if isBuyer is False:
            action = "Sell"
        elif isBuyer is True:
            action = "Buy"

        order = {
            'date': convert_timestamp_to_date(trade.get('time')),
            'position': trade.get('symbol'),
            'action': action, 
            'PIC': owner,
            'exchange': 'binance-spot',
            'exec_qty': quantity,
            'exec_price': price,
            'usd_value': usd_value
        }
    
        binance_orders.append(order)
    
    df_binance_orders = pd.DataFrame(binance_orders)
    return df_binance_orders

# Withdrawal
def get_bin_withdraw(bin_api_key, bin_secret_key, start_time, end_time):

    base_url = 'https://api.binance.com'
    timestamp = int(time.time() * 1000)
    params = f'timestamp={timestamp}&startTime={start_time}&endTime={end_time}'
    signature = hmac.new(bin_secret_key.encode('utf-8'), params.encode('utf-8'), hashlib.sha256).hexdigest()

    headers = {
        'X-MBX-APIKEY': bin_api_key
    }

    url = f"{base_url}/sapi/v1/capital/withdraw/history?{params}&signature={signature}"

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()

        return data

def get_loop_bin_withdraw(bin_api_key, bin_secret_key, start_date, end_date):
    unix_start = convert_to_unix(start_date)
    unix_end = convert_to_unix(end_date)
    print(f"Binance Withdrawal: Full unix range {unix_start}, {unix_end}")

    # Another Loop to collect more than 90 days data
    current_start_time = start_date # Current start time for the loop
    record_history_full = []

    while current_start_time < end_date:
        current_end_time = min(current_start_time + timedelta(days=30), end_date)

        unix_start = convert_to_unix(current_start_time)
        unix_end = convert_to_unix(current_end_time)

        #print(f"Starting at {unix_start}, ending at {unix_end}") # Debug

        raw_record = get_bin_withdraw(bin_api_key, bin_secret_key, unix_start, unix_end)
        
        # Save info and update time
        record_history_full.extend(raw_record)
        current_start_time = current_end_time 

    return record_history_full

def parse_bin_withdrawals(bin_raw_withdrawals, owner):
    binance_orders = []

    for trade in bin_raw_withdrawals:
        
        # Filter only completed transactions
        status = trade.get('status')
        if status != 6:
            continue

        symbol = trade.get('coin') 
        price = float(get_bin_price(symbol))
        amount = float(trade.get('amount'))

        usd_value = price * amount

        date = extract_date(trade.get('completeTime'))

        order = {
            'date': date,
            'position': symbol, 
            'action': 'Withdraw',
            'PIC': owner,
            'exchange': 'binance',
            'exec_qty': amount,
            'exec_price': price,
            'usd_value': usd_value
        }
    
        binance_orders.append(order)
    
    df_bybit_orders = pd.DataFrame(binance_orders)
    return df_bybit_orders

# Deposit
def get_bin_deposit(bin_api_key, bin_secret_key, start_time, end_time):

    base_url = 'https://api.binance.com'
    timestamp = int(time.time() * 1000)
    params = f'timestamp={timestamp}&startTime={start_time}&endTime={end_time}'
    signature = hmac.new(bin_secret_key.encode('utf-8'), params.encode('utf-8'), hashlib.sha256).hexdigest()

    headers = {
        'X-MBX-APIKEY': bin_api_key
    }

    url = f"{base_url}/sapi/v1/capital/deposit/hisrec?{params}&signature={signature}"

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()

        return data

def get_loop_bin_deposit(bin_api_key, bin_secret_key, start_date, end_date):
    unix_start = convert_to_unix(start_date)
    unix_end = convert_to_unix(end_date)
    print(f"Binance Deposit: Full unix range {unix_start}, {unix_end}")

    # Another Loop to collect more than 90 days data
    current_start_time = start_date # Current start time for the loop
    record_history_full = []

    while current_start_time < end_date:
        current_end_time = min(current_start_time + timedelta(days=30), end_date)

        unix_start = convert_to_unix(current_start_time)
        unix_end = convert_to_unix(current_end_time)

        #print(f"Starting at {unix_start}, ending at {unix_end}") # Debug

        raw_record = get_bin_deposit(bin_api_key, bin_secret_key, unix_start, unix_end)
        
        # Save info and update time
        record_history_full.extend(raw_record)
        current_start_time = current_end_time 

    return record_history_full

def parse_bin_deposits(bin_raw_deposits, owner):
    binance_orders = []

    for trade in bin_raw_deposits:
        
        # Filter only completed transactions
        status = trade.get('status')
        if status != 1: 
            continue

        symbol = trade.get('coin') 
        price = float(get_bin_price(symbol))
        amount = float(trade.get('amount'))

        usd_value = price * amount

        date = convert_timestamp_to_date(trade.get('insertTime'))

        order = {
            'date': date,
            'position': symbol, 
            'action': 'Deposit',
            'PIC': owner,
            'exchange': 'binance',
            'exec_qty': amount,
            'exec_price': price,
            'usd_value': usd_value
        }
    
        binance_orders.append(order)
    
    df_bybit_orders = pd.DataFrame(binance_orders)
    return df_bybit_orders

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

def extract_date(datetime_str):
    # Convert the string to a datetime object
    dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")

    date_only = dt.strftime('%Y-%m-%d')
    # Return only the date part as a string
    return date_only

""" Utility Prices """
def get_bybit_price(asset):
    # Check if the price is already in the cache
    if asset in price_cache:
        return price_cache[asset]
    
    # Stablecoins
    if asset in ['USDT', 'USDC', 'BUSD']:
        return 1.0
    
    # If not, fetch the price from the API

    url = f'https://api.bybit.com/v5/market/tickers?category=spot&symbol={asset}USDT'
    response = requests.get(url)
    data = response.json()

    # Convert price to float and cache it
    result = data.get('result', {})
    list = result.get('list')[0]
    lastPrice = list.get('lastPrice')

    price_cache[asset] = lastPrice
    
    return lastPrice

def get_bin_price(asset):
    # Check if the price is already in the cache
    if asset in price_cache:
        return price_cache[asset]
    
    # Stablecoins
    if asset in ['USDT', 'USDC', 'BUSD']:
        return 1.0
    
    # If not, fetch the price from the API
    url = f'https://api.binance.com/api/v3/ticker/price?symbol={asset}USDT'
    response = requests.get(url)
    data = response.json() 
    
    # Convert price to float and cache it
    price = float(data.get('price', 0.0))
    price_cache[asset] = price
    
    return price

spot_pnl()

# order_data = {
#             'status': 'Closed',
#             'date': date,
#             'symbol': symbol,
#             'exchange': 'binance-perps',
#             'invested_value': '', # Cannot Find because leverage is not shown
#             'equity': '',
#             'notional': notional, 
#             'effective_lev': '',
#             'side': side,
#             'uPnL': '0',
#             'rPnL': realizedPnl
#         }

# Scenarios
# Keep funds in unified trading acc (Buy and sell there) - Accounted for Best Scenario
# Buy in unified, move to funding
# Sell by using convert feature (How is this recorded?)

# Rules
# Funding can only sell by using convert feature

# Current Price - API for unrealized PNL
# Current Spot Balance - Unrealized  


# Deposit Records 
# 1. Onchain - https://bybit-exchange.github.io/docs/v5/asset/deposit/deposit-record
# 2. Internal Deposit (Subaccounts) - https://bybit-exchange.github.io/docs/v5/asset/transfer/inter-transfer-list
# 3. Internal Deposit (Other Bybit Acc) - https://bybit-exchange.github.io/docs/v5/asset/transfer/unitransfer-list

# Withdraw Records 
# 1. https://bybit-exchange.github.io/docs/account-asset/withdraw-record

# Unified - Funding (Balance)
# 1. Current Balance

# Process
# 1. Get all buy and sell records
# 2. Add Transfer in and out as buy/sell records


# Flow
# Set Time
# API Calls startTime + 1 Day (The most can be extended is 7 days), until endTime


# Binance Side
# Create a new script to refresh data pass 7 days (but loop for 2 years, should have a way to disable 2 years query)
# Read from database on EC2 
#  
