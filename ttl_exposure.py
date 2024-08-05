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


def view_account(event):
    # Get the payload data 
    start_date = event.get('start_date')
    end_date = event.get('end_date')
    
    # For open position logic
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
    current_date = date.today()

    print(f'Obtaining exchange pnl from {start_date} to {end_date}')
    
    acc_owners = ['J', 'JM2', 'VKEE']
    
    # Owner to PIC
    pic = {
        "A" : "Adam",
        "J" : "Jansen",
        "VKEE": "Vkee",
        "JM": "Joshua Moh",
        "JM2": "Joshua Moh",
        "KS": "KS"
    }
    
    df_all_pnl = pd.DataFrame() 

    for owner in acc_owners:
        
        bybit_api_key = os.environ.get(owner + '_BYBIT_API_KEY', 'none')
        bybit_secret_key = os.environ.get(owner + '_BYBIT_SECRET_KEY', 'none')
        bin_api_key = os.environ.get(owner + '_BIN_API_KEY', 'none')
        bin_secret_key = os.environ.get(owner + '_BIN_SECRET_KEY', 'none')
        
        print(f'{owner} bybit ')
        
        """ Bybit Section """
        df_bb_closed_pnl = pd.DataFrame()
        df_bb_open_pnl = pd.DataFrame()
        
        if bybit_api_key != 'none':
            # Do not take current open position if the timeframe does not include the current date
            if end_date_obj == current_date:
                print(f'{owner} bybit open ') # DEBUG
                bb_open_pnl = bybit_open_pnl(bybit_api_key, bybit_secret_key, 'linear','USDT')

                # Obtain unified data to get Equity for Effective Leverage
                raw_bybit_unified_data = get_bybit_unified_balance(bybit_api_key, bybit_secret_key, "UNIFIED")
                bybit_equity = parse_bybit_unified(raw_bybit_unified_data)

                df_bb_open_pnl = parse_bb_open(bb_open_pnl, bybit_equity)
            
            print(f'{owner} bybit closed ') # DEBUG
            df_bb_closed_pnl = loop_bb_closed(bybit_api_key, bybit_secret_key, 'linear', start_date, end_date)
        
        """ Binance Section """
        df_bin_closed_pnl = pd.DataFrame()
        df_bin_open_pnl = pd.DataFrame()
        
        if bin_api_key != 'none': 

            if end_date_obj == current_date:
                print(f'{owner} binance open ') # DEBUG
                bin_open_pnl = binance_open_pnl(bin_api_key, bin_secret_key)
                
                # Obtain binance perp data to get Equity for Effective Leverage
                raw_binance_futures = get_binance_perp(bin_api_key, bin_secret_key)
                binance_equity = parse_binance_perps(raw_binance_futures)

                df_bin_open_pnl = parse_bin_open(bin_open_pnl, current_date, binance_equity)
            
            print(f'{owner} binance closed ') # DEBUG
            df_bin_closed_pnl = loop_bin_closed(bin_api_key, bin_secret_key, start_date, end_date)
            # save_dataframe_to_csv(df_bin_closed_pnl, "closed_binance.csv") # Debug
        
        """ Combined """
        df_combined = combine_dataframes(df_bb_closed_pnl, df_bb_open_pnl, df_bin_closed_pnl, df_bin_open_pnl)
        # save_dataframe_to_csv(df_combined, 'combined.csv') #Debug
        
        final_owner_pnl = aggregate_df(df_combined, pic.get(owner)) # Causes incorrect string values

        df_all_pnl = pd.concat([df_all_pnl, final_owner_pnl], axis=0)
        
        json_all_pnl = df_all_pnl.to_json(orient='records', date_format='iso')
        print(json_all_pnl)
    
    return {
        'statusCode': 200,
        'body': json_all_pnl
    }

# Utility Functions
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

def combine_dataframes(bb_close, bb_open, bin_close, bin_open ):

    # Filter out empty dataframes
    dataframes = [df for df in [bb_close, bb_open, bin_close, bin_open] if not df.empty]

    if not dataframes:
        return pd.DataFrame()

    # Combine all dataframes
    combined_df = pd.concat(dataframes, axis=0)
    
    # Reset the index if you want a continuous index in the combined dataframe
    combined_df.reset_index(drop=True, inplace=True)
    
    return combined_df

def aggregate_df(df, owner):
    # Conditional because of not all df will exist

    if df.empty:
        return pd.DataFrame()

    if 'uPnL' in df.columns:
        df['uPnL'] = df['uPnL'].astype(float)

    if 'uPnL' in df.columns:
        df['rPnL'] = df['rPnL'].astype(float)

    if 'date' in df.columns:
        df['date'] = df['date'].astype(str)

    # Group by 'status' ,'date', 'symbol' and 'exchange then sum the 'uPnL' and 'rPnL' columns
    combined_df = df.groupby(['status','date', 'symbol', 'exchange'], as_index=False).sum()

    # Calculate total PnL
    combined_df['tPnL'] = combined_df['uPnL'] + combined_df['rPnL']

    # Add the 'category' field
    combined_df['category'] = 'futures'

    # Add the 'owner' field
    combined_df['owner'] = owner
    
    return combined_df

def save_dataframe_to_csv(dataframe, file_path):
    try:
        dataframe.to_csv(file_path, index=False)
        print(f"DataFrame successfully saved to {file_path}")
    except Exception as e:
        print(f"An error occurred while saving the DataFrame to CSV: {e}")

def save_to_json(data, filename):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)


#"""Binance API Caller"""
def binance_open_pnl (bin_api_key, bin_secret_key):
    
    base_url = 'https://fapi.binance.com'
    
    try:
        
        timestamp = int(time.time() * 1000)
        params = f'timestamp={timestamp}'
        signature = hmac.new(bin_secret_key.encode('utf-8'), params.encode('utf-8'), hashlib.sha256).hexdigest()

        headers = {
            'X-MBX-APIKEY': bin_api_key
        }

        url = f"{base_url}/fapi/v2/positionRisk?{params}&signature={signature}"

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Failed with status code {response.status_code}.")
            
    except ConnectTimeout:
        print(f"Binance Open Attempt failed due to connection timeout")
    

def binance_closed_pnl (bin_api_key, bin_secret_key, unix_start , unix_end):
    
    base_url = 'https://fapi.binance.com'
    
    try:

        timestamp = int(time.time() * 1000)
        limit = 1000
        params = f'timestamp={timestamp}&limit={limit}&startTime={unix_start}&endTime={unix_end}'

        signature = hmac.new(bin_secret_key.encode('utf-8'), params.encode('utf-8'), hashlib.sha256).hexdigest()

        headers = {
            'X-MBX-APIKEY': bin_api_key
        }
    
        url = f"{base_url}/fapi/v1/userTrades?{params}&signature={signature}"
    
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Failed with status code {response.status_code}.")
            
    except ConnectTimeout:
        print(f"Binance closed Attempt failed due to connection timeout")


def get_binance_perp(bin_api_key, bin_secret_key):
    try:
        base_url = 'https://fapi.binance.com'
        timestamp = int(time.time() * 1000)
        params = f'timestamp={timestamp}'
        signature = hmac.new(bin_secret_key.encode('utf-8'), params.encode('utf-8'), hashlib.sha256).hexdigest()
    
        headers = {
            'X-MBX-APIKEY': bin_api_key
        }
    
        url = f"{base_url}/fapi/v2/account?{params}&signature={signature}"
    
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            
            return data

    except ConnectTimeout:
        print(f"Binance perps failed due to connection timeout")


#"""Bybit API Caller"""
def bybit_closed_pnl (bb_api_key, bb_secret_key, category, start_time, end_time, cursor) : 

    url = "https://api.bybit.com/v5/position/closed-pnl"
    recv_window = '2000'
    limit = '200'
    parse_cursor = urllib.parse.quote(cursor)
    
    parameters = {
        "category" : category,
        "startTime" : start_time,
        "endTime": end_time,
        "limit": limit,
        "cursor": cursor,
    }
    
    try:
        timestamp = str(int(time.time() * 1000))
        queryString = f"category={category}&startTime={start_time}&endTime={end_time}&limit={limit}&cursor={parse_cursor}"
        param_str = f'{timestamp}{bb_api_key}{recv_window}{queryString}'
        signature = hmac.new(bb_secret_key.encode('utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()
    
        headers = {
            "accept": "application/json",
            'X-BAPI-SIGN': signature,
            'X-BAPI-API-KEY': bb_api_key,
            'X-BAPI-TIMESTAMP': timestamp,
            'X-BAPI-RECV-WINDOW': recv_window
        }

        response = requests.get(url, headers=headers, params = parameters)
        
        if response.status_code == 200 and response is not None:
                data = response.json()
                return data
        else:
            print(f"Attempt failed")

    except ConnectTimeout:
        print(f"Bybit Closed Attempt failed due to connection timeout")
    
    
def bybit_open_pnl (bb_api_key, bb_secret_key, category, settleCoin) : 
    url = "https://api.bybit.com/v5/position/list"
    limit = '200'
    
    print(f"Bybit API Key: {bb_api_key}")
    
    queryString = f"category={category}&limit={limit}&settleCoin={settleCoin}"
    
    parameters = {
        "category" : category,
        "limit": limit,
        "settleCoin": settleCoin # USDT
    }
    
    try:
        timestamp = str(int(time.time() * 1000))
        queryString = f"category={category}&limit={limit}&settleCoin={settleCoin}"
        param_str = f'{timestamp}{bb_api_key}{queryString}'
        signature = hmac.new(bb_secret_key.encode('utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()
    
        headers = {
            "accept": "application/json",
            'X-BAPI-SIGN': signature,
            'X-BAPI-API-KEY': bb_api_key,
            'X-BAPI-TIMESTAMP': timestamp
        }

        response = requests.get(url, headers=headers, params = parameters)
        
        if response.status_code == 200 and response is not None:
            data = response.json()
            return data
        else:
            print(f"Attempt failed")

    except ConnectTimeout:
        print(f"Bybit Open Attempt failed due to connection timeout.")
    

def get_bybit_unified_balance(bb_api_key, bb_secret_key, accountType):
    url = "https://api.bybit.com/v5/account/wallet-balance"
    timestamp = str(int(time.time() * 1000))
    
    parameters = {
        "accountType": accountType
    }
    
    queryString = f"accountType={accountType}"
    param_str = f'{timestamp}{bb_api_key}{queryString}'
    signature = hmac.new(bb_secret_key.encode('utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()
    
    try:
        headers = {
            "accept": "application/json",
            'X-BAPI-SIGN': signature,
            'X-BAPI-API-KEY': bb_api_key,
            'X-BAPI-TIMESTAMP': timestamp,
        }
        
        response = requests.get(url, headers=headers, params=parameters)
        
        if response.status_code == 200:
            data = response.json()
            
            return data
        
    except ConnectTimeout:
        print(f"Bybit unified failed due to connection timeout")


#"""Binance Parsing"""
def parse_bin_closed (bin_api_key, bin_secret_key, unix_start, unix_end):
    bin_closed_data = []
    bin_closed_pnl = binance_closed_pnl(bin_api_key, bin_secret_key, unix_start, unix_end)

    for order in bin_closed_pnl:
        symbol = order.get('symbol')
        unix_time = order.get('time')
        date = convert_timestamp_to_date(unix_time)
        realizedPnl = order.get('realizedPnl')

        # Calculations
        price = float(order.get('price'))
        order_qty = float(order.get('qty'))

        # Calculated Val
        notional = price * order_qty

        order_data = {
            'status': 'Closed',
            'date': date,
            'symbol': symbol,
            'exchange': 'binance-perps',
            'invested_value': '', # Cannot Find because leverage is not shown
            'equity': '',
            'notional': notional, 
            'effective_lev': '',
            'uPnL': '0',
            'rPnL': realizedPnl
        }

        bin_closed_data.append(order_data)
    
    df_bin_closed = pd.DataFrame(bin_closed_data)

    return df_bin_closed

def loop_bin_closed (bin_api_key, bin_secret_key, start_time, end_time):
    start_time = datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.strptime(end_time, '%Y-%m-%d')

    # Current start time for the loop
    current_start_time = start_time
    
    result_df = pd.DataFrame()

    # Loop until current start time exceeds end time
    while current_start_time < end_time:
        current_end_time = min(current_start_time + timedelta(days=1), end_time)

        unix_start = convert_to_unix(current_start_time)
        unix_end = convert_to_unix(current_end_time)

        # Calls function that calls the api
        df_bin_closed_weekly = parse_bin_closed(bin_api_key, bin_secret_key, unix_start, unix_end)
        result_df = pd.concat([result_df, df_bin_closed_weekly], ignore_index=True)
        
        current_start_time = current_end_time 
    
    return result_df

def parse_bin_open (bin_open_pnl, current_date, binance_equity):
    
    bin_open_data = []
    total_notional = 0

    for item in bin_open_pnl:
        symbol = item.get('symbol')
        positionAmt = float(item.get('positionAmt'))

        if positionAmt <= 0:
            continue
        
        notional = float(item.get('notional'))
        total_notional += notional
        unRealizedProfit = item.get('unRealizedProfit')

        order_data = {
            'status': 'Open',
            'date': str(current_date),
            'symbol': symbol,
            'exchange': 'binance-perps',
            'invested_value': '',
            'equity': float(binance_equity),
            'notional': notional, 
            'effective_lev': 0, # Placeholder
            'uPnL': unRealizedProfit,
            'rPnL': '0' # Why is rPnL zero for binance open? - API doesnt show rPnL
        }
        
        bin_open_data.append(order_data)
    
    effective_lev = float(total_notional)/float(binance_equity) # Total Notional / Total Equity
    
    # Update all effective lev value
    for order_data in bin_open_data:
        order_data['effective_lev'] = effective_lev

    df_bin_open = pd.DataFrame(bin_open_data)

    if df_bin_open.empty:
        print(f"No open positions currently")
        return pd.DataFrame()
        
    return df_bin_open

def parse_binance_perps(raw_binance_futures):
    totalMarginBalance = raw_binance_futures.get('totalMarginBalance')

    return totalMarginBalance


#"""Bybit Parsing"""
def parse_bb_closed (bb_api_key, bb_secret_key, category, unix_start, unix_end):
    
    cursor = ""
    df_all = pd.DataFrame()

    while True:

        bb_closed_data = []
        bb_closed_pnl = bybit_closed_pnl(bb_api_key, bb_secret_key, category, unix_start, unix_end, cursor)
    
        result = bb_closed_pnl.get('result', {})

        if not result:
            print(f"No closed positions from {unix_start} to {unix_end}")
            return pd.DataFrame()
    
        list = result.get('list')
        cursor = result.get('nextPageCursor')

        for item in list:
            symbol = item.get('symbol')
            updatedTime = item.get('updatedTime')
            date = convert_timestamp_to_date(updatedTime)
            closedPnl = item.get('closedPnl')

            # Calculations
            avgEntryPrice = float(item.get('avgEntryPrice'))
            order_qty = float(item.get('qty'))
            leverage = float(item.get('leverage'))

            # Calculated Val
            notional = avgEntryPrice * order_qty
            invested_value = (avgEntryPrice * order_qty)/leverage

            order_data = {
                'status': 'Closed',
                'date': date,
                'symbol': symbol,
                'exchange': 'bybit-unified',
                'invested_value': invested_value,
                'equity': '',
                'notional': notional, 
                'effective_lev': '',
                'uPnL' : '0',
                'rPnL': closedPnl
            }
            
            bb_closed_data.append(order_data)
        
        df_bb_closed = pd.DataFrame(bb_closed_data)
        df_all = pd.concat([df_all, df_bb_closed], ignore_index=True)

        if not cursor:
            break
        
    return df_all
    
def loop_bb_closed (bb_api_key, bb_secret_key, category, start_time, end_time):
    
    start_time = datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.strptime(end_time, '%Y-%m-%d')
    
    # Current start time for the loop
    current_start_time = start_time
    
    result_df = pd.DataFrame()
    
    # Loop until current start time exceeds end time
    while current_start_time < end_time:
        current_end_time = min(current_start_time + timedelta(days=1), end_time)

        unix_start = convert_to_unix(current_start_time)
        unix_end = convert_to_unix(current_end_time)

        # Calls function that calls the api
        df_bb_closed_weekly = parse_bb_closed(bb_api_key, bb_secret_key, category, unix_start, unix_end)
        result_df = pd.concat([result_df, df_bb_closed_weekly], ignore_index=True)
        
        current_start_time = current_end_time 
    
    return result_df

def parse_bb_open (bb_open_pnl, bybit_equity):
    
    bb_open_data = []
    result = bb_open_pnl.get('result', {})
    
    if not result:
        print(f"No open positions currently")
        return pd.DataFrame()
    
    list = result.get('list')
    total_notional = 0

    for item in list: 
        symbol = item.get('symbol')
        updatedTime = item.get('updatedTime')
        date = convert_timestamp_to_date(updatedTime)
        notional = float(item.get('positionValue')) # Equity multiplied by leverage
        unrealisedPnl = item.get('unrealisedPnl')
        curRealisedPnl = item.get('curRealisedPnl')
        total_notional += notional
        
        order_data = {
            'status': 'Open',
            'date': date,
            'symbol': symbol,        
            'exchange': 'bybit-unified',
            'invested_value': '',
            'equity': bybit_equity,
            'notional': notional,
            'effective_lev': 0, 
            'uPnL': unrealisedPnl,
            'rPnL': curRealisedPnl
        }
        
        bb_open_data.append(order_data)
    
    effective_lev = float(total_notional)/float(bybit_equity) # Total Notional/ Total Equity
    
    # Update all effective lev value
    for order_data in bb_open_data:
        order_data['effective_lev'] = effective_lev

    df_bb_open = pd.DataFrame(bb_open_data)
        
    return df_bb_open

def parse_bybit_unified(raw_unified_data):
    result = raw_unified_data.get('result')
    data_list = result.get('list')

    totalEquity = float(data_list[0].get('totalEquity') or 0.0)

    return totalEquity
    
