import json
import requests
import hashlib
import hmac
import time
import os
from requests.exceptions import ConnectTimeout
from dotenv import load_dotenv

# Dictionary to cache prices - Shared for binance and bybit for perfomance reasons
price_cache = {}

load_dotenv()

def exchange_assets(event):
    print(event)
    function_to_run = event.get('function_to_run')

    # Owners and mapping to PIC
    #acc_owners = ['A']
    acc_owners = ['J', 'JM2', 'VKEE', 'KS']

    pic = {
        "A": "Test",
        "J": "Jansen",
        "VKEE": "Vkee",
        "JM": "Joshua Moh",
        "JM2": "Joshua Moh",
        "KS": "KS",
    }

    results = []

    for owner in acc_owners:
        print(f'Checking Owner: {owner}')
        
        bin_api_key = os.getenv(f'{owner}_BIN_API_KEY', 'none')
        bin_secret_key = os.getenv(f'{owner}_BIN_SECRET_KEY', 'none')
        bb_api_key = os.getenv(f'{owner}_BYBIT_API_KEY', 'none')
        bb_secret_key = os.getenv(f'{owner}_BYBIT_SECRET_KEY', 'none')
        
        print(f"Function Running: {function_to_run}")
        print(f"Bybit: {bb_api_key}")

        try:
            if function_to_run == 'get_bin_spot' and bin_api_key != 'none':
                response = get_bin_spot(bin_api_key, bin_secret_key)
                statusCode = response.get('statusCode', 500)
                if statusCode == 200:
                    results.append({'owner': pic[owner], 'exchange_asset': json.loads(response.get('body'))})
                else:
                    return response

            elif function_to_run == 'get_bin_perp' and bin_api_key != 'none':
                response = get_bin_perp(bin_api_key, bin_secret_key)
                statusCode = response.get('statusCode', 500)
                if statusCode == 200:
                    results.append({'owner': pic[owner], 'exchange_asset': json.loads(response.get('body'))})
                else:
                    return response

            elif function_to_run == 'get_bybit_bal' and bb_api_key != 'none':
                response = get_bybit_bal(bb_api_key, bb_secret_key, 'FUND')
                if response.get('statusCode') == 200:
                    results.append({'owner': pic[owner], 'exchange_asset': json.loads(response.get('body'))})
                else:
                    return response

            elif function_to_run == 'get_bybit_unified_balance' and bb_api_key != 'none':
                raw_result = get_bybit_unified_balance(bb_api_key, bb_secret_key, 'UNIFIED')
                print(raw_result)
                
                if raw_result.get('statusCode') == 200:
                    
                    result_body = raw_result.get('body')
                    parsed_result = parse_bybit_balance(result_body)
                    exchange_data = parsed_result.get('body')
                    
                    results.append({'owner': pic[owner], 'exchange_asset': exchange_data})
                    
                else:
                    return raw_result
        
        except ConnectTimeout:
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "message": "Lambda Timedout",
                    "details": str(e)
                })
            }

        except Exception as e:
            print(f"Error occurred for owner: {owner}. Exception: {str(e)}")
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "error": "Internal Server Error",
                    "details": str(e)
                })
            }

    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }

def get_bin_spot(bin_api_key, bin_secret_key):
    try:
        base_url = 'https://api.binance.com'
        timestamp = int(time.time() * 1000)
        params = f'timestamp={timestamp}'
        signature = hmac.new(bin_secret_key.encode('utf-8'), params.encode('utf-8'), hashlib.sha256).hexdigest()
    
        headers = {
            'X-MBX-APIKEY': bin_api_key
        }
    
        url = f"{base_url}/sapi/v3/asset/getUserAsset?{params}&signature={signature}"
        
        response = requests.post(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            holdings = []
            
            for token in data:
                asset = token.get('asset','')
                balance = float(token.get('free',0))
                
                if balance <= 0:
                    continue
                
                if asset in ["USDC", "USDT"]:
                    if balance < 10:
                        continue
                    
                    holding = {
                        "exchange": "binance",
                        "type": "spot",
                        "asset": asset,
                        "balance": balance,
                        "price": float(1.0),
                        "total_usd_value": balance
                    }
                
                    holdings.append(holding)
                
                else:
                    price = get_bin_price(asset)
                    total_usd_value = float("{:.2f}".format(price * balance))
                    
                    if total_usd_value < 10:
                        continue
                    
                    holding = {
                        "exchange": "binance",
                        "type": "spot",
                        "asset": asset,
                        "balance": balance,
                        "price": price,
                        "total_usd_value": total_usd_value
                    }
                    
                    holdings.append(holding)
        
            return {
                "statusCode": 200,
                "body": json.dumps(holdings)
            }
        
    except ConnectTimeout:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Request timed out while geting binance spot"
            })
        }
    except requests.exceptions.RequestException as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error fetching balance: " + str(e)
            })
        }
    
def get_bin_perp(bin_api_key, bin_secret_key):
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
            balance = response.json().get('totalMarginBalance', 0)
            holdings = []
            
            holdings_perp = {
                "exchange": "binance",
                "type": "perps",
                "asset": "Binance Perp Equity",
                "balance": balance,
                "price": float(1.0),
                "total_usd_value": balance
            }
            
            holdings.append(holdings_perp)
        
            return {
                "statusCode": 200,
                "body": json.dumps(holdings)
            }
        
    except ConnectTimeout:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Request timed out while getting binance perps"
            })
        }
    except requests.exceptions.RequestException as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error fetching balance: " + str(e)
            })
        }
    
def get_bybit_bal(bb_api_key, bb_secret_key, accountType):
    url = "https://api.bybit.com/v5/asset/transfer/query-account-coins-balance"
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
        print(f"Bybit Bal: {response.json()}")
        
        if response.status_code == 200:
            data = response.json()
            wallet_tokens = data.get('result', {}).get('balance', [])
        
            holdings = []
            
            for token in wallet_tokens:
                asset = token.get('coin', '')
                balance = float(token.get('walletBalance', 0))
                
                if balance <= 0:
                    continue
                
                if asset in ["USDC", "USDT"]:
                    if balance <= 1:
                        continue
                    
                    if accountType == "UNIFIED":
                        asset = "Bybit Perp Equity"
                    
                    holding = {
                        "exchange": "bybit",
                        "type": accountType,
                        "asset": asset,
                        "balance": balance,
                        "price": float(1.0),
                        "total_usd_value": balance
                    }
                
                    holdings.append(holding)
                
                else:
                    price = get_bybit_price(asset)
                    total_usd_value = float(price * balance)
                    
                    if total_usd_value < 10:
                        continue
                    
                    holding = {
                        "exchange": "bybit",
                        "type": accountType,
                        "asset": asset,
                        "balance": balance,
                        "price": price,
                        "total_usd_value": total_usd_value
                    }
                    
                    holdings.append(holding)
            
            return {
                "statusCode": 200,
                "body": json.dumps(holdings)
            }
        
    except ConnectTimeout:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Request timed out while getting bybit funding bal"
            })
        }
    except requests.exceptions.RequestException as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error fetching balance: " + str(e)
            })
        }

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
        print(f"Bybit Unified Bal: {response.json()}")
        
        if response.status_code == 200:
            data = response.json()
            print(data)
            
            return {
                "statusCode": 200,
                "body": data
            }
        
    except ConnectTimeout:
        print("timeout bro")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Request timed out while fetching Bybit unified balance"
            })
        }
    except requests.exceptions.RequestException as e:
        print("something else bro")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error fetching Bybit unified balance: " + str(e)
            })
        }

def parse_bybit_balance(bybit_unified_bal):
    
    holdings = []
    result = bybit_unified_bal.get('result')
    list = result.get('list')
    coins = list[0].get('coin')

    for coin_info in coins:
        asset_name = coin_info.get('coin')
        token_balance = float(coin_info.get('walletBalance'))
        equity = float(coin_info.get('equity'))

        # Filter tokens with 0 balance
        if token_balance <= 0:
            continue
        
        if asset_name == "USDC" or asset_name == "USDT":

            if token_balance <=1:
                continue

            holding = {
                "exchange": "bybit",
                "type": 'UNIFIED',
                "asset": 'Bybit Perp Equity',
                "balance": equity,
                "price": float(1.0),
                "total_usd_value": equity
            }

            holdings.append(holding)
        
        else: 

            token_price = get_bybit_price(asset_name)
            total_usd_value = float(coin_info.get('usdValue'))
            
            # If value is less than 10 USD ignore
            if total_usd_value < 10:
                continue   

            holding = {
                "exchange": "bybit",
                "type": 'UNIFIED',
                "asset": asset_name,
                "balance": token_balance,
                "price": token_price,
                "total_usd_value": total_usd_value
            }
            
            holdings.append(holding)

    return {
        "statusCode": 200,
        "body": holdings
    }


# Utility Functions
def get_bin_price(asset):
    # Check if the price is already in the cache
    if asset in price_cache:
        return price_cache[asset]
    
    # If not, fetch the price from the API
    url = f'https://api.binance.com/api/v3/ticker/price?symbol={asset}USDT'
    response = requests.get(url)
    data = response.json() 
    
    # Convert price to float and cache it
    price = float(data.get('price', 0.0))
    price_cache[asset] = price
    
    return price
    
def get_bybit_price(asset):
    # Check if the price is already in the cache
    if asset in price_cache:
        return price_cache[asset]
    
    # If not, fetch the price from the API
    url = f'https://api.bybit.com/spot/v3/public/quote/ticker/price?symbol={asset}USDT'
    response = requests.get(url)
    data = response.json()
    
    # Convert price to float and cache it
    price = float(data.get('result', {}).get('price', 0.0))
    price_cache[asset] = price
    
    return price

def save_to_json(data, filename):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)