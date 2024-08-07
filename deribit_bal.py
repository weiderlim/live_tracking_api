import json
import requests
import os 
import time
from requests.exceptions import ConnectTimeout
from dotenv import load_dotenv

load_dotenv()

def get_auth_token(api_key, secret_key):
    
    try:
        url = "https://www.deribit.com/api/v2/public/auth"
        timestamp = int(time.time() * 1000)
        grant_type = 'client_credentials' # Auth Method - client_credentials, client_signature, refresh_token	
    
        parameters = {
            "grant_type" : grant_type, 
            "client_id": api_key,
            "client_secret": secret_key,
            "timestamp": timestamp
        }
    
        headers = {
            "accept": "application/json",
        }
    
        response = requests.get(url, headers=headers, params=parameters)
        
        data = response.json()
        print(data) # Display error message

        # Handle the response and return data as needed.
        if response.status_code == 200 and response is not None:
            return data
        else:
            return {"error": f"API request failed : {response.text}", }
            
    except ConnectTimeout:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Request timed out while geting auth token"
            })
        }

def get_account_summary(bearer_token, currency):
    
    try:
        url = "https://www.deribit.com/api/v2/private/get_account_summary"
    
        parameters = {
            "currency": currency,
            "extended": 'true'
        }
    
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {bearer_token}"
        }
    
        response = requests.get(url, headers=headers, params=parameters)
        data = response.json()
        print(data)
    
        # Handle the response and return data as needed.
        if response.status_code == 200 and response is not None:
            return data
        else:
            return {"error": f"API request failed : {response.text}", }
            
    except ConnectTimeout:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Request timed out while geting deribit acc bal"
            })
        }
    
def get_token_price(currency):
    
    try:
        url = "https://www.deribit.com/api/v2/public/get_index_price"
        
        currency_to_index = {
            "ETH": "eth_usd",
            "BTC": "btc_usd",
        }
        
        if currency in ["USDT", "USDC"]:
            return 1  # Return 1 for stablecoins
    
        index_name = currency_to_index.get(currency.upper())
        
        parameters = {
            "index_name": index_name,
        }
    
        headers = {
            "accept": "application/json",
        }
    
        response = requests.get(url, headers=headers, params=parameters)
        data = response.json()
        price = data.get('result').get('index_price')
    
        # Handle the response and return data as needed.
        if response.status_code == 200 and response is not None:
            return price
        else:
            return {"error": f"API request failed : {response.text}", }
            
    except ConnectTimeout:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Request timed out while geting deribit acc bal"
            })
        }

# Main    
def show_deribit_bal():
    
    # Owners
    acc_owners = ['VKEE', 'J']
    all_deribit_data = []
    
    # Owner to PIC
    pic = {
        "J" : "Jansen",
        "VKEE": "Vkee",
        "JM": "Joshua Moh",
        "JM2": "Joshua Moh",
        "KS": "KS",
    }
    
    for owner in acc_owners :
        
        api_key = os.getenv(owner + '_DERIBIT_API_KEY', 'none')
        secret_key = os.getenv(owner + '_DERIBIT_SECRET_KEY', 'none')

        print(f'Checking Owner: {owner}')
        print(f"API Key: {api_key}")
    
        auth_data = get_auth_token(api_key, secret_key)

        print(auth_data)

        bearer_token = auth_data.get('result').get('access_token')
    
        all_currency = ['USDT', 'BTC', 'ETH', 'USDC']
        all_currency_data = []
        
        try:
            
            for currency in all_currency:
                raw_positions = get_account_summary(bearer_token, currency)
                result = raw_positions.get('result')
                equity = result.get('equity')
                
                token_price = get_token_price(currency)
                
                currency_data = {
                    "currency": currency,
                    "equity": equity,
                    "token_price": token_price
                }
                
                all_currency_data.append(currency_data)
            
            total_equity = 0.0
            
            for item in all_currency_data:
                balance = item.get('equity')
                token_price = item.get('token_price')
                usd_value = balance * token_price
                total_equity += usd_value 
            
            deribit_data = {
                "exchange": "deribit",
                "address": f"ex-deribit-{pic.get(owner)}",
                "asset" : "Deribit Equity",
                "token_balance" : total_equity,
                "token_price" : "1",
                "usd_value" : total_equity,
                "PIC" : pic.get(owner)
            }
            
            all_deribit_data.append(deribit_data)
        
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
        'statusCode': 200,
        'body': json.dumps(all_deribit_data)
    }
