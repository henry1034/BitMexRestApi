import json
import hashlib
import requests
from datetime import datetime, timezone

class BitMexRest():

    def __init__(self, api_key, api_secret, test=True):
        
        """"
       Initalise Class for BitMex REST API using api_key and api_secret

       Test = True for testnet, False for mainnet
       
       """  
   
        self.api_key = api_key
        self.api_secret = api_secret
        
        if test:
            
            self.baseurl = 'https://testnet.bitmex.com/api/v1'

        else:
            self.baseurl = 'https://www.bitmex.com/api/v1'
        
    
    def generate_signature(self, secret, verb, url, nonce, data=''):
       
        """
        Generate a request signature compatible with BitMEX
        """

        message = verb + url + str(nonce) + data
        signature = hmac.new(
            bytes(secret, 'utf8'), bytes(message, 'utf8'), digestmod=hashlib.sha256
        ).hexdigest()
        return signature

    def get_headers(self, verb, endpoint, data):
        
        """
        Generate headers for a REST request
        """

        api_url = '/api/v1' + endpoint
        # Ensure nonce is in UTC
        nonce = int(datetime.now(timezone.utc).timestamp() * 1000)
        signature = self.generate_signature(self.api_secret, verb, api_url, nonce, data)
        
        headers = {
            'api-expires': str(nonce),
            'api-key': self.api_key,
            'api-signature': signature,
            'Content-Type': 'application/json'
        }
        return headers

    def post_order(self, symbol, order_qty, price, side, ord_type='Limit'):

        """"
        Post a limit order to the exchange
        Args:
        symbol: str, trading pair
        order_qty: int, quantity to trade
        price: float, price to trade at
        side: str, 'Buy' or 'Sell'
        ord_type: str, 'Limit' or 'Market' 
        
        """

        endpoint = '/order'
        verb = 'POST'
        order = {
            'symbol': symbol,
            'orderQty': order_qty,
            'price': price,
            'side': side,
            'ordType': ord_type
        }
        data = json.dumps(order)
        
        headers = self.get_headers(verb, endpoint, data)
        response = requests.post(self.baseurl + endpoint, headers=headers, data=data)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None

    def cancel_all_orders(self):
      
        """
        Function to kill all resting orders on the bitmex account 

        No args

        Returns response from the exchange
        """

        endpoint = '/order/all'
        verb = 'DELETE'
        data = '' 
        
        headers = self.get_headers(verb, endpoint, data)
        response = requests.delete(self.baseurl + endpoint, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None
        
    def place_market_order(self, symbol, order_qty, side):
        
        """
        Function to place a market order on the bitmex account
        Args:
        symbol: str, trading pair
        order_qty: int, quantity to trade
        side: str, 'Buy' or 'Sell'
        
        Returns response from the exchange

        """

        endpoint = '/order'
        verb = 'POST'
        order = {
            'symbol': symbol,
            'orderQty': order_qty,
            'side': side,
            'ordType': 'Market'
        }
        data = json.dumps(order)
        
        headers = self.get_headers(verb, endpoint, data)
        response = requests.post(self.baseurl + endpoint, headers=headers, data=data)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None

    @staticmethod
    def format_ohlcv(ohlcv_data):
        """
        Formats OHLCV (Open, High, Low, Close, Volume) data into a specific structure.

        Parameters:
        - ohlcv_data (list): List of dictionaries containing OHLCV data.

        Returns:
        - list: Transformed OHLCV data with specific fields (open, close, high, low, volume, time).
        """
        transformed_data = [
            {
                'open': item['open'],
                'close': item['close'],
                'high': item['high'],
                'low': item['low'],
                'volume': item['volume'],
                'time': int(datetime.fromisoformat(item['timestamp'].replace('Z', '+00:00')).timestamp() * 1000)
                # 'time': int(item['timestamp'].timestamp() * 1000)  # Convert to milliseconds
            }
            for item in ohlcv_data
        ]

        return transformed_data

    def get_ohlc(self, ticker, timeframe):
        """
        Retrieves OHLCV (Open, High, Low, Close, Volume) data for a specific ticker and timeframe.

        Parameters:
        - ticker (str): Symbol or identifier of the financial instrument.
        - timeframe (str): Timeframe for the OHLCV data (e.g., '1h', '1d').

        Returns:
        - list: OHLCV data in a specific format after applying formatting.

        Example:
        [{'open': 100, 'close': 110, 'high': 120, 'low': 90, 'volume': 1000, 'time': 1640995200000},
         {'open': 110, 'close': 120, 'high': 130, 'low': 100, 'volume': 1500, 'time': 1641081600000}]

        """
        path = '/trade/bucketed'
        payload = {
            "binSize": f"{timeframe}",
            "partial": False,
            "symbol": f"{ticker}",
            "count": 100,
            "reverse": True
        }
        
        url = self.baseurl + path

        try:
            response = requests.get(url, params=payload, timeout=10)
            response = response.json()
            return self.format_ohlcv(response)
        except (requests.exceptions.ConnectionError, requests.exceptions.JSONDecodeError) as e:
            raise ExchangeConnectionError("Failed to establish a connection to Bitmex. There is an error on their "
                                          "end") from e
        except (requests.exceptions.ReadTimeout, requests.exceptions.Timeout) as e:
            raise ExchangeRequestTimeOut("Connection with Bitmex has timed out") from e
       