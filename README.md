# BitMexRestApi
Functional Fix for placing orders to and from BitMex on testnet and mainnet due to API wrapper issues

This is not a stable relase use at your own risk. 

**Methods:**

init: 

test = True for testmet
test = False for mainnet 

post-order: allows for limit buy/sell

cancel-all-orders: cancels all orders on the bitmex exchange

place_market_order: places order at market with default execution. There is no close function if you want to close reverse the trade using this function 

added in functions from @NeutronBlast 

get_ohlc: gets open, high, low, close data

format_ohlcv: formats the data

