import numpy as np 
import pandas as pd 
import json

# global variables
dashboard_total_pnl = 0
dashboard_total_unrealized_pnl = 0
dashboard_total_realized_pnl = 0

# utility functions
def calc_realized_pnl(trade_df, pnl_df, underlying_list):
    
    for i in range(0, len(underlying_list)):
        curr_market = underlying_list[i]
        
        open_position = 0
        total_open_cost = 0
        total_realized_pnl = 0
        
        for index, row in trade_df.loc[trade_df['market'] == curr_market].iterrows():

            # scenario A: no open position
            if open_position == 0:

                open_position = open_position + row['net_quantity']
                total_open_cost = total_open_cost + (row['effective_price'] * row['net_quantity'])


            # scenario B: currently long
            elif open_position > 0:

                # scenario B(i): currently long, buying more
                if row['side'] == 1:
                    open_position = open_position + row['net_quantity']
                    total_open_cost = total_open_cost + (row['effective_price'] * row['net_quantity'])


                # scenario B(ii): currently long, selling some [realizing gains/losses]
                elif row['side'] == -1:
                    # scenario B(ii)(a): currently long, selling less than or equal to longs owned
                    if row['quantity'] <= open_position:

                        closed_position = row['quantity']
                        exit_price = row['price']

                        open_average_cost = total_open_cost / open_position

                        entry_cost = closed_position * open_average_cost
                        effective_exit = closed_position * exit_price

                        long_realized_pnl = effective_exit - entry_cost - row['fee_usd']
                        total_realized_pnl = total_realized_pnl + long_realized_pnl

                        open_position = open_position + row['net_quantity'] # reduces number of longs owned
                        total_open_cost = total_open_cost - entry_cost

                    # scenario B(ii)(b): currently long, selling more than longs owned
                    elif row['quantity'] > open_position:

                        closed_position = open_position
                        exit_price = row['price']

                        realized_fee = (closed_position / row['quantity']) * row['fee_usd']
                        unrealized_fee = row['fee_usd'] - realized_fee

                        open_average_cost = total_open_cost / open_position

                        entry_cost = closed_position * open_average_cost
                        effective_exit = closed_position * exit_price

                        long_realized_pnl = effective_exit - entry_cost - realized_fee
                        total_realized_pnl = total_realized_pnl + long_realized_pnl

                        open_position = open_position + row['net_quantity']
                        total_open_cost = (open_position * exit_price) - unrealized_fee


            # scenario C: currently short
            elif open_position < 0:

                # scenario C(i): currently short, buying some [realize]
                if row['side'] == 1:

                    # scenario C(i)(a): currently short, buying less than or equal to  shorts owned
                    if row['quantity'] <= abs(open_position):

                        closed_position = row['quantity']
                        exit_price = row['price']

                        open_average_cost = total_open_cost / open_position # average cost of each short in open position

                        entry_cost = closed_position * open_average_cost # entry cost of the closed position
                        effective_exit = closed_position * exit_price # exit cost (excluding exit fee) of closed position

                        short_realized_pnl = entry_cost - effective_exit - row['fee_usd']
                        total_realized_pnl = total_realized_pnl + short_realized_pnl

                        # update global vars open_position and total_open_cost
                        open_position = open_position + row['net_quantity']
                        total_open_cost = total_open_cost + entry_cost

                    # scenario C(i)(b): currently short: buying more than shorts owned
                    elif row['quantity'] > abs(open_position):

                        closed_position = abs(open_position) # closing entire open_position
                        exit_price = row['price']

                        realized_fee = (closed_position / row['quantity']) * row['fee_usd']
                        unrealized_fee = row['fee_usd'] - realized_fee

                        open_average_cost = total_open_cost / open_position

                        entry_cost = closed_position * open_average_cost
                        effective_exit = closed_position * exit_price

                        short_realized_pnl = entry_cost - effective_exit - realized_fee 
                        total_realized_pnl = total_realized_pnl + short_realized_pnl 

                        open_position = open_position + row['net_quantity'] # update open_position, open a new long position, since we bought more longs than numbe of shorts we own
                        total_open_cost = (open_position * exit_price) + unrealized_fee # calculate total_open_cost of new long position


                # scenario C(ii): currently short, selling more
                elif row['side'] == -1:

                    open_position = open_position + row['net_quantity']
                    total_open_cost = total_open_cost + (row['effective_price'] * row['net_quantity']) 
        
        # set total_realized_pnl of the current market to pnl_df
        pnl_df['Realized_P&L'].loc[pnl_df['Underlying'] == curr_market] = total_realized_pnl
    
    return pnl_df

def calc_pnl(trade_df, price_df):
    # typecasting 
    trade_df['trade_datetime'] = pd.to_datetime(trade_df['trade_datetime']).dt.tz_localize(None) # convert to datetime data type 
    price_df['price_datetime'] = pd.to_datetime(price_df['price_datetime']) # convert to datetime data type 

    # sorted list of trade markets
    underlying_list = sorted(trade_df['market'].unique())
    
    # initialize pnl dataframe
    pnl_df = pd.DataFrame(underlying_list, columns=['Underlying'])
    pnl_df['Counterparty'] = "XXX"
    pnl_df['Position'] = np.nan
    pnl_df['Avg_Price'] = np.nan
    pnl_df['Market_Value'] = np.nan
    pnl_df['P&L'] = np.nan
    pnl_df['Unrealized_P&L'] = np.nan
    pnl_df['Realized_P&L'] = np.nan
    
    # replace side with numeric value & create new column net_quantity = quantity * side
    trade_df['side'].loc[(trade_df['side'] == "buy")] = 1
    trade_df['side'].loc[(trade_df['side'] == "sell")] = -1
    trade_df['net_quantity'] = trade_df['quantity'] * trade_df['side']
    


    # create new column for trade_datetime rounded down to nearest hour
    trade_df['trade_datetime_rounded_down_hour'] = trade_df['trade_datetime'].dt.floor('h')
    
    # create new column fee_usd = fee in terms of USD
    trade_df['fee_usd'] = np.nan
    trade_df.loc[trade_df['feecurrency'] == 'USD', 'fee_usd'] = trade_df['fee'] # if feecurrency == 'USD' , fee_usd = fee  
    for index, row in trade_df.iterrows():
        if row['feecurrency'] != 'USD':
            trade_df['fee_usd'].iloc[index] = row['fee'] * price_df['price'].loc[(price_df['market'] == (row['feecurrency']+'/USD')) & (price_df['price_datetime'] == row['trade_datetime_rounded_down_hour'])]

    # create new column total_trade_cost = execution price + fee in each trade
    trade_df['trade_total_cost'] = (trade_df['net_quantity'] * trade_df['price']) + trade_df['fee_usd']
    
    # create new column effective_price, basically average price of bought security with fee included (added)
    trade_df['effective_price'] = (trade_df['notional'] + trade_df['fee_usd']) / trade_df['quantity']
    
    # <<Calculate Position>>
    for i in range(0, len(underlying_list)):
        curr_market = underlying_list[i]
        curr_position = trade_df.loc[trade_df['market'] == curr_market, 'net_quantity'].sum() 
        pnl_df['Position'].iloc[i] = curr_position
    
    # <<Calculate Avg_Price>>
    trade_df['trade_total_cost'] = (trade_df['net_quantity'] * trade_df['price']) + trade_df['fee_usd']
    for i in range(0, len(underlying_list)):
        curr_market = underlying_list[i]
        curr_market_position = pnl_df['Position'].loc[pnl_df['Underlying'] == curr_market].item()
        
        if curr_market_position > 0: # calc Avg_Price by avg buy trades
            market_total_cost = trade_df['trade_total_cost'].loc[(trade_df['market'] == curr_market) & (trade_df['net_quantity'] > 0)].sum() # ignore sell trades
            market_total_net_quantity = trade_df['net_quantity'].loc[(trade_df['market'] == curr_market) & (trade_df['net_quantity'] > 0)].sum() # ignore sell trades
            pnl_df['Avg_Price'].loc[pnl_df['Underlying'] == curr_market] = market_total_cost / market_total_net_quantity
        
        if curr_market_position < 0: # calc Avg_Price by avg sell trades
            market_total_cost = trade_df['trade_total_cost'].loc[(trade_df['market'] == curr_market) & (trade_df['net_quantity'] < 0)].sum()
            market_total_net_quantity = trade_df['net_quantity'].loc[(trade_df['market'] == curr_market) & (trade_df['net_quantity'] < 0)].sum()
            pnl_df['Avg_Price'].loc[pnl_df['Underlying'] == curr_market] = market_total_cost / market_total_net_quantity

        if curr_market_position == 0:
            pnl_df['Avg_Price'].loc[pnl_df['Underlying'] == curr_market] = 0

    # << Calculate Market Value >>
    for i in range(0, len(underlying_list)):
        curr_market = underlying_list[i]
        latest_market_price = float(price_df['price'].loc[price_df['market']==curr_market].tail(1))
        curr_market_position = float(pnl_df['Position'].loc[pnl_df['Underlying'] == curr_market])
        pnl_df['Market_Value'].loc[pnl_df['Underlying'] == curr_market] = curr_market_position * latest_market_price

    # << Calculate Unrealized P&L >>
    pnl_df['Unrealized_P&L'] = pnl_df['Market_Value'] - (pnl_df['Position'] * pnl_df['Avg_Price'])
    
    # << Calculate Realized P&L >>
    pnl_df = calc_realized_pnl(trade_df, pnl_df, underlying_list)
    
    # << Calculate P&L >>
    pnl_df['P&L'] = pnl_df['Unrealized_P&L'] + pnl_df['Realized_P&L']

    print(pnl_df)

    # declare global so we can affect outside variables
    global dashboard_total_pnl
    global dashboard_total_realized_pnl
    global dashboard_total_unrealized_pnl

    dashboard_total_pnl = sum(pnl_df['P&L'])
    dashboard_total_realized_pnl = sum(pnl_df['Realized_P&L'])
    dashboard_total_unrealized_pnl = sum(pnl_df['Unrealized_P&L'])

    # convert to ag grid format
    result = pnl_df.to_json(orient="records")
    parsed = json.loads(result)
    
    return parsed

def dashboard_totals():
    # declare global so we can affect outside variables
    global dashboard_total_pnl
    global dashboard_total_realized_pnl
    global dashboard_total_unrealized_pnl
    
    # initiatialize dict with global vars
    dict = {}
    for variable in ["dashboard_total_pnl", "dashboard_total_unrealized_pnl", "dashboard_total_realized_pnl"]:
        dict[variable] = eval(variable)

    json_totals = json.dumps(dict)

    return json_totals


