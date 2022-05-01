import pandas as pd
import sqlalchemy
import PySimpleGUI as sg
import threading
from binance import Client
import json
import logging as lg
import time
from pprint import pprint
import talib
import numpy as np
from pprint import pprint

display = pd.options.display
display.max_columns = 20
display.max_colwidth = 199
display.width = 1000

# Initial Variables Setup for GUI, TA lib indicators, Binance API
how_many_candles = 500
rsi_param, ema_param, boll_param = 10, 10, 10
ema_period, bband_period, rsi_period = 20, 21, 14
rsi_check, ema_check, boll_check = False, False, False
interval_options = [ '5m', '15m', '30m', "1h", '4h', '6h', '12h', "1d" ]
param_order = {'range': (0, 1000), "orientation": "h", "default_value": 10, "resolution": 10, "tick_interval": 100, "enable_events": True,
               "size": (40, 25)}
param_ind = {'range': (0, 70), "orientation": "h", "default_value": 10, "resolution": 1, "tick_interval": 10, "enable_events": True,
             "size": (40, 15)}

# todo take profit feAture
# todo order history export


# Pysimplegui Window layout setup
def make_window(theme=None):
    sg.theme("DarkBlack")

    layout_l = [
        [ sg.Button('5m', size=(3, 1)), sg.Button('15m', size=(3, 1)), sg.Button('30m', size=(3, 1)), sg.Button("1h", size=(3, 1)) ],
        [ sg.Button('4h', size=(3, 1)), sg.Button('6h', size=(3, 1)), sg.Button('12h', size=(3, 1)), sg.Button("1d", size=(3, 1)) ],
        [ sg.Text("INTERVAL"), sg.Text("15m", k="intv") ],
        [ sg.Combo(symbol_list, default_value="BTCUSDT", s=15, k="symbol_dropdown", enable_events=True) ],
        [ ],
        [ sg.Text("CURR Price  : "), sg.Text("-", k="curr_px") ],
        [ ],
        [ sg.Text("API load  : "), sg.Text("-", k="MBX") ],
        [ ],
        [ sg.Text("Bought?  : "), sg.Text("-", k="bought") ],
        [ ],
        [ sg.Button("EXPORT",k="export") ]

    ]

    layout_r = [
        # [ sg.Button("get db"), sg.Button("df_to_sql"), sg.Button("call_sql"), sg.Button("merge") ],
        [ sg.Text("      "), sg.Text("ORDER", font=('Helvetica', 10), pad=((0, 10), (15, 0)), size=(9, 2), ),
          sg.Slider(**param_order, key='order_param', trough_color="grey35") ],
        [ sg.Checkbox("", k="rsi_check", enable_events=True), sg.Text("RSI\n-", size=(9, 2), k="RSI"),
          sg.Slider(**param_ind, k='rsi_param', trough_color="grey85") ],
        [ sg.Checkbox("", k="ema_check", enable_events=True), sg.Text("EMA\n-", size=(9, 2), k="EMA"),
          sg.Slider(**param_ind, k='ema_param', trough_color="grey85"), sg.Text("-", k="ema_targ") ],
        [ sg.Checkbox("", k="boll_check", enable_events=True), sg.Text("BOLL\n-", size=(9, 2), k="BOLL"),
          sg.Slider(**param_ind, k='boll_param', trough_color="grey85"), sg.Text("-", k="boll_targ") ]
    ]

    layout_bottom = [
        [ sg.Text("STATUS", background_color="IndianRed4", pad=((0, 30), (0, 0)), k="STATUS"), sg.Button("START", button_color="DarkGreen"),
          sg.Button("STOP", button_color="IndianRed"),
          sg.Button("Popup1", visible=False), sg.Button("Popup2", visible=False) ] ]
    layout = [
        [ sg.T('DCA', font='_ 18', justification='c', expand_x=True) ],
        [ sg.Col(layout_l, vertical_alignment="top"), sg.Col(layout_r) ],
        [ sg.Col(layout_bottom, justification="right") ] ]

    window = sg.Window('DCA', layout, finalize=True,
                       keep_on_top=False)

    return window

# Main DCA Loop. It will be executed in a thread from GUI
def main_dca_loop(stop_event, arg):
    # global DF
    interval = window[ 'intv' ].get()
    symbol = window[ 'symbol_dropdown' ].get()
    window[ 'STATUS' ].update(background_color="green1")

    if not (rsi_check or ema_check or boll_check):
        print("please check at least one, otherwise it will do nothing")
        window[ 'Popup1' ].click()  # Workaround for a popup window because of tk threading issue. Refer to the link below
        # https://stackoverflow.com/questions/45799121/runtimeerror-calling-tcl-from-different-appartment-tkinter-and-threading
        return

    while not stop_event.is_set():

        candles = client.get_klines(symbol=symbol, interval=interval, limit=how_many_candles)
        print(pd.Series(client.response.headers))
        header = pd.Series(client.response.headers).loc[ [ "Date", "Connection", 'x-mbx-used-weight', 'x-mbx-used-weight-1m' ], ]
        print(header, "--------------------------------------------------------", sep="\n")

        window[ 'MBX' ].update(header[ 'x-mbx-used-weight-1m' ])

        DF = pd.DataFrame(candles)
        DF.columns = [ 'open_time', 'open', 'high', 'low', 'close', 'vol', 'close_time', 'qoute_asset_val', 'trades', 'buy_qty',
                       'buy_asset_vol', 'ignore' ]
        DF[ 'if_bought' ] = 0
        DF = DF[ [
            'open_time', 'if_bought', 'close_time', 'high', 'low', 'open', 'close', 'vol', 'qoute_asset_val', 'trades', 'buy_qty',
            'buy_asset_vol', 'ignore' ] ]
        DF[ 'open_time' ] = pd.to_datetime(DF[ 'open_time' ], unit="ms")
        DF[ 'close_time' ] = pd.to_datetime(DF[ 'close_time' ], unit="ms")

        try:
            PRV_DF = pd.read_sql(f"{symbol}_{interval}", engine)
            DF = pd.merge(PRV_DF, DF, how='outer')
            DF = DF.drop_duplicates(subset=[ 'open_time' ], keep='first')
            DF.sort_values(by=[ 'open_time' ], inplace=True, ignore_index=True)
        except:
            pass

        curr_px = client.get_symbol_ticker(symbol=symbol)[ 'price' ]
        curr_px = float(curr_px)
        window[ 'curr_px' ].update(curr_px)

        print("this is merged table")
        print(DF.tail())

        close_data = DF.loc[ :, "close" ].astype(float).to_numpy()
        close_data[ -1 ] = curr_px

        rsi = talib.RSI(close_data, timeperiod=rsi_period)
        curr_rsi = rsi[ -1 ]
        targ_rsi = int(values[ "rsi_param" ])
        window[ 'RSI' ].update(f"RSI\n{curr_rsi}")

        ema = talib.EMA(close_data, timeperiod=ema_period)
        curr_ema = ema[ -1 ]
        window[ 'EMA' ].update(f"EMA\n{curr_ema:.2f}")
        ema_param = (100 - int(values[ "ema_param" ])) / 100
        window[ 'ema_targ' ].update(f"% DOWN\n{curr_ema * ema_param:.2f}")
        targ_ema = curr_ema * ema_param
        print(targ_ema)

        upperband, middleband, lowerband = talib.BBANDS(close_data, timeperiod=bband_period, nbdevup=2, nbdevdn=2, matype=0)
        curr_boll = lowerband[ -1 ]
        print(curr_boll)
        window[ 'BOLL' ].update(f"BOLL\n{curr_boll:.2f}")
        boll_param = (100 - int(values[ "boll_param" ])) / 100
        window[ 'boll_targ' ].update(f"% DOWN\n{curr_boll * boll_param:.2f}")
        targ_boll = curr_boll * boll_param

        bought = int(DF.loc[ DF[ 'open_time' ] == DF[ 'open_time' ].max(), "if_bought" ])

        if bought:
            window[ 'bought' ].update("Yes")
        else:
            window[ 'bought' ].update("No")

        if rsi_check or ema_check or boll_check:
            print("good")
            if rsi_check:
                rsi_passed = True if targ_rsi > curr_rsi else False
                if rsi_passed:
                    window.Element('RSI').Update(background_color="green")
                else:
                    window.Element('RSI').Update(background_color="red")

            else:
                rsi_passed = True
                window.Element('RSI').Update(background_color="grey")

            if ema_check:
                ema_passed = True if targ_ema > curr_px else False
                if ema_passed:
                    window.Element('EMA').Update(background_color="green")
                else:
                    window.Element('EMA').Update(background_color="red")

            else:
                ema_passed = True
                window.Element('EMA').Update(background_color="grey")

            if boll_check:
                boll_passed = True if targ_boll > curr_px else False
                if boll_passed:
                    window.Element('BOLL').Update(background_color="green")
                else:
                    window.Element('BOLL').Update(background_color="red")

            else:
                boll_passed = True
                window.Element('BOLL').Update(background_color="grey")

            if rsi_passed and ema_passed and boll_passed and not (bought):

                order_amt_dollar = int(values[ 'order_param' ])
                min_notional = float(ex_info_df.loc[ ex_info_df[ 'symbol' ] == symbol, "min_notional---minNotional" ])

                if order_amt_dollar >= min_notional:
                    print("min notialnal passed")
                    order = client.create_order(symbol=symbol, side='BUY', type='MARKET',
                                                quoteOrderQty=order_amt_dollar)  # only integer is accpeted
                    DF.loc[ DF[ 'open_time' ] == DF[ 'open_time' ].max(), "if_bought" ] = 1
                    print(
                        "--------------------------send order----------------------------------------------------------------------------")
                    order_df = pd.DataFrame(order)
                    order_df.drop([ 'fills' ], axis=1, inplace=True)
                    order_df.to_sql("order_history", engine, if_exists='append', index=False)

                else:
                    print("min notioanal not passed")



        else:
            print("please check at least one, otherwise it will do nothing")
            window[ 'Popup1' ].click()  # Workaround for a popup window because of tk threading issue. Refer to the link below
            # https://stackoverflow.com/questions/45799121/runtimeerror-calling-tcl-from-different-appartment-tkinter-and-threading
            break

        DF.to_sql(f"{symbol}_{interval}", engine, if_exists='replace', index=False)
        print(f"current rsi targ is {targ_rsi}")

        time.sleep(3)

    print("stopped get_data")


def order_filter_check_and_send():

    symbol = window[ 'symbol_dropdown' ].get()
    order_amt_dollar = int(values[ 'order_param' ])
    min_notional = float(ex_info_df.loc[ ex_info_df[ 'symbol' ] == symbol, "min_notional---minNotional" ])
    print(order_amt_dollar)
    print(min_notional)
    print(type(min_notional))

    if order_amt_dollar >= min_notional:
        print("min notialnal passed")
        order = client.create_order(symbol=symbol, side='BUY', type='MARKET', quoteOrderQty=order_amt_dollar)  # only integer is accpeted
        DF = pd.read_sql(f"{interval}", engine)
        DF.loc[ DF[ 'open_time' ] == DF[ 'open_time' ].max(), "if_bought" ] = 1
        print("--------------------------send order----------------------------------------------------------------------------")
        order_df = pd.DataFrame(order)
        order_df.drop([ 'fills' ], axis=1, inplace=True)
        order_df.to_sql("order_history", engine, if_exists='append', index=False)

    else:
        print("min notioanal not passed")



# This is for initial setup for variables that are needed before GUI and GUI
if __name__ == '__main__':
    # Fetching api keys from txt file.
    f = open("api_key.txt", 'r')
    lines = f.readlines()
    api_key = lines[ 0 ].strip()
    api_secret = lines[ 1 ]
    f.close()

    # Binance API Login
    client = Client(api_key, api_secret)
    client.ping()
    print("client connection was successful")

    # Create an engine for sql
    engine = sqlalchemy.create_engine("sqlite:///db.db")


    # Getting Exchange info for available symbols and symbol specific data such as order filtering things
    ex_info = client.get_exchange_info()[ "symbols" ]  # request info on all futures symbols
    pprint(client.get_exchange_info()[ 'rateLimits' ])
    ex_info_df = pd.DataFrame(ex_info)  # Creating a df
    ex_info_df = ex_info_df.loc[ (ex_info_df[ "quoteAsset" ] == "USDT") & (ex_info_df[ "status" ] == "TRADING"), : ]
    ex_info_df.reset_index(inplace=True, drop=True)
    ex_info_df_2 = ex_info_df.filters.apply(pd.Series)  # Making "filters" column to a df
    ex_info_df_2 = ex_info_df_2.iloc[ :, [ 0, 2, 3, 5 ] ]
    ex_info_df_2.columns = [ "px_filter", "lot_filter", "min_notional", "market_filter" ]
    ex_info_df_3 = pd.DataFrame(columns=[ "empty" ])  # Creating a empty df and it also acts as kinda separator in SQL

    for column in ex_info_df_2.columns:
        # df_temp = pd.DataFrame(info_df_2[ column ].values.tolist()) # alternative of the below line
        ex_info_df_temp = pd.DataFrame(ex_info_df_2[ column ].apply(pd.Series))  # Making a col to a df
        ex_info_df_temp.columns = [ column + "---" + element for element in
                                    ex_info_df_temp.columns ]  # Relabling columns because there are cloumns with the same name
        ex_info_df_3 = pd.concat([ ex_info_df_3, ex_info_df_temp ], axis=1)  # Concating two dfs

    ex_info_df = ex_info_df.loc[ :, [ 'symbol', 'baseAssetPrecision', 'quotePrecision' ] ]  # Shaving off unnecessAry cols of info_df
    ex_info_df = pd.concat([ ex_info_df, ex_info_df_3 ], axis=1)  # Concating two dfs

    ex_info_df.to_sql("ex_info", engine, if_exists="replace", index=False)  # Sending df to SQL
    symbol_list = ex_info_df.symbol.to_list()


    # Creating PySimplegui
    window = make_window()
    window.finalize()


    # Loop for GUI
    while True:
        event, values = window.read()
        print(event, values)

        if event == sg.WIN_CLOSED or event == 'Exit':
            break

        elif event == 'START':
            pill2kill = threading.Event()
            get_data_thread = threading.Thread(target=main_dca_loop, args=(pill2kill, "task"))
            get_data_thread.start()

        elif event == 'STOP':
            pill2kill.set()
            get_data_thread.join()
            print('killed the thread')
            window[ 'STATUS' ].update(background_color="red")

        elif event in interval_options:
            window[ 'intv' ].update(event)
            interval = event

        elif event == "symbol_dropdown":
            symbol = window[ 'symbol_dropdown' ].get()
            print(symbol)

        elif event == 'rsi_check':
            rsi_check = True if values[ event ] == True else False

        elif event == 'ema_check':
            ema_check = True if values[ event ] == True else False

        elif event == 'boll_check':
            boll_check = True if values[ event ] == True else False

        elif event == 'Popup1':
            sg.Popup("please check at least one, otherwise it will do nothing")

        elif event == 'Popup2':
            sg.Popup("please check at least one, otherwise it will do nothing")
        elif event == 'export':
            order_history_df = pd.read_sql('order_history',engine)
            order_history_df[ 'transactTime' ] = pd.to_datetime(order_history_df[ 'transactTime' ], unit="ms")
            order_history_df.to_excel('order history.xlsx', index=False)