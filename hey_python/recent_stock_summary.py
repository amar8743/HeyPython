import pandas as pd
import yfinance as yf
import traceback
import sys


def get_data(orders_table_name):
    try:
        stocks = pd.DataFrame()
        tickers = ['AMZN']
        for ticker in tickers:
            try:
                logToFile('ticker: ' + ticker + '  ')
                tkr = yf.Ticker(ticker)
                hist = tkr.history(period='3d')
                hist['Symbol']=ticker
                hist.head(2).to_json("demoout.json", orient="records", indent=4)
                stocks = pd.concat([stocks, hist[['Symbol', 'Close']].rename(columns={'Close': 'Price'})])
                logToFile('Done with ticker: ' + ticker + '\n')
            except Exception:
                print(traceback.format_exc(), file=sys.stderr)

        stocks.head(2).to_json("demoout1.json", orient="records", indent=4)

        stocks['Prev'] = stocks.groupby(['Symbol'])['Price'].shift(1)
        stocks.head(2).to_json("demoout2.json", orient="records", indent=4)

        stocks_to_exclude = stocks[stocks['Price']/stocks['Prev'] < .99]
        stocks_to_exclude.head(2).to_json("demoout3.json", orient="records", indent=4)

        exclude_list = list(set(stocks_to_exclude['Symbol'].tolist()))
        #exclude_list.head(3).to_json("demoout3.json", orient="records", indent=4)

        stocks_filtered = stocks[~stocks['Symbol'].isin(exclude_list)][['Symbol', 'Price']]
        stocks_filtered.head(2).to_json("demoout4.json", orient="records", indent=4)

        stocks_to_db = stocks[['Symbol', 'Price']].reset_index().rename(columns={'Date': 'Dt'}).round(2)
        stocks_to_db.head(2).to_json("demoout5.json", orient="records", indent=4)
        
        stocks_to_db = stocks_to_db.astype({'Dt': str})
        stocks_to_db.head(2).to_json("demoout6.json", orient="records", indent=4)
    except Exception as e:
        print(traceback.format_exc(), file=sys.stderr)
    # finally:
        # # Close the connection
        # if cur:
        #     cur.close()

def logToFile(str):
    f = open("/tmp/demofile2.txt", "a")
    f.write(str)
    f.close()