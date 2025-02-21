import pandas as pd
import yfinance as yf
import traceback
import sys
import utils
import oracledb
import time

def get_data(ticker_list, duration_in_days, dest_table_name=None):
    """
    Persists stock price for tickers with price trends within threshold for a 
    given period, in database table

    Parameters:
    - ticker_list (str): Comma separated list of ticker symbols
    - duration_in_days (str): Numeric string of historical number of days to 
    fetch relative to the current date 
    - dest_table_name (str): Table name to store results in. Defaults to 
    'stocks'.

    Returns:
    - void
    """
    try:
        start_time = time.perf_counter()

        stocks = pd.DataFrame()

        # Step 1: Parse inputs
        tickers = [item.strip() for item in ticker_list.split(',')]
        period = get_duration(duration_in_days)
        for ticker in tickers:
            if ticker:
                try:
                    tkr = yf.Ticker(ticker)
                    
                    # Step 2: Fetch historical data for each ticker 
                    hist = tkr.history(period=period)
                    hist['Symbol']=ticker

                    # Step 3: Append rows corresponding to each ticker to a single df 
                    stocks = pd.concat([stocks, hist[['Symbol', 'Close']].rename(columns={'Close': 'Price'})])

                except Exception:
                    logToFile(traceback.format_exc())
                    print(traceback.format_exc(), file=sys.stderr)

        # Step 4: Add column to track prev day's stock price
        stocks['Prev'] = stocks.groupby(['Symbol'])['Price'].shift(1)

        # Step 5: Identify stocks that observed fall in price by more than 3%, within time period
        stocks_to_exclude = stocks[stocks['Price']/stocks['Prev'] < .97]

        exclude_list = list(set(stocks_to_exclude['Symbol'].tolist()))

        # Step 6: Filter out rows belonging to excluded ticker symbols
        stocks_filtered = stocks[~stocks['Symbol'].isin(exclude_list)][['Symbol', 'Price']]

        stocks_to_db = stocks_filtered[['Symbol', 'Price']].reset_index().rename(columns={'Date': 'Dt'}).round(2)
        
        stocks_to_db = stocks_to_db.astype({'Dt': str})
        
        # Step 7: Save formatted columns in table
        save_df(stocks_to_db, dest_table_name)

        execution_time = time.perf_counter() - start_time
        logToFile(f"Execution Time: {execution_time:.5f} seconds")

    except Exception as e:
        print(traceback.format_exc(), file=sys.stderr)
        logToFile(traceback.format_exc())

def save_df(df, table_name=None):
    dest_table = table_name
    if not table_name:
        dest_table = 'stocks'

    con = utils.getconnection()
    if con is None:
        print("Failed to establish database connection.")
        logToFile(traceback.format_exc())
        return -1

    try:
        cur = con.cursor()

        cur.execute(f"SELECT COUNT(*) FROM all_tables WHERE table_name = UPPER('{dest_table}')")
        table_exists = cur.fetchone()[0] > 0

        if not table_exists:
            # Create the table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE {dest_table} (
                dt VARCHAR2(100),
                symbol VARCHAR2(4000),
                price NUMBER(10,2)
            )
            """
            cur.execute(create_table_query)
            
        for row in df.itertuples(index=False):
            merge_query =f"""
            MERGE INTO {dest_table} dest 
                USING (SELECT :1 as dt, :2 as symbol, :3 as price from dual) src
                ON (dest.dt = src.dt AND dest.symbol = src.symbol)
                WHEN MATCHED THEN
                    UPDATE SET dest.price = src.price WHERE dest.price <> src.price
                WHEN NOT MATCHED THEN
                    INSERT (dt, symbol, price) VALUES (src.dt, src.symbol, src.price)
            """
            param_list = [row.Dt, row.Symbol, row.Price]
            cur.execute(merge_query, param_list)

        # Commit the transaction
        cur.execute("COMMIT")

    except oracledb.DatabaseError as e:
        print(f"Error working with the table '{dest_table}': {e}")
        logToFile(traceback.format_exc())
        return -1
    finally:
        if cur:
            cur.close()

def get_duration(input):
    if str(input).isdigit():
        return str(input) + 'd'
    raise TypeError(f'Duration value {input}, is not numeric')

def logToFile(str):
    f = open("/tmp/demofile2.txt", "a")
    f.write(str + '\n')
    f.close()