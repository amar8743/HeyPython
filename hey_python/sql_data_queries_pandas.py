import pandas as pd
import traceback
import utils
import json
import oracledb
import warnings
import sys
import time

def get_dataframe(table_name):
    print(f"get_dataframe from '{table_name}' \n")

    # Step 1: Establish connection
    con = utils.getconnection()
    if con is None:
        print("Failed to establish database connection.")
        return -1

    try:
        # Step 2: Query data from the specified table and column
        query = f"SELECT * FROM {table_name}"
        cur = con.cursor()
        cur.execute(query)

        # Fetch data and create a DataFrame
        rows = cur.fetchall()
        columns = [col[0] for col in cur.description]  # Get column names

        df = pd.DataFrame(rows, columns=columns)
        return df
    except Exception as e:
        print(f"Error fetching data: {e}", file=sys.stderr)
        return -1
    finally:
        cur.close()

def get_agg_sales(orders_table_name, details_table_name, return_table_name=None):
    """
    Aggregates data from orders and order details tables, to generate summary 
    of

    Parameters:
    - orders_table_name (str): Table to read orders data from
    - details_table_name (str): Table to read order details data from
    - return_table_name (str): Table name to store results in

    Returns:
    - void
    """
    try:
        start_time = time.perf_counter()

        # Read data from tables
        df_orders = get_dataframe(orders_table_name)
        df_details = get_dataframe(details_table_name)

        # Join the two tables
        df_orders_details = df_orders.merge(df_details)

        # Add total col to track final price of each lineitem after discount
        df_orders_details['TOTAL'] = df_orders_details.PRICE * df_orders_details.QUANTITY * (1 - df_orders_details.DISCOUNT/100)

        # Add off col to track final discount amount for each lineitem
        df_orders_details['OFF'] = df_orders_details.PRICE * df_orders_details.QUANTITY * (df_orders_details.DISCOUNT/100)

        df_orders_details = df_orders_details.round(2)

        # Use only columns of interest
        df_sales = df_orders_details[['ORDATE','EMPL', 'TOTAL', 'OFF']]

        df_date_empl = df_sales.groupby(['ORDATE','EMPL']).sum()

        # Calculate total sales per employee per day
        df_aggs = df_sales.groupby(['ORDATE','EMPL']).agg({'TOTAL': ['sum', 'mean'], 'OFF': 'max'}).round(2).reset_index()

        df_aggs.columns = ["_".join(item) if not isinstance(item, str) else item.strip() for item in df_aggs.columns]

        # Save the aggregated data to table
        saveAggSales(df_aggs, return_table_name)
        print("Done processing sales\n")

        execution_time = time.perf_counter() - start_time
        logToFile(f"Execution Time: {execution_time:.5f} seconds\n")
    
    except Exception as e:
        print(traceback.format_exc(), file=sys.stderr)
        logToFile(traceback.format_exc())

def saveAggSales(agg_df, return_table_name=None):
    if return_table_name:
        con = utils.getconnection()
        if con is None:
            print("Failed to establish database connection.")
            return -1
        cur = con.cursor()
        try:
            # Step 4: Check if the table already exists
            cur.execute(f"SELECT COUNT(*) FROM all_tables WHERE table_name = UPPER('{return_table_name}')")
            table_exists = cur.fetchone()[0] > 0

            if not table_exists:
                # Create the table if it doesn't exist
                create_table_query = f"""
                CREATE TABLE {return_table_name} (
                    or_date VARCHAR2(100),
                    empl VARCHAR2(4000),
                    total_sum NUMBER(6,2),
                    total_mean NUMBER(6,2),
                    off_max NUMBER(6,2)
                )
                """
                cur.execute(create_table_query)
            else:
                print(f"Table '{return_table_name}' already exists. Please drop the table or provide a new table name.", file=sys.stderr)
                return

            # Step 5: Insert the results into the table
            for row in agg_df.itertuples(index=False):
                insert_query = f"INSERT INTO {return_table_name} (or_date, empl, total_sum, total_mean, off_max) VALUES (:1, :2, :3, :4, :5)"
                param_list = [row.ORDATE_, row.EMPL_, row.TOTAL_sum, row.TOTAL_mean, row.OFF_max]
                cur.execute(insert_query, param_list)

            # Commit the transaction
            cur.execute("COMMIT")

        except oracledb.DatabaseError as e:
            print(f"Error working with the table '{return_table_name}': {e}")
            return -1
        finally:
            if cur:
                cur.close()

def logToFile(str):
    f = open("/tmp/demofile1.txt", "a")
    f.write(str)
    f.close()
