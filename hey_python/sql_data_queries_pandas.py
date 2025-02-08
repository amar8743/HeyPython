import pandas as pd
import utils
import json
import oracledb
import warnings
import sys

def get_agg_sales(orders_table_name, details_table_name, return_table_name=None):
    print("Start", file=sys.stderr)

    df_orders = get_dataframe(orders_table_name)
    df_details = get_dataframe(details_table_name)

    df_orders_details = df_orders.merge(df_details)

    df_orders_details['TOTAL'] = df_orders_details.PRICE * df_orders_details.QUANTITY * (1 - df_orders_details.DISCOUNT/100)

    df_orders_details['OFF'] = df_orders_details.PRICE * df_orders_details.QUANTITY * (df_orders_details.DISCOUNT/100)

    df_orders_details = df_orders_details.round(2)

    df_sales = df_orders_details[['ORDATE','CUSTOMER', 'TOTAL', 'OFF']]

    df_date_empl = df_sales.groupby(['ORDATE','CUSTOMER']).sum()

    df_aggs = df_sales.groupby(['ORDATE','CUSTOMER']).agg({'TOTAL': ['sum', 'mean'], 'OFF': 'max'}).round(2)

    df_aggs.columns = df_aggs.columns.map('_'.join).str.strip()

    #saveAggSales(df_aggs, return_table_name)
    print("Done updating sales", file=sys.stderr)
    logToFile("Done updating sales")

    except Exception as e:
        print(f"Error fetching data: {e}")
        #return -1

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

            if table_exists:
                print(f"Table '{return_table_name}' already exists. Please drop the table or provide a new table name.")
                return -1

            # Step 5: Create the table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE {return_table_name} (
                or_date VARCHAR2(100),
                customer VARCHAR2(4000),
                total_sum NUMBER(6,2),
                total_mean NUMBER(6,2),
                off_max NUMBER(6,2)
            )
            """
            cur.execute(create_table_query)

            # Step 5.5: Define column order
            column_order = ["ORDATE", "CUSTOMER", "TOTAL_sum", "TOTAL_mean", "OFF_max"]

            # Step 6: Insert the results into the table
            for row in agg_df.iterrows()
                parameter_list = []
                for col in column_order:
                    print(f"{col}: {row[col]}: {type(row[col])}", file=sys.stderr)
                    logToFile("print col data")
                    parameter_list.append(row[col])                    

                insert_query = f"INSERT INTO {return_table_name} (or_date, customer, total_sum, total_mean, off_max) VALUES (:1, :2, :3, :4, :5)"
                print(tuple(parameter_list), file=sys.stderr)
                print("\n", file=sys.stderr)
                #cur.execute(insert_query, tuple(parameter_list))

            # # Step 6: Insert the results into the table
            # # for operation, result in results.items():
            # #     insert_query = f"INSERT INTO {return_table_name} (operation, result, ) VALUES (:1, :2)"
            # #     cur.execute(insert_query, (operation, str(result)))

            # # Commit the transaction
            # cur.execute("COMMIT")

        except oracledb.DatabaseError as e:
            print(f"Error working with the table '{return_table_name}': {e}")
            return -1
        finally:
            cur.close()

    # Step 7: Return the results as a JSON object
    else:
        return json.dumps(results, default=str)

def get_dataframe(table_name):
    print("get_dataframe", file=sys.stderr)
    logToFile("get_dataframe")
    # Step 1: Establish connection
    con = utils.getconnection()
    if con is None:
        print("Failed to establish database connection.")
        return -1
    #time.sleep(1800)
    try:
        # Step 2: Query data from the specified table and column
        query = f"SELECT * FROM {table_name}"
        cur = con.cursor()
        cur.execute(query)

        # Fetch data and create a DataFrame
        rows = cur.fetchall()
        #rows = cur.fetch_arrow_all()
        columns = [col[0] for col in cur.description]  # Get column names
        # pyarrow_table = pa.Table.from_arrays(rows, names=columns)
        df = pd.DataFrame(rows, columns=columns)

    except Exception as e:
        print(f"Error fetching data: {e}")
        return -1
    finally:
        cur.close()

def logToFile(str):
    f = open("/tmp/demofile2.txt", "a")
    f.write(str)
    f.close()