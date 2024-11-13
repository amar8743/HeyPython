import pandas as pd
import utils
import json
import oracledb
import warnings
warnings.filterwarnings("ignore", message="Signature.*longdouble")


def test_dataframe(table_name, column_name, operations, return_table_name=None):
    # Step 1: Establish connection
    con = utils.getconnection()
    if con is None:
        print("Failed to establish database connection.")
        return -1

    try:
        # Step 2: Query data from the specified table and column
        query = f"SELECT {column_name} FROM {table_name}"
        cur = con.cursor()
        cur.execute(query)
        
        # Fetch data and create a DataFrame
        rows = cur.fetchall()
        columns = [col[0] for col in cur.description]  # Get column names
        df = pd.DataFrame(rows, columns=columns)

        # Step 3: Perform specified operations and prepare results in a dictionary
        results = {}
        try:
            if "sum" in operations:
                results["sum"] = df[column_name].sum()
            if "mean" in operations:
                results["mean"] = df[column_name].mean()
            if "std" in operations:
                results["std"] = df[column_name].std()
            if "max" in operations:
                results["max"] = df[column_name].max()
            if "min" in operations:
                results["min"] = df[column_name].min()
            if "variance" in operations:
                results["variance"] = df[column_name].var()
            if "median" in operations:
                results["median"] = df[column_name].median()
            # if "kurtosis" in operations:
            #     results["kurtosis"] = df[column_name].kurtosis()
            # if "skew" in operations:
            #     results["skew"] = df[column_name].skew()
            # if "quantiles" in operations:
            #     results["quantiles"] = df[column_name].quantile([0.25, 0.5, 0.75]).to_dict()
            # if "z_score" in operations:
            #     results["z_score"] = ((df[column_name] - df[column_name].mean()) / df[column_name].std()).tolist()
            # if "exp_moving_avg" in operations:
            #     results["exp_moving_avg"] = df[column_name].ewm(span=10, adjust=False).mean().tolist()

        except Exception as e:
            print(f"Error performing operations: {e}")
            return -1

        # If return_table_name is provided, insert the results into the database table
        if return_table_name:
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
                    operation VARCHAR2(100),
                    result VARCHAR2(4000)
                )
                """
                cur.execute(create_table_query)

                # Step 6: Insert the results into the table
                for operation, result in results.items():
                    insert_query = f"INSERT INTO {return_table_name} (operation, result) VALUES (:1, :2)"
                    cur.execute(insert_query, (operation, str(result)))

                # Commit the transaction
                con.commit()

            except oracledb.DatabaseError as e:
                print(f"Error working with the table '{return_table_name}': {e}")
                return -1
            finally:
                cur.close()

        # Step 7: Return the results as a JSON object
        else:
            return json.dumps(results, default=str)

    except Exception as e:
        print(f"Error fetching data: {e}")
        return -1
    finally:
        #con.close()
        print("Connection is released for reusability")
