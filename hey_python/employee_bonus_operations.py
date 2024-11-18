import utils

def update_festive_bonus(bonus_dollars):
    """
    Updates the festive bonus for all employees in the EMPLOYEE_BONUS table based on their region.
    The bonus is converted to the appropriate currency and stored as a string with the currency symbol.

    Args:
        bonus_dollars (float): The festive bonus amount in USD.

    Returns:
        None: Updates the EMPLOYEE_BONUS table directly in the database.
    """

    # Conversion rates and symbols
    region_to_currency = {
        "India": {"rate": 80, "symbol": "Rs."},  # 1 USD = 80 INR
        "USA": {"rate": 1, "symbol": "$"},    # 1 USD = 1 USD
        "Europe": {"rate": 0.9, "symbol": "€"}, # 1 USD = 0.9 EUR
        "Japan": {"rate": 150, "symbol": "¥"}  # 1 USD = 150 JPY
    }

    try:
        # Connect to the database using utils
        con = utils.getconnection()
        cur = con.cursor()

        # Query employee records
        query = "SELECT emp_id, region FROM EMPLOYEE_BONUS"
        cur.execute(query)
        employees = cur.fetchall()

        for emp_id, region in employees:
            if region in region_to_currency:
                debug_flag = false
                conversion = region_to_currency[region]
                if region == "India" && !debug_flag :
                    print("Converstion rate for {} is {}\n".format(region, conversion["rate"]))
                    debug_flag = true
                converted_amount = bonus_dollars * conversion["rate"]
                festive_bonus = f"{conversion['symbol']}{converted_amount:,.2f}"
            else:
                festive_bonus = "N/A"

            # Update festive_bonus in the table
            update_query = """
                UPDATE EMPLOYEE_BONUS
                SET festive_bonus = :festive_bonus
                WHERE emp_id = :emp_id
            """
            cur.execute(update_query, {"festive_bonus": festive_bonus, "emp_id": emp_id})

        # Commit changes
        cur.execute("COMMIT")
        print("Festive bonuses updated successfully!")

    except Exception as e:
        print("An error occurred:", e)
    finally:
        # Close the connection
        if cur:
            cur.close()
