"""
ETL-Query script
"""

from mylib.spark_db import (
    create_session,
    csv_to_spark,
    execute_query,
    determine_activity,
)
import fire


def main(query_statement):
    spark = create_session()

    csv_to_spark(spark, "2021-2022 NBA Player Stats - Regular.csv")

    execute_query(spark, query_statement)

    # csv_result = determine_activity(csv_result)

    # execute_query(spark, query_statement)


main("SELECT * FROM nba_data WHERE Tm LIKE 'MIA'")

# Following below are sample CRUD Operations
# """SELECT general_name FROM GroceryDB WHERE count_products > 10"""
# """ DELETE FROM GroceryDB WHERE general_name LIKE 'locust bean gum' """
# """SELECT general_name FROM GroceryDB WHERE general_name LIKE '%bean%'"""

# main(sql_statement)
if __name__ == "__main__":
    fire.Fire(main)
