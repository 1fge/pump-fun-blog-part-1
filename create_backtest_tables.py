'''
Create backtest tables to test our strategy on.
This script creates four tables total:
 - 2 tables for coins, where the first table has some percentage of existing coins
 - 2 tables for the trades, using the same ratio as above

With these backtest tables, we can use the backtest_new to simulate our algorithm, based on the current rules.
Partitioning the database based on the exclusion factor allows us to simulate historical trades as if the algorithm was running live.
'''

from trades_db_utils import create_connection

# Fetch the median ID from a table, excluding a certain percentage of rows at the start.
def fetch_median_id_with_exclusion(cursor, table_name, exclusion_factor):
    cursor.execute(f"SELECT id FROM {table_name} ORDER BY id")
    all_ids = cursor.fetchall()

    start_index = int(len(all_ids) * exclusion_factor)
    selected_ids = all_ids[start_index:]

    if not selected_ids:
        raise ValueError(f"No rows remain after applying the exclusion factor {exclusion_factor}.")

    median_index = len(selected_ids) // 2
    median_id = selected_ids[median_index][0]

    return median_id

def setup_backtest_tables(connection, exclusion_factor):
    """Set up backtest tables by clearing old ones and creating new populated ones."""
    cursor = connection.cursor()

    cursor.execute("DROP TABLE IF EXISTS coins_backtest_old;")
    cursor.execute("DROP TABLE IF EXISTS coins_backtest_new;")
    cursor.execute("DROP TABLE IF EXISTS trades_backtest_old;")
    cursor.execute("DROP TABLE IF EXISTS trades_backtest_new;")

    cursor.execute("CREATE TABLE coins_backtest_old LIKE coins;")
    cursor.execute("CREATE TABLE coins_backtest_new LIKE coins;")
    cursor.execute("CREATE TABLE trades_backtest_old LIKE trades;")
    cursor.execute("CREATE TABLE trades_backtest_new LIKE trades;")

    coins_median_id = fetch_median_id_with_exclusion(cursor, "coins", exclusion_factor)
    trades_median_id = fetch_median_id_with_exclusion(cursor, "trades", exclusion_factor)

    cursor.execute(f"INSERT INTO coins_backtest_old SELECT * FROM coins WHERE id <= {coins_median_id};")
    cursor.execute(f"INSERT INTO coins_backtest_new SELECT * FROM coins WHERE id > {coins_median_id};")
    cursor.execute(f"INSERT INTO trades_backtest_old SELECT * FROM trades WHERE id <= {trades_median_id};")
    cursor.execute(f"INSERT INTO trades_backtest_new SELECT * FROM trades WHERE id > {trades_median_id};")

    connection.commit()

    cursor.execute("SELECT COUNT(*) FROM coins_backtest_old;")
    print("coins_backtest_old count:", cursor.fetchone()[0])
    cursor.execute("SELECT COUNT(*) FROM coins_backtest_new;")
    print("coins_backtest_new count:", cursor.fetchone()[0])
    cursor.execute("SELECT COUNT(*) FROM trades_backtest_old;")
    print("trades_backtest_old count:", cursor.fetchone()[0])
    cursor.execute("SELECT COUNT(*) FROM trades_backtest_new;")
    print("trades_backtest_new count:", cursor.fetchone()[0])

    cursor.close()

def main():
    connection = create_connection()
    if connection is not None:
        # excluding the first 75% of entries
        setup_backtest_tables(connection, 0.75)
        connection.close()
    else:
        print("Failed to connect to the database.")

if __name__ == "__main__":
    main()
