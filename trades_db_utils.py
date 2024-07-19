'''
Create the CoinTrades database, add in tables for Coins and Trades.
Also, create some functions used to interact with the db for future analysis.
'''

import mysql.connector
from mysql.connector import Error

class Trade:
    def __init__(self, mint_address, signature, sol_amount, token_amount, is_buy, user, timestamp):
        self.mint_address = mint_address
        self.signature = signature
        self.sol_amount = sol_amount
        self.token_amount = token_amount
        self.is_buy = is_buy
        self.user = user
        self.timestamp = timestamp

def create_connection():
    """Create a database connection."""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='CoinTrades',
            user='root',
            password='XXXXXXX'
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print("Error while connecting to MySQL", e)

def create_tables(connection):
    """Create tables if they do not exist."""
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS coins (
            id INT AUTO_INCREMENT PRIMARY KEY,
            mint_address VARCHAR(55) NOT NULL UNIQUE,
            creator_address VARCHAR(55) NOT NULL
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INT AUTO_INCREMENT PRIMARY KEY,
            coin_id INT,
            signature VARCHAR(120) NOT NULL,
            sol_amount DECIMAL(15, 4) NOT NULL,
            token_amount DECIMAL(30, 2) NOT NULL,
            is_buy BOOLEAN NOT NULL,
            user VARCHAR(55) NOT NULL,
            timestamp BIGINT NOT NULL,  -- Unix seconds timestamp
            FOREIGN KEY (coin_id) REFERENCES coins(id)
        );
    """)
    print("Tables created (if they did not exist)")
    cursor.close()

def create_coin(connection, mint_address, creator_address):
    """Insert a new coin into the coins table."""
    query = "INSERT INTO coins (mint_address, creator_address) VALUES (%s, %s) ON DUPLICATE KEY UPDATE id=id"
    cursor = connection.cursor()
    cursor.execute(query, (mint_address, creator_address))
    connection.commit()
    coin_id = cursor.lastrowid
    cursor.close()
    return coin_id or retrieve_coin_id(connection, mint_address)

def retrieve_coin_id(connection, mint_address):
    """Retrieve coin_id based on mint_address."""
    query = "SELECT id FROM coins WHERE mint_address = %s"
    cursor = connection.cursor()
    cursor.execute(query, (mint_address,))
    row = cursor.fetchone()
    cursor.close()
    return row[0] if row else None

def add_all_trades(connection, trades, creator_addr, token_addr):
    """Insert multiple new trades into the trades table using the mint addresses of the coins."""
    cursor = connection.cursor()

    query = """
    INSERT INTO trades (coin_id, signature, sol_amount, token_amount, is_buy, user, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for trade in trades:
        coin_id = create_coin(connection, trade.mint_address, creator_addr)
        data = (coin_id, trade.signature, trade.sol_amount, trade.token_amount, trade.is_buy, trade.user, trade.timestamp)
        cursor.execute(query, data)

    # handle for coins with no trades so we can skip in future
    if len(trades) == 0:
        coin_id = create_coin(connection, token_addr, creator_addr)
        data = (coin_id, "N/A", -1, -1, False, "N/A", -1)
        cursor.execute(query, data)

    connection.commit()
    print(f"{len(trades)} trades inserted successfully.")
    cursor.close()

def check_token_exists(mint_address, connection):
    """Check if a specific coin and its trades exist in the database."""
    try:
        cursor = connection.cursor()
        query = """
        SELECT COUNT(*) FROM coins
        JOIN trades ON coins.id = trades.coin_id
        WHERE coins.mint_address = %s
        """
        cursor.execute(query, (mint_address,))
        result = cursor.fetchone()
        exists = result[0] > 0
        return exists
    except mysql.connector.Error as e:
        print(f"Error checking token existence: {e}")
        return False

if __name__ == "__main__":
    connection = create_connection()
    create_tables(connection)
    connection.close()
