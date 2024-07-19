'''
This is the script where we can backtest current strategy
So far, strategy is this
- Do not buy coins where creator has already made one
- Do not buy if creator buy is over 2.0 sol
- Do not buy if creator was funded by known creator
- For sake of backtesting, don't buy if no creator buy
'''

import json
from trades_db_utils import create_connection
from create_backtest_tables import setup_backtest_tables

orc_buys = set()
skips_because_no_creator_sell = 0 # used to see how many trades we would still be in..
ORC_ADDRESS = "orcACRJYTFjTeo2pV8TfYRTpmqfoYgbVi9GeANXTCc8"

# check backtest_old
def wallet_made_other_coins(connection, wallet_address):
    cursor = connection.cursor()
    query = f'SELECT id, mint_address, creator_address FROM coins_backtest_old WHERE creator_address = "{wallet_address}"'
    cursor.execute(query)
    coins = cursor.fetchall()
    cursor.close()
    return len(coins) > 0

# fetch all new coins
# from coins_backtest_new
def fetch_all_coins(connection):
    """Retrieve all coins from the database."""
    cursor = connection.cursor()
    query = "SELECT id, mint_address, creator_address, funder_address FROM coins_backtest_new"
    cursor.execute(query)
    coins = cursor.fetchall()
    cursor.close()
    return coins

# fetch new coin trades
# from trades_backtest_new
def fetch_coin_trades(connection, coin_id):
    """Retrieve all trades for a given coin."""
    cursor = connection.cursor()
    query = "SELECT * FROM trades_backtest_new WHERE coin_id = %s"
    cursor.execute(query, (coin_id,))
    trades = cursor.fetchall()
    cursor.close()
    return trades

# attempt to retrieve coin id from coins_backtest_old
def retrieve_coin_id(connection, mint_address):
    """Retrieve coin_id based on mint_address."""
    query = "SELECT id FROM coins_backtest_old WHERE mint_address = %s"
    cursor = connection.cursor()
    cursor.execute(query, (mint_address,))
    row = cursor.fetchone()
    cursor.close()
    return row[0] if row else None


# add our future coin into old coins so we can track moving forward
# add into coins_backtest_old
def create_coin(connection, mint_address, creator_address):
    """Insert a new coin into the coins table."""
    query = "INSERT INTO coins_backtest_old (mint_address, creator_address) VALUES (%s, %s) ON DUPLICATE KEY UPDATE id=id"
    cursor = connection.cursor()
    cursor.execute(query, (mint_address, creator_address))
    connection.commit()
    coin_id = cursor.lastrowid
    cursor.close()
    return coin_id or retrieve_coin_id(connection, mint_address)

def backtest_future_coins(connection):
    creator_trades = []

    all_new_coins = fetch_all_coins(connection)
    for coin in all_new_coins:
        coin_id, mint_address, creator_address, funder_address = coin
        if wallet_made_other_coins(connection, creator_address):
            continue

        if funder_address is None or funder_address == "N/A":
            continue

        if wallet_made_other_coins(connection, funder_address):
            continue

        coin_trades = fetch_coin_trades(connection, coin_id)
        creator_trade = fetch_creator_trade(mint_address, creator_address, coin_trades)
        if creator_trade is not None:
            creator_trades.append(creator_trade)

        # add in this coin to our db...
        create_coin(connection, mint_address, creator_address)

    print("Coins traded", len(creator_trades), "All coins", len(all_new_coins))
    return creator_trades


# this is where we do the majority of our backtesting
# after ensuring creator has not made other coins
def fetch_creator_trade(mint_address, creator_address, coin_trades):
    global orc_buys, skips_because_no_creator_sell

    reversed_trades = reversed(coin_trades)
    creator_trade = {
        "mint_address": mint_address,
        "initial_buy_sol": None,
        "initial_buy_tokens": None,
        "initial_buy_timestamp": None,

        "first_sell_sol": None,
        "first_sell_tokens": None,
        "first_sell_timestamp": None,
    }

    for index, trade in enumerate(reversed_trades):
        _, _, _, sol_amount, token_amount, is_buy, user, timestamp = trade
        if user == ORC_ADDRESS:
            orc_buys.add(mint_address)

        if index < 3 and user == creator_address and is_buy\
                and creator_trade["initial_buy_sol"] is None:
                    # if they bought over 2 sol, we skip
                    if float(sol_amount) > 2:
                        return None

                    creator_trade["initial_buy_sol"] = float(sol_amount)
                    creator_trade["initial_buy_tokens"] = float(token_amount)
                    creator_trade["initial_buy_timestamp"] = timestamp
                    continue

        if user == creator_address and not is_buy and creator_trade["first_sell_sol"] is None:
            creator_trade["first_sell_sol"] = float(sol_amount)
            creator_trade["first_sell_tokens"] = float(token_amount)
            creator_trade["first_sell_timestamp"] = timestamp

            if creator_trade["initial_buy_sol"] is not None and creator_trade["initial_buy_tokens"] is not None:
                return creator_trade

            skips_because_no_creator_sell += 1
            return None

    return None

def simulate_profits(creator_trades):
    profits = []

    for trade in creator_trades:
        buy_price_per_token = trade["initial_buy_sol"] / trade["initial_buy_tokens"]
        sell_price_per_token = trade["first_sell_sol"] / trade["first_sell_tokens"]
        profit_multiplier = sell_price_per_token / buy_price_per_token

        # simulate with 0.5 solana buy
        extrapolated_profit = 0.5 * profit_multiplier
        discounted_profit = (extrapolated_profit * .86) - 0.5 # anticipate 14% reduction always
        profits.append(discounted_profit)

    print(sum(profits))

if __name__ == "__main__":
    connection = create_connection()
    # recreate backtest tables before each run
    setup_backtest_tables(connection, 0.55)

    # backtest our coins by simulating trades
    creator_trades = backtest_future_coins(connection)

    with open("simulated-trades.json", "w") as f:
        json.dump(creator_trades, f)

    simulate_profits(creator_trades)
    print("Number of ORC buys", len(orc_buys))
    print("Trades we'd still be in (skipped because no creator sell)", skips_because_no_creator_sell)

