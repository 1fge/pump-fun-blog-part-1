'''
Using our DB of all coins created along with their trades,
export trading history where orc has bought and sold the coins.
Used for further analysis on their strategy.
'''

import json
from trades_db_utils import create_connection

ORC_ADDRESS = "orcACRJYTFjTeo2pV8TfYRTpmqfoYgbVi9GeANXTCc8"

def fetch_all_coins(connection):
    """Retrieve all coins from the database."""
    cursor = connection.cursor()
    query = "SELECT id, mint_address, creator_address FROM coins"
    cursor.execute(query)
    coins = cursor.fetchall()
    cursor.close()
    return coins

def fetch_trades_for_coin(connection, coin_id):
    """Retrieve all trades for a given coin."""
    cursor = connection.cursor()
    query = "SELECT * FROM trades WHERE coin_id = %s"
    cursor.execute(query, (coin_id,))
    trades = cursor.fetchall()
    cursor.close()
    return trades

def iterate_coins_and_trades():
    orc_trades = []
    '''
    each trade will hold
    mint_address:
    creator:
    buy:
        signature
        timestamp
        sol_amount
        token_amount
        num_buys
    sell:
        signature
        timestamp
        sol_amount
        token_amount
        num_sells
    '''
    connection = create_connection()
    try:
        coins = fetch_all_coins(connection)
        for coin in coins:
            coin_id, mint_address, creator_address = coin
            trades = fetch_trades_for_coin(connection, coin_id)
            if not trades:
                continue

            orc_trade = fetch_orc_trade(mint_address, creator_address, trades)
            if not orc_trade:
                continue

            orc_trades.append(orc_trade)

        print("Total orc Trades", len(orc_trades), "Total Coins Checked", len(coins))

        # write all orc trades to file
        with open("orc-trades.json", "w") as f:
            json.dump(orc_trades, f)

    finally:
        if connection:
            connection.close()

def fetch_orc_trade(mint_address, creator_address, coin_trades):
    orc_trade = {
        "mint_address": mint_address,
        "creator": creator_address,
        "is_copy_blocker": False,
        "creator_initial_buy_sol": None,
        "buy": {
            "buy_entry_id": None,
            "signature": None,
            "timestamp": None,
            "sol_amount": None,
            "token_amount": None,
            "num_buys": 0,
        },
        "sell": {
            "sell_entry_id": None,
            "sig": None,
            "timestamp": None,
            "sol_amount": None,
            "token_amount": None,
            "num_sells": 0,
        },
    }

    coin_trades = reversed(coin_trades)

    # iterate through trades, first to last
    for index, trade in enumerate(coin_trades):
        entry_id, _, sig, sol_amount, token_amount, is_buy, user, timestamp = trade
        if user != ORC_ADDRESS:
            if user == creator_address and index < 10 \
                and is_buy \
                and orc_trade["creator_initial_buy_sol"] is None \
                and orc_trade["buy"]["sol_amount"] is None: # make sure they haven't bought yet
                    orc_trade["creator_initial_buy_sol"] = float(sol_amount)

            # another check for copy_blocker
            # for real coins, orc never sells before creator
            # checks if creator sold and number of orc sells is over 1
            elif user == creator_address and not is_buy \
                and orc_trade["sell"]["num_sells"] > 0 \
                and orc_trade["sell"]["timestamp"] and orc_trade["buy"]["timestamp"] \
                and orc_trade["sell"]["timestamp"] - orc_trade["buy"]["timestamp"] < 75:
                    orc_trade["is_copy_blocker"] = True
            continue

        if is_buy:
            orc_trade["buy"]["buy_entry_id"] = entry_id
            orc_trade["buy"]["sig"] = sig
            orc_trade["buy"]["timestamp"] = timestamp
            orc_trade["buy"]["sol_amount"] = float(sol_amount)
            orc_trade["buy"]["token_amount"] = float(token_amount)
            orc_trade["buy"]["num_buys"] += 1
        else:
            if orc_trade["buy"]["buy_entry_id"] and entry_id == orc_trade["buy"]["buy_entry_id"] - 1:
                orc_trade["is_copy_blocker"] = True

            orc_trade["sell"]["sell_entry_id"] = entry_id
            orc_trade["sell"]["sig"] = sig
            orc_trade["sell"]["timestamp"] = timestamp
            orc_trade["sell"]["sol_amount"] = float(sol_amount)
            orc_trade["sell"]["token_amount"] = float(token_amount)
            orc_trade["sell"]["num_sells"] += 1

    if orc_trade["buy"]["num_buys"] == 0:
        return None
    return orc_trade

iterate_coins_and_trades()
