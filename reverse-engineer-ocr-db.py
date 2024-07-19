'''
This is the script where I add in features / constraints to try and
get closer to orc's 16% buy rate, getting as close to their algorithm as possible

Currently, it excludes:
 - Creator purchased more than 4.0 SOL
 - Creator previously made a coin
 - Creator was funded by someone who previously made a coin
'''

from trades_db_utils import create_connection

def fetch_all_coins(connection):
    """Retrieve all coins from the database."""
    cursor = connection.cursor()
    query = "SELECT id, mint_address, creator_address, funder_address FROM coins"
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
    # bad coins, aka coins we will not attempt to buy
    bad_coins = set()
    creators = set()

    creator_made_multiple_coins = 0
    creator_over_sol_buy_limit = 0

    connection = create_connection()
    try:
        coins = fetch_all_coins(connection)
        for coin in coins:
            coin_id, mint_address, creator_address, funder_address = coin
            trades = fetch_trades_for_coin(connection, coin_id)
            if not trades:
                continue

            if creator_address not in creators:
                creators.add(creator_address)
            else:
                bad_coins.add(mint_address)
                creator_made_multiple_coins += 1

            if funder_address in creators:
                bad_coins.add(mint_address)

            trades = reversed(trades)
            for index, trade in enumerate(trades):
                _, _, _, sol_amount, _, is_buy, user, _ = trade
                if index < 10 and is_buy and user == creator_address:
                    if sol_amount > 2:
                        creator_over_sol_buy_limit += 1
                        bad_coins.add(mint_address)

        print("Total coins", len(coins))
        print("Coins where creator made multiple", creator_made_multiple_coins)
        print("Coins where creator bought over 4.0 sol", creator_over_sol_buy_limit)
        print("Total bad coins:", len(bad_coins))

    finally:
        if connection:
            connection.close()

iterate_coins_and_trades()
