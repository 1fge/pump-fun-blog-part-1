'''
Read from the MySQL DB list of coins and fetch all trade history for them,
if we don't already have it. Add the trade history to DB after fetching it.
'''

from trades_db_utils import Trade, add_all_trades, check_token_exists, create_connection

import time
import requests
import json
import threading
import queue
import os
import sys
import logging

from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
logging.basicConfig(filename='trade-output.log', level=logging.INFO)



LAMPORTS_PER_SOL = 1000000000
NUM_WORKERS = 3
FETCH_HEADERS = {
    'Host': 'client-api-2-74b1891ee9f9.herokuapp.com',
    'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'sec-ch-ua-platform': '"Windows"',
    'Accept': '*/*',
    'dnt': '1',
    'Origin': 'https://pump.fun',
    'Sec-Fetch-Site': 'cross-site',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Referer': 'https://pump.fun/',
    'Accept-Language': 'en-US,en;q=0.9',
}

def fetch_trades(token_addr, coin_details, connection):
    offset = 0
    all_trades = []
    while True:
        url = f"https://client-api-2-74b1891ee9f9.herokuapp.com/trades/{token_addr}?limit=200&offset={offset}"
        response = requests.get(url, headers=FETCH_HEADERS)
        if response.status_code == 429:
            logging.info("Rate Limited, Exiting Program")
            sys.exit(1)

        trades = response.json()
        all_trades.extend(trades)
        if not trades or len(trades) < 190:
            break

        offset += 200
        time.sleep(5.5)

    add_trades_db(connection, all_trades, coin_details["creatorAddr"], coin_details["tokenAddr"])

def add_trades_db(connection, all_trades, creator_addr, token_addr):
    trades = []

    for trade in all_trades:
        mint_addr = trade["mint"]
        sig = trade["signature"]
        sol_amount = trade["sol_amount"] / LAMPORTS_PER_SOL
        token_amount = trade["token_amount"]
        is_buy = trade["is_buy"]
        user = trade["user"]
        timestamp = trade["timestamp"]

        new_trade = Trade(mint_addr, sig, sol_amount, token_amount, is_buy, user, timestamp)
        trades.append(new_trade)

    add_all_trades(connection, trades, creator_addr, token_addr)

def worker(task_queue):
    connection = create_connection()

    while not task_queue.empty():
        try:
            token_addr, coin_details = task_queue.get_nowait()
            if check_token_exists(token_addr, connection):
                print("Skipping", token_addr, "since exists")
                continue

            fetch_trades(token_addr, coin_details, connection)
            time.sleep(5.5)
        except queue.Empty:
            break

    connection.close()

def main():
    coin_map_file = 'coin_map.json'
    if not os.path.exists(coin_map_file):
        logging.info("No coin map file found.")
        return

    with open(coin_map_file, 'r') as file:
        coin_map = json.load(file)


    task_queue = queue.Queue()

    for token_addr, coin_details in coin_map.items():
        task_queue.put((token_addr, coin_details))

    threads = []
    for _ in range(NUM_WORKERS):
        t = threading.Thread(target=worker, args=(task_queue,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
