'''
Simple script which reads the trades we have exported from pump.fun trading history,
and assists in building out an algorithm to match the coins that orc goes for. Currently, we check
- Total Profit
- Average Position Size
- Hours to reach that profit (hours of trading history we have)

Along with other indicators to reverse-engineer their trading strategy.
'''

import json

with open("orc-trades.json") as f:
    trades = json.load(f)

print("Total trades", len(trades))

profit = 0
profit_without_copy_block = 0
copy_block_no_creator = 0
profits_under_two_sol_creator_buy = []

trade_amt = 0
earliest_trade = None
last_trade = None
creator_retrade_count = 0
creator_retrade_profits = []
bought_with_no_creator_buy_count = 0
creators = set()
creator_buy_prices = []
orc_profits = []

for trade in trades:
    is_creator_retrade = False
    creator = trade["creator"]
    if creator not in creators:
        creators.add(creator)
    else:
        is_creator_retrade = True
        creator_retrade_count += 1
    creator = trade["creator"]

    creator_buy_sol = trade["creator_initial_buy_sol"]
    if not creator_buy_sol:
        bought_with_no_creator_buy_count += 1
    else:
        creator_buy_prices.append(creator_buy_sol)

    buy_sol = trade["buy"]["sol_amount"]
    sell_sol = trade["sell"]["sol_amount"]
    ts = trade["buy"]["timestamp"]
    if not earliest_trade or ts < earliest_trade:
        earliest_trade = ts
    if not last_trade or ts > last_trade:
        last_trade = ts


    if not sell_sol:
        continue

    if creator_buy_sol is None or creator_buy_sol < 2:
        profits_under_two_sol_creator_buy.append(sell_sol - buy_sol)

    orc_profits.append(round(sell_sol - buy_sol, 2))
    profit += sell_sol - buy_sol
    if is_creator_retrade:
        creator_retrade_profits.append(sell_sol - buy_sol)

    if not trade["is_copy_blocker"]:
        profit_without_copy_block += sell_sol - buy_sol
    elif not creator_buy_sol:
        copy_block_no_creator += 1

    trade_amt += buy_sol

print("Total Profit:", profit)
print("Average Position Size:", trade_amt / len(trades))
print("Hours for profit:", (last_trade - earliest_trade) / 3600)
print("Highest creator initial buy where orc bought", max(creator_buy_prices))
print("Profit where creator purchased < 2 sol", sum(profits_under_two_sol_creator_buy))
print("Times purchased from previous creators", creator_retrade_count)
print("Repurchased from previous creators profit", sum(creator_retrade_profits))
print("Times purchased with no creator buy / purchased before creator", bought_with_no_creator_buy_count)
print("Non copy-blocked profit", profit_without_copy_block)
print("Copy blocked w/o creator buy times", copy_block_no_creator)
