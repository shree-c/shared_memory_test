from time import sleep
import json


def calculate_depth(table, depth):
    hold = {}
    print(table._getvalue().keys())
    for price in table._getvalue().keys():
        # print(price, value)
        if len(hold) == depth:
            break
        hold[price] = sum(
            map(
                lambda x: x["quantity"] - x["punched"],
                filter(
                    lambda x: x["price"] == price and not x["cancelled"], table[price]._getvalue()
                ),
            )
        )
        if hold[price] == 0:
          hold.pop(price)
    return hold


def bid_ask_spread(buy_price_table, sell_price_table, depth):
    return {
        "bid": calculate_depth(buy_price_table, depth),
        "ask": calculate_depth(sell_price_table, depth),
    }


def broadcast_bid_ask(buy_price_table, sell_price_table, depth, channel, routing_key):
    while True:
        print("BROADCASTING BID ASK ...")
        channel.basic_publish(
            exchange="",
            routing_key=routing_key,
            body=json.dumps(bid_ask_spread(buy_price_table, sell_price_table, depth)),
        )
        sleep(1)


def broadcast_trades(trade_queue, channel, routing_key):
    while True:
        print("BROADCASTING TRADES...")
        trade = trade_queue.get()
        print(trade)
        channel.basic_publish(
            exchange="",
            routing_key=routing_key,
            body=json.dumps(trade),
        )
