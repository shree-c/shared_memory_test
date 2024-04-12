from time import sleep
from multiprocessing import Lock
from datetime import datetime
from utils.priority_queue import PQManager


def match_engine(shared_memory, mutation_lock, trade_publish_queue):
    # sanity
    # print(shared_memory["price_active_set_buy"], "THis should be set")
    # print("should not throw error")
    # shared_memory['price_active_set_buy'].add(10)
    # print("Added 10")
    # normal_set = set()
    # print(10 in normal_set, "For normal set")
    # print(10 in shared_memory['price_active_set_buy'])

    # sanity
    buy_pq = PQManager(
        shared_memory["price_active_set_buy"],
        shared_memory["price_priority_queue_buy"],
        -1,
    )
    sell_pq = PQManager(
        shared_memory["price_active_set_sell"],
        shared_memory["price_priority_queue_sell"],
        1,
    )
    while True:
        print(shared_memory["price_table_buy"], "PTB")
        print(shared_memory["price_table_sell"], "PTS")
        # print(shared_memory["order_book"], "OB")
        print("JAI MAHADEV")
        print("WAITING FOR BUY ")
        top_buy_price = buy_pq.get()
        # No waiting for sell
        top_sell_price = sell_pq.get(no_wait=True)
        if not top_sell_price:
            print("NO Sell order")
            buy_pq.put(top_buy_price)
            continue

        if top_buy_price >= top_sell_price:
            print("Match seem to exist")
            sell_queue_len = len(
                shared_memory["price_table_sell"][top_sell_price]._getvalue()
            )
            buy_queue_len = len(
                shared_memory["price_table_buy"][top_buy_price]._getvalue()
            )
            sell_index = 0
            buy_index = 0
            with mutation_lock:
                while True:
                    if sell_index == sell_queue_len or buy_index == buy_queue_len:
                        break
                    buy_item = shared_memory["price_table_buy"][
                        top_buy_price
                    ]._getvalue()[0]
                    # check for cancelled or updated items
                    if buy_item["price"] != top_buy_price or buy_item["cancelled"]:
                        shared_memory["price_table_buy"][top_buy_price].popleft()
                        buy_index += 1
                        continue

                    sell_item = shared_memory["price_table_sell"][
                        top_sell_price
                    ]._getvalue()[0]
                    # check for cancelled or updated items
                    if sell_item["price"] != top_sell_price or sell_item["cancelled"]:
                        # adding buy item back
                        shared_memory["price_table_buy"][top_buy_price].appendleft(
                            buy_item
                        )
                        shared_memory["price_table_sell"][top_sell_price].popleft()
                        sell_index += 1
                        continue

                    execution_quantity = min(
                        buy_item["quantity"], sell_item["quantity"]
                    )
                    buy_item["punched"] += execution_quantity
                    sell_item["punched"] += execution_quantity
                    trade_publish_queue.put(
                        {
                            "buy_order_id": buy_item["order_id"],
                            "sell_order_id": sell_item["order_id"],
                            "timestamp": datetime.now().timestamp(),
                            "price": (
                                buy_item["price"]
                                if buy_item["timestamp"] >= sell_item["timestamp"]
                                else sell_item["price"]
                            ),
                            "quantity": execution_quantity,
                        }
                    )
                    if buy_item["punched"] == buy_item["quantity"]:
                        print("Completely filled,  BUY", buy_item["order_id"])
                        buy_index += 1
                        shared_memory["price_table_buy"][top_buy_price].popleft()
                    if sell_item["punched"] == sell_item["quantity"]:
                        print("Completely filled, SELL ", sell_item["order_id"])
                        shared_memory["price_table_sell"][top_sell_price].popleft()
                        sell_index += 1
            if sell_index != sell_queue_len:
                sell_pq.put(top_sell_price)
            if buy_index != buy_queue_len:
                buy_pq.put(top_buy_price)
            print(
                "BUY QUEUE: ",
                len(shared_memory["price_table_buy"][top_buy_price]._getvalue()),
            )
            print(
                "SELL QUEUE: ",
                len(shared_memory["price_table_sell"][top_sell_price]._getvalue()),
            )
        else:
            buy_pq.put(top_buy_price)
            sell_pq.put(top_sell_price)
        sleep(0.5)
