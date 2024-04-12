from uuid import uuid4
from time import sleep
from datetime import datetime
import json
from random import randint
from multiprocessing import Manager
import pika
from utils.priority_queue import PQManager

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        "localhost",
    )
)
channel = connection.channel()
channel.queue_declare(queue="hello")
def get_random_order():
    return {
        "order_id": str(uuid4()),
        "quantity": randint(200, 250),
        "timestamp": datetime.now().timestamp(),
        "cancelled": False,
        "pending": 0,
    }


def create_order(body, order_book, manager):
    order_id = uuid4()
    body["order_id"] = str(order_id)
    body["punched"] = 0
    body["cancelled"] = False
    body["timestamp"] = datetime.now().timestamp()
    # store it in permanent memory
    ord = manager.dict()
    ord.update(body)
    print(ord, "PRINT IT AGAIN")
    order_book[str(order_id)] = ord
    print(
        order_book[str(order_id)] is ord,
        ord == order_book[str(order_id)],
        "Is this same baby?",
    )
    return ord


def append_list(price_table, order, manager):
    if not order["price"] in price_table:
        price_table[order["price"]] = manager.deque()
    price_table[order["price"]].append(order)


def update_order(
    body, order_book, price_table_buy, price_table_sell, manager, mutation_lock
):
    if type(price_table_buy) == dict:
        print(price_table_buy)
        for price, o_list in enumerate(price_table_buy):
            print("PRICE ", price)
            print("FULL LIST", o_list)
            for order in o_list:
                print(order)

    if type(price_table_sell) == dict:
        print(price_table_buy)
        for price, o_list in enumerate(price_table_sell):
            print("PRICE ", price)
            print("FULL LIST", o_list)
            for order in o_list:
                print(order)

    with mutation_lock:
        order_id = body["order_id"]
        if order_id in order_book:
            order = order_book[order_id]
            if order["cancelled"]:
                return [False, "Cancelled"]
            else:
                if order["punched"] != 0:
                    return [False, "Order partially executed. You cant modify it"]
                else:
                    if order["direction"] == 1:
                        order["price"] = body["price"]
                        append_list(price_table_buy, order, manager)
                    else:
                        order["price"] = body["price"]
                        append_list(price_table_sell, order, manager)
                    return [True, None]
        else:
            return [False, "Couldn't find order"]


def cancel_order(
    body, order_book, price_table_buy, price_table_sell, mutation_lock, ord
):
    with mutation_lock:
        order_id = body["order_id"]
        print(order_book, order_id, "printing inside babe", order_book[order_id])
        if order_id in order_book:
            order = order_book[order_id]
            print("BIG CHECK...", ord == order)
            if order["cancelled"]:
                return [False, "Cancelled"]
            else:
                if order["punched"] != 0:
                    return [False, "Order partially executed. You cant modify it"]
                else:
                    order["cancelled"] = True
                    if order["direction"] == 1:
                        price_table_buy[order["price"]].remove(order)
                    else:
                        for item in price_table_sell[order["price"]]:
                            print("SINGLE ORDER IN QUEUE", item, item == order, order)
                        price_table_sell[order["price"]].remove(order)
                        print(price_table_sell)
                        print("**************SUCCESS*****************")
                    return [True, None]
        else:
            return [False, "Couldn't find order"]


def accumulator(shared_memory, manager, mutation_lock):
    buy_pq = PQManager(shared_memory['price_active_set_buy'], shared_memory['price_priority_queue_buy'], -1)
    sell_pq = PQManager(shared_memory['price_active_set_sell'], shared_memory['price_priority_queue_sell'], 1)
    def cb(ch, method, props, body):
        body_parsed = json.loads(body.decode())
        if "action" in body_parsed:
            if body_parsed["action"] == "create":
                body_parsed.pop("action")
                create_order(body_parsed, shared_memory["order_book"], manager)
                if body_parsed["direction"] == 1:
                    append_list(
                        shared_memory["price_table_buy"],
                        shared_memory["order_book"][body_parsed["order_id"]],
                        manager,
                    )
                    buy_pq.put(
                      body_parsed['price']
                    )
                else:
                    append_list(
                        shared_memory["price_table_sell"],
                        shared_memory["order_book"][body_parsed["order_id"]],
                        manager,
                    )
                    sell_pq.put(
                      body_parsed['price']
                    )
            elif body_parsed["action"] == "update":
                update_order(
                    body_parsed,
                    shared_memory["order_book"],
                    shared_memory["price_table_buy"],
                    shared_memory["price_table_sell"],
                    manager,
                    mutation_lock,
                )
            elif body_parsed["action"] == "cancel":
                cancel_order(
                    body_parsed,
                    shared_memory["order_book"],
                    shared_memory["price_table_buy"],
                    shared_memory["price_table_sell"],
                    mutation_lock,
                )
            else:
                print("action not implemented")
        else:
            print("No action attr")

    print("START")
    channel.basic_consume("hello", auto_ack=True, on_message_callback=cb)
    channel.start_consuming()
    sleep(2)
    print("Ended consuming")
