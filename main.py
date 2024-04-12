from multiprocessing import Process, Manager, Lock
from bid_ask_spread_broadcast import broadcast_bid_ask, broadcast_trades
from multiprocessing.managers import SyncManager
from queue import PriorityQueue
from accumulator import accumulator
from match_engine import match_engine
from collections import deque
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        "localhost",
    )
)
channel = connection.channel()
BID_ASK_QUEUE_CHANNEL_KEY = "bid_ask"
TRADE_UPDATES_CHANNEL_KEY = "trade_updates"
DEPTH = 5

channel.queue_declare(queue=BID_ASK_QUEUE_CHANNEL_KEY)
channel.queue_declare(queue=TRADE_UPDATES_CHANNEL_KEY)


def f(d, l):
    d[1] = "1"
    d["2"] = 2
    d[0.25] = None
    l.reverse()


def x(d, l):
    d["process2"] = "Fuck"


class MyManager(SyncManager):
    pass


if __name__ == "__main__":
    MyManager.register("priority_queue", PriorityQueue)
    MyManager.register("set", set)
    MyManager.register("deque", deque)
    with MyManager() as manager:
        shared_memory = {
            "order_book": manager.dict(),
            "price_table_buy": manager.dict(),
            "price_table_sell": manager.dict(),
            "price_priority_queue_buy": manager.priority_queue(),
            "price_priority_queue_sell": manager.priority_queue(),
            "price_active_set_buy": manager.set(),
            "price_active_set_sell": manager.set(),
        }
        trade_publish_queue = manager.Queue()
        mutation_lock = Lock()
        accumulator_p = Process(
            target=accumulator,
            args=(shared_memory, manager, mutation_lock),
        )
        matching_engine = Process(
            target=match_engine,
            args=(shared_memory, mutation_lock, trade_publish_queue),
        )
        bid_ask_broadcaster = Process(
            target=broadcast_bid_ask,
            args=(
                shared_memory["price_table_buy"],
                shared_memory["price_table_sell"],
                DEPTH,
                channel,
                BID_ASK_QUEUE_CHANNEL_KEY,
            ),
        )

        trade_broadcaster = Process(
            target=broadcast_trades,
            args=(trade_publish_queue, channel, TRADE_UPDATES_CHANNEL_KEY),
        )
        accumulator_p.start()
        matching_engine.start()
        bid_ask_broadcaster.start()
        trade_broadcaster.start()
        accumulator_p.join()
        matching_engine.join()
        bid_ask_broadcaster.join()
        trade_broadcaster.join()
