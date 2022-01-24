
import sys
import time

import scheduler

from node import Node

if __name__ == "__main__":
    s = scheduler.Scheduler() # TODO: cluster, cloud

    # just testing:
    start = Node(id="start")
    start.add_node(Node(id="end"))

    s.schedule("* * * * *", start.run, {"input":"some data"})

    while True:
        time.sleep(10)
