
import sys
import time

import scheduler

from node import Node

if __name__ == "__main__":
    s = scheduler.Scheduler() # TODO: cluster, cloud

    # just testing:
    start = Node()
    start.add_node(Node())

    s.schedule("* * * * *", start.run, {"input":{}})

    while True:
        time.sleep(10)
