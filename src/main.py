import scheduler
import sys
import time

def test():
    print("ok")

if __name__ == "__main__":
    s = scheduler.Scheduler() # TODO: cluster, cloud
    # just testing:
    s.schedule("* * * * *", test)

    while True:
        time.sleep(10)
