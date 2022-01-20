import scheduler

def test():
    print("ok")

if __name__ == "__main__":
    s = scheduler.Scheduler() # TODO: cluster, cloud
    s.start()

    # just testing:
    s.schedule("* * * * *", test)
