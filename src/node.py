import logging

logger = logging.getLogger('apscheduler')

class Node():

    def __init__(self, id):
        self.id = id
        self.nodes = []

        # loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
        # for l in loggers:
        #    print(l)

    def add_node(self, node):
        self.nodes.append(node)

    def run(self, input):
        logger.info(f"{self.id} in: {input}")
        for n in self.nodes:
            logger.info(f"{self.id} out to {n.id}: {input}")
            n.run(input)

