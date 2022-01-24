
class Node():

    def __init__(self):
        self.nodes = []

    def add_node(self, node):
        self.nodes.append(node)

    def run(self, input):
        for n in self.nodes:
            n.run(input)
        print("done")
