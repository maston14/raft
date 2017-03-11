class Entry(object):

    def __init__(self, term, command):
        self.term = term
        self.command = command

    def getTerm(self):
        return self.term

    def getCommand(self):
        return self.command

    def toString(self):
        return 'entry.Entry(' + str(self.term) + ',' + str(self.command) + ')'