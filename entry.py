class Entry(object):

    # in the application, command is just a number, which means how many tickets are sold; or change config...
    def __init__(self, term, command):
        self.term = term
        self.command = command

    def get_term(self):
        return self.term

    def get_command(self):
        return self.command

    def toString(self):
        return 'entry.Entry(' + str(self.term) + ',' + str(self.command) + ')'