class Query:
    database = ''
    table = ''
    select = None
    where = None

    def __init__(self, database, table, select=None, where=None):

    def __str__(self):
        return "this is the think"