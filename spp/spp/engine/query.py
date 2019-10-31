class Query:
    database = ''
    table = ''
    select = None
    where = None
    query = ''

    def __init__(self, database, table, select=None, where=None):
        self.database = database
        self.table = table
        self.select = select
        self.where = where
        self.formulate_query(self.database, self.table, self.select, self.where)

    def __str__(self):
        return self.query

    def handle_select(self, select):
        sel = None
        if select is None:
            sel = "*"
        elif isinstance(select, list):
            sel = ", ".join(list)
        else:
            sel = select
        return sel

    def handle_where(self, where):
        if where is not None and isinstance(where, dict):
            clause_list = []
            for k,v in where.items():
                clause_list.append("{} {} {} AND".format(k, v["condition"], v["value"]))
            return " AND ".join(clause_list)
        else:
            return None

    def formulate_query(self, database, table, select, where):
        tmp = "SELECT {} FROM {}.{}".format(self.handle_select(select), database,table)
        where_clause = self.handle_where(where)
        if where_clause is not None:
            tmp + " WHERE {}".format(where_clause)
        self.query = tmp + ";"
