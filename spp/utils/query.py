class Query:
    """ Class to create a SQL Query string from the input parameters. """
    database = ''
    table = ''
    select = None
    where = None
    query = ''

    def __init__(self, database, table, select=None, where=None):
        """ Constructor for the Query class takes database and table optional for select whcih can be string or list
            Optional where expects a map of format {"column_name": {"condition": value, "value": value}}
        """
        self.database = database
        self.table = table
        self.select = select
        self.where = where
        self._formulate_query(self.database, self.table, self.select, self.where)

    def __str__(self):
        return self.query

    def _handle_select(self, select):
        """ Method to generate select statements if None default to select all: '*'. """
        if select is None:
            sel = "*"
        elif isinstance(select, list):
            sel = ", ".join(select)
        else:
            sel = select
        return sel

    def _handle_where(self, where):
        """ Method to generate where statements form map"""
        if where is not None and isinstance(where, dict):
            clause_list = []
            for k, v in where.items():
                clause_list.append("{} {} {}".format(k, v["condition"], v["value"]))
            return " AND ".join(clause_list).rstrip(" AND ")
        else:
            return None

    def _formulate_query(self, database, table, select, where):
        """ Method to create the query string and set the query value this is called by the constructor """
        tmp = "SELECT {} FROM {}.{}".format(self._handle_select(select), database,table)
        where_clause = self._handle_where(where)
        if where_clause is not None:
            tmp += " WHERE {}".format(where_clause)
        self.query = tmp + ";"
