from es_aws_functions import general_functions

current_module = "SPP Engine - Query"


class Query:
    """ Class to create a SQL Query string from the input parameters. """
    database = ''
    table = ''
    select = None
    where = None
    query = ''
    run_id = None

    def __init__(self, database, table, select=None,
                 where=None, run_id=None):
        """ Constructor for the Query class takes
            database and table optional for select which can be string or list
            Optional where expects a map of format
            {"column_name": {"condition": value, "value": value}}
        """
        self.database = database
        self.table = table
        self.select = select
        self.where = where
        self.run_id = run_id
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

    def _handle_where(self, where_conds):
        """ Method to generate where statements form map"""
        if where_conds is not None and \
                isinstance(where_conds, list) \
                and len(where_conds) > 0:
            clause_list = []
            condition_str = ''

            for whr in where_conds:
                # Todo remove handle it on lambda itself.
                # currently config(json) string escape not
                # working as expected in lamda/stepfunction.
                # This is a work around
                if whr["column"] == 'run_id':
                    # Replace word 'previous' with
                    # a run_id which got generated in steprunner lambda
                    if whr["value"] == 'previous':
                        whr["value"] = self.run_id
                    whr["value"] = "'" + whr["value"] + "'"
                clause_list.append("{} {} {}".format(whr["column"],
                                                     whr["condition"],
                                                     str(whr["value"])))
                condition_str = " AND ".join(clause_list).rstrip(" AND ")
            return condition_str

        else:
            return None

    def _formulate_query(self, database, table, select, where):
        """ Method to create the query string and set the query value
        this is called by the constructor """
        tmp = "SELECT {} FROM {}.{}".format(self._handle_select(select), database, table)
        where_clause = self._handle_where(where)
        if where_clause is not None:
            tmp += " WHERE {}".format(where_clause)
        self.query = tmp + ";"
