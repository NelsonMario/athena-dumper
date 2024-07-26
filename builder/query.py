class SimpleQueryBuilder:
    def __init__(self, table):
        """
        Initialize the QueryBuilder with a target table name.
        
        Args:
            table (str): The name of the table to query.
        """
        self.table = table
        self.columns = []
        self.conditions = []

    def select(self, *columns):
        """
        Specify the columns to select in the query.
        
        Args:
            columns (str): Column names to include in the SELECT clause. If no columns are provided, selects all columns.
        
        Returns:
            QueryBuilder: The current instance to allow method chaining.
        """
        self.columns = columns
        return self

    def where(self, condition, param):
        """
        Add a condition to the WHERE clause of the query, including support for IN and NOT IN conditions.
        
        Args:
            condition (str): The condition string, using placeholders for parameters (e.g., "column = %s").
                            For IN/NOT IN conditions, use the format "column IN (%s)" or "column NOT IN (%s)".
            params (str or list): The parameters to be used in the condition. For IN/NOT IN, this should be a list of values.
        
        Returns:
            QueryBuilder: The current instance to allow method chaining.
        """
        if isinstance(param, list):
            formatted_param  = "','".join([f"{str(value)}" for value in param])  # Convert each item to a string and wrap it in quotes
            self.conditions.append((condition, formatted_param))
        else:
            self.conditions.append((condition, param))
        return self

    def build(self):
        """
        Build the final SQL query string and extract the parameters.
        
        Returns:
            tuple: A tuple containing the SQL query string and a list of parameters.
        """
        # Construct the SELECT clause
        columns_clause = ", ".join(self.columns) if self.columns else "*"

        # Construct the FROM clause
        from_clause = f"FROM {self.table}"

        # Construct the WHERE clause if conditions are provided
        where_clause = ""
        (self.conditions)
        if self.conditions:
            where_clauses = []
            for condition, params in self.conditions:
                condition = condition.replace("%s", f"'{params}'", 1)
                where_clauses.append(condition)
            where_clause = "WHERE " + " AND ".join(where_clauses)

        # Final query assembly
        query = f"SELECT {columns_clause} {from_clause} {where_clause} LIMIT 50;"

        return query
