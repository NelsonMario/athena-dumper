from pypika import Field

def field(col, alias=None):
    """
    Creates a Pypika Field object with an optional alias.

    This function generates a Pypika `Field` object, which is used to represent
    a column in a SQL query. If an alias is provided, the field is given that alias;
    otherwise, it uses the column name as-is.

    Args:
        col (str): The name of the column to be represented by the Field object.
        alias (str, optional): An optional alias for the column. If provided, 
                               the Field object will use this alias in the SQL query.

    Returns:
        Field: A Pypika Field object, optionally with an alias.
    """
    # Check if an alias is provided
    if alias:
        # Return a Field object with the given column name and alias it
        return Field(col).as_(alias)
    
    # Return a Field object with the given column name without an alias
    return Field(col)
