def in_with_regex(field, values):
    """
    Creates a condition that matches a field against multiple values using regex-like patterns.

    This function constructs a condition that checks if the specified field matches any
    of the given values using a "like" operator with wildcards. This is useful for filtering
    data based on partial matches or patterns.

    Args:
        field (Field): A Pypika Field object representing the column to be checked.
        values (list or str): A list of values or a single value to match against. 
                              If a single value is provided, it is converted to a list.

    Returns:
        Condition: A Pypika condition combining all the individual conditions with OR operators.
    """
    # Ensure values is a list
    if not isinstance(values, list):
        values = [values]
    
    # Build the conditions using the OR operator
    conditions = [field.like(f"%{value}%") for value in values]
    
    # Combine conditions with OR operator
    if conditions:
        combined_condition = conditions[0]
        for condition in conditions[1:]:
            combined_condition |= condition
    else:
        combined_condition = None
        
    return combined_condition
