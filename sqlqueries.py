class SqlQueries:
    """
    This class has all the SQL Queries that will be used for the entire Data Pipeline
    """
    
    query_skip_days = \
    f"""
        select dateadd(day, 1, sql_dt) as skip_day 
        from FIC_PROD_DB.EDW.DAY where bus_day_ind = 'N' 
        and sql_dt >= '2021-01-01' 
        order by sql_dt
    """ 