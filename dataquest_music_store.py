# All code below is a side project done on Dataquest -> Data Engineering Track in the intermediate SQL section
# Our task was to extract data and analyze it; stepping into a music store use case

import sqlite3 
import pandas as pd 

# function that takes a sql query as an argument and returns DF 
def run_query(q):
    with sqlite3.connect({database_name.db}) as conn:
        return pd.read_sql_query(q,conn)
    
# takes SQL command as an arg and executes it using sqlite module
def run_command(command):
    with sqlite3.connect({database_name.db}) as conn:
        #conn.isolaton_level = None
        conn.execute(command)
        
# function showing tables info
def show_tables():
    q = '''
    SELECT
        name,
        type
    FROM sqlite_master
    WHERE type IN ("table","view");'''
    
    return run_query(q)

show_tables()

# Below: A query that returns each genre, with the # of tracks sold where country = 'USA'

albums_to_purchase = '''

WITH usa_sold_tracks AS (

    SELECT 
    il.* 
    FROM invoice_line il
    INNER JOIN invoice i ON il.invoice_id = i.invoice_id
    INNER JOIN customer c on i.customer_id = c.customer_id
    WHERE country = "USA" 
)

SELECT
    g.name,
    COUNT(uts.invoice_line_id) tracks_sold,
    cast(count(uts.invoice_line_id) AS Float) / (
    SELECT COUNT(*) FROM usa_sold_tracks
    ) percentage_sold
    
FROM usa_sold_tracks uts 
INNER JOIN track t on uts.track_id = t.track_id
INNER JOIN genre g on g.genre_id = t.genre_id 
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10 
    
'''
run_query(albums_to_purchase)

# Below: A query that finds the total dollar amount of sales, assignted to each sales support agent of the music store
# added 'hire_date' as extra attribute 

q2 = '''

WITH customer_support_rep_sales AS (

    SELECT
        c.support_rep_id,
        SUM(i.total) total
    FROM customer c 
    INNER JOIN invoice i ON c.customer_id = i.customer_id 
    GROUP BY c.support_rep_id
   
)

SELECT 
    e.first_name || " " || e.last_name employee_name,
    csrs.total total_sales,
    e.hire_date hired_on
FROM employee e
INNER JOIN customer_support_rep_sales csrs 
ON e.employee_id = csrs.support_rep_id
WHERE e.title = "Sales Support Agent" 
AND e.employee_id = csrs.support_rep_id
GROUP BY csrs.support_rep_id
ORDER BY 2 DESC
;
'''
run_query(q2)


