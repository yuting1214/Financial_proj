def update_delisted_stock(stock_ids):
    conn_params = {
    "host" : "localhost",
    "database" : "Fin_proj",
    "user" : "postgres",
    "password" : "nckumark"
    }
    sql = """ UPDATE stock_info
                SET list_status = %s
                WHERE stock_id = ANY(%s); """
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**conn_params)
        # create a new cursor
        cur = conn.cursor()
        # execute the UPDATE  statement
        cur.execute(sql, ('下市', stock_ids))
        # Commit the changes to the database
        conn.commit()
        # Close communication with the PostgreSQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close() 
