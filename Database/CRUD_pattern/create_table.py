def create_table(table_name : str) -> None:
    conn_params = {
    "host" : "localhost",
    "database" : "Fin_proj",
    "user" : "postgres",
    "password" : "nckumark"
    }
    sql = """CREATE TABLE {} (
                id serial PRIMARY KEY,
                num integer,
                data varchar);
          """.format(table_name)
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**conn_params)
        # create a new cursor
        cur = conn.cursor()
        # Execute the create table query
        cur.execute(sql)
        # Commit the changes to the database
        conn.commit()
        # Close communication with the PostgreSQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close() 
