# Insert into database
def insert_function(df, table):
    conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
    }
    conn = psycopg2.connect(**conn_params)
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index = False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",", null = 'None')
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()
    conn.close()
