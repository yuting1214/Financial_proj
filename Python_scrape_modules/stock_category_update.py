def stock_category_update(target_table, source):
    # 1. Retreive last updated date
    def get_last_date(table):
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = """   SELECT *
                    FROM latest_updated_date
                    WHERE table_name = %s
                    """
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**conn_params)
            # create a new cursor
            cur = conn.cursor()
            # execute the SQL statement
            cur.execute(sql, (table,))
            rows = cur.fetchone()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        export_date = rows[1].strftime('%Y-%m-%d')
        return export_date    
    # 2. Scrape 財報狗
    def statementdog_category_scrape():
        '''
        Scrape the info about stock categories from https://statementdog.com/taiex
        '''
        # First layer
        Home = "https://statementdog.com/taiex"
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
        res = requests.get(Home, headers=headers)
        res.encoding = 'utf8'
        soup = bs(res.text, 'html.parser')
        first_layer = soup.find_all('a',{'class':'industry-item'})
        industry_url = [Home + item.get('href')[6:] for item in first_layer] 
        industry_names = [item.h2.string for item in first_layer]
        industry_num = len(industry_names)
        Final_list = []
        # Iterate by industry
        for url_index in range(industry_num) :
            # Second layer
            ## Crawl
            res_layer2 = requests.get(industry_url[url_index], headers=headers)
            res_layer2.encoding = 'utf8'
            soup_2 = bs(res_layer2.text, 'html.parser')
            # Industry Stream 
            stream_name = [item.string for item in soup_2.find_all("div", class_="industry-box-subtitle")] # find stream hierarchy
            stream_num = len(stream_name)
            stream_content = soup_2.find_all("div", class_="industry-stream")
            # Find Ranking stocks 產業漲跌幅排行
            second_layer_rank = soup_2.find("div", class_="industry-ranking-list-body").find_all("ul", class_ = "industry-ranking-item")
            second_layer_rank_stock = []
            for item in second_layer_rank:
                stock_info = item.find("a").string.split(" ")
                sub_industry_name = [item.find("li", class_="industry-ranking-item-info industry-ranking-sub-industry").string]
                second_layer_rank_stock.append(sub_industry_name + stock_info)
            # Iterate by stream
            if stream_num != 0:
                for stream_index in range(stream_num):
                    # Stream -> sub-industry 
                    sub_industry_name = [item.string for item in stream_content[stream_index].find_all("div", class_="industry-stream-sub-industry-name")]
                    sub_industry_num = len(sub_industry_name)
                    sub_industry_content = stream_content[stream_index].find("div", class_ = "industry-stream-list-body").find_all("ul", class_="industry-stream-item")
                    # Iterate by sub-industry
                    # Industry -> Stream -> sub-industry -> stock
                    sub_industry_stock_list = []
                    for sub_industry_index in range(sub_industry_num):
                        stock_list = [item.string.split(" ") for item in sub_industry_content[sub_industry_index].find_all("a", class_="industry-stream-company")]
                        stock = [ [industry_names[url_index]] + [stream_name[stream_index]] + [sub_industry_name[sub_industry_index]] + item for item in stock_list]
                        Final_list += stock
            else:
                stock = [ [industry_names[url_index]] + [None] + item for item in second_layer_rank_stock]
                Final_list += stock
            print(str(url_index+1) + "th industry is finished")
            time.sleep(0.5)
        export_df = pd.DataFrame(Final_list, columns =  ["產業", "產業位階", "子產業", "公司代號", "公司簡稱"])
        export_df['來源'] = '財報狗'
        return export_df[["來源", "產業", "產業位階", "子產業", "公司代號", "公司簡稱"]]
    # 3. Scrape 定錨(skipped)
    # 4. Organize two dfs(skipped)
    # 5. Delete old
    def delete_old_data(source):
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = """   DELETE FROM stock_category
                    WHERE source = %s;
                    """
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**conn_params)
            # create a new cursor
            cur = conn.cursor()
            # execute the SQL statement
            cur.execute(sql, (source,))
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        print(f'stock_category with source {source} is deleted!')
        return None 
    # 6. Insert into database
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
    # 7. Update latest date
    def update_latest_date(date, table):
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = """ UPDATE latest_updated_date
                    SET latest_date = %s
                    WHERE table_name = %s
                    """
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**conn_params)
            # create a new cursor
            cur = conn.cursor()
            # execute the UPDATE  statement
            cur.execute(sql, (date, table))
            # Commit the changes to the database
            conn.commit()
            # Close communication with the PostgreSQL database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
    # Execution
    last_date = get_last_date(target_table)
    today_date = pd.Timestamp.today().strftime('%Y-%m-%d')
    if source == '財報狗':
        dog_df = statementdog_category_scrape()
    delete_old_data(source)
    insert_function(dog_df, target_table)
    update_latest_date(today_date, target_table)
    print(f"Update for {source} on {today_date} is finished")
