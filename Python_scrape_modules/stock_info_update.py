def stock_info_update(target_table):
    # 1. Scrape List
    def List_stock_info_scraper(retry_times = 3):
        url = 'http://mopsfin.twse.com.tw/opendata/t187ap03_L.csv'
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
        while retry_times >= 0:
            try:
                res = requests.get(url, headers=headers)
                res.encoding = "utf-8"
                return_df = pd.read_csv(StringIO(res.text))
                return return_df
            except (requests.ConnectionError, requests.ReadTimeout) as error:
                print(error)
                print('Retry one more time after 40s', retry_times, 'times left')
                time.sleep(40)
                retry_times -= 1   
    # 2. Scrape OTC
    def OTC_stock_info_scraper(retry_times = 3):
        url = 'http://mopsfin.twse.com.tw/opendata/t187ap03_O.csv'
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
        while retry_times >= 0:
            try:
                res = requests.get(url, headers=headers)
                res.encoding = "utf-8"
                return_df = pd.read_csv(StringIO(res.text))
                return return_df
            except (requests.ConnectionError, requests.ReadTimeout) as error:
                print(error)
                print('Retry one more time after 40s', retry_times, 'times left')
                time.sleep(40)
                retry_times -= 1 
    # 3. Organized data
    def organized_scrape_data(listed_df, otc_df):
        # Pre-defined
        industry_name = ['水泥工業', '食品工業', '塑膠工業', '紡織纖維', '電機機械', '電器電纜', '化學工業', '生技醫療業',
         '玻璃陶瓷', '造紙工業', '鋼鐵工業', '橡膠工業', '汽車工業', '半導體業', '電腦及週邊設備業',
         '光電業', '通信網路業', '電子零組件業', '電子通路業', '資訊服務業', '其他電子業', '建材營造',
         '航運業', '觀光事業', '金融保險', '貿易百貨', '油電燃氣業', '綜合', '其他', '文化創意業', 
         '農業科技業', '電子商務', '管理股票', '台灣存託憑證-DR']
        industry_code = ['1', '2', '3', '4', '5', '6', '21', '22', '8', '9', '10', '11', '12', '24', '25', '26', '27',
        '28', '29', '30', '31', '14', '15', '16', '17', '18', '23', '19', '20', '32', '33', '34', '80', '91']
        industry_dict = dict(zip(industry_code, industry_name))
        listed_df['上市櫃'] = '上市'
        otc_df['上市櫃'] = '上櫃'
        merged_df = pd.concat([listed_df, otc_df], ignore_index=True)
        target_df = merged_df[['公司代號', '公司簡稱', '產業別', '上市櫃']].copy()
        target_df['公司代號'] = target_df['公司代號'].astype(str)
        target_df['產業別'] = target_df['產業別'].astype(str)
        target_df['產業名稱'] = target_df['產業別'].map(industry_dict)
        return target_df[['公司代號', '公司簡稱', '產業別', '產業名稱', '上市櫃']]
    # 4. Fetch existing stock_info
    def get_current_stock_id():
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = "SELECT * \
                FROM public.stock_info;"
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**conn_params)
            # create a new cursor
            cur = conn.cursor()
            # execute the SQL statement
            cur.execute(sql)
            rows = cur.fetchall()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        export_stock_list = pd.DataFrame(rows, columns=['公司代號', '公司簡稱', '產業別', '產業名稱', '上市櫃', '交易狀態'])
        return export_stock_list 
    # 5. Update data in db
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
    # 7. Retreive last updated date
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
        date = rows[1]
        return date    
    # 8. Update latest date
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
            cur.execute(sql, (date,table))
            # Commit the changes to the database
            conn.commit()
            # Close communication with the PostgreSQL database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
    
    # Main
    ## (1) Fetch data
    list_df = List_stock_info_scraper()
    otc_df = OTC_stock_info_scraper()
    scraped_df = organized_scrape_data(list_df, otc_df)
    current_df = get_current_stock_id()
    ## (2) Update data
    delisted_stocks = list(set(current_df['公司代號']) - set(scraped_df['公司代號']))
    newlisted_stocks = list(set(scraped_df['公司代號']) - set(current_df['公司代號']))
    newlisted_df = scraped_df[scraped_df['公司代號'].isin(newlisted_stocks)].copy()
    newlisted_df['交易狀態'] = '上市'
    ### Update delisted
    update_delisted_stock(delisted_stocks)
    ### Insert new listed
    insert_function(newlisted_df, target_table)
    ### Retreive last updated date
    last_date = get_last_date(target_table).strftime('%Y/%m/%d')
    ### Update updated date
    today_date = pd.Timestamp.today().strftime('%Y/%m/%d')
    update_latest_date(today_date, target_table)
    print(f'From {last_date} to {today_date}:')
    print(f'Delisted Stock: {delisted_stocks}')
    print()
    print(f'New listed Stock: {newlisted_stocks}')
