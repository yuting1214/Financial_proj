def daily_total_trade_update(target_table, sleep_sec, to_date = None):
    # 1. Get current updated date
    def get_current_updated_date():
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = "SELECT * \
           FROM public.latest_updated_daily_total_trade \
           LIMIT 1;"
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**conn_params)
            # create a new cursor
            cur = conn.cursor()
            # execute the SQL statement
            cur.execute(sql)
            rows = cur.fetchone()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        export_date = pd.Timestamp(rows[0]).strftime('%Y-%m-%d')
        return export_date
    # 2. Create listed month index
    def month_generator_listed(start_date, specified_date = None):
        if specified_date:
            to_date = pd.to_datetime(specified_date)
        else:
            to_date = pd.Timestamp.now()
        return [ month.strftime('%Y%m') for month in pd.period_range(start = pd.to_datetime(start_date), end = to_date, freq='M')]
    # 3. Create OTC month index    
    def month_generator_otc(start_date, specified_date = None):
        if specified_date:
            to_date = pd.to_datetime(specified_date)
        else:
            to_date = pd.Timestamp.now()
        return [ str(date.year - 1911) + '/' + str(date.month) for date in pd.period_range(start = pd.to_datetime(start_date), end = to_date, freq='M')] 
    # 4. Scrape listed by month
    def monthly_index_listed(month):
        # 1. Generate url
        def url_generator(month_str):
            url = f'https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={month_str}01'
            return url
        # 2. Scrape function
        def scrape_unit(url, retry_times = 3):
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            while retry_times >= 0:
                try:
                    res = requests.get(url, headers=headers)
                    if res != None:
                        return res
                except (requests.ConnectionError, requests.ReadTimeout) as error:
                    print(error)
                    print('Retry one more time after 60s', retry_times, 'times left')
                    time.sleep(40)
                retry_times -= 1
        # 3. Parse scraped data
        def parse_return(content):
            default_columns = ["??????", "????????????", "????????????", "????????????", "???????????????????????????", "????????????"]
            content = content.json()
            columns = content['fields']
            assert columns == default_columns
            return_df = pd.DataFrame(content['data'])
            return_df.columns = columns
            return return_df
        # Execution
        url = url_generator(month)
        return_df = scrape_unit(url)
        final_df = parse_return(return_df)
        return final_df
    # 5. Scrape OTC by month
    def monthly_index_otc(month):
        # 1. Generate url
        def url_generator(month_str):
            url = f'https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_index/st41_result.php?l=zh-tw&d={month_str}/01'
            return url
        # 2. Scrape function
        def scrape_unit(url, retry_times = 3):
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            while retry_times >= 0:
                try:
                    res = requests.get(url, headers=headers)
                    if res != None:
                        return res
                except (requests.ConnectionError, requests.ReadTimeout) as error:
                    print(error)
                    print('Retry one more time after 60s', retry_times, 'times left')
                    time.sleep(40)
                retry_times -= 1
        # 3. Parse scraped data
        def parse_return(content):
            col = ['??????', '????????????(??????)', '??????(??????)', '??????', '????????????', '???/???']
            content = content.json()
            return_df = pd.DataFrame(content['aaData'])
            return_df.columns = col
            return return_df
        # Execution
        url = url_generator(month)
        return_df = scrape_unit(url)
        final_df = parse_return(return_df)
        return final_df    
    # 6. Organize scraped df (Prevent duplicate by checking date)
    def organize_df(current_date, listed_df, otc_df):
        # Listed
        listed_df = listed_df.applymap(lambda x: x.replace(',', ''))
        listed_df[listed_df.columns[1:]] = listed_df[listed_df.columns[1:]].astype(float)
        listed_df[['year', 'month', 'day']] = listed_df['??????'].str.split('/', expand=True)
        listed_df[['year', 'month', 'day']] = listed_df[['year', 'month', 'day']].astype(int)
        listed_df['year'] = listed_df['year'] + 1911
        listed_df['Date'] = pd.to_datetime(listed_df[['year', 'month', 'day']]).apply(lambda x: x.strftime('%Y/%m/%d'))
        # OTC
        otc_df = otc_df.applymap(lambda x: x.replace(',', ''))
        otc_df[otc_df.columns[1:]] = otc_df[otc_df.columns[1:]].astype(float)
        otc_df[['year', 'month', 'day']] = otc_df['??????'].str.split('/', expand=True)
        otc_df[['year', 'month', 'day']] = otc_df[['year', 'month', 'day']].astype(int)
        otc_df['year'] = otc_df['year'] + 1911
        otc_df['Date'] = pd.to_datetime(otc_df[['year', 'month', 'day']]).apply(lambda x: x.strftime('%Y/%m/%d'))
        result_df = pd.merge(listed_df, otc_df, how="right", left_on=["Date"],
                             right_on = ["Date"], suffixes = ('_listed', '_otc'))
        target_df = result_df[['Date', '????????????', '????????????', '????????????', '???????????????????????????',
                     '????????????', '????????????(??????)', '??????(??????)', '??????', '????????????', '???/???']].copy()
        target_df.columns = ['Date', 'Taiex_volume', 'Taiex_value', 'Taiex_turnover', 'Taiex_idnex', 'Taiex_spread',
          'TPEx_volume', 'TPEx_value', 'TPEx_turnover', 'TPEx_index', 'TPEx_spread']
        target_df = target_df.replace('nan', 'None')
        target_df[['Taiex_volume','Taiex_value', 'Taiex_turnover', 
                     'TPEx_volume', 'TPEx_value', 'TPEx_turnover']] = target_df[['Taiex_volume','Taiex_value', 'Taiex_turnover',
                                                                                  'TPEx_volume', 'TPEx_value', 'TPEx_turnover']].astype('int64')
        target_df['Date'] = pd.to_datetime(target_df['Date'])
        return_df = target_df[target_df.Date > pd.to_datetime(current_date)].reset_index(drop = True)
        return return_df
    # 7. Insert into database (General Modules)
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
    # 8. Update latest date
    def update_latest_date(date):
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = """ UPDATE latest_updated_daily_total_trade 
                    SET latest_date = %s """
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**conn_params)
            # create a new cursor
            cur = conn.cursor()
            # execute the UPDATE  statement
            cur.execute(sql, (date,))
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
    start_date = get_current_updated_date()
    month_list = [month_generator_listed(start_date, to_date), 
                  month_generator_otc(start_date, to_date)]
    content_len = len(month_list[0])
    assert len(month_list[0]) == len(month_list[1])
    for index in tqdm(range(content_len)):
        listed_df = monthly_index_listed(month_list[0][index])
        otc_df = monthly_index_otc(month_list[1][index])
        inserted_df = organize_df(start_date, listed_df, otc_df)
        insert_function(inserted_df, target_table)
        newest_date = inserted_df.Date.max()
        if pd.notna(newest_date):
            update_latest_date(newest_date)
        content_len -= 1
        if content_len > 0:
            time.sleep(sleep_sec)
