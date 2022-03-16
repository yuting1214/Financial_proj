def daily_stock_price_update(target_table, sleep_sec, to_date = None):
    # 1. Get current updated date
    def get_current_updated_date():
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = "SELECT * \
           FROM public.latest_updated_daily_stock_price \
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
    # 2. Get closed date
    def get_closed_date():
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = "SELECT date \
           FROM public.closed_date ;"
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
        export_date_list = pd.to_datetime(pd.DataFrame(rows)[0])
        return export_date_list
    # 3. Create scraped date
    def create_scrape_date(current_date, closed_date_list, specified_date = None):
        if specified_date:
            to_date = pd.to_datetime(specified_date)
        else:
            to_date = pd.Timestamp.now()
        date_range = pd.date_range(pd.to_datetime(current_date) + pd.Timedelta('1d'), to_date, freq = 'D').to_series()
        # Monday=0, Sunday=6
        open_date = list(date_range.index[(~ date_range.dt.dayofweek.isin([5,6])) & (~date_range.index.isin(closed_date_list))])
        return open_date
    # 4. Get current target stock id
    def get_current_stock_id():
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = "SELECT stock_id \
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
        export_stock_list = pd.DataFrame(rows)[0].tolist()
        return export_stock_list
    # 5. Scrape list
    def daily_scrape_listed(scrape_date):
        # Self-defined function
        # 1. Generate url
        def url_generator(date):
            date_str = date.strftime('%Y%m%d')
            url = f'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALLBUT0999'
            return url
        # 2. Scrape function
        def scrape_unit(url, retry_times = 3):
            default_columns = str(["證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價",
                                   "收盤價", "漲跌(+/-)", "漲跌價差", "最後揭示買價", "最後揭示買量", "最後揭示賣價", "最後揭示賣量",
                                   "本益比"])
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            while retry_times >= 0:
                try:
                    res = requests.get(url, headers=headers)
                    break
                except (requests.ConnectionError, requests.ReadTimeout) as error:
                    print(error)
                    print('Retry one more time after 40s', retry_times, 'times left')
                    time.sleep(40)
                    retry_times -= 1
            try:
                content = res.json()
                columns = str(content['fields9'])
                assert columns == default_columns
                return_df = pd.DataFrame(content['data9'])
                return return_df
            except:
                return None
        # 3. Parse data
        def parse_return(date, content):
            date_str = date.strftime('%Y-%m-%d')
            total_df = content.copy()
            total_df.columns = ["證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額", "開盤價",
                               "最高價", "最低價", "收盤價", "漲跌(+/-)", "漲跌價差", "最後揭示買價",
                               "最後揭示買量", "最後揭示賣價", "最後揭示賣量","本益比"]
            total_df['日期'] = date_str
            total_df['漲跌(+/-)'] = total_df['漲跌(+/-)'].str.extract(r"\>(\W+)\<")
            target_df = total_df[['日期', '證券代號', '成交股數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差', '成交筆數']].copy()
            for column in target_df.columns:
                if column in ['成交股數', '成交金額', '成交筆數']:
                    target_df[column] = target_df[column].str.replace(',', '').astype('int64')
                elif column not in ['日期', '證券代號']:
                    target_df[column] = target_df[column].replace({'--':None}).str.replace(',', '').astype('float')
                else:
                    pass
            target_df['漲跌價差'] = np.where(total_df['漲跌(+/-)'] == '-', -target_df['漲跌價差'], target_df['漲跌價差'])
            target_df.columns = ['Date', 'Stock_id', 'Volume', 'Value', 'Open',
                                 'Max', 'Min', 'Close','Spread', 'Turnover']
            return target_df
        # Execution
        url = url_generator(scrape_date)
        content = scrape_unit(url)
        assert content is not None
        return_df = parse_return(scrape_date, content)
        scrape_date_str = scrape_date.strftime('%Y/%m/%d')
        print(f'The listed data on {scrape_date_str} successfully scraped!')
        return return_df
    # 6. Scrape OTC
    def daily_scrape_otc(scrape_date):
        # Self-defined function
        # 1. Generate url
        def url_generator(date):
            date_str = str(date.year - 1911) + '/' + date.strftime('%m/%d')
            url = f'https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={date_str}&se=EW'
            return url
        # 2. Scrape function
        def scrape_unit(url, retry_times = 3):
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            while retry_times >= 0:
                try:
                    res = requests.get(url, headers=headers)
                    break
                except (requests.ConnectionError, requests.ReadTimeout) as error:
                    print(error)
                    print('Retry one more time after 60s', retry_times, 'times left')
                    time.sleep(40)
                    retry_times -= 1
            try:
                content = res.json()
                return_df = pd.DataFrame(content['aaData'])
                return return_df
            except:
                return None
        # 3. Parse data
        def parse_return(date, content):
            target_df = content.iloc[:, [0, 7, 8, 4, 5, 6, 2, 3, 9]].copy()
            target_df.insert(0, 'Date', date)
            target_df.columns = ['Date', 'Stock_id', 'Volume', 'Value', 'Open', 'Max', 'Min', 'Close',
                        'Spread', 'Turnover']
            for column in target_df.columns:
                if column in ['Volume', 'Value', 'Turnover']:
                    target_df[column] = target_df[column].str.replace(',', '').astype('int64')
                elif column in ['Open', 'Max', 'Min', 'Close']:
                    target_df[column] = target_df[column].replace({'----':None}).str.replace(',', '').astype('float')
                elif column in ['Date', 'Stock_id']:
                    pass
                else:
                    target_df.loc[~target_df.Spread.str.contains(r"\d"), 'Spread'] = None
                    target_df['Spread'] = target_df['Spread'].astype('float')
            return target_df
        # Execution
        url = url_generator(scrape_date)
        content = scrape_unit(url)
        assert content is not None
        return_df = parse_return(scrape_date, content)
        scrape_date_str = scrape_date.strftime('%Y/%m/%d')
        print(f'The otc data on {scrape_date_str} successfully scraped!')
        return return_df
    # 7. Organized data
    def organized_scrape_data(listed_df, otc_df, stock_id_list):
        total_df = pd.concat([listed_df, otc_df], ignore_index=True)
        export_df = total_df[total_df.Stock_id.isin(stock_id_list)].copy().reset_index(drop = True)
        return export_df
    # 8. Insert into database
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
    # 9. Update latest date
    def update_latest_date(date):
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = """ UPDATE latest_updated_daily_stock_price
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
    # 10. Execute update
    current_date = get_current_updated_date()
    closed_date_list = get_closed_date()
    date_list = create_scrape_date(current_date, closed_date_list, specified_date = None)
    start_date = date_list[0].strftime('%Y-%m-%d')
    end_date = date_list[-1].strftime('%Y-%m-%d')
    print(f'Scrape starting from: {start_date} to {end_date}')
    stock_id_list = get_current_stock_id()
    execution_time = len(date_list)
    for date in tqdm(date_list):
        listed_df = daily_scrape_listed(date)
        otc_df = daily_scrape_otc(date)
        target_df = organized_scrape_data(listed_df, otc_df, stock_id_list)
        final_target_df = target_df.where(target_df.notnull(), 'None')
        insert_function(final_target_df, target_table)
        update_latest_date(date)
        print(f"{date} data is finished")
        execution_time -= 1
        if execution_time > 0:
            time.sleep(sleep_sec)
