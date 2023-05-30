def daily_three_insti_buying_update(target_table, sleep_sec, to_date = None):
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
    def scrape_listed_three_insti(scrape_date):
        # 1. url generator
        def url_generator(date):
            date_str = date.strftime('%Y%m%d')
            url = f'https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALLBUT0999'
            return url
        # 2. Scrape function
        def scrape_unit(url, retry_times = 3):
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            while retry_times >= 0:
                try:
                    res = requests.get(url, headers=headers)
                    if res != None:
                        content = res.json()
                        return_df = pd.DataFrame(content['data'])
                        col_num = return_df.shape[1]
                        assert col_num == 19
                        return return_df
                except (requests.ConnectionError, requests.ReadTimeout) as error:
                    print(error)
                    print('Retry one more time after 60s', retry_times, 'times left')
                    time.sleep(30)
                retry_times -= 1
        # 3. Parse scraped data
        def parse_return(date, content):
            content = content.astype(str).applymap(lambda x: x.replace(',', ''))
            content['Date'] = date
            col_name = [
                "Stock_id", "證券名稱", "Foreign_Investor_buy", "Foreign_Investor_sell", "Foreign_Investor_balance",
                "Foreign_Dealer_Self_buy", "Foreign_Dealer_Self_sell", "Foreign_Dealer_Self_balance", "Investment_Trust_buy",
                "Investment_Trust_sell", "Investment_Trust_balance", "Dealer_balance", "Dealer_self_buy", "Dealer_self_sell",
                "Dealer_self_balance", "Dealer_Hedging_buy", "Dealer_Hedging_sell", "Dealer_Hedging_balance", "三大法人買賣超股數",
                "Date"]
            content.columns = col_name
            content[content.columns[2:-1]] = content[content.columns[2:-1]].astype(int)
            # Create columns
            # Foreign
            content['Foreigner_buy'] = content['Foreign_Dealer_Self_buy'] + \
                content['Foreign_Investor_buy']
            content['Foreigner_sell'] = content['Foreign_Dealer_Self_sell'] + \
                content['Foreign_Investor_sell']
            content['Foreigner_balance'] = content['Foreigner_buy'] - content['Foreigner_sell']
            # Dealer
            content['Dealer_buy'] = content['Dealer_self_buy'] + content['Dealer_Hedging_buy']
            content['Dealer_sell'] = content['Dealer_self_sell'] + content['Dealer_Hedging_sell']
            content['Dealer_balance_test'] = content['Dealer_buy'] - content['Dealer_sell']
            assert (content['Dealer_balance'] == content['Dealer_balance_test']).all() 
            export_df = content[['Date', 'Stock_id', 'Foreigner_buy', 'Foreigner_sell', 'Foreigner_balance',
                          'Foreign_Investor_buy', 'Foreign_Investor_sell', 'Foreign_Investor_balance',
                          'Foreign_Dealer_Self_buy', 'Foreign_Dealer_Self_sell', 'Foreign_Dealer_Self_balance',
                          'Dealer_buy', 'Dealer_sell', 'Dealer_balance', 'Dealer_self_buy', 'Dealer_self_sell',
                          'Dealer_self_balance', 'Dealer_Hedging_buy', 'Dealer_Hedging_sell', 'Dealer_Hedging_balance',
                          'Investment_Trust_buy', 'Investment_Trust_sell', 'Investment_Trust_balance']].copy()
            assert not (export_df.filter(like = 'sell') < 0).any().any()
            assert not (export_df.filter(like = 'buy') < 0).any().any()
            return export_df
        # Execution
        url = url_generator(scrape_date)
        content = scrape_unit(url)
        assert content is not None
        return_df = parse_return(scrape_date, content)
        scrape_date_str = scrape_date.strftime('%Y/%m/%d')
        print(f'The listed data on {scrape_date_str} successfully scraped!')
        return return_df 
    # 6. Scrape OTC
    def scrape_otc_three_insti(scrape_date):
        # 1. url generator
        def url_generator(date):
            year_ad = date.year
            year_rc = year_ad - 1911
            month_day = date.strftime('%m/%d')
            url = 'https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=EW&t=D&d=' + \
                    f'{year_rc}/{month_day}'
            return url
        # 2. Scrape function
        def scrape_unit(url, retry_times = 3):
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            while retry_times >= 0:
                try:
                    res = requests.get(url, headers=headers)
                    content = res.json()
                    parsed_date = content['reportDate']
                    return_df = pd.DataFrame(content['aaData'])
                    assert return_df.shape[1] == 25
                    return return_df
                except (requests.ConnectionError, requests.ReadTimeout) as error:
                    print(error)
                    print('Retry one more time after 60s', retry_times, 'times left')
                    time.sleep(40)
                retry_times -= 1
            return pd.DataFrame()
        # 3. Parse scraped data
        def parse_return(date, content):
            content = content.astype(str).applymap(lambda x: x.replace(',', ''))
            content['Date'] = date
            col_name = ["Stock_id", "證券名稱", "Foreign_Investor_buy", "Foreign_Investor_sell", "Foreign_Investor_balance",
                "Foreign_Dealer_Self_buy", "Foreign_Dealer_Self_sell", "Foreign_Dealer_Self_balance", "Foreigner_buy",
                "Foreigner_sell", "Foreigner_balance", "Investment_Trust_buy", "Investment_Trust_sell", "Investment_Trust_balance",
                "Dealer_self_buy", "Dealer_self_sell", "Dealer_self_balance", "Dealer_Hedging_buy", "Dealer_Hedging_sell",
                'Dealer_Hedging_balance', 'Dealer_buy', 'Dealer_sell', 'Dealer_balance', "三大法人買賣超股數", "股票類別", 
                'Date']
            content.columns = col_name
            content[content.columns[2:-2]] = content[content.columns[2:-2]].astype(int)
            # Create columns
            # Foreign
            content['Foreigner_buy'] = content['Foreign_Dealer_Self_buy'] + \
                content['Foreign_Investor_buy']
            content['Foreigner_sell'] = content['Foreign_Dealer_Self_sell'] + \
                content['Foreign_Investor_sell']
            content['Foreigner_balance'] = content['Foreigner_buy'] - content['Foreigner_sell']
            # Dealer
            content['Dealer_buy'] = content['Dealer_self_buy'] + content['Dealer_Hedging_buy']
            content['Dealer_sell'] = content['Dealer_self_sell'] + content['Dealer_Hedging_sell']
            content['Dealer_balance_test'] = content['Dealer_buy'] - content['Dealer_sell']
            export_df = content[['Date', 'Stock_id', 'Foreigner_buy', 'Foreigner_sell', 'Foreigner_balance',
                          'Foreign_Investor_buy', 'Foreign_Investor_sell', 'Foreign_Investor_balance',
                          'Foreign_Dealer_Self_buy', 'Foreign_Dealer_Self_sell', 'Foreign_Dealer_Self_balance',
                          'Dealer_buy', 'Dealer_sell', 'Dealer_balance', 'Dealer_self_buy', 'Dealer_self_sell',
                          'Dealer_self_balance', 'Dealer_Hedging_buy', 'Dealer_Hedging_sell', 'Dealer_Hedging_balance',
                          'Investment_Trust_buy', 'Investment_Trust_sell', 'Investment_Trust_balance']].copy()
            assert not (export_df.filter(like = 'sell') < 0).any().any()
            assert not (export_df.filter(like = 'buy') < 0).any().any()
            return export_df
        # Execution
        url = url_generator(scrape_date)
        content = scrape_unit(url)
        assert content is not None
        return_df = parse_return(scrape_date, content)
        scrape_date_str = scrape_date.strftime('%Y/%m/%d')
        print(f'The otc data on {scrape_date_str} successfully scraped!')
        return return_df
    # 7. Organized data
    def organized_scraped_data(listed_df, otc_df, stock_id_list):
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
    # 10. Execute update
    current_date = get_last_date(target_table)
    closed_date_list = get_closed_date()
    date_list = create_scrape_date(current_date, closed_date_list, specified_date = None)
    start_date = date_list[0].strftime('%Y-%m-%d')
    end_date = date_list[-1].strftime('%Y-%m-%d')
    print(f'Scrape starting from: {start_date} to {end_date}')
    stock_id_list = get_current_stock_id()
    execution_time = len(date_list)
    for date in tqdm(date_list):
        listed_df = scrape_listed_three_insti(date)
        otc_df = scrape_otc_three_insti(date)
        target_df = organized_scraped_data(listed_df, otc_df, stock_id_list)
        final_target_df = target_df.where(target_df.notnull(), 'None')
        insert_function(final_target_df, target_table)
        update_latest_date(date, target_table)
        print(f"{date} data is finished")
        execution_time -= 1
        if execution_time > 0:
            time.sleep(sleep_sec)
