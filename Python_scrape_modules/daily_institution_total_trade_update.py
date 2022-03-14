def daily_institution_total_trade_update(target_table, sleep_sec, to_date = None):
    # 1. Get current updated date
    def get_current_updated_date():
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = "SELECT * \
           FROM public.latest_updated_daily_institution_total_trade \
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
    # 4-1. Scraping function(listed)
    def daily_three_insti_total_buy_listed(scrape_date):
        # Self-defined function
        # 1. Generate url
        def url_generator(date):
            date_str = date.strftime('%Y%m%d')
            url = f'https://www.twse.com.tw/fund/BFI82U?response=json&dayDate={date_str}&type=day'
            return url
        # 2. Scrape function
        def scrape_unit(url, retry_times = 3):
            default_columns = ["單位名稱", "買進金額", "賣出金額", "買賣差額"]
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
                columns = content['fields']
                assert columns == default_columns
                return_df = pd.DataFrame(content['data'])
                return_df.columns = columns
                return return_df
            except:
                return pd.DataFrame()
        # 3. Parse data
        def parse_return(date, content):
            date_str = date.strftime('%Y-%m-%d')
            target_df = content.copy()
            target_df['日期'] = date_str
            tidy_df = pd.DataFrame(target_df.pivot(index = '日期', columns= '單位名稱').to_records())
            tidy_df = tidy_df.applymap(lambda x: x.replace(',', ''))
            tidy_df.iloc[:, 1:] = tidy_df.iloc[:, 1:].astype('int64')
            # Add new columns
            tidy_df["('買進金額', '外資及陸資')"] = tidy_df["('買進金額', '外資及陸資(不含外資自營商)')"] + tidy_df["('買進金額', '外資自營商')"]
            tidy_df["('買進金額', '自營商')"] = tidy_df["('買進金額', '自營商(避險)')"] + tidy_df["('買進金額', '自營商(自行買賣)')"]
            tidy_df["('賣出金額', '外資及陸資')"] = tidy_df["('賣出金額', '外資及陸資(不含外資自營商)')"] + tidy_df["('賣出金額', '外資自營商')"]
            tidy_df["('賣出金額', '自營商')"] = tidy_df["('賣出金額', '自營商(避險)')"] + tidy_df["('賣出金額', '自營商(自行買賣)')"]
            tidy_df["('買賣差額', '外資及陸資')"] = tidy_df["('買進金額', '外資及陸資')"] - tidy_df["('賣出金額', '外資及陸資')"]
            tidy_df["('買賣差額', '自營商')"] = tidy_df["('買進金額', '自營商')"] - tidy_df["('賣出金額', '自營商')"]
            tidy_df = tidy_df[["日期", "('買進金額', '外資及陸資')", "('賣出金額', '外資及陸資')", "('買賣差額', '外資及陸資')", 
                         "('買進金額', '外資及陸資(不含外資自營商)')", "('賣出金額', '外資及陸資(不含外資自營商)')",
                         "('買賣差額', '外資及陸資(不含外資自營商)')", "('買進金額', '外資自營商')",
                         "('賣出金額', '外資自營商')", "('買賣差額', '外資自營商')", "('買進金額', '自營商')",
                         "('賣出金額', '自營商')", "('買賣差額', '自營商')","('買進金額', '自營商(自行買賣)')", "('賣出金額', '自營商(自行買賣)')",
                         "('買賣差額', '自營商(自行買賣)')", "('買進金額', '自營商(避險)')", "('賣出金額', '自營商(避險)')",
                         "('買賣差額', '自營商(避險)')", "('買進金額', '投信')", "('賣出金額', '投信')", "('買賣差額', '投信')"]].copy()
            database_col = ['Date', 'Foreigner_total_buy', 'Foreigner_total_sell', 'Foreigner_total_balance',
             'Foreign_Invester_total_buy', 'Foreign_Invester_total_sell', 'Foreign_Invester_total_balance',
             'Foreign_Dealer_Self_total_buy', 'Foreign_Dealer_Self_total_sell', 'Foreign_Dealer_Self_total_balance',
             'Dealer_total_buy', 'Dealer_total_sell', 'Dealer_total_balance', 'Dealer_self_total_buy',
             'Dealer_self_total_sell', 'Dealer_self_total_balance', 'Dealer_Hedging_total_buy', 'Dealer_Hedging_total_sell',
             'Dealer_Hedging_total_balance', 'Investment_Trust_total_buy', 'Investment_Trust_total_sell', 'Investment_Trust_total_balance'
            ]
            database_col_listed = ['Taiex_' + i for i in database_col]
            tidy_df.columns = database_col_listed
            return tidy_df
        # Execution
        url = url_generator(scrape_date)
        content = scrape_unit(url)
        return_df = parse_return(scrape_date, content)
        return return_df
    # 4-2. Scraping function(otc)
    def daily_three_insti_total_buy_otc(scrape_date):
        # Self-defined function
        # 1. Generate url
        def url_generator(date):
            year = str(date.year - 1911)
            month_day_str = date.strftime('/%m/%d')
            url = f'https://www.tpex.org.tw/web/stock/3insti/3insti_summary/3itrdsum_result.php?l=zh-tw&t=D&p=0&d={year}{month_day_str}'
            return url
        # 2. Scrape function
        def scrape_unit(url, retry_times = 3):
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            while retry_times >= 0:
                try:
                    res = requests.get(url, headers=headers)
                    return res.json()
                    break
                except (requests.ConnectionError, requests.ReadTimeout) as error:
                    print(error)
                    print('Retry one more time after 60s', retry_times, 'times left')
                    time.sleep(40)
                    retry_times -= 1
        # 3. Parse data
        def parse_return(date, content):
            column = ['單位名稱', '買進金額(元)', '賣出金額(元)', '買賣超(元)']
            try:

                target_df = pd.DataFrame(content['aaData'])
                target_df.columns = column
                date_str = date.strftime('%Y-%m-%d')
                target_df['日期'] = date_str
                tidy_df = pd.DataFrame(target_df.pivot(index = '日期',
                                        columns= '單位名稱').to_records())
                tidy_df = tidy_df.astype(str)
                tidy_df = tidy_df.applymap(lambda x: x.replace(',', ''))
                tidy_df.iloc[:, 1:] = tidy_df.iloc[:, 1:].astype('int64')
                col = ['日期'] + [item.replace("(元)", '') for item in tidy_df.columns[1:]]
                col = ['日期'] + [item.replace("\\u3000", '') for item in col[1:]]
                col = ['日期'] + [item.replace("外資及陸資(不含自營商)", "外資及陸資(不含外資自營商)") for item in col[1:]]
                tidy_df.columns = col
                # Add new columns
                tidy_df["('買進金額', '外資及陸資')"] = tidy_df["('買進金額', '外資及陸資(不含外資自營商)')"] + tidy_df["('買進金額', '外資自營商')"]
                tidy_df["('買進金額', '自營商')"] = tidy_df["('買進金額', '自營商(避險)')"] + tidy_df["('買進金額', '自營商(自行買賣)')"]
                tidy_df["('賣出金額', '外資及陸資')"] = tidy_df["('賣出金額', '外資及陸資(不含外資自營商)')"] + tidy_df["('賣出金額', '外資自營商')"]
                tidy_df["('賣出金額', '自營商')"] = tidy_df["('賣出金額', '自營商(避險)')"] + tidy_df["('賣出金額', '自營商(自行買賣)')"]
                tidy_df["('買賣差額', '外資及陸資')"] = tidy_df["('買進金額', '外資及陸資')"] - tidy_df["('賣出金額', '外資及陸資')"]
                tidy_df["('買賣差額', '外資及陸資(不含外資自營商)')"] = tidy_df["('買進金額', '外資及陸資(不含外資自營商)')"] - tidy_df["('賣出金額', '外資及陸資(不含外資自營商)')"]
                tidy_df["('買賣差額', '外資自營商')"] = tidy_df["('買進金額', '外資自營商')"] - tidy_df["('賣出金額', '外資自營商')"]
                tidy_df["('買賣差額', '自營商')"] = tidy_df["('買進金額', '自營商')"] - tidy_df["('賣出金額', '自營商')"]
                tidy_df["('買賣差額', '自營商(避險)')"] = tidy_df["('買進金額', '自營商(避險)')"] - tidy_df["('賣出金額', '自營商(避險)')"]
                tidy_df["('買賣差額', '自營商(自行買賣)')"] = tidy_df["('買進金額', '自營商(自行買賣)')"] - tidy_df["('賣出金額', '自營商(自行買賣)')"]
                tidy_df["('買賣差額', '投信')"] = tidy_df["('買進金額', '投信')"] - tidy_df["('賣出金額', '投信')"]
                tidy_df = tidy_df[["日期", "('買進金額', '外資及陸資')", "('賣出金額', '外資及陸資')", "('買賣差額', '外資及陸資')", 
                             "('買進金額', '外資及陸資(不含外資自營商)')", "('賣出金額', '外資及陸資(不含外資自營商)')",
                             "('買賣差額', '外資及陸資(不含外資自營商)')", "('買進金額', '外資自營商')",
                             "('賣出金額', '外資自營商')", "('買賣差額', '外資自營商')", "('買進金額', '自營商')",
                             "('賣出金額', '自營商')", "('買賣差額', '自營商')","('買進金額', '自營商(自行買賣)')", "('賣出金額', '自營商(自行買賣)')",
                             "('買賣差額', '自營商(自行買賣)')", "('買進金額', '自營商(避險)')", "('賣出金額', '自營商(避險)')",
                             "('買賣差額', '自營商(避險)')", "('買進金額', '投信')", "('賣出金額', '投信')", "('買賣差額', '投信')"]]
                database_col = ['Date', 'Foreigner_total_buy', 'Foreigner_total_sell', 'Foreigner_total_balance',
                 'Foreign_Invester_total_buy', 'Foreign_Invester_total_sell', 'Foreign_Invester_total_balance',
                 'Foreign_Dealer_Self_total_buy', 'Foreign_Dealer_Self_total_sell', 'Foreign_Dealer_Self_total_balance',
                 'Dealer_total_buy', 'Dealer_total_sell', 'Dealer_total_balance', 'Dealer_self_total_buy',
                 'Dealer_self_total_sell', 'Dealer_self_total_balance', 'Dealer_Hedging_total_buy', 'Dealer_Hedging_total_sell',
                 'Dealer_Hedging_total_balance', 'Investment_Trust_total_buy', 'Investment_Trust_total_sell', 'Investment_Trust_total_balance'
                ]
                database_col_otc = ['TPEx_' + i for i in database_col]
                tidy_df.columns = database_col_otc
                return tidy_df
            except:
                return pd.DataFrame()
        # Execution
        url = url_generator(scrape_date)
        content = scrape_unit(url)
        return_df = parse_return(scrape_date, content)
        return return_df
    # 5. Organized data
    def organized_scrape_data(listed_df, otc_df):
        total_df = pd.merge(listed_df, otc_df, how = 'left', left_on = 'Taiex_Date',
                            right_on = 'TPEx_Date')
        total_df.fillna(0, inplace = True)
        total_df.rename(columns = {'Taiex_Date':'date'}, inplace = True)
        export_df = total_df[total_df.columns[total_df.columns != 'TPEx_Date']].copy()
        export_df.iloc[:,1:] = export_df.iloc[:,1:].astype('int64')
        return export_df
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
    def update_latest_date(date):
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = """ UPDATE latest_updated_daily_institution_total_trade
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
    # 8. Execute update
    current_date = get_current_updated_date()
    closed_date_list = get_closed_date()
    date_list = create_scrape_date(current_date, closed_date_list, specified_date = None)
    start_date = date_list[0].strftime('%Y-%m-%d')
    end_date = date_list[-1].strftime('%Y-%m-%d')
    print(f'Scrape starting from: {start_date} to {end_date}')
    execution_time = len(date_list)
    for date in tqdm(date_list):
        listed_df = daily_three_insti_total_buy_listed(date)
        otc_df = daily_three_insti_total_buy_otc(date)
        target_df = organized_scrape_data(listed_df, otc_df)
        final_target_df = target_df.where(target_df.notnull(), 'None')
        insert_function(final_target_df, target_table)
        update_latest_date(date)
        print(f"{date} data is finished")
        execution_time -= 1
        if execution_time > 0:
            time.sleep(sleep_sec)
