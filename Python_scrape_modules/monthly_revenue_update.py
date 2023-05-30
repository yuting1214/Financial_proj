def monthly_revenue_update(target_table, sleep_sec, to_date = None):
    # 1. Get current updated date
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
    # 2. Create listed month index
    def month_generator_listed(start_date, specified_date = None):
        if specified_date:
            to_date = pd.to_datetime(specified_date)
        else:
            to_date = pd.Timestamp.now()
        # Avaiable last month data in each 10th of month 
        if start_date.day > 10:
            month_list = [month.strftime('%Y_%m') for month in pd.period_range(start = pd.to_datetime(start_date), end = to_date, freq='M')]
            if to_date.day > 10:
                if len(month_list) == 1:
                    return []
                else:
                    return month_list[:-1]
            else:
                if len(month_list) == 2:
                    return []
                else:
                    return month_list[:-2]
        else:
            start_date = start_date - pd.offsets.MonthBegin(1, normalize=True) - pd.Timedelta('1d')
            month_list = [month.strftime('%Y_%m') for month in pd.period_range(start = pd.to_datetime(start_date), end = to_date, freq='M')]
            if to_date.day > 10:
                return month_list[:-1]
            else:
                return month_list[:-2]
    # 3. Scraping function
    def scrape_monthly_revenue(stock_type, month_str):
        # 1. Generate url
        def payload_generator(stock_type, month_str):
            stock_type_map = {'listed': 'sii', 'otc':'otc'}
            stock_type = stock_type_map[stock_type]
            year, month = month_str.split('_')
            payload = f'step=9&functionName=show_file2&filePath=%2Ft21%2F{stock_type}%2F&fileName=t21sc03_{int(year)-1911}_{int(month)}.csv'
            return payload
        # 2. Scrape function
        def scrape_unit(payload, retry_times = 3):
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            while retry_times >= 0:
                try:
                    res = requests.post('https://mops.twse.com.tw/server-java/FileDownLoad',
                                        data=payload)
                    if res != None:
                        return res
                except (requests.ConnectionError, requests.ReadTimeout) as error:
                    print(error)
                    print('Retry one more time after 40s', retry_times, 'times left')
                    time.sleep(40)
                retry_times -= 1
        # 3. Parse scraped data
        def parse_return(content):
            content.encoding = 'utf-8'
            raw_df = pd.read_csv(StringIO(content.text))
            target_df = raw_df[['資料年月', '公司代號', '營業收入-當月營收', '備註']].copy()
            target_df[['year', 'month']]= target_df['資料年月'].str.split('/', expand = True)
            target_df['year'] = pd.to_numeric(target_df['year']) + 1911
            target_df['month'] = pd.to_numeric(target_df['month'])
            target_df['營業收入-當月營收'] = target_df['營業收入-當月營收'] * 1000
            return_df = target_df[['year', 'month','公司代號', '營業收入-當月營收', '備註']].copy()
            return_df.columns = ['year', 'month', 'stock_id', 'monthly_revenue', 'note']
            return return_df
        # Execution
        payload = payload_generator(stock_type, month_str)
        content = scrape_unit(payload)
        return_df = parse_return(content)
        return return_df
    # 4. Organized data
    def organized_scrape_data(listed_df, otc_df):
        total_df = pd.concat([listed_df, otc_df], ignore_index=True)
        total_df.sort_values(['year', 'month'], inplace = True, ignore_index=True)
        return total_df
    # 5. Insert into database
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
        df.to_csv(buffer, index = False, header=False, sep = '^')
        buffer.seek(0)

        cursor = conn.cursor()
        try:
            cursor.copy_from(buffer, table, sep="^", null = 'None')
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        cursor.close()
        conn.close()
    # 6. Latest date correction
    def correct_date(month_str):
        last_date_current_month = pd.to_datetime('/'.join(month_str.split('_'))) + pd.offsets.MonthEnd(1, normalize = True)
        return last_date_current_month + pd.Timedelta('11d')
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
    # 8. Execute update
    current_date = pd.to_datetime(get_last_date(target_table))
    month_list = month_generator_listed(current_date)
    if len(month_list) > 0:
        start_date = month_list[0]
        end_date = month_list[-1]
        print(f'Scrape starting from: {start_date} to {end_date}')
    execution_time = len(month_list)
    for month in tqdm(month_list):
        listed_df = scrape_monthly_revenue('listed', month)
        otc_df = scrape_monthly_revenue('otc', month)          
        target_df = organized_scrape_data(listed_df, otc_df)
        insert_function(target_df, target_table)
        update_latest_date(correct_date(month), target_table)
        print(f"{month} data is finished")
        execution_time -= 1
        if execution_time > 0:
            time.sleep(sleep_sec)
