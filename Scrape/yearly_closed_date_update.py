def yearly_closed_date_update(target_table):
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
    # 2. Scrape function
    def scrape_closed_date(specified_year):
        # 1. Url generator
        def url_generator(year):
            url = 'https://www.twse.com.tw/holidaySchedule/holidaySchedule?response=csv&queryYear={}'.format(year-1911)
            return url
        # 2. Scrape function
        def scrape_unit(url, retry_times = 3):
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            while retry_times >= 0:
                try:
                    res = requests.get(url, headers=headers)
                except (requests.ConnectionError, requests.ReadTimeout) as error:
                    print(error)
                    print('Retry one more time after 60s', retry_times, 'times left')
                    time.sleep(40)
                retry_times -= 1
            try:
                return_df = pd.read_csv(StringIO(res.text), header = 1)
                return return_df
            except:
                return pd.DataFrame()
        # 3. Organized function
        def tidy_df(content, year):
            old_treatment = ' 日期' in content.columns
            if old_treatment:
                export_df = content.copy()
                export_df.dropna(axis = 0, how = 'any', inplace = True)
                export_df['date_list'] = export_df[' 日期'].str.split('日').apply(lambda x: x[:-1])
                export_df = export_df.explode('date_list')
                export_df['month_day'] = export_df['date_list'].str.split('月').apply(lambda x: 
                                                                                     '{}/{}'.format(x[0], x[1]))
                export_df.reset_index(inplace=True, drop = True)
                check_wrong_index = export_df['month_day'].apply(len)
                check_wrong_split = check_wrong_index.max() > 5
                if check_wrong_split:
                    remove_index = check_wrong_index.idxmax()
                    export_df = export_df.iloc[:remove_index,]
                export_df['year'] = str(year)
                export_df['Date_str'] = export_df['year'] + '/' + export_df['month_day']
                export_df['Date'] = pd.to_datetime(export_df['Date_str'])
                export_df = export_df[['Date', 'year', 'month_day',  '名稱']]
            else:
                export_df = content[['名稱', '日期']].copy()
                export_df['year'] = str(year)
                export_df['Date_str'] = export_df['year'] + '/' + export_df['日期'].str.replace('月','/', regex=True).str.replace('日', '', regex=True)
                export_df['Date'] = pd.to_datetime(export_df['Date_str'])
                export_df = export_df[['Date', 'year', '日期',  '名稱']]
                export_df.columns = ['Date', 'year', 'month_day',  '名稱']
            return export_df
        # Execution 
        url = url_generator(specified_year)
        content = scrape_unit(url)
        export_df = tidy_df(content, specified_year)
        print('{} data is completed!'.format(specified_year))
        return export_df
    # 3. Tidy the scraped data
    def organized_scraped_data(closed_df):
        final_df = closed_df[['Date', 'year', '名稱']]
        final_df.columns = ['Date', 'Year', 'Holiday']
        # Filter out some date
        final_df = final_df[~final_df['Holiday'].str.contains('交易日', regex=False)].reset_index(drop = True)
        return final_df
    # 4. Insert into database
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
    # 5. Update latest date
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
    current_date = get_last_date(target_table)
    current_updated_year = int(current_date.split('-')[0])
    current_year = pd.Timestamp.now().year
    today_date = pd.Timestamp.today().strftime('%Y/%m/%d')
    update_year_list = [ i for i in range(current_updated_year + 1, current_year + 1)]
    for year in update_year_list:
        scraped_df = scrape_closed_date(year)
        tidy_df = organized_scraped_data(scraped_df)
        insert_function(tidy_df, 'closed_date')
    update_latest_date(today_date, target_table)
