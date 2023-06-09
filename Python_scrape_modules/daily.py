import psycopg2
import os
import pandas as pd
import time
import requests
from io import StringIO
from tqdm import tqdm
import numpy as np

# (1) 
#---------------------------------------------------------------------------------------------------------------
#  Update daily stock price information (OHLC data) into "daily_stock_price" table
#
#---------------------------------------------------------------------------------------------------------------
def daily_stock_price_update(target_table: str, sleep_sec: int, to_date = None) -> None:
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
                    content = res.json()
                    columns = str(content['fields9'])
                    assert columns == default_columns
                    return_df = pd.DataFrame(content['data9'])
                    return return_df
                except (requests.ConnectionError, requests.ReadTimeout) as error:
                    print(error)
                    print('Retry one more time after 40s', retry_times, 'times left')
                    time.sleep(40)
                    retry_times -= 1
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
                                 'High', 'Low', 'Close','Spread', 'Turnover']
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
                    content = res.json()
                    return_df = pd.DataFrame(content['aaData'])
                    return return_df
                except (requests.ConnectionError, requests.ReadTimeout) as error:
                    print(error)
                    print('Retry one more time after 60s', retry_times, 'times left')
                    time.sleep(40)
                    retry_times -= 1
        # 3. Parse data
        def parse_return(date, content):
            target_df = content.iloc[:, [0, 7, 8, 4, 5, 6, 2, 3, 9]].copy()
            target_df.insert(0, 'Date', date)
            target_df.columns = ['Date', 'Stock_id', 'Volume', 'Value', 'Open', 'High', 'Low', 'Close',
                        'Spread', 'Turnover']
            for column in target_df.columns:
                if column in ['Volume', 'Value', 'Turnover']:
                    target_df[column] = target_df[column].str.replace(',', '').astype('int64')
                elif column in ['Open', 'High', 'Low', 'Close']:
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
        listed_df = daily_scrape_listed(date)
        otc_df = daily_scrape_otc(date)
        target_df = organized_scrape_data(listed_df, otc_df, stock_id_list)
        final_target_df = target_df.where(target_df.notnull(), 'None')
        insert_function(final_target_df, target_table)
        update_latest_date(date, target_table)
        print(f"{date} data is finished")
        execution_time -= 1
        if execution_time > 0:
            time.sleep(sleep_sec)

# (2)
#---------------------------------------------------------------------------------------------------------------
#  Update daily three institution buying information into "daily_institution_trade" table
#
#---------------------------------------------------------------------------------------------------------------
def daily_three_insti_buying_update(target_table: str, sleep_sec: int, to_date = None) -> None:
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

# (3)
#---------------------------------------------------------------------------------------------------------------
#  Update daily total transaction information(value, volume, turnover) from List and OTC buying information 
#  into "daily_total_trade" table
#
#---------------------------------------------------------------------------------------------------------------
def daily_total_trade_update(target_table: str, sleep_sec: int, to_date = None) -> None:
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
            default_columns = ["日期", "成交股數", "成交金額", "成交筆數", "發行量加權股價指數", "漲跌點數"]
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
            col = ['日期', '成交股數(仟股)', '金額(仟元)', '筆數', '櫃買指數', '漲/跌']
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
        listed_df[['year', 'month', 'day']] = listed_df['日期'].str.split('/', expand=True)
        listed_df[['year', 'month', 'day']] = listed_df[['year', 'month', 'day']].astype(int)
        listed_df['year'] = listed_df['year'] + 1911
        listed_df['Date'] = pd.to_datetime(listed_df[['year', 'month', 'day']]).apply(lambda x: x.strftime('%Y/%m/%d'))
        # OTC
        otc_df = otc_df.applymap(lambda x: x.replace(',', ''))
        otc_df[otc_df.columns[1:]] = otc_df[otc_df.columns[1:]].astype(float)
        otc_df[['year', 'month', 'day']] = otc_df['日期'].str.split('/', expand=True)
        otc_df[['year', 'month', 'day']] = otc_df[['year', 'month', 'day']].astype(int)
        otc_df['year'] = otc_df['year'] + 1911
        otc_df['Date'] = pd.to_datetime(otc_df[['year', 'month', 'day']]).apply(lambda x: x.strftime('%Y/%m/%d'))
        result_df = pd.merge(listed_df, otc_df, how="right", left_on=["Date"],
                             right_on = ["Date"], suffixes = ('_listed', '_otc'))
        target_df = result_df[['Date', '成交股數', '成交金額', '成交筆數', '發行量加權股價指數',
                     '漲跌點數', '成交股數(仟股)', '金額(仟元)', '筆數', '櫃買指數', '漲/跌']].copy()
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
    start_date = get_last_date(target_table)
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
            update_latest_date(newest_date, target_table)
        content_len -= 1
        if content_len > 0:
            time.sleep(sleep_sec)

# (4)
#---------------------------------------------------------------------------------------------------------------
#  Update daily three institution total buying information into "daily_institution_total_trade" table
#
#---------------------------------------------------------------------------------------------------------------
def daily_institution_total_trade_update(target_table: str, sleep_sec: int, to_date = None) -> None:
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


# (5)
#---------------------------------------------------------------------------------------------------------------
#  Update daily List and OTC index information into "daily_total_return_index" table
#
#---------------------------------------------------------------------------------------------------------------
def daily_total_return_index_update(target_table: str, sleep_sec: int, to_date = None) -> None:
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
            url = f'https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date={month_str}01'
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
            default_columns = ["日期", "開盤指數", "最高指數", "最低指數", "收盤指數"]
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
            url = f'https://www.tpex.org.tw/web/stock/iNdex_info/inxh/Inx_result.php?l=zh-tw&d={month_str}/01'
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
            col = ['日期', '開市', '最高', '最低', '收市', '漲/跌']
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
        listed_df[['year', 'month', 'day']] = listed_df['日期'].str.split('/', expand=True)
        listed_df[['year', 'month', 'day']] = listed_df[['year', 'month', 'day']].astype(int)
        listed_df['year'] = listed_df['year'] + 1911
        listed_df['Date'] = pd.to_datetime(listed_df[['year', 'month', 'day']]).apply(lambda x: x.strftime('%Y/%m/%d'))
        # OTC
        otc_df = otc_df.applymap(lambda x: x.replace(',', ''))
        otc_df[otc_df.columns[1:]] = otc_df[otc_df.columns[1:]].astype(float)
        result_df = pd.merge(listed_df, otc_df, how="right", left_on=["Date"],
                             right_on = ["日期"], suffixes = ('_listed', '_otc'))
        target_df = result_df[['日期_otc', '開盤指數', '最高指數', '最低指數', '收盤指數',
                          '開市', '最高', '最低', '收市']].copy()
        target_df.columns = ['Date', 'Taiex_open', 'Taiex_max', 'Taiex_min', 'Taiex_close',
                        'TPEx_open', 'TPEx_max', 'TPEx_min', 'TPEx_close']
        target_df['Date'] = pd.to_datetime(target_df['Date'])
        return_df = target_df[target_df.Date > pd.to_datetime(current_date)].reset_index(drop = True)
        return return_df
    # 7. Insert into database
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
    start_date = get_last_date(target_table)
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
            update_latest_date(newest_date, target_table)
        content_len -= 1
        if content_len > 0:
            time.sleep(sleep_sec)
