import psycopg2
import os
from io import StringIO
import pandas as pd
from typing import List

class Queryer:
    def __init__(self, today_date, conn_params):
        self.conn_params = conn_params
        self.bday_Taiwan_stock = pd.offsets.CustomBusinessDay(holidays= self.get_closed_date())
        self.start_date = today_date if today_date.weekday() not in [5, 6] else today_date - 1 * self.bday_Taiwan_stock
        self.date = pd.to_datetime(self.start_date).strftime('%Y/%m/%d')

    # CRUD function
    ## (1) Get closed date
    def get_closed_date(self):
        sql = "SELECT date \
            FROM public.closed_date ;"
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**self.conn_params)
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
    ## (2) Insert df into DB
    def insert_df(self, df, table):
        conn = psycopg2.connect(**self.conn_params)
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
    ## (3) Update strategy table
    def update_strategy_table(self):
        sql = """ UPDATE strategy
                    SET select_status = %s
                    WHERE select_status = %s
                    """
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**self.conn_params)
            # create a new cursor
            cur = conn.cursor()
            # execute the UPDATE  statement
            cur.execute(sql, (False, True))
            # Commit the changes to the database
            conn.commit()
            # Close communication with the PostgreSQL database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
    ## (4) Get current strategy stock_ids
    def get_strategy_stock_ids(self, strategy_name):
        sql = """
                SELECT * \
                FROM public.strategy \
                Where strategy_name = %s AND \
                select_status = %s

              """
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**self.conn_params)
            # create a new cursor
            cur = conn.cursor()
            # execute the SQL statement
            cur.execute(sql, (strategy_name, True))
            rows = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        return_df = pd.DataFrame(rows, columns=column_names)
        return return_df

    ## (5) Get stock OHLC (single date)
    def get_stock_OHLC_date(self, date, stock_ids):
        sql = """   SELECT  date, stock_id, open, high, low, close, volume, value, spread
                    FROM daily_stock_price
                    WHERE date = (%s) AND \
                    stock_id = ANY(%s);
                    """
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**self.conn_params)
            # create a new cursor
            cur = conn.cursor()
            # execute the SQL statement
            cur.execute(sql, (date, stock_ids))
            rows = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        return_df = pd.DataFrame(rows, columns=column_names)
        return return_df
   
    ## (6) Get stock OHLC (since)
    def get_stock_OHLC_since(self, date, stock_ids):
        sql = """   SELECT  date, stock_id, open, high, low, close, volume, value, spread
                    FROM daily_stock_price
                    WHERE date >= (%s) AND \
                    stock_id = ANY(%s);
                    """
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**self.conn_params)
            # create a new cursor
            cur = conn.cursor()
            # execute the SQL statement
            cur.execute(sql, (date, stock_ids))
            rows = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        return_df = pd.DataFrame(rows, columns=column_names)
        return return_df
   
    # Organize df for table
    def table_data(self, stock_ids: List[str]) -> pd.DataFrame:
        # (1) Retreive stock_category
        def get_stock_category(source):
            sql = """   SELECT *
                        FROM stock_category
                        WHERE source = (%s);
                        """
            try:
                # connect to the PostgreSQL database
                conn = psycopg2.connect(**self.conn_params)
                # create a new cursor
                cur = conn.cursor()
                # execute the SQL statement
                cur.execute(sql, (source,))
                rows = cur.fetchall()
                cur.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
            finally:
                if conn is not None:
                    conn.close()
            return pd.DataFrame(rows, columns=['Source', 'Industry_name', 'stream', 'subindustry_name', 'stock_id', 'stock_name', 'link'])
        
        source='財報狗'
        info_df = get_stock_category(source)
        ohlc_df = self.get_stock_OHLC_date(self.date, stock_ids)
        info_df = info_df[info_df.stock_id.isin(stock_ids)]
        final_info_df = info_df.groupby(['stock_id', 'stock_name', 'Industry_name'], as_index = False).agg(
            subindustry_name = ('subindustry_name', lambda x: '|'.join(x))).groupby(
            ['stock_id', 'stock_name'], as_index = False).agg(
            Industry_name = ('Industry_name', lambda x: '/'.join(x)),
            subindustry_name = ('subindustry_name', lambda x: '/'.join(x)))
        table_df = pd.merge(final_info_df, ohlc_df, on = ['stock_id'])[['stock_id', 'stock_name', 'Industry_name',
        'subindustry_name', 'open', 'high', 'low', 'close', 'volume','value', 'spread']]
        table_df.loc[table_df['spread'].isna(), 'spread'] = 0
        table_df['pct'] = table_df['spread'] / (table_df['close'] - table_df['spread'])

        return table_df
      