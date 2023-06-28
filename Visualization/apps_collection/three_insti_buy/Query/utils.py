import psycopg2
import pandas as pd
from dash import dcc, html
import dash_bootstrap_components as dbc

# 1. Create the available date list(transaction date)
def date_prepare(date: str, pre_day_num: int):
    def get_previous_date(date: str, day_num: int):
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = """   
                    SELECT date
                    FROM public.daily_total_trade
                    ORDER BY ABS(date - (%s)::date)
                    LIMIT (%s);"""
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**conn_params)
            # create a new cursor
            cur = conn.cursor()
            # execute the SQL statement
            cur.execute(sql, (date, day_num))
            rows = cur.fetchall()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        return [pd.to_datetime(row[0]) for row in rows] 
    # Execution
    if date == None:
        today_date = pd.Timestamp.today().strftime('%Y/%m/%d')
    else:
        today_date = pd.to_datetime(date).strftime('%Y/%m/%d')
    date_list = get_previous_date(today_date, pre_day_num)
    return date_list

# 2. Generate the dfs for visualization
def stock_selection(source, value_name, date_list):    
    # Pre-defined functions
    ## (1) Retreive stock_category
    def get_stock_category(source):
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = """   SELECT *
                    FROM stock_category
                    WHERE source = (%s);
                    """
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**conn_params)
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
    ## (2) Retrieve stock info
    def get_stock_info(stock_ids):
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = """   SELECT *
                    FROM stock_info
                    WHERE stock_id = ANY(%s);
                    """
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**conn_params)
            # create a new cursor
            cur = conn.cursor()
            # execute the SQL statement
            cur.execute(sql, (stock_ids,))
            rows = cur.fetchall()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        return pd.DataFrame(rows, columns=['公司代號', '公司簡稱', '產業別', '產業名稱', '上市櫃', '交易狀態'])
    ## (3)
    def get_stock_insti_balance(date_interval):
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = """   SELECT  date, stock_id,  
                    foreigner_balance + dealer_balance + investment_trust_balance AS total_balance
                    FROM daily_institution_trade
                    WHERE date >= (%s) AND \
                    date <= (%s);
                    """
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**conn_params)
            # create a new cursor
            cur = conn.cursor()
            # execute the SQL statement
            cur.execute(sql, (date_interval[0], date_interval[1]))
            rows = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        return_df = pd.DataFrame(rows, columns=column_names)
        return_df['date'] = pd.to_datetime(return_df['date'])
        return return_df
    ## (4)
    def generate_agg_df(stream: str, 
                        value_name: str,
                        data_df: pd.DataFrame, info_df: pd.DataFrame) -> pd.DataFrame:
        merged_df = pd.merge(data_df, info_df[['stock_id', 'stock_name', stream]], on = 'stock_id')
        groupby_df = merged_df.groupby(['date', stream], as_index = False)[value_name].sum()
        agg_df = groupby_df.pivot(index='date', columns=stream, values=value_name)
        return agg_df
    ## (5)
    def get_stock_OHLC(date, stock_ids):
        conn_params = {
        "host" : "localhost",
        "database" : "Fin_proj",
        "user" : "postgres",
        "password" : "nckumark"
        }
        sql = """   SELECT  date, stock_id, open, high, low, close, volume, value, spread
                    FROM daily_stock_price
                    WHERE date = (%s) AND \
                    stock_id = ANY(%s);
                    """
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**conn_params)
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
    
    # Execution
    date_index = date_list[0]
    date_index_str = date_index.strftime('%Y/%m/%d')
    print(f"Select date: {date_index_str}")
    info_df = get_stock_category(source)
    data_df = get_stock_insti_balance([date_list[-1].strftime('%Y/%m/%d'), date_list[0].strftime('%Y/%m/%d')])
    today_data_df = data_df[data_df.date==date_index]
    ## Aggregation
    industry_agg_df = generate_agg_df('Industry_name', value_name, data_df, info_df)
    subindustry_agg_df = generate_agg_df('subindustry_name', value_name, data_df, info_df)
    ##*** Criterion:(Percentage change > 0, three days in a row)
    period = 3
    industry_criterion_df = ((((industry_agg_df - industry_agg_df.shift(1)) / abs(industry_agg_df.shift(1)))) > 0).rolling(period).sum() == period #(industry_agg_df.pct_change() > 0).rolling(period).sum() == period
    subindustry_criterion_df = ((((subindustry_agg_df - subindustry_agg_df.shift(1)) / abs(subindustry_agg_df.shift(1)))) > 0).rolling(period).sum() == period
    target_industries = industry_criterion_df.columns[industry_criterion_df[industry_criterion_df.index==date_index].values[0]].tolist()
    target_subindustries = subindustry_criterion_df.columns[subindustry_criterion_df[subindustry_criterion_df.index==date_index].values[0]].tolist()
    print(f'Target industries: {target_industries}')
    print(f'Target Sub-industries: {target_subindustries}')
    ## (1) Extract stocks in the target industries or  Sub-industries
    ## (2) Aggregate value_name per stock_id at today_date
    ## (3) For the same stock_id, fold all subindustries with '|'
    selected_industry_df = info_df.loc[info_df['Industry_name'].isin(target_industries), ['Industry_name', 'subindustry_name', 'stock_name', 'stock_id']].reset_index(drop=True)
    selected_subindustry_df = info_df.loc[info_df['subindustry_name'].isin(target_subindustries), ['Industry_name', 'subindustry_name', 'stock_name', 'stock_id']].reset_index(drop=True)
    industry_df = pd.merge(selected_industry_df, today_data_df.loc[today_data_df.stock_id.isin(selected_industry_df['stock_id'].unique().tolist()),
                                                     ['stock_id', value_name]], on = 'stock_id')
    final_industry_df = industry_df.groupby(['Industry_name', 'stock_name', 'stock_id'], as_index = False).agg(
       subindustry_name = ('subindustry_name', lambda x: '|'.join(x)),
       total_value = ('total_balance', 'mean')
    )
    final_industry_df.rename(columns={'total_value': value_name}, inplace = True)
    final_industry_df = final_industry_df[['stock_id', 'stock_name', value_name, 'Industry_name', 'subindustry_name']]

    subindustry_df = pd.merge(selected_subindustry_df, today_data_df.loc[today_data_df.stock_id.isin(selected_subindustry_df['stock_id'].unique().tolist()),
                                                     ['stock_id', value_name]], on = 'stock_id')
    final_subindustry_df = subindustry_df.groupby(['Industry_name', 'stock_name', 'stock_id'], as_index = False).agg(
       subindustry_name = ('subindustry_name', lambda x: '|'.join(x)),
       total_value = ('total_balance', 'mean')
    )
    final_subindustry_df.rename(columns={'total_value': value_name}, inplace = True)
    final_subindustry_df = final_subindustry_df[['stock_id', 'stock_name', value_name, 'Industry_name', 'subindustry_name']]
    # Final printout selection
    
    # List top 10 from all
    #     top_n = 10
    #     top_n_final_industry_df = final_industry_df.iloc[final_industry_df[value_name].nlargest(top_n).index]
    #     top_n_final_subindustry_df = final_subindustry_df.iloc[final_subindustry_df[value_name].nlargest(top_n).index]

    # List top 1 based on {value_name} at today_date in each industry or subindustry
    if len(target_industries) != 0:
        ind_index = final_industry_df.groupby(['Industry_name'])[value_name].nlargest(1).reset_index()['level_1']
        top_n_final_industry_df = final_industry_df.iloc[ind_index]
    else:
        top_n_final_industry_df = final_industry_df
    
    if len(target_subindustries) != 0:
        subind_index = final_subindustry_df.groupby(['subindustry_name'])[value_name].nlargest(1).reset_index()['level_1']
        top_n_final_subindustry_df = final_subindustry_df.iloc[subind_index]
    else:
         top_n_final_subindustry_df = final_subindustry_df
    # Final groupby in case a company has multiple industries or subindustries
    industry_info_df = top_n_final_industry_df.groupby(['stock_id', 'stock_name'], as_index = False).agg(
        total_value = ('total_balance', 'mean'),
        Industry_name = ('Industry_name', lambda x: '/'.join(x)),
        subindustry_name = ('subindustry_name', lambda x: '/'.join(x))).sort_values('total_value', ascending=False)
    subindustry_info_df = top_n_final_subindustry_df.groupby(['stock_id', 'stock_name'], as_index = False).agg(
        total_value = ('total_balance', 'mean'),
        Industry_name = ('Industry_name', lambda x: '/'.join(x)),
        subindustry_name = ('subindustry_name', lambda x: '/'.join(x))).sort_values('total_value', ascending=False)
    ## *Attach industry link
    # industry_info_df = pd.merge(industry_info_df, info_df[['stock_id', 'Industry_name', 'link']], on = ['stock_id', 'Industry_name'])
    # subindustry_info_df = pd.merge(subindustry_info_df, info_df[['stock_id', 'Industry_name', 'link']], on = ['stock_id', 'Industry_name']).drop_duplicates(ignore_index=True)
    # Prepare OHLC df
    industry_ohlc_df = get_stock_OHLC(date_index_str, industry_info_df.stock_id.tolist())
    industry_ohlc_df['pct'] = industry_ohlc_df['spread'] / (industry_ohlc_df['close'] - industry_ohlc_df['spread'])
    subindustry_ohlc_df = get_stock_OHLC(date_index_str, subindustry_info_df.stock_id.tolist())
    subindustry_ohlc_df['pct'] = subindustry_ohlc_df['spread'] / (subindustry_ohlc_df['close'] - subindustry_ohlc_df['spread'])
    # Prepare Industry df & Plot df
    temp_industry_df = industry_agg_df.loc[:,industry_agg_df.columns.isin(target_industries)]
    target_industry_df = pd.melt(temp_industry_df.reset_index(), id_vars='date',
                                 value_vars=temp_industry_df.columns, value_name=value_name)
    temp_subindustry_df = subindustry_agg_df.loc[:,subindustry_agg_df.columns.isin(target_subindustries)]
    target_subindustry_df = pd.melt(temp_subindustry_df.reset_index(), id_vars='date',
                                 value_vars=temp_subindustry_df.columns, value_name=value_name)
    ## If num of target industry or subindustry > 10, only list top 10 in the latest day
    n_limit = 10
    if target_industry_df.Industry_name.nunique() > n_limit:
        industry_index = target_industry_df.loc[target_industry_df[target_industry_df.date == date_index].total_balance.nlargest(n_limit).index, 'Industry_name'].tolist()
        plot_industry_df = target_industry_df[target_industry_df['Industry_name'].isin(industry_index)]
    else:
        plot_industry_df = target_industry_df

    if target_subindustry_df.subindustry_name.nunique() > n_limit:
        subindustry_index = target_subindustry_df.loc[target_subindustry_df[target_subindustry_df.date == date_index].total_balance.nlargest(n_limit).index, 'subindustry_name'].tolist()
        plot_subindustry_df = target_subindustry_df[target_subindustry_df['subindustry_name'].isin(subindustry_index)]
    else:
        plot_subindustry_df = target_subindustry_df

    return (industry_info_df, subindustry_info_df), \
           (industry_ohlc_df, subindustry_ohlc_df), \
           (plot_industry_df, plot_subindustry_df)

# 3. Organize dfs for AG table
def ag_table_df(info_df, ohlc_df, value_name):
    info_df = info_df.rename(columns={"total_value": value_name})
    table_df = pd.merge(info_df, ohlc_df, on = ['stock_id'])[['stock_id', 'stock_name', 'Industry_name',
       'subindustry_name', 'open', 'high', 'low', 'close', 'volume','value', 'spread', 'pct', value_name]]
    return table_df

# 4. Generate cards for OHLC info
def card_generate(merged_df):
    def one_card(stock_name, stock_id, close_price, spread, pct):
        icon = "bi bi-caret-down-fill" if spread < 0 else "bi bi-caret-up-fill"
        color = "danger" if spread < 0 else "success"
        dbc_card = dbc.Card(
            [
                dbc.CardHeader(
                    [
                        html.H4(stock_name, style={'text-align': 'left', 'align-self': 'flex-start', 'font-size':'22px'}),
                        html.H6(stock_id, style={'text-align': 'right', 'align-self': 'flex-end', 'font-size':'16px'}),
                    ], style={'display': 'flex', 'flex-direction': 'row','justify-content': 'space-between'}),
                dbc.CardBody(
                    [
                        html.H4(f"{close_price:,}", style={'text-align': 'center'}, className=f"text-{color}"),
                        html.Div([
                            html.Div([ html.I(className=icon),html.H5(spread)], style={'display': 'flex', 'flex-direction': 'row'}),
                            html.H5(f"{pct:.2%}")
                        ], style={'display': 'flex', 'flex-direction': 'row','justify-content': 'space-between'}, className=f"text-{color}")   
                    ],)
            ],
            style={'width': '5em'}
        )
        return dbc_card
    
    # Layout
    if merged_df.shape[0] == 0:
        return dbc.Col([],  width=3, className="d-flex flex-column justify-content-between")
    card_layout = []
    for idx, row in merged_df.iterrows():
        card = one_card(row['stock_name'], row['stock_id'], row['close'], row['spread'], row['pct'])
        if idx % 2 == 0:
            card_list = []
            card_list.append(card)
        else:
            card_list.append(card)
            card_layout.append(dbc.Row(dbc.CardGroup(card_list)))
    # Include the last one
    if idx %2 == 0:
        card_layout.append(dbc.Row(dbc.CardGroup(card_list)))
    return dbc.Col(card_layout,  width=3, className="d-flex flex-column justify-content-between")

