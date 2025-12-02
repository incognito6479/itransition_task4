import pandas as pd 
import yaml, re
from helper_funcs import reprocessYear, reprocessTitle, reprocessAuthor, \
reprocessPublisher, reprocessPhone, reprocessPrice, addCurrencyType, \
convertEURtoUSD, authorSet, findIds, uniqueUsers, combineIds, fillShippingInfo, \
extractDate, dailyRevenue
from load_into_db import loadIntoDB


def reprocessBooksData(data_source):
    with open(f"DATAs/{data_source}/books.yaml", "r") as file:
        yaml_data = re.sub(r':(\w+):', r'\1:', file.read())
        yaml_data = yaml.safe_load(yaml_data)
    df = pd.DataFrame(yaml_data)
    df['year'] = reprocessYear(df)
    df['title'] = reprocessTitle(df)
    df['author'] = reprocessAuthor(df)
    df['publisher'] = reprocessPublisher(df)
    df['data_source'] = data_source
    unique_author_set = authorSet(df)
    df = df[['id', 'title', 'author', 'genre', 'publisher', 'year', 'data_source']]
    loadIntoDB(df, table_name='books', data_source=data_source)
    df = pd.DataFrame({
        'title': ['unique author set'],
        'value': [unique_author_set],
        'data_source': [data_source]
    })
    loadIntoDB(df, table_name='unique_numbers', data_source=data_source, unique_title='unique author set')

def reprocessUsersData(data_source):
    df = pd.read_csv(f"DATAs/{data_source}/users.csv")
    df['phone'] = reprocessPhone(df)
    group1 = " ".join(['name', 'address', 'phone'])
    group2 = " ".join(['name', 'address', 'email'])
    group3 = " ".join(['name', 'phone', 'email'])
    group4 = " ".join(['address', 'phone', 'email'])
    df = findIds(df, group1)  
    df = findIds(df, group2)
    df = findIds(df, group3)  
    df = findIds(df, group4)  
    df['duplicated_user_ids'] = combineIds(df, group1, group2, group3, group4)
    df['data_source'] = data_source 
    df = df[['id', 'name', 'address', 'phone', 'email', 'duplicated_user_ids', 'data_source']]
    loadIntoDB(df, table_name='users', data_source=data_source) 
    unique_users = uniqueUsers(df)
    df = pd.DataFrame({
        'title': ['unique users'],
        'value': [unique_users],
        'data_source': [data_source]
    })
    loadIntoDB(df, table_name='unique_numbers', data_source=data_source, unique_title='unique users')

def reprocessOrdersData(data_source):
    df = pd.read_parquet(f"DATAs/{data_source}/orders.parquet", engine="pyarrow")
    df['unit_price'] = reprocessPrice(df)
    df['currency_type'] = addCurrencyType(df)
    df['unit_price'] = convertEURtoUSD(df)
    df['shipping'] = fillShippingInfo(df)
    df['currency_type'] = 'USD'
    df = extractDate(df)
    dailyRevenue(df, data_source)
    df['data_source'] = data_source
    df = df[['user_id', 'book_id', 'quantity', 'unit_price', 'paid_price', \
             'timestamp', 'shipping', 'data_source', 'currency_type']]
    loadIntoDB(df, table_name='orders', data_source=data_source) 
    

for data in ['DATA1', 'DATA2', 'DATA3']:
    reprocessBooksData(data_source=data)
    reprocessUsersData(data_source=data)
    reprocessOrdersData(data_source=data)
