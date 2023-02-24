import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

def get_links(page, link):
    return[link + page.select('figure div a')[i]['href'] for i in range(len(page.select('figure div a')))]

def get_books(page,link):
    links = get_links(page,link)
    data = []
    for link in links:
        page= bs(requests.get(link).text)
        dat = {
            'name':page.select('h1 a')[1]['title'],
            'price':page.select('div meta')[1]['content'],
            'pages':page.select('ul li span')[3].text,
            'date': datetime.now()
        }
        data.append(dat)
    return pd.DataFrame(data)

def create_conn():
    engine = create_engine('postgresql+psycopg2://{user}:{password}@{host}/{database}')
    return engine.connect()

def extract_data():
    
    link1 = 'https://www.cuspide.com/top100.aspx'
    link2 = 'https://www.cuspide.com/'
    page = bs(requests.get(link1).text)
    
    df = get_books(page,link2)
    
    conn = create_conn()
    return df.to_sql('first_step',conn, schema='public',if_exists='replace',index=True), conn.close()