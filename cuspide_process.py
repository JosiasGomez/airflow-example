import re
import requests
import pandas as pd
from datetime import datetime
from cuspide_scrape import create_conn

#Cómo parte del procesamiento, identificamos si se relevó algún dato sin su correspondiente
#precio. Se realiza en SQL debido a que es más simple y más rápido que hacerlo en Pandas,
#especialmente si lo pensamos para grandes volumenes de datos.

def testing_books():
    conn = create_conn()
    
    conn.execute("""ALTER TABLE first_step
	                ADD null_excent INTEGER NULL""")
    
    conn.execute("""UPDATE first_step
                    SET null_excent= CASE 
                    WHEN price IS NULL 
                    THEN 1 ELSE 0 END """)
    
    return print('todo ok')

def get_scraped_books():
    conn = create_conn()
    testing_books()
    return pd.read_sql_query('SELECT * FROM first_step',conn)

def transform_data():
    
    df = get_scraped_books()
    
    #Realizamos un poco de cleaning. Ponemos todo en minúscula, sacamos el símbolo "$" para
    #trabajar con los precios como float y corregimos el formato de la columna date
    
    df['name'] = df['name'].lower()
    df['price'] = [re.sub('\$','',i) for i in df['price']] #También puede usarse el método replace(). En lo personal prefiero usar directamente Expresiones Regulares
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    
    conn = create_conn()
    
    return df.to_sql('second_step',conn, schema='public',if_exists='replace',index=True), conn.close()