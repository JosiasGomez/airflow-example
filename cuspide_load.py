from sqlalchemy import create_engine
from cuspide_scrape import create_conn

def create_final_table():
    conn = create_conn()
    conn.execute("""CREATE TABLE IF NOT EXISTS third_step(
                    name VARCHAR(50) NOT NULL,
                    price FLOAT NOT NULL,
                    pages INT NOT NULL,
                    date DATETIME NOT NULL,
                    null_excent INT NOT NULL
                    )""")
    return print('tabla creada'), conn.close()

def load_data():
    
    create_final_table()
    
    conn = create_conn()
    conn.execute('INSERT INTO third_step SELECT * FROM second_step')
    
    return print('todo ok'), conn.close()