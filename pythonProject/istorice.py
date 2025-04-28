import sqlite3
import yfinance as yf
import pandas as pd

db_path = r"C:\Users\Stancu Diana\Desktop\StockData\pythonProject\db.db"
conn = sqlite3.connect(db_path)

symbols = ["HPE", "CSCO", "JNPR", "ANET", "CIEN", "DELL", "IBM", "ORCL"]
start_date = "2024-01-01"
end_date = "2025-01-01"

table_name = "historical_data"

try:
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume INTEGER,
            symbol TEXT,
            PRIMARY KEY (date, symbol)
        )
    """)
    conn.commit()

    for symbol in symbols:
        try:
            data = yf.download(symbol, start=start_date, end=end_date, progress=False)
            if data.empty:
                print(f"⚠️ Can't find data for {symbol}")
                continue
            data = data.reset_index()

            required_columns = {
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Adj Close': 'adj_close',
                'Volume': 'volume'
            }

            processed_data = pd.DataFrame()

            for original_col, new_col in required_columns.items():
                if original_col in data.columns:
                    processed_data[new_col] = data[original_col]
                else:
                    if new_col == 'adj_close' and 'Close' in data.columns:
                        processed_data[new_col] = data['Close']
                    else:
                        processed_data[new_col] = None

            processed_data['symbol'] = symbol


            processed_data.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"✅ Saved dates for {symbol} in table {table_name}")

        except Exception as e:
            print(f"❌ Error for {symbol}: {str(e)}")
            continue

except Exception as e:
    print(f"❌ Error creating table: {str(e)}")

finally:
    if conn:
        conn.close()
