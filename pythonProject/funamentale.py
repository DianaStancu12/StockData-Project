import sqlite3
import yfinance as yf
import pandas as pd
from datetime import datetime

db_path = r"C:\Users\Stancu Diana\Desktop\StockData\pythonProject\db.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

symbols = ["HPE", "CSCO", "JNPR", "ANET", "CIEN", "DELL", "IBM", "ORCL"]

cursor.execute("""
CREATE TABLE IF NOT EXISTS fundamentals_ttm (
    symbol TEXT,
    report_date TEXT,
    period_type TEXT,  
    revenue REAL,
    net_income REAL,
    total_assets REAL,
    total_debt REAL,
    free_cash_flow REAL,
    pe_ratio REAL,
    ps_ratio REAL,
    pb_ratio REAL,
    dividend_yield REAL,
    marketCap REAL,
    PRIMARY KEY (symbol, report_date, period_type)  
)
""")
conn.commit()

for symbol in symbols:
    try:
        ticker = yf.Ticker(symbol)
        financials = ticker.quarterly_financials
        balance_sheet = ticker.quarterly_balance_sheet
        cashflow = ticker.quarterly_cashflow
        info = ticker.info

        if financials.empty or len(financials.columns) < 4:
            print(f"⚠️ Insufficient data for {symbol} LTM calculation")
            continue

        ltm_date = financials.columns[0].strftime('%Y-%m-%d')

        cursor.execute("""
        SELECT 1 FROM fundamentals_ttm 
        WHERE symbol = ? AND report_date = ? AND period_type = 'LTM'
        """, (symbol, ltm_date))

        if cursor.fetchone() is not None:
            print(f"ℹ️ LTM dates already existing for {symbol} ({ltm_date}) - ignore")
            continue


        def get_ltm(data, metric):
            try:
                return data.loc[metric].iloc[:4].sum()
            except:
                return None

        ltm_data = {
            'symbol': symbol,
            'report_date': ltm_date,
            'period_type': 'LTM',
            'revenue': get_ltm(financials, "Total Revenue"),
            'net_income': get_ltm(financials, "Net Income"),
            'total_assets': balance_sheet.loc["Total Assets"].iloc[0] if not balance_sheet.empty else None,
            'total_debt': get_ltm(balance_sheet, "Total Debt") if not balance_sheet.empty else None,
            'free_cash_flow': get_ltm(cashflow, "Free Cash Flow"),
            'pe_ratio': info.get('trailingPE'),
            'ps_ratio': info.get('priceToSalesTrailing12Months'),
            'pb_ratio': info.get('priceToBook'),
            'dividend_yield': info.get('dividendYield', 0) * 100,
            'marketCap':info.get('marketCap', None)
        }


        try:
            pd.DataFrame([ltm_data]).to_sql(
                'fundamentals_ttm',
                conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            print(f"✅ LTM dates saved for {symbol} (until {ltm_date})")
        except sqlite3.IntegrityError:
            print(f"ℹ️ LTM dates already existing for {symbol} ({ltm_date}) - ignore")

        for i, quarter_date in enumerate(financials.columns[:4]):
            q_date_str = quarter_date.strftime('%Y-%m-%d')
            period_type = f'Q{i + 1}'

            cursor.execute("""
            SELECT 1 FROM fundamentals_ttm 
            WHERE symbol = ? AND report_date = ? AND period_type = ?
            """, (symbol, q_date_str, period_type))

            if cursor.fetchone() is None:
                q_data = {
                    'symbol': symbol,
                    'report_date': q_date_str,
                    'period_type': period_type,
                    'revenue': financials.loc[
                        "Total Revenue", quarter_date] if "Total Revenue" in financials.index else None,
                    'net_income': financials.loc[
                        "Net Income", quarter_date] if "Net Income" in financials.index else None,
                    'total_assets': balance_sheet.loc[
                        "Total Assets", quarter_date] if not balance_sheet.empty else None,
                    'total_debt': balance_sheet.loc["Total Debt", quarter_date] if not balance_sheet.empty else None,
                    'free_cash_flow': cashflow.loc["Free Cash Flow", quarter_date] if not cashflow.empty else None,
                    'pe_ratio': info.get('trailingPE'),
                    'ps_ratio': info.get('priceToSalesTrailing12Months'),
                    'pb_ratio': info.get('priceToBook'),
                    'dividend_yield': info.get('dividendYield', 0) * 100,
                    'marketCap': info.get('marketCap', None)
                }
                try:
                    pd.DataFrame([q_data]).to_sql(
                        'fundamentals_ttm',
                        conn,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    print(f"✅ Date {period_type} saved for {symbol} ({q_date_str})")
                except sqlite3.IntegrityError:
                    print(f"ℹ️ Date {period_type} existing already for {symbol} ({q_date_str}) - ignore")
            else:
                print(f"ℹ️ Date {period_type} existing already for {symbol} ({q_date_str}) - ignore")

    except Exception as e:
        print(f"❌ Error processing {symbol}: {type(e).__name__} - {str(e)}")
        continue

conn.close()
print("✅ Completed!")
