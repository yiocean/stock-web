import time
import json
import os
import glob
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

def get_previous_data(current_dir, target_folder):
    """
    Finds the 'latest' Excel file in the target folder and returns a dictionary of {Stock Code: Shares}.
    """
    search_path = os.path.join(current_dir, target_folder, "*.xlsx")
    files = sorted(glob.glob(search_path)) # Sort by filename (Date: Old -> New)
    
    # --- Fix 1: Filter out temporary files created when Excel is open (~$...) ---
    valid_files = [f for f in files if not os.path.basename(f).startswith('~$')]
    
    if len(valid_files) < 1:
        return {} # No previous files found
    
    last_file = valid_files[-1] # The most recent file
    try:
        print(f"[INFO] Reading previous data for comparison: {os.path.basename(last_file)}")
        df = pd.read_excel(last_file, skiprows=2)
        
        # Ensure correct column names (Handles both English and legacy Chinese headers)
        col_code = 'Stock Code' if 'Stock Code' in df.columns else '股票代號'
        col_shares = 'Shares' if 'Shares' in df.columns else '股數'
        
        # Create a dictionary mapping codes to shares
        stock_map = dict(zip(df[col_code].astype(str), df[col_shares]))
        return stock_map
    except Exception as e:
        print(f"[WARN] Failed to read previous file: {e}")
        return {}

def fetch_stock_prices(stock_codes, target_date_str):
    """
    Fetches closing prices for Taiwan stocks on a 'specific date' using yfinance.
    Smart Logic: Tries .TW (Listed) first; if not found, tries .TWO (OTC).
    """
    if not stock_codes:
        return {}
    
    print(f"[INFO] Fetching closing prices for date: {target_date_str}...")
    
    # Calculate date range (start date + 1 day)
    target_date_obj = datetime.strptime(target_date_str, "%Y-%m-%d")
    next_day = target_date_obj + timedelta(days=1)
    end_date_str = next_day.strftime("%Y-%m-%d")

    price_map = {}
    
    # --- Phase 1: Attempt download with .TW suffix (Listed stocks) ---
    tickers_tw = [f"{code}.TW" for code in stock_codes]
    try:
        data = yf.download(tickers_tw, start=target_date_str, end=end_date_str, progress=False)
        
        # Check data
        if 'Close' in data and not data['Close'].empty:
            closes = data['Close'].iloc[0] # Get data for the first day
            for code in stock_codes:
                ticker = f"{code}.TW"
                try:
                    price = closes[ticker]
                    if not pd.isna(price) and price > 0:
                        price_map[code] = float(price)
                except:
                    pass # Skip if not found, wait for Phase 2
    except Exception as e:
        print(f"[WARN] TW download error: {e}")

    # --- Phase 2: Retry missing stocks with .TWO suffix (OTC stocks) ---
    missing_codes = [code for code in stock_codes if code not in price_map]
    
    if missing_codes:
        print(f"[INFO] Retrying {len(missing_codes)} stocks with .TWO suffix (OTC)...")
        tickers_two = [f"{code}.TWO" for code in missing_codes]
        try:
            data = yf.download(tickers_two, start=target_date_str, end=end_date_str, progress=False)
            
            if 'Close' in data and not data['Close'].empty:
                closes = data['Close'].iloc[0]
                for code in missing_codes:
                    ticker = f"{code}.TWO"
                    try:
                        price = closes[ticker]
                        if not pd.isna(price) and price > 0:
                            price_map[code] = float(price)
                    except:
                        pass
        except Exception as e:
            print(f"[WARN] TWO download error: {e}")

    # Finally, fill 0 for stocks truly not found (e.g., Emerging Board or invalid codes)
    for code in stock_codes:
        if code not in price_map:
            price_map[code] = 0.0
            
    return price_map

def scrape_00981a_data():
    # --- 1. Setup Chrome ---
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")

    print("[INFO] Launching browser...")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    target_url = "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW"
    
    try:
        print(f"[INFO] Go to target URL: {target_url}")
        driver.get(target_url)
        time.sleep(3) 

        # --- 2. Parse JSON Data ---
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        data_div = soup.find('div', id='DataAsset')
        
        if not data_div or not data_div.get('data-content'):
            print("[ERROR] Data block not found.")
            return

        try:
            asset_data = json.loads(data_div.get('data-content'))
            print("[INFO] Successfully extracted JSON data")
        except json.JSONDecodeError:
            print("[ERROR] JSON extraction failed")
            return

        # --- 3. Extract Raw Data ---
        net_asset = 0
        raw_portfolio = []
        data_date = time.strftime("%Y-%m-%d")

        for item in asset_data:
            code = item.get('AssetCode', '')
            if code == 'NAV':
                net_asset = item.get('Value', 0)
                print(f"[DATA] Net Asset: {net_asset:,.0f}")
            elif code == 'ST':
                details = item.get('Details', [])
                if details and 'TranDate' in details[0]:
                    data_date = details[0]['TranDate'].split('T')[0]

                for stock in details:
                    raw_portfolio.append({
                        'code': stock.get('DetailCode', '').strip(),
                        'name': stock.get('DetailName', '').strip(),
                        'shares': float(stock.get('Share', 0)),
                        'weight_str': f"{stock.get('NavRate', 0)}%" 
                    })
        
        print(f"[INFO] Data Date is: {data_date}")
        print(f"[INFO] Found {len(raw_portfolio)} stocks. Fetching extra data...")

        # --- 4. Fetch Extra Data ---
        
        # Prepare paths
        current_dir = os.path.dirname(os.path.abspath(__file__))
        target_folder = "data"
        output_dir = os.path.join(current_dir, target_folder)
        os.makedirs(output_dir, exist_ok=True)

        # A. Get Previous Shares
        prev_shares_map = get_previous_data(current_dir, target_folder)

        # B. Get Stock Prices (Smart Mode)
        all_codes = [p['code'] for p in raw_portfolio]
        price_map = fetch_stock_prices(all_codes, data_date)

        # --- 5. Calculate & Merge ---
        final_portfolio = []
        
        for item in raw_portfolio:
            code = item['code']
            shares = item['shares']
            
            # Price
            price = price_map.get(code, 0)
            
            # Market Value
            market_value = shares * price
            
            # Share Change
            prev_share = prev_shares_map.get(code, 0)
            share_change = shares - prev_share
            
            # Net Amount (Buy/Sell Value)
            net_buy_sell = share_change * price

            final_portfolio.append({
                'Stock Code': code,
                'Stock Name': item['name'],
                'Shares': shares,
                'Weight': item['weight_str'],
                'Close Price': price,
                'Market Value': market_value,
                'Share Change': share_change,
                'Net Amount': net_buy_sell
            })

    except Exception as e:
        print(f"[ERROR] Error occurred: {e}")
        return
    finally:
        driver.quit()
        print("[INFO] Browser closed")

    # --- 6. Save to Excel ---
    if final_portfolio:
        try:
            file_name = f"00981A_Holdings_{data_date.replace('-', '')}.xlsx"
            output_file = os.path.join(output_dir, file_name)

            df_stocks = pd.DataFrame(final_portfolio)
            
            # Create Header info
            header_row = ['Date', data_date, 'Net Asset', net_asset]
            header_df = pd.DataFrame([header_row])

            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Write header at row 0, data starts at row 2
                header_df.to_excel(writer, index=False, header=False, startrow=0)
                df_stocks.to_excel(writer, index=False, startrow=2)

            print(f"[SUCCESS] File saved to: {output_file}")
            
        except Exception as e:
            print(f"[ERROR] Save failed: {e}")
    else:
        print("[WARN] No data to save")

if __name__ == "__main__":
    scrape_00981a_data()