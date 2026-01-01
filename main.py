import time
import json
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

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

        # --- 3. Extract Data ---
        net_asset = 0
        portfolio_list = []
        data_date = time.strftime("%Y-%m-%d")

        for item in asset_data:
            code = item.get('AssetCode', '')
            
            # A. Net Asset Value (NAV)
            if code == 'NAV':
                net_asset = item.get('Value', 0)
                print(f"[DATA] Net Asset: {net_asset:,.0f}")
            
            # B. Holdings List (ST)
            elif code == 'ST':
                details = item.get('Details', [])
                if details and 'TranDate' in details[0]:
                    data_date = details[0]['TranDate'].split('T')[0]

                for stock in details:
                    portfolio_list.append({
                        'Stock Code': stock.get('DetailCode', ''),
                        'Stock Name': stock.get('DetailName', ''),
                        'Shares': stock.get('Share', 0),
                        'Weight': f"{stock.get('NavRate', 0)}%" 
                    })
                print(f"[INFO] Successfully extracted {len(portfolio_list)} stocks")

    except Exception as e:
        print(f"[ERROR] Error occurred: {e}")
        return
    finally:
        driver.quit()
        print("[INFO] Browser closed")

    # --- 4. Save to Excel ---
    if portfolio_list:
        try:
            # 1. Set paths
            current_dir = os.path.dirname(os.path.abspath(__file__))
            target_folder = "data"
            output_dir = os.path.join(current_dir, target_folder)
            
            # Create directory if not exists
            os.makedirs(output_dir, exist_ok=True)

            # 2. File name
            file_name = f"00981A_Holdings_{data_date.replace('-', '')}.xlsx"
            output_file = os.path.join(output_dir, file_name)

            df_stocks = pd.DataFrame(portfolio_list)
            
            # Header info (English)
            header_row = ['Date', data_date, 'Net Asset', net_asset]
            header_df = pd.DataFrame([header_row])

            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Write Header (Row 1)
                header_df.to_excel(writer, index=False, header=False, startrow=0)
                # Write Stock List (Row 3)
                df_stocks.to_excel(writer, index=False, startrow=2)

            print(f"[SUCCESS] File saved to: {output_file}")
            
        except Exception as e:
            print(f"[ERROR] Save failed: {e}")
    else:
        print("[WARN] No data to save")

if __name__ == "__main__":
    scrape_00981a_data()