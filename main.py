import time
import json
import os
import glob
import re
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# ===== ETF 配置清單 =====
ETF_CONFIG = {
    '00981A': {
        'source': 'ezmoney',
        'fund_code': '49YTW',
        'url': 'https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW'
    },
    '00982A': {
        'source': 'capital',
        'product_id': '399',
        'url': 'https://www.capitalfund.com.tw/etf/product/detail/399/portfolio'
    },
    '00992A': {
        'source': 'capital',
        'product_id': '500',
        'url': 'https://www.capitalfund.com.tw/etf/product/detail/500/portfolio'
    },
}

def get_previous_data(current_dir, target_folder, etf_code):
    """取得上一次的持股資料"""
    search_path = os.path.join(current_dir, target_folder, f"{etf_code}_Holdings_*.xlsx")
    files = sorted(glob.glob(search_path))
    valid_files = [f for f in files if not os.path.basename(f).startswith('~$')]
    
    if len(valid_files) < 1:
        return {}
    
    last_file = valid_files[-1]
    try:
        print(f"[INFO] Reading previous data: {os.path.basename(last_file)}")
        df = pd.read_excel(last_file, skiprows=2)
        col_code = 'Stock Code' if 'Stock Code' in df.columns else '股票代號'
        col_shares = 'Shares' if 'Shares' in df.columns else '股數'
        stock_map = dict(zip(df[col_code].astype(str), df[col_shares]))
        return stock_map
    except Exception as e:
        print(f"[WARN] Failed to read previous file: {e}")
        return {}

def fetch_stock_prices(stock_codes, target_date_str):
    """使用 yfinance 抓取股票收盤價"""
    if not stock_codes:
        return {}
    
    print(f"[INFO] Fetching prices for {target_date_str}...")
    print(f"[INFO] Stock codes: {stock_codes}")
    
    target_date_obj = datetime.strptime(target_date_str, "%Y-%m-%d")
    next_day = target_date_obj + timedelta(days=1)
    end_date_str = next_day.strftime("%Y-%m-%d")
    price_map = {}
    
    # Phase 1: .TW (上市)
    tickers_tw = [f"{code}.TW" for code in stock_codes]
    try:
        data = yf.download(tickers_tw, start=target_date_str, end=end_date_str, progress=False)
        if 'Close' in data and not data['Close'].empty:
            closes = data['Close'].iloc[0] if len(data['Close']) > 0 else data['Close']
            for code in stock_codes:
                ticker = f"{code}.TW"
                try:
                    price = closes[ticker] if isinstance(closes, pd.Series) else closes
                    if not pd.isna(price) and price > 0:
                        price_map[code] = float(price)
                        print(f"[DEBUG] {code}: NT$ {price:.2f} (.TW)")
                except:
                    pass
    except Exception as e:
        print(f"[WARN] TW download error: {e}")

    # Phase 2: .TWO (上櫃)
    missing_codes = [code for code in stock_codes if code not in price_map]
    if missing_codes:
        print(f"[INFO] Retrying {len(missing_codes)} stocks with .TWO...")
        tickers_two = [f"{code}.TWO" for code in missing_codes]
        try:
            data = yf.download(tickers_two, start=target_date_str, end=end_date_str, progress=False)
            if 'Close' in data and not data['Close'].empty:
                closes = data['Close'].iloc[0] if len(data['Close']) > 0 else data['Close']
                for code in missing_codes:
                    ticker = f"{code}.TWO"
                    try:
                        price = closes[ticker] if isinstance(closes, pd.Series) else closes
                        if not pd.isna(price) and price > 0:
                            price_map[code] = float(price)
                            print(f"[DEBUG] {code}: NT$ {price:.2f} (.TWO)")
                    except:
                        pass
        except Exception as e:
            print(f"[WARN] TWO download error: {e}")

    # 報告找不到的股票
    still_missing = [code for code in stock_codes if code not in price_map]
    if still_missing:
        print(f"[WARN] Could not find prices for: {still_missing}")
        print(f"[INFO] These may be delisted, emerging market, or invalid codes")
    
    # 填入 0 給找不到的股票
    for code in stock_codes:
        if code not in price_map:
            price_map[code] = 0.0
            
    print(f"[INFO] Price fetch success: {len([v for v in price_map.values() if v > 0])}/{len(stock_codes)}")
    return price_map

def scrape_capital_etf(driver, etf_code, target_url):
    """抓取群益投信官網的 ETF 資料"""
    print(f"[INFO] Scraping Capital ETF: {etf_code}")
    print(f"[INFO] URL: {target_url}")
    
    try:
        driver.get(target_url)
        print("[INFO] Page loaded, waiting for content...")
        time.sleep(5)
        
        # 嘗試點擊「展開全部」按鈕
        try:
            print("[INFO] Looking for 'expand all' button...")
            # 尋找包含「展開全部」或「顯示全部」文字的按鈕
            expand_buttons = driver.find_elements(By.XPATH, "//*[contains(text(), '展開全部') or contains(text(), '顯示全部')]")
            
            if expand_buttons:
                print(f"[INFO] Found {len(expand_buttons)} expand button(s), clicking...")
                for btn in expand_buttons:
                    try:
                        # 使用 JavaScript 點擊,避免被其他元素擋住
                        driver.execute_script("arguments[0].click();", btn)
                        print("[INFO] Clicked expand button with JavaScript")
                        time.sleep(3)  # 等待展開動畫完成
                        break  # 只點擊第一個找到的按鈕
                    except Exception as e:
                        print(f"[WARN] Failed to click button: {e}")
            else:
                print("[INFO] No expand button found, may already showing all")
        except Exception as e:
            print(f"[WARN] Error finding expand button: {e}")
        
        page_source = driver.page_source
        if len(page_source) < 1000:
            print("[ERROR] Page content too short")
            return None
        
        print(f"[DEBUG] Page source length: {len(page_source)}")
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # 找資料日期
        data_date = datetime.now().strftime("%Y-%m-%d")
        date_elements = soup.find_all(string=lambda text: text and '資料日期' in text)
        if date_elements:
            date_text = date_elements[0]
            date_match = re.search(r'(\d{4})[/\-年](\d{1,2})[/\-月](\d{1,2})', date_text)
            if date_match:
                year = int(date_match.group(1))
                if year > datetime.now().year:
                    year = datetime.now().year
                data_date = f"{year}-{date_match.group(2).zfill(2)}-{date_match.group(3).zfill(2)}"
        
        print(f"[INFO] Data Date: {data_date}")
        
        # 找持股表格: pct-stock-table-tbody
        table = soup.find('div', class_='pct-stock-table-tbody')
        
        if not table:
            print("[ERROR] Portfolio table not found")
            return None
        
        print(f"[INFO] Found portfolio table")
        
        raw_portfolio = []
        rows = table.find_all('div', class_='tr')
        print(f"[DEBUG] Found {len(rows)} holdings")
        
        for idx, row in enumerate(rows):
            try:
                ths = row.find_all('div', class_='th')
                tds = row.find_all('div', class_='td')
                
                if len(ths) >= 2 and len(tds) >= 2:
                    # 清理股票代碼 - 移除特殊符號
                    code = ths[0].get_text(strip=True)
                    code = code.replace('$', '').replace('￥', '').replace('¥', '').strip()
                    
                    name = ths[1].get_text(strip=True)
                    weight_text = tds[0].get_text(strip=True)
                    shares_text = tds[-1].get_text(strip=True).replace(',', '')
                    
                    if idx < 3:
                        print(f"[DEBUG] Row {idx}: code={code}, name={name}, weight={weight_text}, shares={shares_text}")
                    
                    if code and (code.isdigit() or len(code) == 4):
                        raw_portfolio.append({
                            'code': code,
                            'name': name,
                            'shares': float(shares_text) if shares_text and shares_text.replace('.', '').isdigit() else 0,
                            'weight_str': weight_text
                        })
            except Exception as e:
                print(f"[WARN] Error parsing row {idx}: {e}")
                continue
        
        # 取得淨資產
        net_asset = 0
        nav_table = soup.find('div', class_='table--product_buyback')
        if nav_table:
            nav_rows = nav_table.find_all('div', class_='tr')
            for row in nav_rows:
                th = row.find('div', class_='th')
                td = row.find('div', class_='td')
                if th and td and '基金淨資產價值' in th.get_text():
                    nav_text = td.get_text(strip=True).replace('TWD', '').replace(',', '').strip()
                    try:
                        net_asset = float(nav_text)
                        print(f"[INFO] Net Asset: {net_asset:,.0f}")
                    except:
                        pass
        
        print(f"[INFO] Found {len(raw_portfolio)} holdings")
        
        if not raw_portfolio:
            print("[WARN] No portfolio data found")
            return None
        
        return {
            'data_date': data_date,
            'net_asset': net_asset,
            'portfolio': raw_portfolio
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to scrape: {e}")
        import traceback
        traceback.print_exc()
        return None

def scrape_ezmoney_etf(driver, etf_code, target_url):
    """抓取 ezmoney 的 ETF 資料"""
    print(f"[INFO] Scraping EZMoney ETF: {etf_code}")
    
    driver.get(target_url)
    time.sleep(3)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    data_div = soup.find('div', id='DataAsset')
    
    if not data_div or not data_div.get('data-content'):
        print("[ERROR] Data block not found")
        return None

    try:
        asset_data = json.loads(data_div.get('data-content'))
        print("[INFO] Successfully extracted JSON data")
    except json.JSONDecodeError:
        print("[ERROR] JSON extraction failed")
        return None

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
    
    print(f"[INFO] Data Date: {data_date}")
    print(f"[INFO] Found {len(raw_portfolio)} holdings")
    
    return {
        'data_date': data_date,
        'net_asset': net_asset,
        'portfolio': raw_portfolio
    }

def process_etf_data(etf_code, etf_data, current_dir, target_folder):
    """處理並儲存 ETF 資料"""
    
    data_date = etf_data['data_date']
    net_asset = etf_data['net_asset']
    raw_portfolio = etf_data['portfolio']
    
    # 取得歷史資料
    prev_shares_map = get_previous_data(current_dir, target_folder, etf_code)
    
    # 取得股價
    all_codes = [p['code'] for p in raw_portfolio]
    price_map = fetch_stock_prices(all_codes, data_date)
    
    # 計算並合併
    final_portfolio = []
    for item in raw_portfolio:
        code = item['code']
        shares = item['shares']
        price = price_map.get(code, 0)
        market_value = shares * price
        prev_share = prev_shares_map.get(code, 0)
        share_change = shares - prev_share
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

    # 儲存檔案
    if final_portfolio:
        try:
            file_name = f"{etf_code}_Holdings_{data_date.replace('-', '')}.xlsx"
            output_file = os.path.join(current_dir, target_folder, file_name)

            df_stocks = pd.DataFrame(final_portfolio)
            header_row = ['Date', data_date, 'Net Asset', net_asset]
            header_df = pd.DataFrame([header_row])

            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                header_df.to_excel(writer, index=False, header=False, startrow=0)
                df_stocks.to_excel(writer, index=False, startrow=2)

            print(f"[SUCCESS] File saved: {output_file}")
            return True
        except Exception as e:
            print(f"[ERROR] Save failed: {e}")
            return False
    return False

def scrape_etf(etf_code, config):
    """統一的 ETF 抓取介面"""
    print(f"\n{'='*60}")
    print(f"[START] Processing {etf_code}")
    print(f"{'='*60}")
    
    # Setup Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    try:
        # 根據來源選擇抓取方式
        if config['source'] == 'capital':
            etf_data = scrape_capital_etf(driver, etf_code, config['url'])
        elif config['source'] == 'ezmoney':
            etf_data = scrape_ezmoney_etf(driver, etf_code, config['url'])
        else:
            print(f"[ERROR] Unknown source: {config['source']}")
            return False
        
        if not etf_data:
            return False
        
        # 處理並儲存資料
        current_dir = os.path.dirname(os.path.abspath(__file__))
        target_folder = "data"
        os.makedirs(os.path.join(current_dir, target_folder), exist_ok=True)
        
        return process_etf_data(etf_code, etf_data, current_dir, target_folder)
        
    except Exception as e:
        print(f"[ERROR] {etf_code} failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        driver.quit()
        print(f"[INFO] Browser closed for {etf_code}")

def main():
    """主程式"""
    print("=" * 60)
    print("ETF Holdings Scraper - Multi-Source Version")
    print("=" * 60)
    
    # 選擇要抓取的 ETF
    etf_list = ['00981A', '00982A', '00992A']
    
    success_count = 0
    fail_count = 0
    
    for etf_code in etf_list:
        if etf_code not in ETF_CONFIG:
            print(f"[WARN] {etf_code} not in config, skipping...")
            fail_count += 1
            continue
        
        config = ETF_CONFIG[etf_code]
        result = scrape_etf(etf_code, config)
        
        if result:
            success_count += 1
        else:
            fail_count += 1
        
        time.sleep(2)  # 避免請求過快
    
    print("\n" + "=" * 60)
    print(f"[SUMMARY] Success: {success_count}, Failed: {fail_count}")
    print("=" * 60)

if __name__ == "__main__":
    main()