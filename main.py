import time
import json
import os
import glob
import pandas as pd
import yfinance as yf
import requests
import urllib3
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_previous_data(current_dir, target_folder, etf_code):
    """
    Finds the 'latest' CSV file for a specific ETF and returns a dictionary of {Stock Code: Shares}.
    """
    search_path = os.path.join(current_dir, target_folder, f"{etf_code}_Holdings_*.csv")
    files = sorted(glob.glob(search_path))
    
    valid_files = [f for f in files if not os.path.basename(f).startswith('~$')]
    
    if len(valid_files) < 1:
        return {}
    
    last_file = valid_files[-1]
    try:
        print(f"[INFO] Reading previous data for {etf_code}: {os.path.basename(last_file)}")
        df = pd.read_csv(last_file, skiprows=2, encoding='utf-8-sig')
        
        col_code = 'Stock Code' if 'Stock Code' in df.columns else '股票代號'
        col_shares = 'Shares' if 'Shares' in df.columns else '股數'
        
        stock_map = dict(zip(df[col_code].astype(str), df[col_shares]))
        return stock_map
    except Exception as e:
        print(f"[WARN] Failed to read previous file: {e}")
        return {}

def fetch_stock_prices(stock_codes, target_date_str, etf_code=''):
    """
    Fetches closing prices for stocks on a specific date using yfinance.
    Supports both Taiwan stocks and international stocks.
    For international stocks, will look back up to 5 days to find valid trading data.
    """
    if not stock_codes:
        return {}
    
    print(f"[INFO] Fetching closing prices for date: {target_date_str}...")
    
    target_date_obj = datetime.strptime(target_date_str, "%Y-%m-%d")

    price_map = {}
    
    # Separate Taiwan stocks and international stocks
    taiwan_stocks = []
    international_stocks = []
    
    for code in stock_codes:
        code_stripped = code.strip()
        # Check if it's an international stock (has suffix like US, JT, HK, etc.)
        if ' ' in code_stripped:
            parts = code_stripped.split()
            if len(parts) == 2 and parts[1] in ['US', 'JT', 'HK', 'GY', 'FP', 'UN', 'TT', 'CN']:
                international_stocks.append(code_stripped)
            else:
                taiwan_stocks.append(code_stripped)
        else:
            taiwan_stocks.append(code_stripped)
    
    print(f"[INFO] Taiwan stocks: {len(taiwan_stocks)}, International stocks: {len(international_stocks)}")
    
    # Fetch international stocks (with lookback for holidays)
    if international_stocks:
        print(f"[INFO] Fetching {len(international_stocks)} international stocks...")
        
        for code in international_stocks:
            try:
                parts = code.split()
                if len(parts) == 2:
                    ticker_symbol = parts[0]
                    exchange = parts[1]
                    
                    # Convert exchange code to yfinance ticker format
                    ticker_map = {
                        'US': ticker_symbol,  # US stocks don't need suffix
                        'TT': f"{ticker_symbol}.TW",  # Taiwan stocks
                        'HK': f"{ticker_symbol}.HK",  # Hong Kong
                        'JT': f"{ticker_symbol}.T",   # Japan (Tokyo)
                        'GY': f"{ticker_symbol}.DE",  # Germany
                        'FP': f"{ticker_symbol}.PA",  # France (Paris)
                        'UN': f"{ticker_symbol}.TO",  # Canada (Toronto)
                        'CN': f"{ticker_symbol}.SS",  # China (Shanghai)
                    }
                    
                    yf_ticker = ticker_map.get(exchange, ticker_symbol)
                    
                    # Try to fetch data, looking back up to 5 days for holidays
                    price_found = False
                    for days_back in range(6):  # Try today and up to 5 days back
                        try_date = target_date_obj - timedelta(days=days_back)
                        try_date_str = try_date.strftime("%Y-%m-%d")
                        next_day = try_date + timedelta(days=1)
                        next_day_str = next_day.strftime("%Y-%m-%d")
                        
                        try:
                            stock = yf.Ticker(yf_ticker)
                            hist = stock.history(start=try_date_str, end=next_day_str)
                            
                            if not hist.empty:
                                price = hist['Close'].iloc[-1]  # Get the last available price
                                price_map[code] = float(price)
                                if days_back > 0:
                                    print(f"[INFO] {code} ({yf_ticker}): {price:.2f} (from {try_date_str})")
                                else:
                                    print(f"[INFO] {code} ({yf_ticker}): {price:.2f}")
                                price_found = True
                                break
                        except:
                            continue
                    
                    if not price_found:
                        print(f"[WARN] No recent data for {code} ({yf_ticker})")
                        price_map[code] = 0.0
                        
            except Exception as e:
                print(f"[WARN] Failed to fetch {code}: {e}")
                price_map[code] = 0.0
    
    # Fetch Taiwan stocks (original logic)
    if taiwan_stocks:
        print(f"[INFO] Fetching {len(taiwan_stocks)} Taiwan stocks...")
        
        next_day = target_date_obj + timedelta(days=1)
        end_date_str = next_day.strftime("%Y-%m-%d")
        
        # Phase 1: Try .TW suffix
        tickers_tw = [f"{code}.TW" for code in taiwan_stocks]
        try:
            data = yf.download(tickers_tw, start=target_date_str, end=end_date_str, progress=False)
            
            if 'Close' in data and not data['Close'].empty:
                closes = data['Close'].iloc[0]
                for code in taiwan_stocks:
                    ticker = f"{code}.TW"
                    try:
                        price = closes[ticker]
                        if not pd.isna(price) and price > 0:
                            price_map[code] = float(price)
                    except:
                        pass
        except Exception as e:
            print(f"[WARN] TW download error: {e}")

        # Phase 2: Try .TWO suffix for missing stocks
        missing_codes = [code for code in taiwan_stocks if code not in price_map]
        
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

    # Fill 0 for stocks not found
    for code in stock_codes:
        if code not in price_map:
            price_map[code] = 0.0
    
    successful = sum(1 for v in price_map.values() if v > 0)
    print(f"[INFO] Successfully fetched {successful}/{len(stock_codes)} stock prices")
            
    return price_map

def scrape_00981a_data():
    """Scrape 00981A using Selenium (original method)"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

    print("\n[INFO] === Scraping 00981A ===")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    target_url = "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW"
    
    try:
        print(f"[INFO] Go to target URL: {target_url}")
        driver.get(target_url)
        time.sleep(3)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        data_div = soup.find('div', id='DataAsset')
        
        if not data_div or not data_div.get('data-content'):
            print("[ERROR] Data block not found.")
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
        print(f"[INFO] Found {len(raw_portfolio)} stocks")

        return {
            'etf_code': '00981A',
            'data_date': data_date,
            'net_asset': net_asset,
            'portfolio': raw_portfolio
        }

    except Exception as e:
        print(f"[ERROR] Error occurred: {e}")
        return None
    finally:
        driver.quit()
        print("[INFO] Browser closed")

def scrape_capital_fund_etf(etf_code, fund_id):
    """
    Generic scraper for Capital Fund ETFs using API request
    Args:
        etf_code: ETF code (e.g., '00982A', '00992A')
        fund_id: Fund ID for API request (e.g., '399', '500')
    """
    print(f"\n[INFO] === Scraping {etf_code} ===")
    
    url = "https://www.capitalfund.com.tw/CFWeb/api/etf/buyback"
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15',
        'Referer': f'https://www.capitalfund.com.tw/etf/product/detail/{fund_id}/portfolio'
    }
    payload = {"fundId": fund_id, "date": None}
    
    # Add retry logic
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                print(f"[INFO] Retry attempt {attempt + 1}/{max_retries}")
                time.sleep(2)  # Wait before retry
            
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') != 200:
                print(f"[ERROR] API returned error code: {data.get('code')}")
                if attempt < max_retries - 1:
                    continue
                return None
            
            pcf_data = data['data']['pcf']
            stocks_data = data['data']['stocks']
            
            net_asset = pcf_data.get('nav', 0)
            data_date = pcf_data.get('date1', time.strftime("%Y-%m-%d"))
            
            print(f"[DATA] Net Asset: {net_asset:,.0f}")
            print(f"[INFO] Data Date: {data_date}")
            print(f"[INFO] Found {len(stocks_data)} stocks")
            
            raw_portfolio = []
            for stock in stocks_data:
                raw_portfolio.append({
                    'code': stock.get('stocNo', '').strip(),
                    'name': stock.get('stocName', '').strip(),
                    'shares': float(stock.get('share', 0)),
                    'weight_str': f"{stock.get('weight', 0):.2f}%"
                })
            
            return {
                'etf_code': etf_code,
                'data_date': data_date,
                'net_asset': net_asset,
                'portfolio': raw_portfolio
            }
            
        except requests.Timeout:
            print(f"[WARN] Request timed out on attempt {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                continue
            print("[ERROR] All retry attempts failed due to timeout")
            return None
        except requests.RequestException as e:
            print(f"[ERROR] API request failed: {e}")
            if attempt < max_retries - 1:
                continue
            return None
        except Exception as e:
            print(f"[ERROR] Error occurred: {e}")
            return None
    
    return None

def scrape_00982a_data():
    """Scrape 00982A using API request"""
    return scrape_capital_fund_etf('00982A', '399')

def scrape_00992a_data():
    """Scrape 00992A using API request"""
    return scrape_capital_fund_etf('00992A', '500')

def scrape_nomura_etf(etf_code):
    """
    Generic scraper for Nomura Funds ETFs using API
    Args:
        etf_code: ETF code (e.g., '00980A', '00985A')
    """
    print(f"\n[INFO] === Scraping {etf_code} ===")
    
    url = "https://www.nomurafunds.com.tw/API/ETFAPI/api/Fund/GetFundAssets"
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15',
        'Referer': f'https://www.nomurafunds.com.tw/ETFWEB/product-description?fundNo={etf_code}&tab=basic',
        'Origin': 'https://www.nomurafunds.com.tw'
    }
    payload = {"FundID": etf_code, "SearchDate": None}
    
    # Retry logic
    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"[INFO] Attempt {attempt + 1}/{max_retries}...")
            # Disable SSL verification and increase timeout
            response = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('StatusCode') != 0:
                print(f"[ERROR] API returned error code: {data.get('StatusCode')}")
                return None
            
            entries = data.get('Entries', {})
            fund_data = entries.get('Data', {})
            
            # Get fund asset info
            fund_asset = fund_data.get('FundAsset', {})
            net_asset = float(fund_asset.get('Aum', 0))
            data_date_raw = fund_asset.get('NavDate', '')
            
            # Convert date format from "2026/01/05" to "2026-01-05"
            data_date = data_date_raw.replace('/', '-') if data_date_raw else time.strftime("%Y-%m-%d")
            
            print(f"[DATA] Net Asset: {net_asset:,.0f}")
            print(f"[INFO] Data Date: {data_date}")
            
            # Get stock holdings
            tables = fund_data.get('Table', [])
            stock_table = None
            
            for table in tables:
                if table.get('TableTitle') == '股票':
                    stock_table = table
                    break
            
            if not stock_table:
                print("[ERROR] Stock table not found")
                return None
            
            rows = stock_table.get('Rows', [])
            print(f"[INFO] Found {len(rows)} stocks")
            
            raw_portfolio = []
            for row in rows:
                if len(row) >= 4:
                    code = row[0].strip()
                    name = row[1].strip()
                    shares = float(row[2].replace(',', ''))
                    weight = row[3].strip()
                    
                    raw_portfolio.append({
                        'code': code,
                        'name': name,
                        'shares': shares,
                        'weight_str': f"{weight}%"
                    })
            
            return {
                'etf_code': etf_code,
                'data_date': data_date,
                'net_asset': net_asset,
                'portfolio': raw_portfolio
            }
            
        except requests.Timeout:
            print(f"[WARN] Request timed out on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                print(f"[INFO] Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            continue
        except requests.RequestException as e:
            print(f"[ERROR] API request failed: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                print(f"[INFO] Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            continue
        except Exception as e:
            print(f"[ERROR] Error occurred: {e}")
            return None
    
    print("[ERROR] All retry attempts failed")
    return None

def scrape_00980a_data():
    """Scrape 00980A using Nomura Funds API"""
    return scrape_nomura_etf('00980A')

def scrape_00985a_data():
    """Scrape 00985A using Nomura Funds API"""
    return scrape_nomura_etf('00985A')

def scrape_00991a_with_date(target_date):
    """Internal function to scrape 00991A with specific date"""
    url = f"https://www.fhtrust.com.tw/api/assets?fundID=ETF23&qDate={target_date}"
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
        'Referer': 'https://www.fhtrust.com.tw/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin'
    }
    
    max_retries = 2
    for attempt in range(max_retries):
        try:
            if attempt == 0:
                print(f"[INFO] Trying date: {target_date}")
            else:
                print(f"[INFO] Retry {attempt}/{max_retries} for date: {target_date}")
            
            response = requests.get(url, headers=headers, timeout=15)
            
            if not response.text:
                print("[ERROR] Empty response received")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return None
            
            response.raise_for_status()
            
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                print(f"[ERROR] JSON decode failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return None
            
            if data.get('status') != 0:
                print(f"[ERROR] API returned error code: {data.get('status')}")
                return None
            
            result = data.get('result', [])
            if not result:
                print("[ERROR] No data found in result")
                return None
            
            fund_data = result[0]
            
            # Get fund info - handle None values
            net_asset_raw = fund_data.get('pcf_FundNav', '0')
            if net_asset_raw is None:
                net_asset_raw = '0'
            net_asset = float(str(net_asset_raw).replace(',', ''))
            
            data_date_raw = fund_data.get('dDate', '')
            if data_date_raw is None:
                data_date_raw = ''
            
            # Convert date format from "2026/01/06" to "2026-01-06"
            data_date = data_date_raw.replace('/', '-') if data_date_raw else time.strftime("%Y-%m-%d")
            
            print(f"[DATA] Net Asset: {net_asset:,.0f}")
            print(f"[INFO] Data Date: {data_date}")
            
            # Get stock holdings
            details = fund_data.get('detail', [])
            
            # Check if details is None
            if details is None:
                print(f"[DEBUG] Details field is None for date {target_date}")
                
                # Try to find data in a different location
                if 'result' in fund_data and fund_data['result']:
                    print("[INFO] Checking nested 'result' field...")
                    details = fund_data.get('result', [])
                    if details is None:
                        details = []
                else:
                    details = []
            
            # Filter only stock type items
            stock_details = [d for d in details if d.get('ftype') == '股票']
            
            if not stock_details:
                print(f"[WARN] No stock data found for {target_date}")
                # Return empty result to try next date
                return {
                    'etf_code': '00991A',
                    'data_date': data_date,
                    'net_asset': net_asset,
                    'portfolio': []
                }
            
            print(f"[INFO] Found {len(stock_details)} stocks")
            
            raw_portfolio = []
            for stock in stock_details:
                code = stock.get('stockid', '').strip() if stock.get('stockid') else ''
                name = stock.get('stockname', '').strip() if stock.get('stockname') else ''
                
                # Handle qshare - could be string with commas or number
                qshare_raw = stock.get('qshare', 0)
                if isinstance(qshare_raw, str):
                    shares = float(qshare_raw.replace(',', ''))
                else:
                    shares = float(qshare_raw) if qshare_raw else 0.0
                
                weight = stock.get('prate_addaccint', '0%')
                if weight is None:
                    weight = '0%'
                weight = str(weight).strip()
                
                raw_portfolio.append({
                    'code': code,
                    'name': name,
                    'shares': shares,
                    'weight_str': weight
                })
            
            return {
                'etf_code': '00991A',
                'data_date': data_date,
                'net_asset': net_asset,
                'portfolio': raw_portfolio
            }
            
        except requests.Timeout:
            print(f"[WARN] Request timed out on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                time.sleep(2)
            continue
        except requests.RequestException as e:
            print(f"[ERROR] API request failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            continue
        except Exception as e:
            print(f"[ERROR] Error occurred: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    print("[ERROR] All retry attempts failed")
    return None

def scrape_00991a_data():
    """Scrape 00991A using Fuh Hwa Funds API"""
    print(f"\n[INFO] === Scraping 00991A ===")
    
    # Try multiple dates (today and past few days)
    dates_to_try = []
    for i in range(5):  # Try today and past 4 days
        date_obj = datetime.now() - timedelta(days=i)
        dates_to_try.append(date_obj.strftime("%Y/%m/%d"))
    
    for try_date in dates_to_try:
        result = scrape_00991a_with_date(try_date)
        if result and result.get('portfolio'):  # Check if we got actual data
            return result
        elif result:  # Got response but no data
            print(f"[INFO] No data for {try_date}, trying earlier date...")
    
    print("[ERROR] No data found for any recent dates")
    return None

def scrape_00986a_data():
    """Scrape 00986A using Selenium from Taiwan Shin Kong Securities Investment Trust"""
    print(f"\n[INFO] === Scraping 00986A ===")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    target_url = "https://www.tsit.com.tw/ETF/Home/ETFSeriesDetail/00986A"
    
    try:
        print(f"[INFO] Go to target URL: {target_url}")
        driver.get(target_url)
        time.sleep(8)  # Wait for page to fully load
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Get data date from hidden input
        data_date_input = soup.find('input', {'id': 'PUB_DATE'})
        if data_date_input:
            data_date = data_date_input.get('value', '')
            print(f"[INFO] Data Date: {data_date}")
        else:
            data_date = time.strftime("%Y-%m-%d")
            print("[WARN] Could not find data date, using current date")
        
        # Get net asset from the table
        net_asset = 0
        table_rows = soup.find_all('tr')
        for row in table_rows:
            th = row.find('th')
            if th and '基金淨資產價值' in th.get_text():
                td = row.find('td')
                if td:
                    # Extract number from "TWD 785,281,163"
                    text = td.get_text().strip()
                    net_asset_str = text.replace('TWD', '').replace(',', '').strip()
                    try:
                        net_asset = float(net_asset_str)
                        print(f"[DATA] Net Asset: {net_asset:,.0f}")
                    except:
                        print("[WARN] Could not parse net asset")
        
        # Find the stock holdings table
        raw_portfolio = []
        
        # Find all panels with "股票" in heading
        panels = soup.find_all('div', class_='panel-heading')
        stock_panel = None
        for panel in panels:
            if '股票' in panel.get_text():
                stock_panel = panel
                break
        
        if stock_panel:
            panel_body = stock_panel.find_next_sibling('div', class_='panel-body')
            
            if panel_body:
                table = panel_body.find('table')
                
                if table:
                    tbody = table.find('tbody')
                    
                    if tbody:
                        rows = tbody.find_all('tr')
                        
                        for row in rows:
                            cols = row.find_all('td')
                            
                            if len(cols) >= 4:
                                col0 = cols[0].get_text().strip()
                                col1 = cols[1].get_text().strip()
                                col2 = cols[2].get_text().strip()
                                col3 = cols[3].get_text().strip()
                                
                                # Skip summary row
                                if '股票合計' in col0 or '合計' in col1:
                                    continue
                                
                                code = col0
                                name = col1
                                shares_str = col2.replace(',', '')
                                weight = col3
                                
                                try:
                                    shares = float(shares_str)
                                    raw_portfolio.append({
                                        'code': code,
                                        'name': name,
                                        'shares': shares,
                                        'weight_str': weight
                                    })
                                except ValueError as e:
                                    print(f"[WARN] Could not parse shares for {code}: {e}")
                                    continue
        
        print(f"[INFO] Found {len(raw_portfolio)} stocks")
        
        if not raw_portfolio:
            print("[ERROR] No stock data found")
            return None
        
        return {
            'etf_code': '00986A',
            'data_date': data_date,
            'net_asset': net_asset,
            'portfolio': raw_portfolio
        }
        
    except Exception as e:
        print(f"[ERROR] Error occurred: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        driver.quit()
        print("[INFO] Browser closed")

def process_etf_data(etf_data, output_dir):
    """Process and save ETF data to CSV"""
    if not etf_data:
        return
    
    etf_code = etf_data['etf_code']
    data_date = etf_data['data_date']
    net_asset = etf_data['net_asset']
    raw_portfolio = etf_data['portfolio']

    for item in raw_portfolio:
        item['code'] = str(item['code']).strip().replace('$', '')
    
    # Get previous data for comparison
    current_dir = os.path.dirname(os.path.abspath(__file__))
    prev_shares_map = get_previous_data(current_dir, "data", etf_code)
    
    # Get stock prices
    all_codes = [p['code'] for p in raw_portfolio]
    price_map = fetch_stock_prices(all_codes, data_date, etf_code)
    
    # Calculate final data
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
    
    # Save to CSV
    try:
        file_name = f"{etf_code}_Holdings_{data_date.replace('-', '')}.csv"
        output_file = os.path.join(output_dir, file_name)

        df_stocks = pd.DataFrame(final_portfolio)
        
        # Create header info
        header_info = f"Date,{data_date},Net Asset,{net_asset}\n"
        
        # Write to CSV with header
        with open(output_file, 'w', encoding='utf-8-sig') as f:
            f.write(header_info)
            f.write("\n")  # Empty line separator
            df_stocks.to_csv(f, index=False)

        print(f"[SUCCESS] File saved to: {output_file}")
        
    except Exception as e:
        print(f"[ERROR] Save failed: {e}")

def main():
    """Main execution function"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, "data")
    os.makedirs(output_dir, exist_ok=True)
    
    print("=== ETF Holdings Scraper ===")
    print("Target ETFs: 00980A, 00981A, 00982A, 00985A, 00986A, 00991A, 00992A\n")
    
    # Scrape 00980A
    etf_980a = scrape_00980a_data()
    if etf_980a:
        process_etf_data(etf_980a, output_dir)
    else:
        print("[WARN] 00980A scraping failed")
    
    # Scrape 00981A
    etf_981a = scrape_00981a_data()
    if etf_981a:
        process_etf_data(etf_981a, output_dir)
    else:
        print("[WARN] 00981A scraping failed")
    
    # Scrape 00982A
    etf_982a = scrape_00982a_data()
    if etf_982a:
        process_etf_data(etf_982a, output_dir)
    else:
        print("[WARN] 00982A scraping failed")
    
    # Scrape 00985A
    etf_985a = scrape_00985a_data()
    if etf_985a:
        process_etf_data(etf_985a, output_dir)
    else:
        print("[WARN] 00985A scraping failed")
    
    # Scrape 00986A
    etf_986a = scrape_00986a_data()
    if etf_986a:
        process_etf_data(etf_986a, output_dir)
    else:
        print("[WARN] 00986A scraping failed")
    
    # Scrape 00991A
    etf_991a = scrape_00991a_data()
    if etf_991a:
        process_etf_data(etf_991a, output_dir)
    else:
        print("[WARN] 00991A scraping failed")
    
    # Scrape 00992A
    etf_992a = scrape_00992a_data()
    if etf_992a:
        process_etf_data(etf_992a, output_dir)
    else:
        print("[WARN] 00992A scraping failed")
    
    print("\n=== All Done ===")

if __name__ == "__main__":
    main()