import streamlit as st
import pandas as pd
import os
import glob
import plotly.express as px

# --- Page Config ---
st.set_page_config(page_title="00981A Holdings Tracker", layout="wide")
st.title("00981A ETF Holdings Tracker")

# --- 1. Helper: Get available files ---
@st.cache_data
def get_available_files():
    files = glob.glob(os.path.join("data", "*.xlsx"))
    file_map = {}
    dates = []
    
    for f in files:
        filename = os.path.basename(f)
        try:
            # Parse filename: 00981A_Holdings_20251231.xlsx
            date_str = filename.split('_')[-1].replace('.xlsx', '')
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            dates.append(formatted_date)
            file_map[formatted_date] = f
        except:
            continue
            
    return sorted(dates, reverse=True), file_map

# --- 2. Helper: Load specific Excel file ---
@st.cache_data
def load_data(filepath):
    try:
        # Read Header (A1:D1)
        header_df = pd.read_excel(filepath, nrows=1, header=None)
        
        # Check if the file is in English or Chinese format to avoid errors
        # Column 1 is Date, Column 3 is Net Asset Value
        data_date = header_df.iloc[0, 1]
        net_asset = header_df.iloc[0, 3]

        # Read Stock List (skip first 2 rows)
        df_stocks = pd.read_excel(filepath, skiprows=2)
        
        # Determine column name for 'Weight' based on language
        weight_col = 'Weight' if 'Weight' in df_stocks.columns else '持股權重'
        
        # Data Cleaning: Remove '%' and convert to float
        df_stocks['Weight_Val'] = df_stocks[weight_col].astype(str).str.replace('%', '').astype(float)
        
        return data_date, net_asset, df_stocks, weight_col
    except Exception as e:
        st.error(f"Failed to load file: {e}")
        return None, None, None, None

# --- Main Logic ---

available_dates, file_map = get_available_files()

if not available_dates:
    st.warning("No data found in 'data' folder. Please wait for the scraper to run.")
    st.stop()

# Sidebar: Date Filter
with st.sidebar:
    st.header("Date Filter")
    selected_date = st.selectbox("Select Date", available_dates)
    target_file = file_map[selected_date]
    st.caption(f"Loaded: {os.path.basename(target_file)}")

# Load Data
data_date_str, net_asset_val, df_stocks, weight_col = load_data(target_file)

if df_stocks is not None:
    # Key Metrics
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Date", data_date_str)
    with col2:
        # Use comma for thousands separator
        st.metric("Net Asset Value", f"{net_asset_val:,.0f} TWD")
    
    st.divider()

    # Tabs
    tab1, tab2 = st.tabs(["Weight Distribution", "Holdings List"])

    with tab1:
        # Determine Name column
        name_col = 'Stock Name' if 'Stock Name' in df_stocks.columns else '股票名稱'
        code_col = 'Stock Code' if 'Stock Code' in df_stocks.columns else '股票代號'
        shares_col = 'Shares' if 'Shares' in df_stocks.columns else '股數'

        fig = px.bar(
            df_stocks, 
            x=name_col, 
            y='Weight_Val',
            hover_data=[code_col, shares_col, weight_col],
            labels={'Weight_Val': 'Weight (%)'},
            title=f"Top Holdings Weight Distribution ({selected_date})",
            color='Weight_Val',
            color_continuous_scale=px.colors.sequential.Viridis
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        # Display Table
        display_df = df_stocks.drop(columns=['Weight_Val'])
        
        # Dynamic column config
        col_config = {}
        if shares_col in display_df.columns:
            col_config[shares_col] = st.column_config.NumberColumn(format="%d")

        st.dataframe(
            display_df,
            column_config=col_config,
            use_container_width=True,
            hide_index=True
        )

else:
    st.error("Data could not be displayed.")