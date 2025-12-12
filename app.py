import streamlit as st
import pandas as pd
import io
import re
import traceback

# ==========================================
# 1. SMART HEADER & SHEET DETECTION
# ==========================================
def find_header_row_index(df, keywords, search_limit=20):
    """
    Scans rows to find the one containing specific keywords.
    Returns the index of that row.
    """
    if isinstance(keywords, str):
        keywords = [keywords]
        
    for idx, row in df.head(search_limit).iterrows():
        # Convert row to string, handle newlines and spaces
        row_str = " ".join(row.astype(str)).lower()
        row_str = row_str.replace("\n", " ").replace("  ", " ")
        
        # Check if ANY of the keywords exist in this row
        for kw in keywords:
            if kw.lower() in row_str:
                return idx
    return None

def read_excel_smart(file, target_sheet_names, header_keywords, dtype=None):
    """
    Tries to find the correct sheet and the correct header row automatically.
    """
    xl = pd.ExcelFile(file)
    sheet_names = xl.sheet_names
    
    # 1. Try finding specific sheet names (case insensitive)
    target_sheet = None
    for sheet in sheet_names:
        if sheet.lower() in [name.lower() for name in target_sheet_names]:
            target_sheet = sheet
            break
            
    # If explicit sheet not found, we will iterate through ALL sheets to find data
    sheets_to_scan = [target_sheet] if target_sheet else sheet_names
    
    for sheet in sheets_to_scan:
        try:
            # Read first 20 rows to find header
            raw_df = pd.read_excel(file, sheet_name=sheet, header=None, nrows=20)
            header_idx = find_header_row_index(raw_df, header_keywords)
            
            if header_idx is not None:
                # Found the right sheet and right header!
                file.seek(0)
                df = pd.read_excel(file, sheet_name=sheet, header=header_idx, dtype=dtype)
                return df
        except Exception:
            continue
            
    return None

def load_multiple_files(uploaded_files, target_sheet_names, header_keywords, dtype=None):
    if not uploaded_files:
        return None
    
    df_list = []
    for file in uploaded_files:
        try:
            df = read_excel_smart(file, target_sheet_names, header_keywords, dtype)
            if df is not None:
                df_list.append(df)
            else:
                st.warning(f"⚠️ Could not find valid data in file: {file.name}. Skipped.")
        except Exception as e:
            st.error(f"Error reading {file.name}: {e}")
            
    if df_list:
        return pd.concat(df_list, ignore_index=True)
    return None

# ==========================================
# 2. CORE PROCESSING LOGIC
# ==========================================
class ReconciliationEngine:
    def __init__(self):
        self.logs = []

    def log(self, message):
        self.logs.append(message)

    def clean_sku(self, sku):
        if pd.isna(sku):
            return ""
        sku = str(sku)
        sku = re.sub(r'SKU:', '', sku, flags=re.IGNORECASE)
        sku = sku.replace('"""', '')
        return sku.strip()

    def find_column_by_substring(self, df, substring):
        for col in df.columns:
            col_clean = str(col).replace("\n", " ").strip()
            if substring.lower() in col_clean.lower():
                return col
        return None

    def process(self, sales_df, settlement_df, cost_df, auto_clean_sku=True, handle_merged=True):
        self.logs = [] 
        
        try:
            # --- 1. PROCESS SALES REPORT ---
            if sales_df is None or sales_df.empty:
                 return None, None, ["Error: No valid Sales Data found. Check if sheet 'Sales Report' exists."]

            # Standardize Columns
            sales_cols_map = {
                'Order Item ID': 'Order Item ID', 
                'Order Date': 'Order Date',
                'SKU': 'SKU',
                'Item Quantity': 'Item Quantity',
                'Final Invoice Amount (Price after discount+Shipping Charges)': 'Final Invoice Amount'
            }
            
            # Fuzzy match for column names
            sales_df.columns = [str(c).strip().replace("\n", " ") for c in sales_df.columns]
            
            # Find 'Order Item ID' specifically
            oid_col = self.find_column_by_substring(sales_df, "Order Item ID")
            if oid_col:
                sales_df.rename(columns={oid_col: 'Order Item ID'}, inplace=True)
            
            sales_df.rename(columns=sales_cols_map, inplace=True)
            
            # Clean Data Types
            if 'Item Quantity' in sales_df.columns:
                sales_df['Item Quantity'] = pd.to_numeric(sales_df['Item Quantity'], errors='coerce').fillna(0)
            if 'Final Invoice Amount' in sales_df.columns:
                sales_df['Final Invoice Amount'] = pd.to_numeric(sales_df['Final Invoice Amount'], errors='coerce').fillna(0)

            if auto_clean_sku and 'SKU' in sales_df.columns:
                sales_df['SKU'] = sales_df['SKU'].apply(self.clean_sku)

            # --- 2. PROCESS SETTLEMENT REPORT ---
            if settlement_df is None or settlement_df.empty:
                self.log("Error: No valid Settlement Data found. Check if sheet 'Orders' exists.")
                return None, None, self.logs

            if handle_merged:
                settlement_df = settlement_df.ffill() 

            # Identify "Bank Settlement Value"
            bsv_col = self.find_column_by_substring(settlement_df, "Bank Settlement Value")
            if not bsv_col:
                bsv_col = self.find_column_by_substring(settlement_df, "Settlement Value") # Fallback
            
            if not bsv_col:
                self.log("Error: Could not locate 'Bank Settlement Value' column.")
                self.log(f"Columns found: {list(settlement_df.columns)}")
                return None, None, self.logs

            # Identify "Order Item ID" in Settlement
            oid_col_settlement = self.find_column_by_substring(settlement_df, "Order Item ID")
            if not oid_col_settlement:
                 # Fallback to index 8 if names fail (Common structure)
                 if len(settlement_df.columns) > 8:
                    oid_col_settlement = settlement_df.columns[8]
            
            # Normalize Settlement Data
            settlement_df[oid_col_settlement] = settlement_df[oid_col_settlement].astype(str).str.strip()
            settlement_df[bsv_col] = pd.to_numeric(settlement_df[bsv_col], errors='coerce').fillna(0)

            # Aggregate Duplicates
            settlement_agg = settlement_df.groupby(oid_col_settlement)[bsv_col].sum().reset_index()
            settlement_agg.rename(columns={bsv_col: 'Bank Settlement Value (Rs.)', oid_col_settlement: 'Order Item ID'}, inplace=True)

            # --- 3. PROCESS COST PRICE ---
            if cost_df is not None and not cost_df.empty:
                cost_df.columns = [str(c).strip() for c in cost_df.columns]
                # Find SKU and Cost cols fuzzily
                c_sku = self.find_column_by_substring(cost_df, "SKU")
                c_price = self.find_column_by_substring(cost_df, "Cost Price")
                
                if c_sku and c_price:
                    cost_df.rename(columns={c_sku: 'SKU', c_price: 'Cost Price'}, inplace=True)
                    cost_df['SKU'] = cost_df['SKU'].astype(str).str.strip()
                    if auto_clean_sku:
                         cost_df['SKU'] = cost_df['SKU'].apply(self.clean_sku)
                else:
                    cost_df = pd.DataFrame(columns=['SKU', 'Cost Price'])
            else:
                cost_df = pd.DataFrame(columns=['SKU', 'Cost Price'])

            # --- 4. MERGING ---
            if 'Order Item ID' in sales_df.columns:
                sales_df['Order Item ID'] = sales_df['Order Item ID'].astype(str).str.strip()
                merged_df = pd.merge(sales_df, settlement_agg, on='Order Item ID', how='left')
            else:
                 return None, None, ["Error: 'Order Item ID' column missing in Sales Report."]

            if 'SKU' in sales_df.columns:
                merged_df = pd.merge(merged_df, cost_df[['SKU', 'Cost Price']], on='SKU', how='left')
            else:
                merged_df['Cost Price'] = 0

            # --- 5. CALCULATIONS ---
            merged_df.fillna({
                'Bank Settlement Value (Rs.)': 0,
                'Cost Price': 0
            }, inplace=True)

            merged_df['Profit/Loss'] = merged_df['Bank Settlement Value (Rs.)'] - (merged_df['Cost Price'] * merged_df['Item Quantity'])
            
            unmatched_rows = merged_df[merged_df['Bank Settlement Value (Rs.)'] == 0]

            # Output formatting
            merged_df['Order Item ID'] = "'" + merged_df['Order Item ID'].astype(str)

            final_columns = [
                'Order Date', 'Order Item ID', 'SKU', 'Item Quantity', 
                'Final Invoice Amount', 'Bank Settlement Value (Rs.)', 
                'Cost Price', 'Profit/Loss'
            ]
            final_columns = [c for c in final_columns if c in merged_df.columns]
            final_output = merged_df[final_columns]

            stats = {
                "Total Orders": len(final_output),
                "Matched with Settlement": len(final_output) - len(unmatched_rows),
                "Unmatched": len(unmatched_rows),
                "Total Settlement Amount": final_output['Bank Settlement Value (Rs.)'].sum(),
                "Total Invoice Amount": final_output['Final Invoice Amount'].sum() if 'Final Invoice Amount' in final_output else 0
            }

            return final_output, stats, self.logs

        except Exception as e:
            self.log(f"Critical Error: {str(e)}")
            self.log(traceback.format_exc())
            return None, None, self.logs

# ==========================================
# 3. STREAMLIT UI
# ==========================================

st.set_page_config(page_title="Flipkart Reconciliation Tool", layout="wide")
st.title("📊 Flipkart Reconciliation Tool")

# --- SIDEBAR CONFIGURATION ---
st.sidebar.header("1. Upload Files")

# 1. SALES UPLOAD
st.sidebar.subheader("Sales Reports")
sales_files = st.sidebar.file_uploader(
    "Upload Sales (e.g. Sales Report.xlsx)", 
    type=['xlsx', 'xls'], 
    accept_multiple_files=True,
    key="sales"
)

# 2. SETTLEMENT UPLOAD
st.sidebar.subheader("Settlement Reports")
settlement_files = st.sidebar.file_uploader(
    "Upload Settlement (e.g. September ST.xlsx)", 
    type=['xlsx', 'xls'], 
    accept_multiple_files=True,
    key="settlement"
)

# 3. COST UPLOAD
st.sidebar.subheader("SKU Cost")
cost_files = st.sidebar.file_uploader(
    "Upload SKU Cost", 
    type=['xlsx', 'xls'], 
    accept_multiple_files=True,
    key="cost"
)

st.sidebar.markdown("---")
st.sidebar.header("2. Settings")
opt_clean_sku = st.sidebar.checkbox("Auto-clean SKU", value=True)
opt_merged = st.sidebar.checkbox("Handle Merged Cells", value=True)

# -----------------------------

# Process Logic
if st.sidebar.button("Run Reconciliation", type="primary"):
    
    if not sales_files or not settlement_files:
        st.error("⚠️ Please upload at least Sales Reports and Settlement Reports.")
    else:
        engine = ReconciliationEngine()
        
        with st.spinner("Analyzing and Reading files..."):
            
            # 1. Load SALES (Priority Sheet: 'Sales Report', Header Keyword: 'Order Item ID')
            sales_master_df = load_multiple_files(
                sales_files, 
                target_sheet_names=["Sales Report", "Sales"], 
                header_keywords=["Order Item ID", "Order ID"], 
                dtype=str
            )
            
            # 2. Load SETTLEMENT (Priority Sheet: 'Orders', Header Keyword: 'Bank Settlement Value')
            settlement_master_df = load_multiple_files(
                settlement_files, 
                target_sheet_names=["Orders", "Settlement"], 
                header_keywords=["Bank Settlement Value", "Settlement Value", "Order Item ID"]
            )
            
            # 3. Load COST (Priority Sheet: 'Sheet1', Header Keyword: 'Cost Price')
            cost_master_df = load_multiple_files(
                cost_files, 
                target_sheet_names=["Sheet1", "Cost"], 
                header_keywords=["Cost Price", "Cost"]
            )
            
            # Debug Info
            if sales_master_df is not None:
                st.toast(f"✅ Loaded {len(sales_master_df)} Sales records")
            if settlement_master_df is not None:
                st.toast(f"✅ Loaded {len(settlement_master_df)} Settlement records")
        
        # Only proceed if data loaded
        if sales_master_df is not None and settlement_master_df is not None:
            with st.spinner("Reconciling..."):
                result_df, stats, logs = engine.process(
                    sales_master_df, 
                    settlement_master_df, 
                    cost_master_df,
                    auto_clean_sku=opt_clean_sku,
                    handle_merged=opt_merged
                )

            if result_df is not None:
                # Layout: Stats & Charts
                col1, col2 = st.columns([1, 2])
                
                with col1:
                    st.subheader("Summary")
                    st.metric("Total Orders", stats["Total Orders"])
                    st.metric("Total Settlement", f"₹{stats['Total Settlement Amount']:,.2f}")
                    st.metric("Total Invoice Value", f"₹{stats['Total Invoice Amount']:,.2f}")
                
                with col2:
                    st.subheader("Match Rate")
                    chart_data = pd.DataFrame({
                        "Category": ["Matched", "Unmatched"],
                        "Count": [stats["Matched with Settlement"], stats["Unmatched"]]
                    })
                    st.bar_chart(chart_data.set_index("Category"))

                # Download Section
                st.subheader("Download Output")
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    result_df.to_excel(writer, index=False, sheet_name='Reconciled')
                    worksheet = writer.sheets['Reconciled']
                    for idx, col in enumerate(result_df.columns):
                        worksheet.set_column(idx, idx, 20)
                output.seek(0)
                
                st.download_button(
                    label="📥 Download Reconciled_Output.xlsx",
                    data=output,
                    file_name="Reconciled_Output.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

                st.dataframe(result_df.head(100))
                
                # Logs
                if logs:
                    with st.expander("Processing Logs", expanded=False):
                        for log in logs:
                            st.write(f"- {log}")
            else:
                st.error("Processing failed.")
                for log in logs:
                    st.write(log)
        else:
            st.error("Failed to load valid data from the uploaded files.")
else:
    st.info("👈 Upload files in the sidebar and click 'Run Reconciliation'.")
