import streamlit as st
import pandas as pd
import numpy as np
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
        row_str = " ".join(row.astype(str)).lower()
        row_str = row_str.replace("\n", " ").replace("  ", " ")
        for kw in keywords:
            if kw.lower() in row_str:
                return idx
    return None

def read_excel_smart(file, target_sheet_names, header_keywords, dtype=None):
    xl = pd.ExcelFile(file)
    sheet_names = xl.sheet_names
    target_sheet = None
    for sheet in sheet_names:
        if sheet.lower() in [name.lower() for name in target_sheet_names]:
            target_sheet = sheet
            break
    sheets_to_scan = [target_sheet] if target_sheet else sheet_names
    
    for sheet in sheets_to_scan:
        try:
            raw_df = pd.read_excel(file, sheet_name=sheet, header=None, nrows=20)
            header_idx = find_header_row_index(raw_df, header_keywords)
            if header_idx is not None:
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

    def clean_id(self, val):
        if pd.isna(val):
            return ""
        val = str(val).strip()
        if val.startswith("'"):
            val = val[1:]
        return val

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

            sales_cols_map = {
                'Order Item ID': 'Order Item ID', 
                'Order Date': 'Order Date',
                'SKU': 'SKU',
                'Item Quantity': 'Item Quantity',
                'Final Invoice Amount (Price after discount+Shipping Charges)': 'Final Invoice Amount'
            }
            
            sales_df.columns = [str(c).strip().replace("\n", " ") for c in sales_df.columns]
            
            oid_col = self.find_column_by_substring(sales_df, "Order Item ID")
            if oid_col:
                sales_df.rename(columns={oid_col: 'Order Item ID'}, inplace=True)
            
            sales_df.rename(columns=sales_cols_map, inplace=True)
            
            if 'Order Item ID' in sales_df.columns:
                sales_df['Order Item ID'] = sales_df['Order Item ID'].apply(self.clean_id)

            if 'Item Quantity' in sales_df.columns:
                sales_df['Item Quantity'] = pd.to_numeric(sales_df['Item Quantity'], errors='coerce').fillna(0)
            if 'Final Invoice Amount' in sales_df.columns:
                sales_df['Final Invoice Amount'] = pd.to_numeric(sales_df['Final Invoice Amount'], errors='coerce').fillna(0)

            if auto_clean_sku and 'SKU' in sales_df.columns:
                sales_df['SKU'] = sales_df['SKU'].apply(self.clean_sku)

            # --- 2. PROCESS SETTLEMENT REPORT ---
            if settlement_df is None or settlement_df.empty:
                self.log("Error: No valid Settlement Data found.")
                return None, None, self.logs

            if handle_merged:
                settlement_df = settlement_df.ffill() 

            bsv_col = self.find_column_by_substring(settlement_df, "Bank Settlement Value")
            if not bsv_col:
                bsv_col = self.find_column_by_substring(settlement_df, "Settlement Value")
            
            if not bsv_col:
                self.log("Error: Could not locate 'Bank Settlement Value' column.")
                return None, None, self.logs

            oid_col_settlement = self.find_column_by_substring(settlement_df, "Order Item ID")
            if not oid_col_settlement:
                 if len(settlement_df.columns) > 8:
                    oid_col_settlement = settlement_df.columns[8]
            
            settlement_df[oid_col_settlement] = settlement_df[oid_col_settlement].apply(self.clean_id)
            settlement_df[bsv_col] = pd.to_numeric(settlement_df[bsv_col], errors='coerce').fillna(0)

            # PIVOT LOGIC
            settlement_agg = settlement_df.groupby(oid_col_settlement)[bsv_col].sum().reset_index()
            settlement_agg.rename(columns={bsv_col: 'Bank Settlement Value (Rs.)', oid_col_settlement: 'Order Item ID'}, inplace=True)
            
            self.log(f"Settlement Data Aggregated: {len(settlement_df)} rows reduced to {len(settlement_agg)} unique IDs.")

            # --- 3. PROCESS COST PRICE ---
            if cost_df is not None and not cost_df.empty:
                cost_df.columns = [str(c).strip() for c in cost_df.columns]
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

            # ----------------------------------------------
            # NEW: ROYALTY & BASE COST LOGIC
            # ----------------------------------------------
            # 1. Manufacturing Cost (Unit Cost * Quantity)
            merged_df['Manufacturing Cost'] = merged_df['Cost Price'] * merged_df['Item Quantity']

            # 2. Royalty Logic
            # MKUC, DKUC, MAC -> 10%
            # DYK, MYK -> 7%
            conditions_royalty = [
                merged_df['SKU'].str.upper().str.startswith(('MKUC', 'DKUC', 'MAC'), na=False),
                merged_df['SKU'].str.upper().str.startswith(('DYK', 'MYK'), na=False)
            ]
            choices_royalty = [0.10, 0.07]
            
            merged_df['Royalty Rate'] = np.select(conditions_royalty, choices_royalty, default=0.0)
            merged_df['Royalty Amount'] = merged_df['Final Invoice Amount'] * merged_df['Royalty Rate']

            # 3. Base Total Cost = Mfg Cost + Royalty
            merged_df['Base Total Cost'] = merged_df['Manufacturing Cost'] + merged_df['Royalty Amount']

            # ----------------------------------------------
            # LOGIC FOR APPLIED COST (Sale vs Cancel vs Return)
            # ----------------------------------------------
            # S > 0: Sale (100% Cost)
            # S == 0: Unmatched/Pending (100% Cost - Loss)
            # S <= -50: Cancellation (Recoup 80% Cost -> Net 20% Cost)
            # -50 < S < 0: Return (Recoup 50% Cost -> Net 50% Cost)
            
            conditions_status = [
                merged_df['Bank Settlement Value (Rs.)'] > 0,   # Sale
                merged_df['Bank Settlement Value (Rs.)'] == 0,  # Unmatched
                merged_df['Bank Settlement Value (Rs.)'] <= -50, # Cancellation
                (merged_df['Bank Settlement Value (Rs.)'] > -50) & (merged_df['Bank Settlement Value (Rs.)'] < 0) # Return
            ]
            
            choices_cost = [
                merged_df['Base Total Cost'],          # Sale: 100% Cost
                merged_df['Base Total Cost'],          # Unmatched: 100% Cost
                merged_df['Base Total Cost'] * -0.80,  # Cancel: -80% Cost (Negative Applied means cost recovery)
                merged_df['Base Total Cost'] * -0.50   # Return: -50% Cost (Negative Applied means cost recovery)
                # Note: This logic assumes 'Profit = Settlement - Applied Cost'. 
                # If we subtract a negative cost, we are adding profit (recovery).
                # Wait, earlier you said "Net Cost 20%".
                # If Base Cost = 100. Cancel Logic = -80?
                # P/L = Settlement (-60) - Applied Cost (-80) = +20 (Profit?? No)
                # Let's re-verify the request: "Cancel wale ki cost value terko 20% leni h"
                # If Settlement is -60 (Loss), and we treat cost as 20.
                # P/L should be -60 - 20 = -80.
                # BUT you said "80 percent negetive m... final cost ho jaegi iski 20"
                # If I use 0.20 as choice:
                # P/L = -60 - 20 = -80. This looks correct for a cancellation penalty + small cost.
                
                # Let's look at your previous prompt: "Cancel -80 final cost ho jaegi iski 20"
                # "negetive m le" -> This implied Recouping.
                # If I use 0.20 * BaseCost:
                #   Cost = 20.
                #   PL = -60 (Settlement) - 20 (Cost) = -80.
                # If I use -0.80 * BaseCost (Negative Cost logic):
                #   Cost = -80.
                #   PL = -60 (Settlement) - (-80) = +20.
                
                # WHICH ONE DO YOU WANT? 
                # "cancel -80 final cost ho jaegi iski 20"
                # Usually in cancellations, you lose the shipping/penalty (-60) AND you incur some packing cost (20). Total Loss = -80.
                # So Applied Cost should be POSITIVE 20% of Base Cost.
                # BUT you said "negetive m le".
                # If I take -80%, it implies I SAVED 80% of the cost (Inventory back).
                # So if I spent 100, got product back, my real cost is only 20.
                # So P/L = Settlement - NetCost(20).
                
                # Let's stick to the multiplier logic I used in previous successful code, 
                # but modified for your "80% negative" instruction which I interpreted as:
                # "Reduce cost by 80%" => Cost becomes 20%.
                # So I will use 0.20 and 0.50 POSITIVE multipliers.
                # Because "Profit = Settlement - Cost".
                # If Settlement = -50 (Penalty). Cost = 20 (Packaging).
                # Net P/L = -50 - 20 = -70.
            ]
            
            # Re-reading: "80 percent negetive m... final cost ho jaegi iski 20"
            # This confirms Final Cost = 20.
            # So the applied cost value in the column should be 20.
            # So multiplier is 0.20.
            
            choices_cost = [
                 merged_df['Base Total Cost'],        # Sale
                 merged_df['Base Total Cost'],        # Unmatched
                 merged_df['Base Total Cost'] * 0.20, # Cancel (Net Cost is 20%)
                 merged_df['Base Total Cost'] * 0.50  # Return (Net Cost is 50%)
            ]
            
            # Apply Logic
            merged_df['Applied Cost'] = np.select(conditions_status, choices_cost, default=merged_df['Base Total Cost'])
            
            # Add Order Status Column
            status_choices = ['Sale', 'Unmatched/Pending', 'Cancellation', 'Return']
            merged_df['Order Status'] = np.select(conditions_status, status_choices, default='Unknown')

            # 4. Final Profit/Loss Calculation
            # P/L = Settlement - Applied Cost
            merged_df['Profit/Loss'] = merged_df['Bank Settlement Value (Rs.)'] - merged_df['Applied Cost']
            
            unmatched_rows = merged_df[merged_df['Bank Settlement Value (Rs.)'] == 0]

            # Output formatting
            merged_df['Order Item ID'] = "'" + merged_df['Order Item ID'].astype(str)

            final_columns = [
                'Order Date', 'Order Item ID', 'SKU', 'Item Quantity', 'Order Status',
                'Final Invoice Amount', 'Bank Settlement Value (Rs.)', 
                'Cost Price', 'Royalty Rate', 'Royalty Amount', 'Base Total Cost', 'Applied Cost', 'Profit/Loss'
            ]
            final_columns = [c for c in final_columns if c in merged_df.columns]
            final_output = merged_df[final_columns]

            stats = {
                "Total Orders": len(final_output),
                "Matched with Settlement": len(final_output) - len(unmatched_rows),
                "Unmatched": len(unmatched_rows),
                "Total Settlement Amount": final_output['Bank Settlement Value (Rs.)'].sum(),
                "Total Invoice Amount": final_output['Final Invoice Amount'].sum() if 'Final Invoice Amount' in final_output else 0,
                "Total Applied Cost": final_output['Applied Cost'].sum() if 'Applied Cost' in final_output else 0,
                "Total Net Profit/Loss": final_output['Profit/Loss'].sum()
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

# --- INITIALIZE SESSION STATE ---
if "result_df" not in st.session_state:
    st.session_state.result_df = None
if "stats" not in st.session_state:
    st.session_state.stats = None
if "logs" not in st.session_state:
    st.session_state.logs = []

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
st.sidebar.info("Upload 'Merge ST.xlsx' or 'Settlement.xlsx' here.")
settlement_files = st.sidebar.file_uploader(
    "Upload Settlement Files", 
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
            
            # 1. Load SALES
            sales_master_df = load_multiple_files(
                sales_files, 
                target_sheet_names=["Sales Report", "Sales"], 
                header_keywords=["Order Item ID", "Order ID"], 
                dtype=str
            )
            
            # 2. Load SETTLEMENT
            settlement_master_df = load_multiple_files(
                settlement_files, 
                target_sheet_names=["Sheet1", "Working", "Orders", "Settlement"], 
                header_keywords=["Bank Settlement Value", "Settlement Value", "Order Item ID"]
            )
            
            # 3. Load COST
            cost_master_df = load_multiple_files(
                cost_files, 
                target_sheet_names=["Sheet1", "Cost"], 
                header_keywords=["Cost Price", "Cost"]
            )
            
            if sales_master_df is not None:
                st.toast(f"✅ Loaded {len(sales_master_df)} Sales records")
            if settlement_master_df is not None:
                st.toast(f"✅ Loaded {len(settlement_master_df)} Settlement records")
        
        if sales_master_df is not None and settlement_master_df is not None:
            with st.spinner("Reconciling..."):
                result_df, stats, logs = engine.process(
                    sales_master_df, 
                    settlement_master_df, 
                    cost_master_df,
                    auto_clean_sku=opt_clean_sku,
                    handle_merged=opt_merged
                )
            
            st.session_state.result_df = result_df
            st.session_state.stats = stats
            st.session_state.logs = logs
            
            if result_df is not None:
                st.rerun() 
            else:
                 st.error("Processing failed.")
                 for log in logs:
                    st.write(log)
        else:
            st.error("Failed to load valid data from the uploaded files.")
elif not sales_files and not settlement_files:
    st.info("👈 Upload files in the sidebar and click 'Run Reconciliation'.")

# --- DISPLAY RESULTS FROM SESSION STATE ---
if st.session_state.result_df is not None:
    result_df = st.session_state.result_df
    stats = st.session_state.stats
    logs = st.session_state.logs

    # Layout: Stats & Charts
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        st.subheader("Overview")
        st.metric("Total Orders", stats["Total Orders"])
        st.metric("Total Settlement", f"₹{stats['Total Settlement Amount']:,.2f}")
    
    with col2:
        st.subheader("Financials")
        st.metric("Total Applied Cost", f"₹{stats['Total Applied Cost']:,.2f}")
        st.metric("Net Profit/Loss", f"₹{stats['Total Net Profit/Loss']:,.2f}")
        
    with col3:
        st.subheader("Order Split")
        if 'Order Status' in result_df.columns:
            status_counts = result_df['Order Status'].value_counts()
            st.bar_chart(status_counts)

    # Download Section
    st.subheader("Download Output")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        result_df.to_excel(writer, index=False, sheet_name='Reconciled')
        worksheet = writer.sheets['Reconciled']
        for idx, col in enumerate(result_df.columns):
            worksheet.set_column(idx, idx, 22)
    output.seek(0)
    
    st.download_button(
        label="📥 Download Reconciled_Output.xlsx",
        data=output,
        file_name="Reconciled_Output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.dataframe(result_df.head(100))
    
    if logs:
        with st.expander("Processing Logs", expanded=False):
            for log in logs:
                st.write(f"- {log}")
