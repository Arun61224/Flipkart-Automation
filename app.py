import streamlit as st
import pandas as pd
import io
import re
import traceback

# ==========================================
# CORE LOGIC
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
        # Remove "SKU:" and triple quotes, then strip whitespace
        sku = re.sub(r'SKU:', '', sku, flags=re.IGNORECASE)
        sku = sku.replace('"""', '')
        return sku.strip()

    def find_column_by_substring(self, df, substring):
        """Finds a column name that contains the substring."""
        for col in df.columns:
            if substring.lower() in str(col).lower():
                return col
        return None

    # Ab ye function direct DataFrames leta hai, files nahi
    def process(self, sales_df, settlement_df, cost_df, auto_clean_sku=True, handle_merged=True):
        self.logs = [] # Reset logs
        
        try:
            # 1. PROCESS SALES REPORT
            if sales_df is None or sales_df.empty:
                 return None, None, ["Error: No Sales Data provided."]

            # Map known columns
            sales_cols_map = {
                'Order Item ID': 'Order Item ID', 
                'Order Date': 'Order Date',
                'SKU': 'SKU',
                'Item Quantity': 'Item Quantity',
                'Final Invoice Amount (Price after discount+Shipping Charges)': 'Final Invoice Amount'
            }
            
            # Normalize headers
            sales_df.columns = [str(c).strip() for c in sales_df.columns]
            
            # Rename columns if they exist
            sales_df.rename(columns=sales_cols_map, inplace=True)
            
            # Data Type Conversion
            if 'Item Quantity' in sales_df.columns:
                sales_df['Item Quantity'] = pd.to_numeric(sales_df['Item Quantity'], errors='coerce').fillna(0)
            if 'Final Invoice Amount' in sales_df.columns:
                sales_df['Final Invoice Amount'] = pd.to_numeric(sales_df['Final Invoice Amount'], errors='coerce').fillna(0)

            # CLEAN SKU
            if auto_clean_sku and 'SKU' in sales_df.columns:
                sales_df['SKU'] = sales_df['SKU'].apply(self.clean_sku)

            # 2. PROCESS SETTLEMENT REPORT
            if settlement_df is None or settlement_df.empty:
                self.log("Error: No Settlement Data provided.")
                return None, None, self.logs

            # Handle Merged Cells (Forward Fill)
            if handle_merged:
                settlement_df = settlement_df.ffill() 

            # Identify "Bank Settlement Value" column
            bsv_col = self.find_column_by_substring(settlement_df, "Bank Settlement Value")
            if not bsv_col:
                # Fallback logic
                if len(settlement_df.columns) > 3:
                    bsv_col = settlement_df.columns[3]
                else:
                    self.log("Error: Could not locate 'Bank Settlement Value' column.")
                    return None, None, self.logs

            # Identify "Order Item ID"
            oid_col = self.find_column_by_substring(settlement_df, "Order Item ID")
            if not oid_col:
                 if len(settlement_df.columns) > 8:
                    oid_col = settlement_df.columns[8]
            
            # Normalize Settlement Data
            settlement_df[oid_col] = settlement_df[oid_col].astype(str).str.strip()
            settlement_df[bsv_col] = pd.to_numeric(settlement_df[bsv_col], errors='coerce').fillna(0)

            # Aggregate Duplicates (Sum values for same ID)
            settlement_agg = settlement_df.groupby(oid_col)[bsv_col].sum().reset_index()
            settlement_agg.rename(columns={bsv_col: 'Bank Settlement Value (Rs.)', oid_col: 'Order Item ID'}, inplace=True)

            # 3. PROCESS COST PRICE
            if cost_df is not None and not cost_df.empty:
                cost_df.columns = [str(c).strip() for c in cost_df.columns]
                if 'SKU' in cost_df.columns and 'Cost Price' in cost_df.columns:
                    cost_df['SKU'] = cost_df['SKU'].astype(str).str.strip()
                    if auto_clean_sku:
                         cost_df['SKU'] = cost_df['SKU'].apply(self.clean_sku)
                else:
                    cost_df = pd.DataFrame(columns=['SKU', 'Cost Price'])
            else:
                cost_df = pd.DataFrame(columns=['SKU', 'Cost Price'])

            # 4. MERGING
            if 'Order Item ID' in sales_df.columns:
                sales_df['Order Item ID'] = sales_df['Order Item ID'].astype(str).str.strip()
                merged_df = pd.merge(sales_df, settlement_agg, on='Order Item ID', how='left')
            else:
                 return None, None, ["Error: 'Order Item ID' column missing in Sales Report."]

            if 'SKU' in sales_df.columns:
                merged_df = pd.merge(merged_df, cost_df[['SKU', 'Cost Price']], on='SKU', how='left')
            else:
                merged_df['Cost Price'] = 0

            # 5. CALCULATIONS
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
# HELPER TO READ MULTIPLE FILES
# ==========================================
def load_files_into_df(uploaded_files, dtype=None):
    if not uploaded_files:
        return None
    
    df_list = []
    for file in uploaded_files:
        try:
            # Read 1st sheet (sheet_name=0) to be safe
            df = pd.read_excel(file, sheet_name=0, dtype=dtype)
            df_list.append(df)
        except Exception as e:
            st.error(f"Error reading file {file.name}: {e}")
    
    if df_list:
        return pd.concat(df_list, ignore_index=True)
    return None

# ==========================================
# STREAMLIT UI
# ==========================================

st.set_page_config(page_title="Flipkart Reconciliation Tool", layout="wide")
st.title("📊 Flipkart Reconciliation Tool")

# --- SIDEBAR CONFIGURATION ---
st.sidebar.header("1. Upload Files")

# 1. SALES UPLOAD (Multiple)
st.sidebar.subheader("Sales Reports")
sales_files = st.sidebar.file_uploader(
    "Upload Sales (Multiple allowed)", 
    type=['xlsx', 'xls'], 
    accept_multiple_files=True,
    key="sales"
)

# 2. SETTLEMENT UPLOAD (Multiple)
st.sidebar.subheader("Settlement Reports")
settlement_files = st.sidebar.file_uploader(
    "Upload Settlement (Multiple allowed)", 
    type=['xlsx', 'xls'], 
    accept_multiple_files=True,
    key="settlement"
)

# 3. COST UPLOAD (Multiple)
st.sidebar.subheader("SKU Cost")
cost_files = st.sidebar.file_uploader(
    "Upload SKU Cost (Multiple allowed)", 
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
        
        with st.spinner("Reading and Combining files..."):
            # Load and Combine Sales Files
            # Reading as string to keep IDs safe
            sales_master_df = load_files_into_df(sales_files, dtype=str)
            
            # Load and Combine Settlement Files
            settlement_master_df = load_files_into_df(settlement_files)
            
            # Load Cost Files (Optional)
            cost_master_df = load_files_into_df(cost_files)
            
            st.info(f"Loaded {len(sales_files)} Sales files, {len(settlement_files)} Settlement files.")

        with st.spinner("Running Reconciliation Logic..."):
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
                st.metric("Total Orders Processed", stats["Total Orders"])
                st.metric("Total Settlement", f"₹{stats['Total Settlement Amount']:,.2f}")
                st.metric("Total Invoice Value", f"₹{stats['Total Invoice Amount']:,.2f}")
            
            with col2:
                st.subheader("Match Rate")
                chart_data = pd.DataFrame({
                    "Category": ["Matched", "Unmatched"],
                    "Count": [stats["Matched with Settlement"], stats["Unmatched"]]
                })
                st.bar_chart(chart_data.set_index("Category"))

            # Logs
            with st.expander("Processing Logs", expanded=False):
                for log in logs:
                    st.write(f"- {log}")

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
            
        else:
            st.error("Processing failed. Check logs below.")
            with st.expander("Error Logs"):
                for log in logs:
                    st.write(log)
else:
    st.info("👈 Upload files in the sidebar and click 'Run Reconciliation'.")
