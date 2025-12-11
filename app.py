import streamlit as st
import pandas as pd
import io
import re
import traceback

# ==========================================
# CORE LOGIC (Formerly reconcile.py)
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

    def process(self, uploaded_file, auto_clean_sku=True, handle_merged=True):
        self.logs = [] # Reset logs
        
        try:
            # 1. LOAD SALES REPORT
            try:
                sales_df = pd.read_excel(uploaded_file, sheet_name='Sales Report', dtype=str)
            except ValueError:
                return None, None, ["Error: Sheet 'Sales Report' not found."]

            # Map known columns to internal names
            sales_cols_map = {
                'Order Item ID': 'Order Item ID', 
                'Order Date': 'Order Date',
                'SKU': 'SKU',
                'Item Quantity': 'Item Quantity',
                'Final Invoice Amount (Price after discount+Shipping Charges)': 'Final Invoice Amount'
            }
            
            # Normalize headers for matching
            sales_df.columns = [c.strip() for c in sales_df.columns]
            
            # Rename for consistency
            sales_df.rename(columns=sales_cols_map, inplace=True)
            
            # Data Type Conversion for Math
            sales_df['Item Quantity'] = pd.to_numeric(sales_df['Item Quantity'], errors='coerce').fillna(0)
            sales_df['Final Invoice Amount'] = pd.to_numeric(sales_df['Final Invoice Amount'], errors='coerce').fillna(0)

            # CLEAN SKU
            if auto_clean_sku:
                sales_df['SKU'] = sales_df['SKU'].apply(self.clean_sku)

            # 2. LOAD SETTLEMENT REPORT ('Orders')
            try:
                settlement_df = pd.read_excel(uploaded_file, sheet_name='Orders')
            except ValueError:
                self.log("Error: Sheet 'Orders' not found.")
                return None, None, self.logs

            # Handle Merged Cells (Forward Fill)
            if handle_merged:
                settlement_df = settlement_df.ffill() 

            # Identify "Bank Settlement Value" column
            bsv_col = self.find_column_by_substring(settlement_df, "Bank Settlement Value")
            if not bsv_col:
                if len(settlement_df.columns) > 3:
                    bsv_col = settlement_df.columns[3]
                else:
                    self.log("Error: Could not locate 'Bank Settlement Value' column.")
                    return None, None, self.logs

            # Identify "Order Item ID" in Settlement
            oid_col = self.find_column_by_substring(settlement_df, "Order Item ID")
            if not oid_col:
                 if len(settlement_df.columns) > 8:
                    oid_col = settlement_df.columns[8]
            
            # Normalize Settlement Data
            settlement_df[oid_col] = settlement_df[oid_col].astype(str).str.strip()
            settlement_df[bsv_col] = pd.to_numeric(settlement_df[bsv_col], errors='coerce').fillna(0)

            # Aggregate Duplicates
            settlement_agg = settlement_df.groupby(oid_col)[bsv_col].sum().reset_index()
            settlement_agg.rename(columns={bsv_col: 'Bank Settlement Value (Rs.)', oid_col: 'Order Item ID'}, inplace=True)

            # 3. LOAD SKU COST PRICE
            try:
                cost_df = pd.read_excel(uploaded_file, sheet_name='SKU Cost Price')
                cost_df.columns = [c.strip() for c in cost_df.columns]
                if 'SKU' in cost_df.columns and 'Cost Price' in cost_df.columns:
                    cost_df['SKU'] = cost_df['SKU'].astype(str).str.strip()
                    if auto_clean_sku:
                         cost_df['SKU'] = cost_df['SKU'].apply(self.clean_sku)
                else:
                    cost_df = pd.DataFrame(columns=['SKU', 'Cost Price'])
            except ValueError:
                cost_df = pd.DataFrame(columns=['SKU', 'Cost Price'])

            # 4. MERGING
            sales_df['Order Item ID'] = sales_df['Order Item ID'].str.strip()
            merged_df = pd.merge(sales_df, settlement_agg, on='Order Item ID', how='left')
            merged_df = pd.merge(merged_df, cost_df[['SKU', 'Cost Price']], on='SKU', how='left')

            # 5. CALCULATIONS
            merged_df['Profit/Loss'] = merged_df['Bank Settlement Value (Rs.)'] - (merged_df['Cost Price'] * merged_df['Item Quantity'])
            
            # Handle Lookups Fails (NaNs)
            merged_df.fillna({
                'Bank Settlement Value (Rs.)': 0,
                'Cost Price': 0,
                'Profit/Loss': 0
            }, inplace=True)
            
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
                "Total Invoice Amount": final_output['Final Invoice Amount'].sum()
            }

            return final_output, stats, self.logs

        except Exception as e:
            self.log(f"Critical Error: {str(e)}")
            self.log(traceback.format_exc())
            return None, None, self.logs

# ==========================================
# STREAMLIT UI (Formerly app.py)
# ==========================================

# Page Config
st.set_page_config(page_title="Flipkart Reconciliation Tool", layout="wide")

st.title("📊 Flipkart Reconciliation Tool")

# --- SIDEBAR: FILE UPLOAD BUTTON IS HERE ---
st.sidebar.header("Upload & Settings")

# This is the button you asked for 👇
uploaded_file = st.sidebar.file_uploader("Upload Excel File", type=['xlsx', 'xls'])

st.sidebar.markdown("---")
opt_clean_sku = st.sidebar.checkbox("Auto-clean SKU", value=True)
opt_merged = st.sidebar.checkbox("Handle Merged Cells", value=True)
# -------------------------------------------

if uploaded_file:
    # Preview Sheets
    try:
        xl = pd.ExcelFile(uploaded_file)
        st.success(f"File loaded successfully. Detected sheets: {', '.join(xl.sheet_names)}")
        
        st.subheader("Data Preview (Sales Report)")
        if 'Sales Report' in xl.sheet_names:
            preview_df = pd.read_excel(uploaded_file, sheet_name='Sales Report', nrows=5)
            st.dataframe(preview_df)
            
            # RUN BUTTON
            if st.button("Run Reconciliation", type="primary"):
                engine = ReconciliationEngine()
                
                with st.spinner("Processing data..."):
                    uploaded_file.seek(0)
                    result_df, stats, logs = engine.process(
                        uploaded_file, 
                        auto_clean_sku=opt_clean_sku,
                        handle_merged=opt_merged
                    )

                if result_df is not None:
                    # Stats
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.subheader("Summary")
                        st.metric("Total Orders", stats["Total Orders"])
                        st.metric("Total Settlement", f"₹{stats['Total Settlement Amount']:,.2f}")
                    
                    with col2:
                        st.subheader("Match Rate")
                        chart_data = pd.DataFrame({
                            "Category": ["Matched", "Unmatched"],
                            "Count": [stats["Matched with Settlement"], stats["Unmatched"]]
                        })
                        st.bar_chart(chart_data.set_index("Category"))

                    # Download Button
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
                    st.dataframe(result_df)
                else:
                    st.error("Processing failed. Check logs.")
                    for log in logs:
                        st.write(log)
        else:
            st.error("Sheet 'Sales Report' missing!")

    except Exception as e:
        st.error(f"Error reading file: {e}")
else:
    st.info("👈 Please use the 'Browse files' button in the sidebar to upload your Excel file.")
