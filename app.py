import streamlit as st
import pandas as pd
import io
import sys
import os

# --- DEBUGGING: Check if reconcile.py exists ---
if not os.path.exists("reconcile.py"):
    st.error("🚨 CRITICAL ERROR: 'reconcile.py' not found!")
    st.info("Make sure 'reconcile.py' is in the SAME folder as 'app.py' in your GitHub repo.")
    st.stop()
# -----------------------------------------------

from reconcile import ReconciliationEngine

# Page Config
st.set_page_config(page_title="Flipkart Reconciliation Tool", layout="wide")

st.title("📊 Flipkart Reconciliation Tool")

# --- SIDEBAR CONFIGURATION ---
st.sidebar.header("Upload & Settings")

# 1. File Uploader Moved to Sidebar
uploaded_file = st.sidebar.file_uploader("Upload Excel File", type=['xlsx', 'xls'])

st.sidebar.markdown("---")
st.sidebar.subheader("Options")
opt_clean_sku = st.sidebar.checkbox("Auto-clean SKU", value=True, help="Removes 'SKU:' and triple quotes")
opt_merged = st.sidebar.checkbox("Handle Merged Cells", value=True, help="Expands merged cells in Settlement report")
opt_compute_pl = st.sidebar.checkbox("Compute Profit/Loss", value=True)
# -----------------------------

if uploaded_file:
    # Preview Sheets
    try:
        xl = pd.ExcelFile(uploaded_file)
        st.success(f"File loaded successfully. Detected sheets: {', '.join(xl.sheet_names)}")
        
        st.subheader("Data Preview (Sales Report)")
        if 'Sales Report' in xl.sheet_names:
            preview_df = pd.read_excel(uploaded_file, sheet_name='Sales Report', nrows=5)
            st.dataframe(preview_df)
        else:
            st.error("Sheet 'Sales Report' missing!")
            
        # Move the Run button to the main area or sidebar? 
        # Usually better in the main area after preview, or sidebar bottom.
        if st.sidebar.button("Run Reconciliation", type="primary"):
            engine = ReconciliationEngine()
            
            with st.spinner("Processing data..."):
                # Reset pointer
                uploaded_file.seek(0)
                
                result_df, stats, logs = engine.process(
                    uploaded_file, 
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

                st.dataframe(result_df.head(50))
                
            else:
                st.error("Processing failed. Check logs below.")
                with st.expander("Error Logs"):
                    for log in logs:
                        st.write(log)

    except Exception as e:
        st.error(f"Error reading file: {e}")
else:
    st.info("👈 Please upload your Excel file in the sidebar to begin.")
