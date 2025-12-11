import streamlit as st
import pandas as pd
import io
from reconcile import ReconciliationEngine

# Page Config
st.set_page_config(page_title="Flipkart Reconciliation Tool", layout="wide")

st.title("📊 Flipkart Reconciliation Tool")
st.markdown("""
Upload your workbook containing **Sales Report**, **Orders** (Settlement), and **SKU Cost Price**.
""")

# Sidebar Options
st.sidebar.header("Configuration")
opt_clean_sku = st.sidebar.checkbox("Auto-clean SKU", value=True, help="Removes 'SKU:' and triple quotes")
opt_merged = st.sidebar.checkbox("Handle Merged Cells", value=True, help="Expands merged cells in Settlement report")
opt_compute_pl = st.sidebar.checkbox("Compute Profit/Loss", value=True)

# File Uploader
uploaded_file = st.file_uploader("Upload Excel File", type=['xlsx', 'xls'])

if uploaded_file:
    # Preview Sheets
    try:
        xl = pd.ExcelFile(uploaded_file)
        st.success(f"File loaded successfully. Detected sheets: {', '.join(xl.sheet_names)}")
        
        st.subheader("Data Preview (First 5 Rows of Sales Report)")
        if 'Sales Report' in xl.sheet_names:
            preview_df = pd.read_excel(uploaded_file, sheet_name='Sales Report', nrows=5)
            st.dataframe(preview_df)
        else:
            st.error("Sheet 'Sales Report' missing!")
    except Exception as e:
        st.error(f"Error reading file: {e}")

    # Process Button
    if st.button("Run Reconciliation"):
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
            
            # Save to BytesIO
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                result_df.to_excel(writer, index=False, sheet_name='Reconciled')
                # Auto-adjust column width (basic)
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
