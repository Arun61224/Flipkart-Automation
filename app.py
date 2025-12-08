import streamlit as st
import pandas as pd
from io import BytesIO

# -----------------------
# Helper function: Process Orders sheet
# -----------------------
def process_orders_excel(excel_file):
    # Excel file open
    xls = pd.ExcelFile(excel_file)

    # Check if "Orders" sheet exists
    if "Orders" not in xls.sheet_names:
        raise ValueError('Sheet named "Orders" not found in the uploaded file.')

    # Read Orders sheet
    orders = xls.parse("Orders")

    # Column mapping (sample sheet ke hisaab se)
    col_order_id = "Transaction Summary"          # H column -> Order ID
    col_bank_value = "Unnamed: 3"                 # D column -> Bank Settlement Value (Rs.)

    if col_order_id not in orders.columns or col_bank_value not in orders.columns:
        raise ValueError(
            'Expected columns not found. Check if "Transaction Summary" (Order ID) '
            'and "Unnamed: 3" (Bank Settlement Value) exist in the "Orders" sheet.'
        )

    # Filter rows jahan Order ID start hota hai "OD" se (actual orders)
    df = orders.copy()
    df = df[df[col_order_id].astype(str).str.startswith("OD", na=False)]

    if df.empty:
        raise ValueError("No order rows found starting with 'OD' in Order ID column.")

    # Bank Settlement Value ko numeric bana do
    df[col_bank_value] = pd.to_numeric(df[col_bank_value], errors="coerce")

    # Null hatao (jahan amount missing hai)
    df = df[df[col_bank_value].notna()]

    # Rename for clean display
    df_clean = df[[col_order_id, col_bank_value]].rename(
        columns={
            col_order_id: "Order ID",
            col_bank_value: "Bank Settlement Value (Rs.)"
        }
    )

    # Pivot: per Order ID summary
    pivot = df_clean.groupby("Order ID", as_index=False).agg(
        Total_Bank_Settlement_Value=("Bank Settlement Value (Rs.)", "sum"),
        Number_of_Records=("Bank Settlement Value (Rs.)", "size"),
        Average_Bank_Settlement_Value=("Bank Settlement Value (Rs.)", "mean")
    )

    # Sorting (optional) - highest total value on top
    pivot = pivot.sort_values("Total_Bank_Settlement_Value", ascending=False)

    # Overall summary
    total_orders = pivot.shape[0]
    total_records = df_clean.shape[0]
    grand_total_value = df_clean["Bank Settlement Value (Rs.)"].sum()

    summary = {
        "total_unique_orders": total_orders,
        "total_rows_considered": total_records,
        "grand_total_bank_settlement_value": grand_total_value
    }

    return df_clean, pivot, summary


# -----------------------
# Helper: Download Excel
# -----------------------
def to_excel_bytes(df_raw, df_pivot):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_raw.to_excel(writer, index=False, sheet_name="Raw_Orders_Data")
        df_pivot.to_excel(writer, index=False, sheet_name="Pivot_By_Order_ID")
    processed_data = output.getvalue()
    return processed_data


# -----------------------
# Streamlit App
# -----------------------
def main():
    st.set_page_config(
        page_title="Settlement Pivot Tool",
        page_icon="📊",
        layout="wide"
    )

    st.title("📊 Bank Settlement Pivot Tool")
    st.write(
        """
        Upload your settlement Excel file (multi-sheet).  
        App automatically:
        - Uses only the **"Orders"** sheet  
        - Picks **Order ID (Column H)** and **Bank Settlement Value (Column D)**  
        - Creates a pivot: **per Order ID total value + count + average**
        """
    )

    # File uploader
    uploaded_file = st.file_uploader(
        "📁 Upload your settlement file",
        type=["xlsx", "xls"],
        help="Upload the original settlement Excel with multiple sheets."
    )

    if uploaded_file is not None:
        try:
            with st.spinner("Processing your file..."):
                df_clean, pivot_df, summary = process_orders_excel(uploaded_file)

            # Summary KIPs
            st.subheader("📌 Summary")
            col1, col2, col3 = st.columns(3)
            col1.metric("Unique Orders", summary["total_unique_orders"])
            col2.metric("Total Rows Considered", summary["total_rows_considered"])
            col3.metric(
                "Grand Total Bank Settlement (Rs.)",
                f"{summary['grand_total_bank_settlement_value']:.2f}"
            )

            # Show raw data
            with st.expander("🔍 View cleaned Orders data (Order ID + Bank Settlement Value)", expanded=False):
                st.dataframe(df_clean, use_container_width=True)

            # Show pivot
            st.subheader("📑 Pivot by Order ID")
            st.dataframe(pivot_df, use_container_width=True)

            # Filter: Search specific order
            st.markdown("---")
            st.subheader("🎯 Search / Filter")
            search_order = st.text_input("Enter Order ID to filter (optional)")
            if search_order:
                filtered = pivot_df[pivot_df["Order ID"].astype(str).str.contains(search_order.strip(), case=False)]
                st.write(f"Showing result(s) for: **{search_order}**")
                st.dataframe(filtered, use_container_width=True)

            # Download button
            st.markdown("---")
            st.subheader("⬇️ Download Results")
            excel_bytes = to_excel_bytes(df_clean, pivot_df)
            st.download_button(
                label="Download Pivot as Excel",
                data=excel_bytes,
                file_name="settlement_pivot_output.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        except Exception as e:
            st.error(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
