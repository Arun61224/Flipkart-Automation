import streamlit as st
import pandas as pd
from io import BytesIO

# -----------------------
# Helper function: Process Orders sheet
# -----------------------
def process_orders_excel(excel_file):
    xls = pd.ExcelFile(excel_file)

    if "Orders" not in xls.sheet_names:
        raise ValueError('Sheet named "Orders" not found in the uploaded file.')

    orders = xls.parse("Orders")

    # Column mapping
    col_order_id = "Transaction Summary"          # H
    col_bank_value = "Unnamed: 3"                 # D
    col_seller_sku = "Seller SKU"                 # BG
    col_qty = "Quantity"                          # BH (assuming correct header name if present)

    # Possible mismatch fix (BG/BH sometimes show as Unnamed if empty header)
    for col in orders.columns:
        if "SKU" in str(col):
            col_seller_sku = col
        if "Qty" in str(col) or "Quantity" in str(col):
            col_qty = col

    required_cols = [col_order_id, col_bank_value, col_seller_sku, col_qty]
    for c in required_cols:
        if c not in orders.columns:
            raise ValueError(f"Column missing in sheet → {c}")

    # Keep only order rows
    df = orders.copy()
    df = df[df[col_order_id].astype(str).str.startswith("OD", na=False)]

    if df.empty:
        raise ValueError("No Order IDs starting with 'OD' found.")

    # Cast values
    df[col_bank_value] = pd.to_numeric(df[col_bank_value], errors="coerce")
    df[col_qty] = pd.to_numeric(df[col_qty], errors="coerce")

    df = df[
        df[col_order_id].notna() &
        df[col_bank_value].notna() &
        df[col_seller_sku].notna() &
        df[col_qty].notna()
    ]

    # Clean data
    df_clean = df[[col_order_id, col_seller_sku, col_qty, col_bank_value]].rename(
        columns={
            col_order_id: "Order ID",
            col_seller_sku: "Seller SKU",
            col_qty: "Qty",
            col_bank_value: "Bank Settlement Value (Rs.)"
        }
    )

    # -----------------------
    # Pivot summary
    # Group by Order ID + Seller SKU
    # -----------------------
    pivot = df_clean.groupby(["Order ID", "Seller SKU"], as_index=False).agg(
        Total_Qty=("Qty", "sum"),
        Total_Bank_Settlement_Value=("Bank Settlement Value (Rs.)", "sum"),
        Number_of_Records=("Bank Settlement Value (Rs.)", "size"),
        Average_Bank_Settlement_Value=("Bank Settlement Value (Rs.)", "mean")
    ).sort_values("Total_Bank_Settlement_Value", ascending=False)

    # Summary KPIs
    summary = {
        "total_unique_orders": df_clean["Order ID"].nunique(),
        "total_unique_sku": df_clean["Seller SKU"].nunique(),
        "total_rows": df_clean.shape[0],
        "grand_qty": df_clean["Qty"].sum(),
        "grand_total_bank": df_clean["Bank Settlement Value (Rs.)"].sum()
    }

    return df_clean, pivot, summary


# -----------------------
# Helper: Excel Download
# -----------------------
def to_excel_bytes(df_raw, df_pivot):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_raw.to_excel(writer, index=False, sheet_name="Raw_Data")
        df_pivot.to_excel(writer, index=False, sheet_name="Pivot")
    return output.getvalue()


# -----------------------
# Streamlit App
# -----------------------
def main():
    st.set_page_config(page_title="Settlement Pivot Tool", page_icon="📊", layout="wide")
    st.title("📊 Settlement Pivot Tool — Orders + SKU + Qty")

    uploaded_file = st.file_uploader("📁 Upload your Settlement Excel file", type=["xlsx", "xls"])

    if uploaded_file:
        try:
            with st.spinner("Processing your Excel file..."):
                df_clean, pivot_df, summary = process_orders_excel(uploaded_file)

            st.subheader("📌 Summary")
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Unique Orders", summary["total_unique_orders"])
            col2.metric("Unique SKUs", summary["total_unique_sku"])
            col3.metric("Total Rows", summary["total_rows"])
            col4.metric("Total Qty", summary["grand_qty"])
            col5.metric("Total Bank Settlement (Rs.)", f"{summary['grand_total_bank']:.2f}")

            with st.expander("📄 View Raw Data", expanded=False):
                st.dataframe(df_clean, use_container_width=True)

            st.subheader("📑 Pivot (Order ID + Seller SKU + Qty + Bank Amount)")
            st.dataframe(pivot_df, use_container_width=True)

            excel_bytes = to_excel_bytes(df_clean, pivot_df)
            st.download_button(
                "⬇️ Download Pivot Excel",
                data=excel_bytes,
                file_name="settlement_sku_pivot.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        except Exception as e:
            st.error(f"❌ Error: {e}")


if __name__ == "__main__":
    main()

