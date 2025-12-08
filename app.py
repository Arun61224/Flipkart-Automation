import streamlit as st
import pandas as pd
from io import BytesIO

# -----------------------
# Helper: Excel column name -> 0-based index
# Example: "A"->0, "H"->7, "BG"->58, "BH"->59
# -----------------------
def excel_col_to_idx(col_name: str) -> int:
    col_name = col_name.strip().upper()
    idx = 0
    for c in col_name:
        if "A" <= c <= "Z":
            idx = idx * 26 + (ord(c) - ord("A") + 1)
    return idx - 1  # 0-based index


# -----------------------
# Helper function: Process Orders sheet
# -----------------------
def process_orders_excel(excel_file):
    xls = pd.ExcelFile(excel_file)

    if "Orders" not in xls.sheet_names:
        raise ValueError('Sheet named "Orders" not found in the uploaded file.')

    orders = xls.parse("Orders")

    # ---- Fixed columns (by header name – ye pehle se sahi chal rahe the) ----
    col_order_id = "Transaction Summary"          # H column -> Order ID
    col_bank_value = "Unnamed: 3"                 # D column -> Bank Settlement Value (Rs.)

    if col_order_id not in orders.columns or col_bank_value not in orders.columns:
        raise ValueError(
            'Expected columns not found. Check if "Transaction Summary" (Order ID) '
            'and "Unnamed: 3" (Bank Settlement Value) exist in the "Orders" sheet.'
        )

    # ---- NEW: Seller SKU (BG) and Qty (BH) by column index, not header text ----
    sku_idx = excel_col_to_idx("BG")   # 0-based index for BG
    qty_idx = excel_col_to_idx("BH")   # 0-based index for BH

    if len(orders.columns) <= max(sku_idx, qty_idx):
        raise ValueError(
            "Orders sheet does not have enough columns to read BG/BH (Seller SKU / Qty)."
        )

    col_seller_sku = orders.columns[sku_idx]
    col_qty = orders.columns[qty_idx]

    # -----------------------
    # Filter rows jahan Order ID "OD" se start hota hai
    # -----------------------
    df = orders.copy()
    df = df[df[col_order_id].astype(str).str.startswith("OD", na=False)]

    if df.empty:
        raise ValueError("No order rows found starting with 'OD' in Order ID column.")

    # Numeric conversion
    df[col_bank_value] = pd.to_numeric(df[col_bank_value], errors="coerce")
    df[col_qty] = pd.to_numeric(df[col_qty], errors="coerce")

    # Remove rows with missing key values
    df = df[
        df[col_order_id].notna()
        & df[col_bank_value].notna()
        & df[col_seller_sku].notna()
        & df[col_qty].notna()
    ]

    # Clean dataframe with nice column names
    df_clean = df[[col_order_id, col_seller_sku, col_qty, col_bank_value]].rename(
        columns={
            col_order_id: "Order ID",
            col_seller_sku: "Seller SKU",
            col_qty: "Qty",
            col_bank_value: "Bank Settlement Value (Rs.)",
        }
    )

    # -----------------------
    # Pivot: group by Order ID + Seller SKU
    # -----------------------
    pivot = (
        df_clean.groupby(["Order ID", "Seller SKU"], as_index=False)
        .agg(
            Total_Qty=("Qty", "sum"),
            Total_Bank_Settlement_Value=(
                "Bank Settlement Value (Rs.)",
                "sum",
            ),
            Number_of_Records=("Bank Settlement Value (Rs.)", "size"),
            Average_Bank_Settlement_Value=(
                "Bank Settlement Value (Rs.)",
                "mean",
            ),
        )
        .sort_values("Total_Bank_Settlement_Value", ascending=False)
    )

    # Summary KPIs
    summary = {
        "total_unique_orders": df_clean["Order ID"].nunique(),
        "total_unique_sku": df_clean["Seller SKU"].nunique(),
        "total_rows": df_clean.shape[0],
        "grand_qty": df_clean["Qty"].sum(),
        "grand_total_bank": df_clean["Bank Settlement Value (Rs.)"].sum(),
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
    st.set_page_config(
        page_title="Settlement Pivot Tool",
        page_icon="📊",
        layout="wide",
    )
    st.title("📊 Settlement Pivot Tool — Orders + SKU + Qty")

    st.write(
        """
        Upload your settlement Excel file.  
        App uses **Orders** sheet and reads:
        - H → Order ID  
        - D → Bank Settlement Value (Rs.)  
        - BG → Seller SKU (data from row 4)  
        - BH → Qty (data from row 4)
        """
    )

    uploaded_file = st.file_uploader(
        "📁 Upload your Settlement Excel file", type=["xlsx", "xls"]
    )

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
            col5.metric(
                "Total Bank Settlement (Rs.)",
                f"{summary['grand_total_bank']:.2f}",
            )

            with st.expander("📄 View Raw Data", expanded=False):
                st.dataframe(df_clean, use_container_width=True)

            st.subheader("📑 Pivot (Order ID + Seller SKU + Qty + Bank Amount)")
            st.dataframe(pivot_df, use_container_width=True)

            excel_bytes = to_excel_bytes(df_clean, pivot_df)
            st.download_button(
                "⬇️ Download Pivot Excel",
                data=excel_bytes,
                file_name="settlement_sku_pivot.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        except Exception as e:
            st.error(f"❌ Error: {e}")


if __name__ == "__main__":
    main()


