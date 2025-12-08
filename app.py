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
# PROCESS 1: Settlement file (Orders sheet)
# -----------------------
def process_orders_excel(excel_file):
    xls = pd.ExcelFile(excel_file)

    if "Orders" not in xls.sheet_names:
        raise ValueError('Sheet named "Orders" not found in the uploaded file.')

    orders = xls.parse("Orders")

    # Fixed columns by header name
    col_order_id = "Transaction Summary"          # H column -> Order ID
    col_bank_value = "Unnamed: 3"                 # D column -> Bank Settlement Value (Rs.)

    if col_order_id not in orders.columns or col_bank_value not in orders.columns:
        raise ValueError(
            'Expected columns not found. Check if "Transaction Summary" (Order ID) '
            'and "Unnamed: 3" (Bank Settlement Value) exist in the "Orders" sheet.'
        )

    # Seller SKU (BG) and Qty (BH) by index (header may be merged)
    sku_idx = excel_col_to_idx("BG")
    qty_idx = excel_col_to_idx("BH")

    if len(orders.columns) <= max(sku_idx, qty_idx):
        raise ValueError(
            "Orders sheet does not have enough columns to read BG/BH (Seller SKU / Qty)."
        )

    col_seller_sku = orders.columns[sku_idx]
    col_qty = orders.columns[qty_idx]

    # Filter rows where Order ID starts with "OD"
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

    # Pivot: group by Order ID + Seller SKU
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
# PROCESS 2: Sale Report file (Sale Report sheet)
# -----------------------
def clean_sku_text(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    # remove surrounding quotes
    s = s.strip('"').strip("'").strip()
    # remove "SKU:" prefix (case-insensitive)
    if s.upper().startswith("SKU:"):
        s = s[4:]
    return s.strip()


def process_sales_excel(excel_file):
    xls = pd.ExcelFile(excel_file)

    # ---- Sheet name ko flexibly detect karo ----
    target_sheet = None
    for s in xls.sheet_names:
        s_clean = s.strip().lower().replace(" ", "")
        # exact style: "Sale Report", "sale report", etc.
        if s_clean == "salereport":
            target_sheet = s
            break
        # generic: jisme "sale" + "report" dono hon
        if "sale" in s_clean and "report" in s_clean:
            target_sheet = s
            break

    if target_sheet is None:
        raise ValueError(
            f'Sale report sheet not found. Available sheets: {", ".join(xls.sheet_names)}'
        )

    # ab jo sheet mila hai use parse karein
    sales = xls.parse(target_sheet)

    # Columns by index: C (Order ID), F (Seller SKU), N (Qty)
    order_idx = excel_col_to_idx("C")
    sku_idx = excel_col_to_idx("F")
    qty_idx = excel_col_to_idx("N")

    if len(sales.columns) <= max(order_idx, sku_idx, qty_idx):
        raise ValueError(
            "Sale Report sheet does not have enough columns for C/F/N (Order ID / Seller SKU / Qty)."
        )

    col_order = sales.columns[order_idx]
    col_sku = sales.columns[sku_idx]
    col_qty = sales.columns[qty_idx]

    df = sales[[col_order, col_sku, col_qty]].copy()

    df["Order ID"] = df[col_order].astype(str).str.strip()
    df["Seller SKU"] = df[col_sku].apply(clean_sku_text)
    df["Sale Qty"] = pd.to_numeric(df[col_qty], errors="coerce")

    df = df[
        df["Order ID"].notna()
        & df["Seller SKU"].notna()
        & df["Sale Qty"].notna()
    ]

    # Pivot by Order ID + Seller SKU
    pivot = (
        df.groupby(["Order ID", "Seller SKU"], as_index=False)
        .agg(
            Total_Sale_Qty=("Sale Qty", "sum"),
            Number_of_Sale_Rows=("Sale Qty", "size"),
        )
        .sort_values("Total_Sale_Qty", ascending=False)
    )

    summary = {
        "total_unique_orders": df["Order ID"].nunique(),
        "total_unique_sku": df["Seller SKU"].nunique(),
        "total_rows": df.shape[0],
        "grand_sale_qty": df["Sale Qty"].sum(),
    }

    return df, pivot, summary


# -----------------------
# Helper: Excel Download
# -----------------------
def to_excel_bytes(df_orders_raw, df_orders_pivot,
                   df_sales_raw=None, df_sales_pivot=None,
                   df_mapping=None):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_orders_raw.to_excel(writer, index=False, sheet_name="Orders_Raw")
        df_orders_pivot.to_excel(writer, index=False, sheet_name="Orders_Pivot")

        if df_sales_raw is not None:
            df_sales_raw.to_excel(writer, index=False, sheet_name="Sales_Raw")
        if df_sales_pivot is not None:
            df_sales_pivot.to_excel(writer, index=False, sheet_name="Sales_Pivot")
        if df_mapping is not None:
            df_mapping.to_excel(writer, index=False, sheet_name="Mapping")
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
    st.title("📊 Settlement Pivot & Sales Mapping Tool")

    st.markdown(
        """
        **Step 1:** Upload your **Settlement Excel** (Orders sheet)  
        **Step 2:** Upload your **Sale Report Excel** (sheet jiske naam me 'Sale' + 'Report' ho)  

        Mapping is done on **Order ID + Seller SKU**  
        - Settlement: H (Order ID), BG (Seller SKU), BH (Qty), D (Bank Settlement)  
        - Sales: C (Order ID), F (Seller SKU - cleaned), N (Qty)
        """
    )

    col_up1, col_up2 = st.columns(2)

    with col_up1:
        settlement_file = st.file_uploader(
            "📁 Upload Settlement Excel (Orders sheet)",
            type=["xlsx", "xls"],
            key="settlement_file",
        )

    with col_up2:
        sales_file = st.file_uploader(
            "📁 Upload Sale Report Excel",
            type=["xlsx", "xls"],
            key="sales_file",
        )

    if settlement_file:
        try:
            with st.spinner("Processing Settlement (Orders) file..."):
                orders_raw, orders_pivot, orders_summary = process_orders_excel(
                    settlement_file
                )

            st.subheader("📌 Settlement Summary (Orders)")
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Unique Orders", orders_summary["total_unique_orders"])
            c2.metric("Unique SKUs", orders_summary["total_unique_sku"])
            c3.metric("Total Rows", orders_summary["total_rows"])
            c4.metric("Total Qty", orders_summary["grand_qty"])
            c5.metric(
                "Total Bank Settlement (Rs.)",
                f"{orders_summary['grand_total_bank']:.2f}",
            )

            with st.expander("📄 View Settlement Raw Data", expanded=False):
                st.dataframe(orders_raw, use_container_width=True)

            st.subheader("📑 Settlement Pivot (Order ID + Seller SKU)")
            st.dataframe(orders_pivot, use_container_width=True)

            # ---- If Sales Report also uploaded, process & map ----
            sales_raw = sales_pivot = mapping_df = sales_summary = None

            if sales_file:
                with st.spinner("Processing Sale Report file..."):
                    sales_raw, sales_pivot, sales_summary = process_sales_excel(
                        sales_file
                    )

                st.subheader("🧾 Sales Summary (Sale Report)")
                s1, s2, s3, s4 = st.columns(4)
                s1.metric("Unique Orders", sales_summary["total_unique_orders"])
                s2.metric("Unique SKUs", sales_summary["total_unique_sku"])
                s3.metric("Total Rows", sales_summary["total_rows"])
                s4.metric("Total Sale Qty", sales_summary["grand_sale_qty"])

                with st.expander("📄 View Sale Report Raw Data", expanded=False):
                    st.dataframe(sales_raw, use_container_width=True)

                st.subheader("📑 Sales Pivot (Order ID + Seller SKU)")
                st.dataframe(sales_pivot, use_container_width=True)

                # -------- MAPPING --------
                st.subheader("🔗 Mapping: Settlement vs Sale Report")

                mapping_df = orders_pivot.merge(
                    sales_pivot,
                    on=["Order ID", "Seller SKU"],
                    how="outer",
                )

                # Fill NaNs
                if "Total_Qty" in mapping_df.columns:
                    mapping_df["Total_Qty"] = mapping_df["Total_Qty"].fillna(0)
                if "Total_Sale_Qty" in mapping_df.columns:
                    mapping_df["Total_Sale_Qty"] = mapping_df["Total_Sale_Qty"].fillna(0)

                # Qty difference (Settlement - Sales)
                if "Total_Qty" in mapping_df.columns and "Total_Sale_Qty" in mapping_df.columns:
                    mapping_df["Qty_Diff (Settle - Sale)"] = (
                        mapping_df["Total_Qty"] - mapping_df["Total_Sale_Qty"]
                    )

                st.dataframe(mapping_df, use_container_width=True)

            # ---- Download (Settlement only OR Settlement + Sales + Mapping) ----
            st.markdown("---")
            st.subheader("⬇️ Download Report Excel")

            excel_bytes = to_excel_bytes(
                orders_raw,
                orders_pivot,
                df_sales_raw=sales_raw,
                df_sales_pivot=sales_pivot,
                df_mapping=mapping_df,
            )

            st.download_button(
                "Download Complete Report (Excel)",
                data=excel_bytes,
                file_name="settlement_sales_mapping.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        except Exception as e:
            st.error(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
