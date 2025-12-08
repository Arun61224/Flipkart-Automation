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


# ======================================================
#   PART 1 – SETTLEMENT FILES (Orders sheet)
# ======================================================
def process_orders_excel(excel_file: BytesIO) -> pd.DataFrame:
    xls = pd.ExcelFile(excel_file)

    if "Orders" not in xls.sheet_names:
        raise ValueError('Sheet named "Orders" not found in the uploaded file.')

    orders = xls.parse("Orders")

    # Fixed columns by header name
    col_order_id = "Transaction Summary"          # H column -> Order ID (numeric / whatever Ajio ne diya)
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
            col_qty: "Settlement Qty",
            col_bank_value: "Payment Received",
        }
    )

    return df_clean


def summarize_orders(df_clean: pd.DataFrame):
    # Pivot per Order ID + Seller SKU
    pivot = (
        df_clean.groupby(["Order ID", "Seller SKU"], as_index=False)
        .agg(
            Settlement_Qty=("Settlement Qty", "sum"),
            Payment_Received=("Payment Received", "sum"),
            Number_of_Records=("Payment Received", "size"),
            Avg_Payment_Received=("Payment Received", "mean"),
        )
        .sort_values("Payment_Received", ascending=False)
    )

    summary = {
        "total_unique_orders": df_clean["Order ID"].nunique(),
        "total_unique_sku": df_clean["Seller SKU"].nunique(),
        "total_rows": df_clean.shape[0],
        "grand_qty": df_clean["Settlement Qty"].sum(),
        "grand_total_payment": df_clean["Payment Received"].sum(),
    }
    return pivot, summary


def process_multiple_orders(files):
    all_raw = []
    for f in files:
        df_clean = process_orders_excel(f)
        all_raw.append(df_clean)

    combined_raw = pd.concat(all_raw, ignore_index=True)
    combined_pivot, combined_summary = summarize_orders(combined_raw)
    return combined_raw, combined_pivot, combined_summary


# ======================================================
#   PART 2 – SALES REPORT FILES
# ======================================================
def clean_sku_text(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    s = s.strip('"').strip("'").strip()
    if s.upper().startswith("SKU:"):
        s = s[4:]
    return s.strip()


def _load_single_sales_df(excel_file: BytesIO) -> pd.DataFrame:
    xls = pd.ExcelFile(excel_file)

    # Sheet name detect (something with "sale" + "report")
    target_sheet = None
    for s in xls.sheet_names:
        s_clean = s.strip().lower().replace(" ", "")
        if s_clean == "salereport":
            target_sheet = s
            break
        if "sale" in s_clean and "report" in s_clean:
            target_sheet = s
            break

    if target_sheet is None:
        raise ValueError(
            f'Sale report sheet not found. Available sheets: {", ".join(xls.sheet_names)}'
        )

    sales = xls.parse(target_sheet)

    # 🔴 CHANGE: B = Actual Order ID (alpha+numeric), NOT C
    order_idx = excel_col_to_idx("B")   # Order ID with OD...
    sku_idx = excel_col_to_idx("F")     # SKU
    qty_idx = excel_col_to_idx("N")     # Qty
    event_idx = excel_col_to_idx("H")   # Event

    if len(sales.columns) <= max(order_idx, sku_idx, qty_idx, event_idx):
        raise ValueError(
            "Sale Report sheet does not have enough columns for B/F/N/H "
            "(Order ID / Seller SKU / Qty / Event)."
        )

    col_order = sales.columns[order_idx]
    col_sku = sales.columns[sku_idx]
    col_qty = sales.columns[qty_idx]
    col_event = sales.columns[event_idx]

    # Order Date column – by name
    col_order_date = None
    for c in sales.columns:
        if "order date" in str(c).lower():
            col_order_date = c
            break
    if col_order_date is None:
        raise ValueError(
            "Could not find 'Order Date' column in Sale Report. "
            f"Available columns: {list(sales.columns)}"
        )

    # Final Invoice Amount column – by name
    col_invoice = None
    for c in sales.columns:
        txt = str(c).lower()
        if "final invoice amount" in txt or "price after discount" in txt:
            col_invoice = c
            break
    if col_invoice is None:
        raise ValueError(
            "Could not find 'Final Invoice Amount (Price after discount+Shipping Charges)' column "
            "in Sale Report."
        )

    df = sales[[col_order_date, col_order, col_sku, col_qty, col_event, col_invoice]].copy()

    df["Order Date"] = pd.to_datetime(df[col_order_date], errors="coerce")
    df["Order ID"] = df[col_order].astype(str).str.strip()
    df["SKU"] = df[col_sku].apply(clean_sku_text)
    df["Item Quantity"] = pd.to_numeric(df[col_qty], errors="coerce")
    df["Event"] = df[col_event].astype(str).str.strip()
    df["Final Invoice Amount (Price after discount+Shipping Charges)"] = pd.to_numeric(
        df[col_invoice], errors="coerce"
    )

    df = df[
        df["Order ID"].notna()
        & df["SKU"].notna()
        & df["Item Quantity"].notna()
        & df["Final Invoice Amount (Price after discount+Shipping Charges)"].notna()
        & df["Event"].notna()
    ]

    return df[
        [
            "Order Date",
            "Order ID",
            "SKU",
            "Item Quantity",
            "Final Invoice Amount (Price after discount+Shipping Charges)",
            "Event",
        ]
    ]


def summarize_sales(df: pd.DataFrame):
    # remove only RETURN rows where same (Order ID, SKU, Qty) has both Sale & Return
    group_key = ["Order ID", "SKU", "Item Quantity"]

    def pair_flag(s):
        vals = s.str.lower().values
        return int(("sale" in vals) and ("return" in vals))

    pair_exists = (
        df.groupby(group_key)["Event"]
        .transform(pair_flag)
        .astype(bool)
    )

    is_return = df["Event"].str.lower().eq("return")
    drop_mask = pair_exists & is_return
    df_clean = df[~drop_mask].copy()

    # Sales pivot (Order ID + SKU)
    sales_pivot = (
        df_clean.groupby(["Order ID", "SKU"], as_index=False)
        .agg(
            Order_Date=("Order Date", "min"),
            Item_Quantity=("Item Quantity", "sum"),
            Final_Invoice_Amount=(
                "Final Invoice Amount (Price after discount+Shipping Charges)",
                "sum",
            ),
        )
        .rename(
            columns={
                "Order_Date": "Order Date",
                "Item_Quantity": "Item Quantity",
                "Final_Invoice_Amount": "Final Invoice Amount (Price after discount+Shipping Charges)",
            }
        )
    )

    summary = {
        "total_unique_orders": df_clean["Order ID"].nunique(),
        "total_unique_sku": df_clean["SKU"].nunique(),
        "total_rows": df_clean.shape[0],
        "grand_sale_qty": df_clean["Item Quantity"].sum(),
        "grand_invoice_amount": df_clean[
            "Final Invoice Amount (Price after discount+Shipping Charges)"
        ].sum(),
    }

    return df_clean, sales_pivot, summary


def process_multiple_sales(files):
    all_raw = []
    for f in files:
        df_one = _load_single_sales_df(f)
        all_raw.append(df_one)

    combined_raw = pd.concat(all_raw, ignore_index=True)
    cleaned_sales, pivot, summary = summarize_sales(combined_raw)
    return cleaned_sales, pivot, summary


# ======================================================
#   HELPER: EXCEL DOWNLOAD  -> ONLY FINAL MAPPED SHEET
# ======================================================
def final_report_to_excel_bytes(df_mapping: pd.DataFrame):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_mapping.to_excel(writer, index=False, sheet_name="Final_Mapped_Report")
    return output.getvalue()


# ======================================================
#   STREAMLIT APP (MINIMAL UI)
# ======================================================
def main():
    st.set_page_config(
        page_title="Settlement–Sales Mapping Tool",
        page_icon="📊",
        layout="wide",
    )

    st.title("📊 Settlement & Sales Mapping")

    col_up1, col_up2 = st.columns(2)

    with col_up1:
        settlement_files = st.file_uploader(
            "Upload Settlement Excel file(s)",
            type=["xlsx", "xls"],
            key="settlement_files",
            accept_multiple_files=True,
        )

    with col_up2:
        sales_files = st.file_uploader(
            "Upload Sale Report Excel file(s)",
            type=["xlsx", "xls"],
            key="sales_files",
            accept_multiple_files=True,
        )

    st.markdown("---")

    if settlement_files:
        try:
            # -------- Settlement processing --------
            with st.spinner("Processing Settlement file(s)..."):
                orders_raw, orders_pivot, orders_summary = process_multiple_orders(
                    settlement_files
                )

            st.subheader("Settlement Summary")
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Unique Orders", orders_summary["total_unique_orders"])
            c2.metric("Unique SKUs", orders_summary["total_unique_sku"])
            c3.metric("Rows", orders_summary["total_rows"])
            c4.metric("Settlement Qty", orders_summary["grand_qty"])
            c5.metric(
                "Payment Received (Total)",
                f"{orders_summary['grand_total_payment']:.2f}",
            )

            with st.expander("Settlement Raw Data (Combined)", expanded=False):
                st.dataframe(orders_raw, use_container_width=True)

            st.subheader("Settlement Pivot (Order ID + SKU)")
            st.dataframe(orders_pivot, use_container_width=True)

            # -------- Sales processing + mapping --------
            sales_raw = sales_pivot = mapping_df = sales_summary = None

            if sales_files:
                with st.spinner("Processing Sale Report file(s)..."):
                    sales_raw, sales_pivot, sales_summary = process_multiple_sales(
                        sales_files
                    )

                st.subheader("Sales Summary")
                s1, s2, s3, s4, s5 = st.columns(5)
                s1.metric("Unique Orders", sales_summary["total_unique_orders"])
                s2.metric("Unique SKUs", sales_summary["total_unique_sku"])
                s3.metric("Rows (after returns)", sales_summary["total_rows"])
                s4.metric("Sale Qty", sales_summary["grand_sale_qty"])
                s5.metric(
                    "Invoice Amount (Total)",
                    f"{sales_summary['grand_invoice_amount']:.2f}",
                )

                with st.expander("Sales Raw Data (Cleaned)", expanded=False):
                    st.dataframe(sales_raw, use_container_width=True)

                st.subheader("Sales Pivot (Order ID + SKU)")
                st.dataframe(sales_pivot, use_container_width=True)

                # ---- Final mapping: Sales → Settlement ----
                st.subheader("Final Mapped Report")

                mapping_df = sales_pivot.merge(
                    orders_pivot[["Order ID", "Seller SKU", "Settlement_Qty", "Payment_Received"]],
                    left_on=["Order ID", "SKU"],
                    right_on=["Order ID", "Seller SKU"],
                    how="left",
                )

                mapping_df = mapping_df.drop(columns=["Seller SKU"], errors="ignore")
                mapping_df["Settlement_Qty"] = mapping_df["Settlement_Qty"].fillna(0)
                mapping_df["Payment_Received"] = mapping_df["Payment_Received"].fillna(0.0)

                mapping_df["Qty_Diff (Settlement - Sale)"] = (
                    mapping_df["Settlement_Qty"] - mapping_df["Item Quantity"]
                )

                col_order = [
                    "Order Date",
                    "Order ID",
                    "SKU",
                    "Item Quantity",
                    "Final Invoice Amount (Price after discount+Shipping Charges)",
                    "Settlement_Qty",
                    "Qty_Diff (Settlement - Sale)",
                    "Payment_Received",
                ]
                mapping_df = mapping_df[[c for c in col_order if c in mapping_df.columns]]

                st.dataframe(mapping_df, use_container_width=True)

                # -------- Download ONLY Final sheet --------
                st.markdown("---")
                st.subheader("Download Excel")

                excel_bytes = final_report_to_excel_bytes(mapping_df)

                st.download_button(
                    "Download Final Mapped Report",
                    data=excel_bytes,
                    file_name="Final_Mapped_Report.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

        except Exception as e:
            st.error(f"Error: {e}")


if __name__ == "__main__":
    main()
