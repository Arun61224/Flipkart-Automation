import streamlit as st
import pandas as pd
from io import BytesIO

# -----------------------
# Helper: Excel column name -> 0-based index
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
def process_orders_excel_from_bytes(file_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)

    if "Orders" not in xls.sheet_names:
        raise ValueError('Sheet named "Orders" not found in the uploaded file.')

    bio.seek(0)
    orders = pd.read_excel(bio, sheet_name="Orders", engine="openpyxl")

    col_order_id = "Transaction Summary"          # H column -> Order ID
    col_bank_value = "Unnamed: 3"                 # D column -> Bank Settlement Value (Rs.)

    if col_order_id not in orders.columns or col_bank_value not in orders.columns:
        raise ValueError(
            'Expected columns not found. Check if "Transaction Summary" (Order ID) '
            'and "Unnamed: 3" (Bank Settlement Value) exist in the "Orders" sheet.'
        )

    # Seller SKU (BG) and Qty (BH) by index
    sku_idx = excel_col_to_idx("BG")
    qty_idx = excel_col_to_idx("BH")

    if len(orders.columns) <= max(sku_idx, qty_idx):
        raise ValueError(
            "Orders sheet does not have enough columns to read BG/BH (Seller SKU / Qty)."
        )

    col_seller_sku = orders.columns[sku_idx]
    col_qty = orders.columns[qty_idx]

    df = orders.copy()
    df = df[df[col_order_id].astype(str).str.startswith("OD", na=False)]

    if df.empty:
        raise ValueError("No order rows found starting with 'OD' in Order ID column.")

    df[col_bank_value] = pd.to_numeric(df[col_bank_value], errors="coerce")
    df[col_qty] = pd.to_numeric(df[col_qty], errors="coerce")

    df = df[
        df[col_order_id].notna()
        & df[col_bank_value].notna()
        & df[col_seller_sku].notna()
        & df[col_qty].notna()
    ]

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


@st.cache_data(show_spinner=False)
def process_multiple_orders_cached(files_bytes_list):
    all_raw = []
    for fb in files_bytes_list:
        df_clean = process_orders_excel_from_bytes(fb)
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


def load_single_sales_df_from_bytes(file_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)

    # sheet name detect (contains "sale" + "report")
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

    bio.seek(0)
    sales = pd.read_excel(bio, sheet_name=target_sheet, engine="openpyxl")

    # B: Order ID (OD...), F: SKU, N: Qty, H: Event
    order_idx = excel_col_to_idx("B")
    sku_idx = excel_col_to_idx("F")
    qty_idx = excel_col_to_idx("N")
    event_idx = excel_col_to_idx("H")

    if len(sales.columns) <= max(order_idx, sku_idx, qty_idx, event_idx):
        raise ValueError(
            "Sale Report sheet does not have enough columns for B/F/N/H "
            "(Order ID / Seller SKU / Qty / Event)."
        )

    col_order = sales.columns[order_idx]
    col_sku = sales.columns[sku_idx]
    col_qty = sales.columns[qty_idx]
    col_event = sales.columns[event_idx]

    # Order Date
    col_order_date = None
    for c in sales.columns:
        if "order date" in str(c).lower():
            col_order_date = c
            break
    if col_order_date is None:
        raise ValueError("Could not find 'Order Date' column in Sale Report.")

    # Final Invoice Amount
    col_invoice = None
    for c in sales.columns:
        txt = str(c).lower()
        if "final invoice amount" in txt or "price after discount" in txt:
            col_invoice = c
            break
    if col_invoice is None:
        raise ValueError(
            "Could not find 'Final Invoice Amount (Price after discount+Shipping Charges)' column."
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
    """
    NEW LOGIC:
    - saari 'Return' rows hata do
    - baaki (Sale, etc.) pe pivot banao
    """
    # drop all returns
    df_clean = df[~df["Event"].str.lower().eq("return")].copy()

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


@st.cache_data(show_spinner=False)
def process_multiple_sales_cached(files_bytes_list):
    all_raw = []
    for fb in files_bytes_list:
        df_one = load_single_sales_df_from_bytes(fb)
        all_raw.append(df_one)

    combined_raw = pd.concat(all_raw, ignore_index=True)
    cleaned_sales, pivot, summary = summarize_sales(combined_raw)
    return cleaned_sales, pivot, summary


# ======================================================
#   PART 3 – COST PRICE FILE (SKU, Cost Price)
# ======================================================
@st.cache_data(show_spinner=False)
def process_cost_file_cached(file_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    df = pd.read_excel(bio, sheet_name=0, engine="openpyxl")

    sku_col = None
    cost_col = None
    for c in df.columns:
        txt = str(c).lower()
        if "sku" in txt and sku_col is None:
            sku_col = c
        if ("cost" in txt or "cost price" in txt or txt == "cp") and cost_col is None:
            cost_col = c

    if sku_col is None:
        raise ValueError("SKU column not found in Cost Price file.")
    if cost_col is None:
        raise ValueError("Cost Price column not found in Cost Price file.")

    out = df[[sku_col, cost_col]].copy()
    out["SKU"] = out[sku_col].astype(str).str.strip()
    out["Cost Price"] = pd.to_numeric(out[cost_col], errors="coerce")

    out = out[out["SKU"].notna() & out["Cost Price"].notna()]
    out = out[["SKU", "Cost Price"]]
    return out


# ======================================================
#   FINAL REPORT → SINGLE SHEET EXCEL
# ======================================================
def final_report_to_excel_bytes(df_mapping: pd.DataFrame):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_mapping.to_excel(writer, index=False, sheet_name="Final_Mapped_Report")
    return output.getvalue()


# ======================================================
#   STREAMLIT APP  +  DASHBOARD
# ======================================================
def main():
    st.set_page_config(
        page_title="Flipkart Reconciliation Dashboard",
        page_icon="📊",
        layout="wide",
    )

    st.title("📊 Flipkart Reconciliation Dashboard")

    col_up1, col_up2, col_up3 = st.columns(3)

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

    with col_up3:
        cost_file = st.file_uploader(
            "Upload SKU Cost Price file",
            type=["xlsx", "xls"],
            key="cost_file",
            accept_multiple_files=False,
        )

    st.markdown("---")

    if settlement_files and sales_files:
        try:
            settlement_bytes_list = [f.getvalue() for f in settlement_files]
            sales_bytes_list = [f.getvalue() for f in sales_files]

            with st.spinner("Processing Settlement file(s)..."):
                orders_raw, orders_pivot, orders_summary = process_multiple_orders_cached(
                    settlement_bytes_list
                )

            with st.spinner("Processing Sale Report file(s)..."):
                sales_raw, sales_pivot, sales_summary = process_multiple_sales_cached(
                    sales_bytes_list
                )

            # ---- Final mapping: Sales → Settlement ----
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
            mapping_df["Amount_Diff (Settlement - Invoice)"] = (
                mapping_df["Payment_Received"]
                - mapping_df["Final Invoice Amount (Price after discount+Shipping Charges)"]
            )

            # ---- Merge Cost Price ----
            if cost_file is not None:
                cost_bytes = cost_file.getvalue()
                with st.spinner("Processing Cost Price file..."):
                    cost_df = process_cost_file_cached(cost_bytes)

                mapping_df = mapping_df.merge(cost_df, on="SKU", how="left")
                mapping_df["Cost Price"] = mapping_df["Cost Price"].fillna(0.0)
                mapping_df["Total Cost (Qty * Cost)"] = (
                    mapping_df["Item Quantity"] * mapping_df["Cost Price"]
                )
            else:
                mapping_df["Cost Price"] = 0.0
                mapping_df["Total Cost (Qty * Cost)"] = 0.0

            col_order = [
                "Order Date",
                "Order ID",
                "SKU",
                "Item Quantity",
                "Final Invoice Amount (Price after discount+Shipping Charges)",
                "Settlement_Qty",
                "Qty_Diff (Settlement - Sale)",
                "Payment_Received",
                "Amount_Diff (Settlement - Invoice)",
                "Cost Price",
                "Total Cost (Qty * Cost)",
            ]
            mapping_df = mapping_df[[c for c in col_order if c in mapping_df.columns]]

            # ---------------- DASHBOARD -----------------
            st.subheader("Dashboard")

            f_col1, f_col2, f_col3 = st.columns(3)

            with f_col1:
                min_date = mapping_df["Order Date"].min()
                max_date = mapping_df["Order Date"].max()
                date_range = st.date_input(
                    "Order Date range",
                    value=(min_date, max_date) if not pd.isna(min_date) else None,
                )

            with f_col2:
                sku_list = sorted(mapping_df["SKU"].dropna().unique().tolist())
                sel_skus = st.multiselect("SKU filter", options=sku_list)

            with f_col3:
                order_id_search = st.text_input("Order ID contains")

            df_filt = mapping_df.copy()
            if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
                start, end = date_range
                if start and end:
                    df_filt = df_filt[
                        (df_filt["Order Date"].dt.date >= start)
                        & (df_filt["Order Date"].dt.date <= end)
                    ]
            if sel_skus:
                df_filt = df_filt[df_filt["SKU"].isin(sel_skus)]
            if order_id_search:
                df_filt = df_filt[
                    df_filt["Order ID"].astype(str).str.contains(order_id_search.strip(), case=False)
                ]

            total_rows = df_filt.shape[0]
            unique_orders = df_filt["Order ID"].nunique()
            total_invoice = df_filt["Final Invoice Amount (Price after discount+Shipping Charges)"].sum()
            total_payment = df_filt["Payment_Received"].sum()
            total_qty_diff = df_filt["Qty_Diff (Settlement - Sale)"].sum()
            total_cost_filtered = df_filt["Total Cost (Qty * Cost)"].sum()

            reconciled_rows = df_filt[df_filt["Qty_Diff (Settlement - Sale)"] == 0].shape[0]
            not_reconciled_rows = total_rows - reconciled_rows

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Rows (Order+SKU)", total_rows)
            k2.metric("Unique Orders", unique_orders)
            k3.metric("Invoice Total", f"{total_invoice:,.2f}")
            k4.metric("Payment Total", f"{total_payment:,.2f}")

            k5, k6, k7 = st.columns(3)
            k5.metric("Reconciled Rows (Qty=0)", reconciled_rows)
            k6.metric("Not Reconciled Rows", not_reconciled_rows)
            k7.metric("Net Qty Diff", int(total_qty_diff))

            if cost_file is not None:
                k8, _ = st.columns(2)
                k8.metric("Total Cost (filtered)", f"{total_cost_filtered:,.2f}")

            tab1, tab2, tab3 = st.tabs(["Final Table", "Qty Diff by SKU", "Raw Pivots"])

            with tab1:
                st.dataframe(df_filt, use_container_width=True)

            with tab2:
                df_chart = (
                    df_filt.groupby("SKU", as_index=False)["Qty_Diff (Settlement - Sale)"]
                    .sum()
                    .sort_values("Qty_Diff (Settlement - Sale)", ascending=False)
                )
                st.bar_chart(
                    data=df_chart,
                    x="SKU",
                    y="Qty_Diff (Settlement - Sale)",
                )

            with tab3:
                st.write("Settlement Pivot")
                st.dataframe(orders_pivot, use_container_width=True)
                st.write("Sales Pivot (after removing all returns)")
                st.dataframe(sales_pivot, use_container_width=True)

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
    else:
        st.info("Upload Settlement, Sale Report and (optional) Cost Price file to start reconciliation.")


if __name__ == "__main__":
    main()
