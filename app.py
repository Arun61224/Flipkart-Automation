# app.py
import streamlit as st
import pandas as pd
from io import BytesIO

# -----------------------
# Excel column name -> 0-based index (A=0, B=1, ..., BG=58, BH=59)
# -----------------------
def excel_col_to_idx(col_name: str) -> int:
    col_name = col_name.strip().upper()
    idx = 0
    for c in col_name:
        if "A" <= c <= "Z":
            idx = idx * 26 + (ord(c) - ord("A") + 1)
    return idx - 1


# ========================= SETTLEMENT (Orders sheet) =========================
def process_orders_excel_from_bytes(file_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)

    if "Orders" not in xls.sheet_names:
        raise ValueError('Sheet named "Orders" not found in settlement file.')

    bio.seek(0)
    orders = pd.read_excel(bio, sheet_name="Orders", engine="openpyxl")

    col_order_id = "Transaction Summary"   # H column header in sample
    col_bank_value = "Unnamed: 3"          # D column (Bank Settlement Value (Rs.)) in sample

    if col_order_id not in orders.columns or col_bank_value not in orders.columns:
        raise ValueError(
            'Settlement "Orders" sheet must contain columns: '
            f'"{col_order_id}" and "{col_bank_value}" (check merged headers).'
        )

    # Seller SKU (BG) and Qty (BH) by column index (header may be merged)
    sku_idx = excel_col_to_idx("BG")
    qty_idx = excel_col_to_idx("BH")

    if len(orders.columns) <= max(sku_idx, qty_idx):
        raise ValueError("Orders sheet doesn't have BG/BH columns (Seller SKU / Qty).")

    col_seller_sku = orders.columns[sku_idx]
    col_qty = orders.columns[qty_idx]

    # Keep only real order rows (Order ID starting with OD typically)
    df = orders.copy()
    df = df[df[col_order_id].astype(str).str.strip().str.startswith("OD", na=False)]

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
        )
    )
    summary = {
        "rows": df_clean.shape[0],
        "orders": df_clean["Order ID"].nunique(),
        "skus": df_clean["Seller SKU"].nunique(),
        "qty": df_clean["Settlement Qty"].sum(),
        "payment": df_clean["Payment Received"].sum(),
    }
    return pivot, summary


@st.cache_data(show_spinner=False)
def process_multiple_orders_cached(files_bytes_list):
    all_raw = [process_orders_excel_from_bytes(fb) for fb in files_bytes_list]
    combined = pd.concat(all_raw, ignore_index=True)
    pivot, summary = summarize_orders(combined)
    return combined, pivot, summary


# ========================= SALES REPORT =========================
def clean_sku_text(v):
    if pd.isna(v):
        return None
    s = str(v).strip().strip('"').strip("'")
    if s.upper().startswith("SKU:"):
        s = s[4:]
    return s.strip()


def load_single_sales_df_from_bytes(file_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)

    # detect Sale Report sheet (flexible)
    target_sheet = None
    for s in xls.sheet_names:
        chk = s.lower().replace(" ", "")
        if "sale" in chk and "report" in chk:
            target_sheet = s
            break
    if not target_sheet:
        raise ValueError('Sheet named like "Sale Report" not found in sales file.')

    bio.seek(0)
    sales = pd.read_excel(bio, sheet_name=target_sheet, engine="openpyxl")

    # Column positions: B=Order ID, F=SKU, N=Qty, H=Event
    order_idx = excel_col_to_idx("B")
    sku_idx = excel_col_to_idx("F")
    qty_idx = excel_col_to_idx("N")
    event_idx = excel_col_to_idx("H")

    if len(sales.columns) <= max(order_idx, sku_idx, qty_idx, event_idx):
        raise ValueError("Sale Report sheet missing expected columns at B/F/N/H positions.")

    col_order = sales.columns[order_idx]
    col_sku = sales.columns[sku_idx]
    col_qty = sales.columns[qty_idx]
    col_event = sales.columns[event_idx]

    # Order Date column (detect by name)
    order_date_candidates = [c for c in sales.columns if "order date" in str(c).lower()]
    if not order_date_candidates:
        raise ValueError("Could not find 'Order Date' column in Sale Report.")
    col_order_date = order_date_candidates[0]

    # Final Invoice Amount (exact long name present in file)
    invoice_candidates = [c for c in sales.columns if "final invoice amount" in str(c).lower()]
    if not invoice_candidates:
        raise ValueError(
            "Could not find 'Final Invoice Amount (Price after discount+Shipping Charges)' column in Sale Report."
        )
    col_invoice = invoice_candidates[0]

    # Price Before Discount (optional)
    col_price_before = None
    for c in sales.columns:
        if "price before discount" in str(c).lower():
            col_price_before = c
            break

    # Take columns (include price-before if present)
    cols = [col_order_date, col_order, col_sku, col_qty, col_event, col_invoice]
    if col_price_before is not None:
        cols.append(col_price_before)

    df = sales[cols].copy()

    df["Order Date"] = pd.to_datetime(df[col_order_date], errors="coerce")
    df["Order ID"] = df[col_order].astype(str).str.strip()
    df["SKU"] = df[col_sku].apply(clean_sku_text)
    df["Item Quantity"] = pd.to_numeric(df[col_qty], errors="coerce")
    df["Event"] = df[col_event].astype(str).fillna("").str.strip()

    # Map the original long invoice column into a short name for internal use/display
    df["Invoice Amount"] = pd.to_numeric(df[col_invoice], errors="coerce")

    if col_price_before is not None:
        df["Price Before Discount"] = pd.to_numeric(df[col_price_before], errors="coerce")
    else:
        df["Price Before Discount"] = 0.0

    # ---------- NEW: convert Return rows to NEGATIVE values instead of removing ----------
    df["IsReturn"] = df["Event"].str.lower().eq("return")

    df.loc[df["IsReturn"], "Item Quantity"] = df.loc[df["IsReturn"], "Item Quantity"] * -1
    df.loc[df["IsReturn"], "Invoice Amount"] = df.loc[df["IsReturn"], "Invoice Amount"] * -1
    df.loc[df["IsReturn"], "Price Before Discount"] = df.loc[df["IsReturn"], "Price Before Discount"] * -1

    # Keep rows that have necessary values
    df = df[
        df["Order ID"].notna()
        & df["SKU"].notna()
        & df["Item Quantity"].notna()
        & df["Invoice Amount"].notna()
    ]

    # Return selected columns (sale + return both present; returns are negative now)
    return df[
        [
            "Order Date",
            "Order ID",
            "SKU",
            "Item Quantity",
            "Invoice Amount",
            "Price Before Discount",
        ]
    ]


def summarize_sales(df: pd.DataFrame):
    pivot = (
        df.groupby(["Order ID", "SKU"], as_index=False)
        .agg(
            Order_Date=("Order Date", "min"),
            Item_Quantity=("Item Quantity", "sum"),
            Invoice_Amount=("Invoice Amount", "sum"),
            Price_Before_Discount=("Price Before Discount", "sum"),
        )
        .rename(
            columns={
                "Order_Date": "Order Date",
                "Item_Quantity": "Item Quantity",
                "Invoice_Amount": "Invoice Amount",
                "Price_Before_Discount": "Price Before Discount",
            }
        )
    )
    summary = {
        "rows": df.shape[0],
        "orders": df["Order ID"].nunique(),
        "skus": df["SKU"].nunique(),
        "qty": df["Item Quantity"].sum(),
        "invoice": df["Invoice Amount"].sum(),
    }
    return pivot, summary


@st.cache_data(show_spinner=False)
def process_multiple_sales_cached(files_bytes_list):
    all_raw = [load_single_sales_df_from_bytes(fb) for fb in files_bytes_list]
    combined = pd.concat(all_raw, ignore_index=True)
    pivot, summary = summarize_sales(combined)
    return combined, pivot, summary


# ========================= COST FILE (SKU, Cost Price) =========================
@st.cache_data(show_spinner=False)
def process_cost_file_cached(file_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    df = pd.read_excel(bio, sheet_name=0, engine="openpyxl")

    # Detect SKU and Cost columns (flexible on header naming)
    sku_col = None
    cost_col = None
    for c in df.columns:
        txt = str(c).lower()
        if "sku" in txt and sku_col is None:
            sku_col = c
        if ("cost" in txt or "cost price" in txt) and cost_col is None:
            cost_col = c

    if sku_col is None or cost_col is None:
        raise ValueError("Cost file must contain SKU and Cost/Cost Price columns (headers detected incorrectly).")

    out = df[[sku_col, cost_col]].copy()
    out["SKU"] = out[sku_col].astype(str).str.strip()
    out["Cost Price"] = pd.to_numeric(out[cost_col], errors="coerce")
    out = out[out["SKU"].notna() & out["Cost Price"].notna()]
    return out[["SKU", "Cost Price"]]


# ========================= EXPORT TO EXCEL (single sheet) =========================
def final_report_to_excel_bytes(df_mapping: pd.DataFrame):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_mapping.to_excel(writer, index=False, sheet_name="Final_Mapped_Report")
    return output.getvalue()


# ========================= STREAMLIT APP =========================
def main():
    st.set_page_config(page_title="Flipkart Reconciliation Tool", page_icon="📊", layout="wide")
    st.title("📊 Flipkart Reconciliation Tool")

    # ---------- Sidebar uploads ----------
    st.sidebar.header("Upload Files")
    settlement_files = st.sidebar.file_uploader(
        "Settlement Excel file(s) (Orders sheet inside)", type=["xlsx", "xls"], accept_multiple_files=True
    )
    sales_files = st.sidebar.file_uploader(
        "Sale Report Excel file(s) (Sale Report sheet inside)", type=["xlsx", "xls"], accept_multiple_files=True
    )
    cost_file = st.sidebar.file_uploader(
        "SKU Cost Price file (optional)", type=["xlsx", "xls"], accept_multiple_files=False
    )

    st.markdown("---")

    if settlement_files and sales_files:
        try:
            # Read & process (cached)
            set_raw, set_pivot, set_summary = process_multiple_orders_cached([f.getvalue() for f in settlement_files])
            sale_raw, sale_pivot, sale_summary = process_multiple_sales_cached([f.getvalue() for f in sales_files])

            # Map Sales -> Settlement on Order ID + SKU
            mapping = sale_pivot.merge(
                set_pivot[["Order ID", "Seller SKU", "Settlement_Qty", "Payment_Received"]],
                left_on=["Order ID", "SKU"],
                right_on=["Order ID", "Seller SKU"],
                how="left",
            )

            # clean and fillna
            mapping = mapping.drop(columns=["Seller SKU"], errors="ignore")
            mapping["Settlement_Qty"] = mapping["Settlement_Qty"].fillna(0)
            mapping["Payment_Received"] = mapping["Payment_Received"].fillna(0.0)

            # Qty diff (calculation - can be negative/positive)
            mapping["Qty_Diff (Settlement - Sale)"] = mapping["Settlement_Qty"] - mapping["Item Quantity"]

            # Amount diff CALCULATION (backend only – not shown)
            mapping["Amount_Diff (Settlement - Invoice)"] = mapping["Payment_Received"] - mapping["Invoice Amount"]

            # Payment received against this Amount = aggregated Price Before Discount
            mapping["Payment received agaist this Amount"] = mapping["Price Before Discount"]

            # Merge cost file if provided
            if cost_file is not None:
                cost_df = process_cost_file_cached(cost_file.getvalue())
                mapping = mapping.merge(cost_df, on="SKU", how="left")
                # if cost missing, set 0 to avoid NaN
                mapping["Cost Price"] = mapping["Cost Price"].fillna(0.0)
            else:
                mapping["Cost Price"] = 0.0

            # ---------------- NEW: Adjust Cost Price based on rules ----------------
            # Rules:
            # - If return entry (Item Quantity < 0) -> use 50% of Cost Price
            # - If Item Quantity == 0 -> use 20% of Cost Price
            # - Else normal sale -> 100% Cost Price
            mapping["Cost Price Adjusted"] = mapping["Cost Price"]  # default

            # Apply return rule (50%)
            mask_return = mapping["Item Quantity"] < 0
            mapping.loc[mask_return, "Cost Price Adjusted"] = mapping.loc[mask_return, "Cost Price"] * 0.5

            # Apply zero-qty rule (20%)
            mask_zero = mapping["Item Quantity"] == 0
            mapping.loc[mask_zero, "Cost Price Adjusted"] = mapping.loc[mask_zero, "Cost Price"] * 0.2

            # Total Cost uses adjusted price
            mapping["Total Cost (Qty * Adjusted Cost)"] = mapping["Item Quantity"] * mapping["Cost Price Adjusted"]

            # Keep original Total Cost column name compatibility (optional)
            # Replace previous "Total Cost (Qty * Cost)" with adjusted version
            mapping["Total Cost (Qty * Cost)"] = mapping["Total Cost (Qty * Adjusted Cost)"]

            # ---------- Summary / top metrics ----------
            st.subheader("Summary")
            total_rows = mapping.shape[0]
            unique_orders = mapping["Order ID"].nunique()
            unique_skus = mapping["SKU"].nunique()
            total_invoice = mapping["Invoice Amount"].sum()
            total_payment = mapping["Payment_Received"].sum()
            total_qty_diff = mapping["Qty_Diff (Settlement - Sale)"].sum()
            total_cost = mapping["Total Cost (Qty * Cost)"].sum()

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Rows (Order+SKU)", total_rows)
            c2.metric("Unique Orders", unique_orders)
            c3.metric("Unique SKUs", unique_skus)
            c4.metric("Net Qty Diff", int(total_qty_diff))

            c5, c6, c7 = st.columns(3)
            c5.metric("Total Invoice Amount", f"{total_invoice:,.2f}")
            c6.metric("Total Settlement Payment", f"{total_payment:,.2f}")
            c7.metric("Total Cost (adjusted, filtered)", f"{total_cost:,.2f}")

            st.markdown("---")

            # ---------- Build final view (don't show backend-only columns) ----------
            final_cols = [
                "Order Date",
                "Order ID",
                "SKU",
                "Item Quantity",
                "Invoice Amount",
                "Payment received agaist this Amount",
                "Settlement_Qty",
                "Qty_Diff (Settlement - Sale)",
                "Payment_Received",
                "Cost Price",                # original cost price
                "Cost Price Adjusted",       # show adjusted for clarity
                "Total Cost (Qty * Cost)",   # adjusted total cost (kept name for compatibility)
            ]
            # ensure only existing cols used
            mapping_view = mapping[[c for c in final_cols if c in mapping.columns]].copy()

            st.subheader("Final Mapped Report")
            st.dataframe(mapping_view, use_container_width=True)

            # ---------- Download only final mapped report sheet ----------
            st.markdown("---")
            st.subheader("Download")
            excel_bytes = final_report_to_excel_bytes(mapping_view)
            st.download_button(
                "Download Final Mapped Report",
                data=excel_bytes,
                file_name="Final_Mapped_Report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        except Exception as e:
            st.error(f"Error: {e}")
    else:
        st.info("Upload Settlement and Sale Report files from the sidebar to start reconciliation.")


if __name__ == "__main__":
    main()
