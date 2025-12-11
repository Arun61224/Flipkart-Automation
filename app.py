# app.py
import re
import streamlit as st
import pandas as pd
from io import BytesIO

# -----------------------
# Helpers
# -----------------------
def excel_col_to_idx(col_name: str) -> int:
    col_name = col_name.strip().upper()
    idx = 0
    for c in col_name:
        if "A" <= c <= "Z":
            idx = idx * 26 + (ord(c) - ord("A") + 1)
    return idx - 1

def find_col_by_keywords(cols, keywords):
    """Return first column name that contains all keywords (case-insensitive)"""
    low = [c.lower() for c in cols]
    for i, c in enumerate(low):
        if all(k.lower() in c for k in keywords):
            return cols[i]
    return None

def find_18digit_column(df):
    """Try to detect a column where many values are exactly 18 digits."""
    candidate = None
    best_score = 0
    pattern = re.compile(r"^\d{18}$")
    for c in df.columns:
        s = df[c].dropna().astype(str).str.strip()
        if s.empty:
            continue
        # check fraction of values matching 18 digits
        match_frac = (s.str.match(pattern)).mean()
        if match_frac > best_score and match_frac >= 0.2:  # threshold 20%
            best_score = match_frac
            candidate = c
    return candidate

def ensure_order_item_id_column_settlement(df):
    """Return column name to use as Order Item ID in settlement (and add normalized col)."""
    # 1) try header-based detection
    cand = find_col_by_keywords(df.columns, ["order", "item", "id"])
    if cand is None:
        cand = find_col_by_keywords(df.columns, ["orderitem", "id"])
    # 2) try detect 18-digit column
    if cand is None:
        cand = find_18digit_column(df)
    # 3) if found, create normalized 'Order Item ID' (string with leading apostrophe)
    if cand is not None:
        df["Order Item ID"] = "'" + df[cand].astype(str).str.strip()
    return cand

def ensure_order_item_id_column_sales(df):
    """Same for sales file."""
    # try header 'order item id' or 'order item'
    cand = find_col_by_keywords(df.columns, ["order", "item", "id"])
    if cand is None:
        cand = find_col_by_keywords(df.columns, ["orderitem", "id"])
    if cand is None:
        cand = find_18digit_column(df)
    if cand is not None:
        df["Order Item ID"] = "'" + df[cand].astype(str).str.strip()
    return cand

# ======================================================
#   PART 1 – SETTLEMENT FILES (Orders sheet)
# ======================================================
def process_orders_excel_from_bytes(file_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)

    if "Orders" not in xls.sheet_names:
        raise ValueError('Sheet named "Orders" not found in the uploaded settlement file.')

    bio.seek(0)
    orders = pd.read_excel(bio, sheet_name="Orders", engine="openpyxl")

    # Try to find Order ID column (fallback) and Bank Settlement Value column
    # In earlier sample: "Transaction Summary" (H) used; bank value was Unnamed: 3 (D)
    col_order_id = find_col_by_keywords(orders.columns, ["transaction", "summary"]) or find_col_by_keywords(orders.columns, ["order", "id"]) or orders.columns[0]
    # bank/settlement value detection
    col_bank_value = None
    for c in orders.columns:
        if "bank" in str(c).lower() and "settlement" in str(c).lower():
            col_bank_value = c
            break
    if col_bank_value is None:
        # fallback to columns containing "payment" or "amount" or the 4th column like sample
        for c in orders.columns:
            if any(k in str(c).lower() for k in ["payment", "amount", "settlement", "bank"]):
                col_bank_value = c
                break
    if col_bank_value is None:
        # fallback to 4th column (index 3) if exists
        if len(orders.columns) >= 4:
            col_bank_value = orders.columns[3]
        else:
            raise ValueError("Cannot detect Bank Settlement Value column in Orders sheet.")

    # Detect Seller SKU (BG) and Qty (BH) by index if possible
    sku_idx = excel_col_to_idx("BG")
    qty_idx = excel_col_to_idx("BH")
    if len(orders.columns) <= max(sku_idx, qty_idx):
        # fallback: try to find SKU/Qty headers
        col_seller_sku = find_col_by_keywords(orders.columns, ["seller", "sku"]) or find_col_by_keywords(orders.columns, ["sku"])
        col_qty = find_col_by_keywords(orders.columns, ["qty", "quantity"]) or find_col_by_keywords(orders.columns, ["quantity"])
        if col_seller_sku is None or col_qty is None:
            raise ValueError("Orders sheet missing Seller SKU (BG) / Qty (BH) columns and fallback detection failed.")
    else:
        col_seller_sku = orders.columns[sku_idx]
        col_qty = orders.columns[qty_idx]

    # Filter typical order rows (Order ID starts with OD usually) using fallback col_order_id if present
    df = orders.copy()
    # if col_order_id present use it; else use first column
    if col_order_id in df.columns:
        df = df[df[col_order_id].astype(str).str.strip().str.startswith("OD", na=False) | df[col_order_id].notna()]
        # above ensures we don't remove everything if 'OD' not present in sample
    # Make numeric conversions
    df[col_bank_value] = pd.to_numeric(df[col_bank_value], errors="coerce")
    df[col_qty] = pd.to_numeric(df[col_qty], errors="coerce")

    # Clean rows where important fields not null
    df = df[
        df[col_bank_value].notna()
        & df[col_seller_sku].notna()
        & df[col_qty].notna()
    ]

    # Ensure Order Item ID normalized if available
    found_order_item_col = ensure_order_item_id_column_settlement(df)
    # If no Order Item ID found, create fallback Order ID column as string (prefixed)
    df["Order ID"] = df[col_order_id].astype(str).str.strip()
    # If Order Item ID not found, keep only Order ID for mapping later
    # Rename and return necessary columns
    df_clean = df[[ "Order ID", col_seller_sku, col_qty, col_bank_value] + (["Order Item ID"] if "Order Item ID" in df.columns else [])].rename(
        columns={
            col_seller_sku: "Seller SKU",
            col_qty: "Settlement Qty",
            col_bank_value: "Payment Received",
        }
    )
    return df_clean

def summarize_orders(df_clean: pd.DataFrame):
    # If Order Item ID exists, group by it + SKU, else group by Order ID + SKU
    group_cols = ["Seller SKU"]
    if "Order Item ID" in df_clean.columns:
        group_cols = ["Order Item ID", "Seller SKU"]
    else:
        group_cols = ["Order ID", "Seller SKU"]

    pivot = (
        df_clean.groupby(group_cols, as_index=False)
        .agg(
            Settlement_Qty=("Settlement Qty", "sum"),
            Payment_Received=("Payment Received", "sum"),
        )
        .reset_index(drop=True)
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

# ======================================================
#   PART 2 – SALES REPORT FILES
# ======================================================
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

    # sheet detection
    target_sheet = None
    for s in xls.sheet_names:
        chk = s.lower().replace(" ", "")
        if "sale" in chk and "report" in chk:
            target_sheet = s
            break
    if target_sheet is None:
        raise ValueError('Sale Report sheet not found in uploaded sales file.')

    bio.seek(0)
    sales = pd.read_excel(bio, sheet_name=target_sheet, engine="openpyxl")

    # Expected positions B=Order ID, F=SKU, N=Qty, H=Event (fallback)
    order_idx = excel_col_to_idx("B")
    sku_idx = excel_col_to_idx("F")
    qty_idx = excel_col_to_idx("N")
    event_idx = excel_col_to_idx("H")

    if len(sales.columns) <= max(order_idx, sku_idx, qty_idx, event_idx):
        # continue but will try header detection
        pass

    # map columns by position with safety
    try:
        col_order = sales.columns[order_idx]
    except:
        col_order = find_col_by_keywords(sales.columns, ["order", "id"]) or sales.columns[0]

    try:
        col_sku = sales.columns[sku_idx]
    except:
        col_sku = find_col_by_keywords(sales.columns, ["sku"]) or sales.columns[1]

    try:
        col_qty = sales.columns[qty_idx]
    except:
        col_qty = find_col_by_keywords(sales.columns, ["qty", "quantity"]) or sales.columns[2]

    # Event Sub Type detection (priority) else fallback to event column
    col_event_sub = find_col_by_keywords(sales.columns, ["event", "sub", "type"]) or find_col_by_keywords(sales.columns, ["event", "subtype"]) 
    if col_event_sub is None:
        # fallback to Event at position H if exists
        try:
            col_event_sub = sales.columns[event_idx]
        except:
            col_event_sub = find_col_by_keywords(sales.columns, ["event"]) or sales.columns[4]

    # Order Date detection
    order_date_candidates = [c for c in sales.columns if "order date" in str(c).lower()]
    if not order_date_candidates:
        raise ValueError("Could not find 'Order Date' column in Sale Report.")
    col_order_date = order_date_candidates[0]

    # Invoice detection (long name)
    invoice_candidates = [c for c in sales.columns if "final invoice amount" in str(c).lower()]
    if not invoice_candidates:
        raise ValueError("Could not find 'Final Invoice Amount (Price after discount+Shipping Charges)' column in Sale Report.")
    col_invoice = invoice_candidates[0]

    # Price before discount (optional)
    col_price_before = None
    for c in sales.columns:
        if "price before discount" in str(c).lower():
            col_price_before = c
            break

    # select columns
    cols = [col_order_date, col_order, col_sku, col_qty, col_event_sub, col_invoice]
    if col_price_before is not None:
        cols.append(col_price_before)

    df = sales[cols].copy()

    # Normalize basic columns
    df["Order Date"] = pd.to_datetime(df[col_order_date], errors="coerce")
    df["Order ID"] = df[col_order].astype(str).str.strip()
    df["SKU"] = df[col_sku].apply(clean_sku_text)
    df["Item Quantity"] = pd.to_numeric(df[col_qty], errors="coerce")
    df["Event Sub Type"] = df[col_event_sub].astype(str).fillna("").str.strip()

    df["Invoice Amount"] = pd.to_numeric(df[col_invoice], errors="coerce")
    if col_price_before is not None:
        df["Price Before Discount"] = pd.to_numeric(df[col_price_before], errors="coerce")
    else:
        df["Price Before Discount"] = 0.0

    # ---------- NEW: keep only Event Sub Type = 'Sale' or 'Return' ----------
    df = df[df["Event Sub Type"].str.lower().isin(["sale", "return"])]

    # ---------- NEW: detect & normalize Order Item ID (18-digit) ----------
    found_order_item_col = ensure_order_item_id_column_sales(df)
    # if no Order Item ID found, we will fall back later to Order ID mapping
    # If Order Item ID exists, it is stored in df['Order Item ID'] with leading apostrophe

    # ---------- Convert Return rows to NEGATIVE values (but keep rows) ----------
    df["IsReturn"] = df["Event Sub Type"].str.lower().eq("return")
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

    # if Order Item ID exists ensure it's string with apostrophe as earlier
    if "Order Item ID" in df.columns:
        # ensured by ensure_order_item_id_column_sales already
        pass

    # return selection (note Order Item ID may or may not be present)
    cols_return = ["Order Date", "Order ID", "SKU", "Item Quantity", "Invoice Amount", "Price Before Discount"]
    if "Order Item ID" in df.columns:
        cols_return.insert(1, "Order Item ID")  # place after Order Date or position as needed
    return df[cols_return]

def summarize_sales(df: pd.DataFrame):
    # If Order Item ID exists, group by it + SKU, else group by Order ID + SKU
    if "Order Item ID" in df.columns:
        group_cols = ["Order Item ID", "SKU"]
    else:
        group_cols = ["Order ID", "SKU"]

    pivot = (
        df.groupby(group_cols, as_index=False)
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

# ========================= EXPORT TO EXCEL =========================
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
            # Process files
            set_raw, set_pivot, set_summary = process_multiple_orders_cached([f.getvalue() for f in settlement_files])
            sale_raw, sale_pivot, sale_summary = process_multiple_sales_cached([f.getvalue() for f in sales_files])

            # Determine whether both sides have Order Item ID:
            left_has_oi = "Order Item ID" in set_pivot.columns or "Order Item ID" in set_raw.columns
            right_has_oi = "Order Item ID" in sale_pivot.columns or "Order Item ID" in sale_raw.columns

            # If both sides have Order Item ID, merge on that; else fallback to Order ID
            if left_has_oi and right_has_oi:
                left_on = ["Order Item ID", "Seller SKU"] if "Order Item ID" in set_pivot.columns else ["Order Item ID", "Seller SKU"]
                right_on = ["Order Item ID", "SKU"]
                mapping = sale_pivot.merge(
                    set_pivot[[left_on[0], left_on[1], "Settlement_Qty", "Payment_Received"]],
                    left_on=right_on,
                    right_on=left_on,
                    how="left",
                )
            else:
                # fallback to Order ID mapping (previous behavior)
                mapping = sale_pivot.merge(
                    set_pivot[["Order ID", "Seller SKU", "Settlement_Qty", "Payment_Received"]],
                    left_on=["Order ID", "SKU"],
                    right_on=["Order ID", "Seller SKU"],
                    how="left",
                )

            mapping = mapping.drop(columns=["Seller SKU"], errors="ignore")
            mapping["Settlement_Qty"] = mapping["Settlement_Qty"].fillna(0)
            mapping["Payment_Received"] = mapping["Payment_Received"].fillna(0.0)

            # Qty diff
            mapping["Qty_Diff (Settlement - Sale)"] = mapping["Settlement_Qty"] - mapping["Item Quantity"]

            # Backend amount diff (hidden)
            mapping["Amount_Diff (Settlement - Invoice)"] = mapping["Payment_Received"] - mapping["Invoice Amount"]

            # Payment received against this Amount = aggregated Price Before Discount
            mapping["Payment received agaist this Amount"] = mapping["Price Before Discount"]

            # Merge cost file if provided
            if cost_file is not None:
                cost_df = process_cost_file_cached(cost_file.getvalue())
                mapping = mapping.merge(cost_df, on="SKU", how="left")
                mapping["Cost Price"] = mapping["Cost Price"].fillna(0.0)
            else:
                mapping["Cost Price"] = 0.0

            # Adjusted cost rules
            mapping["Cost Price Adjusted"] = mapping["Cost Price"]
            mask_return = mapping["Item Quantity"] < 0
            mapping.loc[mask_return, "Cost Price Adjusted"] = mapping.loc[mask_return, "Cost Price"] * 0.5
            mask_zero = mapping["Item Quantity"] == 0
            mapping.loc[mask_zero, "Cost Price Adjusted"] = mapping.loc[mask_zero, "Cost Price"] * 0.2
            mapping["Total Cost (Qty * Adjusted Cost)"] = mapping["Item Quantity"] * mapping["Cost Price Adjusted"]
            mapping["Total Cost (Qty * Cost)"] = mapping["Total Cost (Qty * Adjusted Cost)"]

            # ---------- Summary / dashboard metrics ----------
            st.subheader("Summary")
            total_rows = mapping.shape[0]
            unique_orders = mapping["Order ID"].nunique() if "Order ID" in mapping.columns else mapping.shape[0]
            unique_skus = mapping["SKU"].nunique()
            total_invoice = mapping["Invoice Amount"].sum()
            total_payment = mapping["Payment_Received"].sum()
            total_cost = mapping["Total Cost (Qty * Cost)"].sum()

            # Top metrics (Net Qty Diff removed)
            c1, c2, c3 = st.columns(3)
            c1.metric("Rows (Order+SKU)", total_rows)
            c2.metric("Unique Orders", unique_orders)
            c3.metric("Unique SKUs", unique_skus)

            c5, c6, c7 = st.columns(3)
            c5.metric("Total Invoice Amount", f"{total_invoice:,.2f}")
            c6.metric("Total Settlement Payment", f"{total_payment:,.2f}")
            c7.metric("Total Cost (adjusted, filtered)", f"{total_cost:,.2f}")

            # ---------- Consolidated royalty across prefixes ----------
            royalty_rules = {
                "MKUC": 0.10,
                "DKUC": 0.10,
                "DYK": 0.07,
                "MYK": 0.07,
                "KYK": 0.01,
                "MAC": 0.10,
            }
            total_royalty = 0.0
            for prefix, rate in royalty_rules.items():
                mask = mapping["SKU"].astype(str).str.upper().str.startswith(prefix)
                net_invoice = mapping.loc[mask, "Invoice Amount"].sum()
                net_positive = net_invoice if net_invoice > 0 else 0.0
                total_royalty += net_positive * rate

            st.markdown("")  # small gap
            st.metric("Total Royalty (dashboard only)", f"{total_royalty:,.2f}")

            # Net settlement after cost & royalty (dashboard only)
            net_settlement_after = total_payment - total_cost - total_royalty
            st.markdown("")  # small gap
            st.metric("Net Settlement after Cost & Royalty (dashboard only)", f"{net_settlement_after:,.2f}")

            st.markdown("---")

            # Final columns to show (hide backend-only columns)
            final_cols = [
                "Order Date",
                "Order Item ID" if "Order Item ID" in mapping.columns else None,
                "Order ID",
                "SKU",
                "Item Quantity",
                "Invoice Amount",
                "Payment received agaist this Amount",
                "Settlement_Qty",
                "Qty_Diff (Settlement - Sale)",
                "Payment_Received",
                "Cost Price",
                "Cost Price Adjusted",
                "Total Cost (Qty * Cost)",
            ]
            # filter None and missing
            final_cols = [c for c in final_cols if c and c in mapping.columns]
            mapping_view = mapping[final_cols].copy()

            st.subheader("Final Mapped Report")
            st.dataframe(mapping_view, use_container_width=True)

            # Download final mapped report
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
