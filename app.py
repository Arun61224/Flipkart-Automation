# app.py
import re
import streamlit as st
import pandas as pd
from io import BytesIO

# -----------------------
# Helper functions
# -----------------------
def excel_col_to_idx(col_name: str) -> int:
    col_name = col_name.strip().upper()
    idx = 0
    for c in col_name:
        if "A" <= c <= "Z":
            idx = idx * 26 + (ord(c) - ord("A") + 1)
    return idx - 1


def find_col_by_keywords(cols, keywords):
    """Return first column name that contains all keywords (case-insensitive)."""
    low = [str(c).lower() for c in cols]
    for i, c in enumerate(low):
        if all(k.lower() in c for k in keywords):
            return cols[i]
    return None


def find_18digit_column(df: pd.DataFrame):
    """Detect a column where many values look like 18+ digit numeric IDs."""
    candidate = None
    best_score = 0
    pattern = re.compile(r"^\d{18,}$")  # 18 or more digits
    for c in df.columns:
        s = df[c].dropna().astype(str).str.strip()
        if s.empty:
            continue
        match_frac = (s.str.match(pattern)).mean()
        if match_frac > best_score and match_frac >= 0.2:  # >=20% rows look like ID
            best_score = match_frac
            candidate = c
    return candidate


def normalize_order_item_id(series: pd.Series) -> pd.Series:
    """
    Convert Order Item ID series to TEXT-like string, remove trailing .0, and prefix with '.
    """
    s = series.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    return "'" + s


def ensure_order_item_id_column_settlement(df: pd.DataFrame):
    """Detect Order Item ID column in settlement df and create normalized 'Order Item ID'."""
    cand = find_col_by_keywords(df.columns, ["order", "item", "id"])
    if cand is None:
        cand = find_col_by_keywords(df.columns, ["orderitem", "id"])
    if cand is None:
        cand = find_18digit_column(df)

    if cand is not None:
        df["Order Item ID"] = normalize_order_item_id(df[cand])
    return cand


def ensure_order_item_id_column_sales(df: pd.DataFrame):
    """Detect Order Item ID column in sales df and create normalized 'Order Item ID'."""
    cand = find_col_by_keywords(df.columns, ["order", "item", "id"])
    if cand is None:
        cand = find_col_by_keywords(df.columns, ["orderitem", "id"])
    if cand is None:
        cand = find_18digit_column(df)

    if cand is not None:
        df["Order Item ID"] = normalize_order_item_id(df[cand])
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

    col_order_id = (
        find_col_by_keywords(orders.columns, ["transaction", "summary"])
        or find_col_by_keywords(orders.columns, ["order", "id"])
        or orders.columns[0]
    )

    col_bank_value = None
    for c in orders.columns:
        txt = str(c).lower()
        if "bank" in txt and "settlement" in txt:
            col_bank_value = c
            break
    if col_bank_value is None:
        for c in orders.columns:
            if any(k in str(c).lower() for k in ["payment", "amount", "settlement", "bank"]):
                col_bank_value = c
                break
    if col_bank_value is None:
        if len(orders.columns) >= 4:
            col_bank_value = orders.columns[3]
        else:
            raise ValueError("Cannot detect Bank Settlement Value column in Orders sheet.")

    sku_idx = excel_col_to_idx("BG")
    qty_idx = excel_col_to_idx("BH")
    if len(orders.columns) > max(sku_idx, qty_idx):
        col_seller_sku = orders.columns[sku_idx]
        col_qty = orders.columns[qty_idx]
    else:
        col_seller_sku = find_col_by_keywords(orders.columns, ["seller", "sku"]) or find_col_by_keywords(
            orders.columns, ["sku"]
        )
        col_qty = find_col_by_keywords(orders.columns, ["qty", "quantity"]) or find_col_by_keywords(
            orders.columns, ["quantity"]
        )
        if col_seller_sku is None or col_qty is None:
            raise ValueError(
                "Orders sheet missing Seller SKU (BG) / Qty (BH) columns and fallback detection failed."
            )

    df = orders.copy()

    ensure_order_item_id_column_settlement(df)

    df = df[df[col_bank_value].notna() & df[col_seller_sku].notna() & df[col_qty].notna()]

    df["Order ID"] = df[col_order_id].astype(str).str.strip()
    df["Seller SKU"] = df[col_seller_sku].astype(str).str.strip()
    df["Settlement Qty"] = pd.to_numeric(df[col_qty], errors="coerce")
    df["Payment Received"] = pd.to_numeric(df[col_bank_value], errors="coerce")

    df = df[df["Settlement Qty"].notna() & df["Payment Received"].notna()]

    keep_cols = ["Order ID", "Seller SKU", "Settlement Qty", "Payment Received"]
    if "Order Item ID" in df.columns:
        keep_cols.insert(1, "Order Item ID")
    df_clean = df[keep_cols].copy()

    return df_clean


def summarize_orders(df_clean: pd.DataFrame):
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
        "orders": df_clean["Order ID"].nunique() if "Order ID" in df_clean.columns else df_clean.shape[0],
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

    order_idx = excel_col_to_idx("B")
    sku_idx = excel_col_to_idx("F")
    qty_idx = excel_col_to_idx("N")
    event_idx = excel_col_to_idx("H")

    try:
        col_order = sales.columns[order_idx]
    except Exception:
        col_order = find_col_by_keywords(sales.columns, ["order", "id"]) or sales.columns[0]

    try:
        col_sku = sales.columns[sku_idx]
    except Exception:
        col_sku = find_col_by_keywords(sales.columns, ["sku"]) or sales.columns[1]

    try:
        col_qty = sales.columns[qty_idx]
    except Exception:
        col_qty = find_col_by_keywords(sales.columns, ["qty", "quantity"]) or sales.columns[2]

    col_event_sub = find_col_by_keywords(sales.columns, ["event", "sub", "type"]) or find_col_by_keywords(
        sales.columns, ["event", "subtype"]
    )
    if col_event_sub is None:
        try:
            col_event_sub = sales.columns[event_idx]
        except Exception:
            col_event_sub = find_col_by_keywords(sales.columns, ["event"]) or sales.columns[4]

    order_date_candidates = [c for c in sales.columns if "order date" in str(c).lower()]
    if not order_date_candidates:
        raise ValueError("Could not find 'Order Date' column in Sale Report.")
    col_order_date = order_date_candidates[0]

    invoice_candidates = [c for c in sales.columns if "final invoice amount" in str(c).lower()]
    if not invoice_candidates:
        raise ValueError(
            "Could not find 'Final Invoice Amount (Price after discount+Shipping Charges)' column in Sale Report."
        )
    col_invoice = invoice_candidates[0]

    col_price_before = None
    for c in sales.columns:
        if "price before discount" in str(c).lower():
            col_price_before = c
            break

    cols = [col_order_date, col_order, col_sku, col_qty, col_event_sub, col_invoice]
    if col_price_before is not None:
        cols.append(col_price_before)

    df = sales[cols].copy()

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

    # keep only Sale + Return
    df = df[df["Event Sub Type"].str.lower().isin(["sale", "return"])]

    # detect / normalize Order Item ID
    ensure_order_item_id_column_sales(df)

    # return rows negative
    df["IsReturn"] = df["Event Sub Type"].str.lower().eq("return")
    df.loc[df["IsReturn"], "Item Quantity"] *= -1
    df.loc[df["IsReturn"], "Invoice Amount"] *= -1
    df.loc[df["IsReturn"], "Price Before Discount"] *= -1

    df = df[
        df["Order ID"].notna()
        & df["SKU"].notna()
        & df["Item Quantity"].notna()
        & df["Invoice Amount"].notna()
    ]

    cols_return = ["Order Date", "Order ID", "SKU", "Item Quantity", "Invoice Amount", "Price Before Discount"]
    if "Order Item ID" in df.columns:
        cols_return.insert(1, "Order Item ID")
    return df[cols_return]


def summarize_sales(df: pd.DataFrame):
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


# ========================= COST FILE =========================
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
        raise ValueError("Cost file must contain SKU and Cost/Cost Price columns.")

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

    # Sidebar uploads
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
            set_raw, set_pivot, set_summary = process_multiple_orders_cached([f.getvalue() for f in settlement_files])
            sale_raw, sale_pivot, sale_summary = process_multiple_sales_cached([f.getvalue() for f in sales_files])

            # ---------- Robust merge & safe metrics / final view ----------

            # Detect presence of Order Item ID on both sides
            left_has_oi = "Order Item ID" in set_pivot.columns
            right_has_oi = "Order Item ID" in sale_pivot.columns

            # Build merge columns dynamically based on what's present in each pivot
            if left_has_oi and right_has_oi:
                # both sides have Order Item ID -> merge on that + SKU
                left_merge_cols = ["Order Item ID", "Seller SKU", "Settlement_Qty", "Payment_Received"]
                right_merge_on = ["Order Item ID", "SKU"]
                left_merge_on = ["Order Item ID", "Seller SKU"]
                mapping = sale_pivot.merge(
                    set_pivot[left_merge_cols],
                    left_on=right_merge_on,
                    right_on=left_merge_on,
                    how="left",
                )
            else:
                # fallback: try Order ID + SKU. But check columns exist first.
                left_id_col = "Order ID" if "Order ID" in set_pivot.columns else ("Order Item ID" if "Order Item ID" in set_pivot.columns else None)
                right_id_col = "Order ID" if "Order ID" in sale_pivot.columns else ("Order Item ID" if "Order Item ID" in sale_pivot.columns else None)

                set_tmp = set_pivot.copy()
                sale_tmp = sale_pivot.copy()

                if left_id_col and left_id_col != "Order ID":
                    set_tmp = set_tmp.rename(columns={left_id_col: "Order ID"})
                if right_id_col and right_id_col != "Order ID":
                    sale_tmp = sale_tmp.rename(columns={right_id_col: "Order ID"})

                if "Seller SKU" not in set_tmp.columns:
                    possible = [c for c in set_tmp.columns if "sku" in str(c).lower()]
                    if possible:
                        set_tmp = set_tmp.rename(columns={possible[0]: "Seller SKU"})

                left_cols_for_merge = [c for c in ["Order ID", "Seller SKU", "Settlement_Qty", "Payment_Received"] if c in set_tmp.columns]
                right_on = ["Order ID", "SKU"]
                left_on = ["Order ID", "Seller SKU"]
                if "Seller SKU" not in set_tmp.columns or "SKU" not in sale_tmp.columns:
                    mapping = sale_tmp.merge(
                        set_tmp[left_cols_for_merge],
                        left_on=["Order ID"],
                        right_on=["Order ID"],
                        how="left",
                    )
                else:
                    mapping = sale_tmp.merge(
                        set_tmp[left_cols_for_merge],
                        left_on=right_on,
                        right_on=left_on,
                        how="left",
                    )

            mapping = mapping.drop(columns=["Seller SKU"], errors="ignore")

            mapping["Settlement_Qty"] = mapping.get("Settlement_Qty", pd.Series(0, index=mapping.index)).fillna(0)
            mapping["Payment_Received"] = mapping.get("Payment_Received", pd.Series(0.0, index=mapping.index)).fillna(0.0)

            mapping["Qty_Diff (Settlement - Sale)"] = mapping["Settlement_Qty"] - mapping["Item Quantity"]

            mapping["Amount_Diff (Settlement - Invoice)"] = mapping["Payment_Received"] - mapping["Invoice Amount"]

            mapping["Payment received agaist this Amount"] = mapping.get("Price Before Discount", 0.0)

            if cost_file is not None:
                cost_df = process_cost_file_cached(cost_file.getvalue())
                mapping = mapping.merge(cost_df, on="SKU", how="left")
                mapping["Cost Price"] = mapping["Cost Price"].fillna(0.0)
            else:
                mapping["Cost Price"] = 0.0

            mapping["Cost Price Adjusted"] = mapping["Cost Price"]
            mask_return = mapping["Item Quantity"] < 0
            mapping.loc[mask_return, "Cost Price Adjusted"] = mapping.loc[mask_return, "Cost Price"] * 0.5
            mask_zero = mapping["Item Quantity"] == 0
            mapping.loc[mask_zero, "Cost Price Adjusted"] = mapping.loc[mask_zero, "Cost Price"] * 0.2
            mapping["Total Cost (Qty * Adjusted Cost)"] = mapping["Item Quantity"] * mapping["Cost Price Adjusted"]
            mapping["Total Cost (Qty * Cost)"] = mapping["Total Cost (Qty * Adjusted Cost)"]

            # ---------- Safe dashboard metrics ----------
            total_rows = mapping.shape[0]
            if "Order ID" in mapping.columns:
                unique_orders = mapping["Order ID"].nunique()
            elif "Order Item ID" in mapping.columns:
                unique_orders = mapping["Order Item ID"].nunique()
            else:
                unique_orders = mapping.shape[0]

            unique_skus = mapping["SKU"].nunique() if "SKU" in mapping.columns else 0
            total_invoice = mapping["Invoice Amount"].sum() if "Invoice Amount" in mapping.columns else 0.0
            total_payment = mapping["Payment_Received"].sum() if "Payment_Received" in mapping.columns else 0.0
            total_cost = mapping["Total Cost (Qty * Cost)"].sum() if "Total Cost (Qty * Cost)" in mapping.columns else 0.0

            c1, c2, c3 = st.columns(3)
            c1.metric("Rows (Order+SKU)", total_rows)
            c2.metric("Unique Orders", unique_orders)
            c3.metric("Unique SKUs", unique_skus)

            c5, c6, c7 = st.columns(3)
            c5.metric("Total Invoice Amount", f"{total_invoice:,.2f}")
            c6.metric("Total Settlement Payment", f"{total_payment:,.2f}")
            c7.metric("Total Cost (adjusted, filtered)", f"{total_cost:,.2f}")

            # ---------- consolidated royalty ----------
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
                mask = mapping.get("SKU", pd.Series([], index=mapping.index)).astype(str).str.upper().str.startswith(prefix)
                net_invoice = mapping.loc[mask, "Invoice Amount"].sum() if "Invoice Amount" in mapping.columns else 0.0
                net_positive = net_invoice if net_invoice > 0 else 0.0
                total_royalty += net_positive * rate

            st.metric("Total Royalty (dashboard only)", f"{total_royalty:,.2f}")

            net_settlement_after = total_payment - total_cost - total_royalty
            st.metric("Net Settlement after Cost & Royalty (dashboard only)", f"{net_settlement_after:,.2f}")

            st.markdown("---")

            # ---------- Safe final view selection ----------
            final_cols = [
                "Order Date",
                "Order Item ID" if "Order Item ID" in mapping.columns else None,
                "Order ID" if "Order ID" in mapping.columns else None,
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
            final_cols = [c for c in final_cols if c and c in mapping.columns]
            mapping_view = mapping[final_cols].copy()

            st.subheader("Final Mapped Report")
            st.dataframe(mapping_view, use_container_width=True)

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
