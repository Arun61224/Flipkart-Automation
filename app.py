import re
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO

def excel_col_to_idx(col_name: str) -> int:
    col_name = col_name.strip().upper()
    idx = 0
    for c in col_name:
        if "A" <= c <= "Z":
            idx = idx * 26 + (ord(c) - ord("A") + 1)
    return idx - 1

def find_col_by_keywords(cols, keywords):
    low = [str(c).lower() for c in cols]
    for i, c in enumerate(low):
        if all(k.lower() in c for k in keywords):
            return cols[i]
    return None

def find_header_row_dynamic(bio, sheet_name, keywords):
    bio.seek(0)
    df_preview = pd.read_excel(bio, sheet_name=sheet_name, header=None, nrows=20, engine="openpyxl")
    for i, row in df_preview.iterrows():
        row_str = row.astype(str).str.lower().tolist()
        if any(k in str(x) for x in row_str for k in keywords):
            return i
    return 0

def normalize_order_item_id(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.str.replace(r"^'+", "", regex=True)
    is_nan = s.str.lower() == 'nan'
    s_final = "'" + s
    s_final[is_nan] = np.nan
    return s_final

def ensure_order_item_id_column_settlement(df: pd.DataFrame, original_columns):
    col_i_idx = excel_col_to_idx("I")
    if len(original_columns) > col_i_idx:
        target_col = original_columns[col_i_idx]
        if target_col in df.columns:
            df["Order Item ID"] = normalize_order_item_id(df[target_col])
            return target_col
            
    cand = find_col_by_keywords(df.columns, ["order", "item", "id"])
    if cand is None:
        cand = find_col_by_keywords(df.columns, ["orderitem", "id"])
    
    if cand is not None:
        df["Order Item ID"] = normalize_order_item_id(df[cand])
    return cand

def ensure_order_item_id_column_sales(df: pd.DataFrame, original_columns):
    col_c_idx = excel_col_to_idx("C")
    if len(original_columns) > col_c_idx:
        target_col = original_columns[col_c_idx]
        if target_col in df.columns:
            df["Order Item ID"] = normalize_order_item_id(df[target_col])
            return target_col

    cand = find_col_by_keywords(df.columns, ["order", "item", "id"])
    if cand is None:
        cand = find_col_by_keywords(df.columns, ["orderitem", "id"])
    
    if cand is not None:
        df["Order Item ID"] = normalize_order_item_id(df[cand])
    return cand

def process_orders_excel_from_bytes(file_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)

    target_sheet = next((s for s in xls.sheet_names if "order" in s.lower()), None)
    if not target_sheet:
        if "Orders" in xls.sheet_names:
            target_sheet = "Orders"
        else:
            raise ValueError('Sheet named "Orders" or similar not found in Settlement file.')

    header_idx = find_header_row_dynamic(bio, target_sheet, ["order item id", "order id", "settlement value"])
    
    bio.seek(0)
    orders = pd.read_excel(bio, sheet_name=target_sheet, header=header_idx, engine="openpyxl")
    original_cols = orders.columns.tolist()

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
    
    col_seller_sku = None
    col_qty = None

    if len(orders.columns) > max(sku_idx, qty_idx):
        col_seller_sku = orders.columns[sku_idx]
        col_qty = orders.columns[qty_idx]
    
    if col_seller_sku is None or col_seller_sku not in orders.columns:
        col_seller_sku = find_col_by_keywords(orders.columns, ["seller", "sku"]) or find_col_by_keywords(orders.columns, ["sku"])
    
    if col_qty is None or col_qty not in orders.columns:
        col_qty = find_col_by_keywords(orders.columns, ["qty", "quantity"]) or find_col_by_keywords(orders.columns, ["quantity"])

    if col_seller_sku is None or col_qty is None:
         col_seller_sku = orders.columns[sku_idx] if len(orders.columns) > sku_idx else orders.columns[0]
         col_qty = orders.columns[qty_idx] if len(orders.columns) > qty_idx else orders.columns[0]

    df = orders.copy()

    ensure_order_item_id_column_settlement(df, original_cols)

    df = df[df[col_bank_value].notna()]

    df["Order ID"] = df[col_order_id].astype(str).str.strip()
    df["Seller SKU"] = df[col_seller_sku].astype(str).str.strip()
    df["Settlement Qty"] = pd.to_numeric(df[col_qty], errors="coerce").fillna(0)
    df["Payment Received"] = pd.to_numeric(df[col_bank_value], errors="coerce").fillna(0)

    keep_cols = ["Order ID", "Seller SKU", "Settlement Qty", "Payment Received"]
    if "Order Item ID" in df.columns:
        keep_cols.insert(1, "Order Item ID")
        df = df[df["Order Item ID"].notna()]
    
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
        target_sheet = xls.sheet_names[0]

    header_idx = find_header_row_dynamic(bio, target_sheet, ["order item id", "sku", "invoice"])

    bio.seek(0)
    sales = pd.read_excel(bio, sheet_name=target_sheet, header=header_idx, engine="openpyxl")
    original_cols = sales.columns.tolist()

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
        col_order_date = sales.columns[0]
    else:
        col_order_date = order_date_candidates[0]

    invoice_candidates = [c for c in sales.columns if "final invoice amount" in str(c).lower()]
    if not invoice_candidates:
        invoice_candidates = [c for c in sales.columns if "invoice" in str(c).lower() and "amount" in str(c).lower()]
    if not invoice_candidates:
        raise ValueError("Could not find 'Final Invoice Amount' column in Sale Report.")
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

    ensure_order_item_id_column_sales(df, original_cols)

    df["Order Date"] = pd.to_datetime(df[col_order_date], errors="coerce")
    df["Order ID"] = df[col_order].astype(str).str.strip()
    df["SKU"] = df[col_sku].apply(clean_sku_text)
    df["Item Quantity"] = pd.to_numeric(df[col_qty], errors="coerce").fillna(0)
    df["Event Sub Type"] = df[col_event_sub].astype(str).fillna("").str.strip()
    df["Invoice Amount"] = pd.to_numeric(df[col_invoice], errors="coerce").fillna(0)
    if col_price_before is not None:
        df["Price Before Discount"] = pd.to_numeric(df[col_price_before], errors="coerce").fillna(0)
    else:
        df["Price Before Discount"] = 0.0

    df = df[df["Event Sub Type"].str.lower().isin(["sale", "return"])]

    df["IsReturn"] = df["Event Sub Type"].str.lower().eq("return")
    df.loc[df["IsReturn"], "Item Quantity"] *= -1
    df.loc[df["IsReturn"], "Invoice Amount"] *= -1
    df.loc[df["IsReturn"], "Price Before Discount"] *= -1

    df = df[
        df["Order ID"].notna()
        & df["SKU"].notna()
        & df["Invoice Amount"].notna()
    ]

    cols_return = ["Order Date", "Order ID", "SKU", "Item Quantity", "Invoice Amount", "Price Before Discount"]
    if "Order Item ID" in df.columns:
        cols_return.insert(1, "Order Item ID")
        df = df[df["Order Item ID"].notna()]
    
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
        return pd.DataFrame(columns=["SKU", "Cost Price"])

    out = df[[sku_col, cost_col]].copy()
    out["SKU"] = out[sku_col].astype(str).str.strip()
    out["Cost Price"] = pd.to_numeric(out[cost_col], errors="coerce")
    out = out[out["SKU"].notna() & out["Cost Price"].notna()]
    return out[["SKU", "Cost Price"]]

def final_report_to_excel_bytes(df_mapping: pd.DataFrame):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_mapping.to_excel(writer, index=False, sheet_name="Final_Mapped_Report")
    return output.getvalue()

def main():
    st.set_page_config(page_title="Flipkart Reconciliation Tool", page_icon="📊", layout="wide")
    st.title("📊 Flipkart Reconciliation Tool")

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

            left_has_oi = "Order Item ID" in set_pivot.columns
            right_has_oi = "Order Item ID" in sale_pivot.columns

            if left_has_oi and right_has_oi:
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
                st.warning("Order Item ID not found in both. Falling back to Order ID match.")
                left_id_col = "Order ID" if "Order ID" in set_pivot.columns else ("Order Item ID" if "Order Item ID" in set_pivot.columns else None)
                right_id_col = "Order ID" if "Order ID" in sale_pivot.columns else ("Order Item ID" if "Order Item ID" in sale_pivot.columns else None)

                set_tmp = set_pivot.copy()
                sale_tmp = sale_pivot.copy()

                if left_id_col and left_id_col != "Order ID":
                    set_tmp = set_tmp.rename(columns={left_id_col: "Order ID"})
                if right_id_col and right_id_col != "Order ID":
                    sale_tmp = sale_tmp.rename(columns={right_id_col: "Order ID"})
                
                if "Seller SKU" not in set_tmp.columns:
                     set_tmp = set_tmp.rename(columns={set_tmp.columns[1]: "Seller SKU"})

                left_cols_for_merge = [c for c in ["Order ID", "Seller SKU", "Settlement_Qty", "Payment_Received"] if c in set_tmp.columns]
                right_on = ["Order ID", "SKU"]
                left_on = ["Order ID", "Seller SKU"]
                
                mapping = sale_tmp.merge(
                    set_tmp[left_cols_for_merge],
                    left_on=right_on,
                    right_on=left_on,
                    how="left",
                )

            mapping = mapping.drop(columns=["Seller SKU"], errors="ignore")

            if "Settlement_Qty" in mapping.columns:
                mapping["Settlement_Qty"] = mapping["Settlement_Qty"].fillna(0)
            else:
                mapping["Settlement_Qty"] = 0

            if "Payment_Received" in mapping.columns:
                mapping["Payment_Received"] = mapping["Payment_Received"].fillna(0.0)
            else:
                mapping["Payment_Received"] = 0.0

            mapping["Qty_Diff (Settlement - Sale)"] = mapping["Settlement_Qty"] - mapping["Item Quantity"]
            mapping["Amount_Diff (Settlement - Invoice)"] = mapping["Payment_Received"] - mapping["Invoice Amount"]

            if "Price Before Discount" in mapping.columns:
                mapping["Payment received agaist this Amount"] = mapping["Price Before Discount"]
            else:
                mapping["Payment received agaist this Amount"] = 0.0

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

            total_rows = mapping.shape[0]
            unique_orders = mapping["Order ID"].nunique() if "Order ID" in mapping.columns else mapping.shape[0]
            unique_skus = mapping["SKU"].nunique() if "SKU" in mapping.columns else 0
            total_invoice = mapping["Invoice Amount"].sum() if "Invoice Amount" in mapping.columns else 0.0
            total_payment = mapping["Payment_Received"].sum() if "Payment_Received" in mapping.columns else 0.0
            total_cost = mapping["Total Cost (Qty * Cost)"].sum() if "Total Cost (Qty * Cost)" in mapping.columns else 0.0

            c1, c2, c3 = st.columns(3)
            c1.metric("Rows", total_rows)
            c2.metric("Orders", unique_orders)
            c3.metric("SKUs", unique_skus)

            c5, c6, c7 = st.columns(3)
            c5.metric("Total Invoice", f"{total_invoice:,.2f}")
            c6.metric("Total Payment", f"{total_payment:,.2f}")
            c7.metric("Total Cost", f"{total_cost:,.2f}")

            royalty_rules = {"MKUC": 0.10, "DKUC": 0.10, "DYK": 0.07, "MYK": 0.07, "KYK": 0.01, "MAC": 0.10}
            total_royalty = 0.0
            if "SKU" in mapping.columns:
                sku_series = mapping["SKU"].astype(str).fillna("")
                for prefix, rate in royalty_rules.items():
                    mask = sku_series.str.upper().str.startswith(prefix)
                    net_invoice = mapping.loc[mask, "Invoice Amount"].sum()
                    net_positive = net_invoice if net_invoice > 0 else 0.0
                    total_royalty += net_positive * rate

            st.metric("Total Royalty", f"{total_royalty:,.2f}")
            net_settlement_after = total_payment - total_cost - total_royalty
            st.metric("Net Settlement", f"{net_settlement_after:,.2f}")

            st.markdown("---")

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
                "Amount_Diff (Settlement - Invoice)",
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
        st.info("Upload files to start.")

if __name__ == "__main__":
    main()
