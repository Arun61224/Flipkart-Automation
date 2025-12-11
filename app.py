import re
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO

# -------------------------------------------------------------------------
# Helper Functions
# -------------------------------------------------------------------------

def excel_col_to_idx(col_name: str) -> int:
    """Converts Excel column letter (e.g., 'A', 'Z', 'AA') to 0-based index."""
    col_name = col_name.strip().upper()
    idx = 0
    for c in col_name:
        if "A" <= c <= "Z":
            idx = idx * 26 + (ord(c) - ord("A") + 1)
    return idx - 1

def find_col_by_keywords(cols, keywords):
    """
    Finds a column name containing ALL the provided keywords (case-insensitive).
    Handles multi-line headers by replacing newlines with spaces.
    """
    # Normalize headers: convert to string, lower case, replace newlines with spaces
    low = [str(c).lower().replace('\n', ' ').strip() for c in cols]
    
    for i, c in enumerate(low):
        if all(k.lower() in c for k in keywords):
            return cols[i]
    return None

def find_header_row_dynamic(bio, sheet_name, keywords):
    """
    Scans the first 20 rows to find the header row containing specific keywords.
    """
    bio.seek(0)
    # Read first 20 rows without header
    df_preview = pd.read_excel(bio, sheet_name=sheet_name, header=None, nrows=20, engine="openpyxl")
    
    for i, row in df_preview.iterrows():
        # Convert row values to string, lowercase, and join to search
        row_str = " ".join([str(x).lower() for x in row.tolist()])
        # Check if any of the keywords are present in the row string
        if any(k in row_str for k in keywords):
            return i
    return 0

def normalize_order_item_id(series: pd.Series) -> pd.Series:
    """
    Normalizes Order Item IDs to ensure exact matching:
    - Converts to string
    - Removes trailing '.0'
    - Removes existing single quotes
    - Adds a single quote prefix "'" to treat as text in Excel
    """
    s = series.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.str.replace(r"^'+", "", regex=True)
    
    # Preserve NaNs
    is_nan = s.str.lower() == 'nan'
    s_final = "'" + s
    s_final[is_nan] = np.nan
    return s_final

# -------------------------------------------------------------------------
# Settlement File Processing
# -------------------------------------------------------------------------

def ensure_order_item_id_column_settlement(df: pd.DataFrame, original_columns):
    """Ensures 'Order Item ID' column exists, checking Column I first."""
    # 1. Try Column I (Index 8) explicitly
    col_i_idx = excel_col_to_idx("I")
    if len(original_columns) > col_i_idx:
        target_col = original_columns[col_i_idx]
        if target_col in df.columns:
            df["Order Item ID"] = normalize_order_item_id(df[target_col])
            return target_col
            
    # 2. Fallback: Search by name
    cand = find_col_by_keywords(df.columns, ["order", "item", "id"])
    if cand is None:
        cand = find_col_by_keywords(df.columns, ["orderitem", "id"])
    
    if cand is not None:
        df["Order Item ID"] = normalize_order_item_id(df[cand])
    return cand

def process_orders_excel_from_bytes(file_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)

    # Find the "Orders" sheet
    target_sheet = next((s for s in xls.sheet_names if "order" in s.lower()), None)
    if not target_sheet:
        if "Orders" in xls.sheet_names:
            target_sheet = "Orders"
        else:
            raise ValueError('Sheet named "Orders" or similar not found in Settlement file.')

    # Dynamic Header Detection
    # Look for "neft id" OR "settlement value" OR "order item id"
    header_idx = find_header_row_dynamic(bio, target_sheet, ["neft id", "settlement value", "order item id"])
    
    bio.seek(0)
    orders = pd.read_excel(bio, sheet_name=target_sheet, header=header_idx, engine="openpyxl")
    original_cols = orders.columns.tolist()

    # Identify Key Columns
    
    # 1. Order ID
    col_order_id = (
        find_col_by_keywords(orders.columns, ["transaction", "summary"])
        or find_col_by_keywords(orders.columns, ["order", "id"])
        or orders.columns[0]
    )

    # 2. Bank Settlement Value
    # Strategy: Look for "bank" AND "settlement" AND "value" (handles newlines due to helper update)
    col_bank_value = find_col_by_keywords(orders.columns, ["bank", "settlement", "value"])
    
    # Fallback Strategy: Look for "payment" or "amount" if specific name not found
    if col_bank_value is None:
        col_bank_value = find_col_by_keywords(orders.columns, ["payment", "amount"])
        
    # Hard Fallback: Column D (Index 3) as per file structure
    if col_bank_value is None:
        if len(orders.columns) >= 4:
            col_bank_value = orders.columns[3]
        else:
            raise ValueError("Cannot detect Bank Settlement Value column in Orders sheet.")

    # 3. Seller SKU
    sku_idx = excel_col_to_idx("BG") # Based on previous knowledge, usually far right
    col_seller_sku = None
    if len(orders.columns) > sku_idx:
        col_seller_sku = orders.columns[sku_idx]
    
    if col_seller_sku is None or col_seller_sku not in orders.columns:
        col_seller_sku = find_col_by_keywords(orders.columns, ["seller", "sku"]) or find_col_by_keywords(orders.columns, ["sku"])

    # 4. Quantity
    qty_idx = excel_col_to_idx("BH")
    col_qty = None
    if len(orders.columns) > qty_idx:
        col_qty = orders.columns[qty_idx]
        
    if col_qty is None or col_qty not in orders.columns:
        col_qty = find_col_by_keywords(orders.columns, ["qty", "quantity"])

    # Fallback for SKU/Qty if still not found (take first column just to prevent crash, though data will be wrong)
    if col_seller_sku is None: col_seller_sku = orders.columns[0]
    if col_qty is None: col_qty = orders.columns[0]

    df = orders.copy()

    # Normalize ID
    ensure_order_item_id_column_settlement(df, original_cols)

    # Filter invalid rows based on settlement value
    df = df[df[col_bank_value].notna()]

    # Extract Data
    df["Order ID"] = df[col_order_id].astype(str).str.strip()
    df["Seller SKU"] = df[col_seller_sku].astype(str).str.strip()
    df["Settlement Qty"] = pd.to_numeric(df[col_qty], errors="coerce").fillna(0)
    
    # Safe Numeric Conversion for Payment (Handle commas)
    df["Payment Received"] = (
        df[col_bank_value]
        .astype(str)
        .str.replace(",", "")
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
    )

    # Select final columns
    keep_cols = ["Order ID", "Seller SKU", "Settlement Qty", "Payment Received"]
    if "Order Item ID" in df.columns:
        keep_cols.insert(1, "Order Item ID")
        df = df[df["Order Item ID"].notna()]
    
    return df[keep_cols].copy()

def summarize_orders(df_clean: pd.DataFrame):
    """Aggregates settlement data by Order Item ID."""
    if "Order Item ID" in df_clean.columns:
        group_cols = ["Order Item ID"]
    else:
        group_cols = ["Order ID", "Seller SKU"]

    pivot = (
        df_clean.groupby(group_cols, as_index=False)
        .agg(
            Settlement_Qty=("Settlement Qty", "sum"),
            Payment_Received=("Payment Received", "sum"),
            Seller_SKU=("Seller SKU", "first") 
        )
        .reset_index(drop=True)
    )
    
    if "Seller_SKU" in pivot.columns:
         pivot = pivot.rename(columns={"Seller_SKU": "Seller SKU"})

    return pivot

@st.cache_data(show_spinner=False)
def process_multiple_orders_cached(files_bytes_list):
    all_raw = [process_orders_excel_from_bytes(fb) for fb in files_bytes_list]
    combined = pd.concat(all_raw, ignore_index=True)
    return summarize_orders(combined)

# -------------------------------------------------------------------------
# Sales File Processing
# -------------------------------------------------------------------------

def clean_sku_text(v):
    if pd.isna(v):
        return None
    s = str(v).strip().strip('"').strip("'")
    if s.upper().startswith("SKU:"):
        s = s[4:]
    return s.strip()

def ensure_order_item_id_column_sales(df: pd.DataFrame, original_columns):
    """Ensures 'Order Item ID' column exists, checking Column C first."""
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

def load_single_sales_df_from_bytes(file_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)

    # Find "Sales Report" sheet
    target_sheet = None
    for s in xls.sheet_names:
        # Normalize sheet name
        chk = s.lower().replace(" ", "")
        if "sales" in chk and "report" in chk: # Look for 'Sales Report' specifically
            target_sheet = s
            break
        elif "sale" in chk and "report" in chk:
            target_sheet = s
            break
            
    if target_sheet is None:
        target_sheet = xls.sheet_names[0]

    # Header Detection
    header_idx = find_header_row_dynamic(bio, target_sheet, ["order item id", "invoice amount", "sku"])

    bio.seek(0)
    sales = pd.read_excel(bio, sheet_name=target_sheet, header=header_idx, engine="openpyxl")
    original_cols = sales.columns.tolist()

    # Identify Key Columns
    col_order = find_col_by_keywords(sales.columns, ["order", "id"])
    if not col_order: col_order = sales.columns[0] # Fallback

    col_sku = find_col_by_keywords(sales.columns, ["sku"])
    if not col_sku: col_sku = sales.columns[1] # Fallback

    col_qty = find_col_by_keywords(sales.columns, ["qty", "quantity"])
    if not col_qty: col_qty = sales.columns[2] # Fallback

    col_event_sub = find_col_by_keywords(sales.columns, ["event", "sub", "type"])
    if not col_event_sub: col_event_sub = find_col_by_keywords(sales.columns, ["event"])

    col_order_date = find_col_by_keywords(sales.columns, ["order", "date"])
    if not col_order_date: col_order_date = sales.columns[0]

    # Final Invoice Amount
    col_invoice = find_col_by_keywords(sales.columns, ["final", "invoice", "amount"])
    if not col_invoice:
        col_invoice = find_col_by_keywords(sales.columns, ["invoice", "amount"])
    if not col_invoice:
        raise ValueError("Could not find 'Final Invoice Amount' column in Sale Report.")

    # Price Before Discount (for user reference column)
    col_price_before = find_col_by_keywords(sales.columns, ["price", "before", "discount"])

    cols = [col_order_date, col_order, col_sku, col_qty, col_event_sub, col_invoice]
    if col_price_before is not None:
        cols.append(col_price_before)

    df = sales[cols].copy()

    ensure_order_item_id_column_sales(df, original_cols)

    # Formatting Data
    df["Order Date"] = pd.to_datetime(df[col_order_date], errors="coerce")
    df["Order ID"] = df[col_order].astype(str).str.strip()
    df["SKU"] = df[col_sku].apply(clean_sku_text)
    df["Item Quantity"] = pd.to_numeric(df[col_qty], errors="coerce").fillna(0)
    
    if col_event_sub:
        df["Event Sub Type"] = df[col_event_sub].astype(str).fillna("").str.strip()
    else:
        df["Event Sub Type"] = "Sale" # Default if column missing

    df["Invoice Amount"] = pd.to_numeric(df[col_invoice], errors="coerce").fillna(0)
    
    if col_price_before is not None:
        df["Price Before Discount"] = pd.to_numeric(df[col_price_before], errors="coerce").fillna(0)
    else:
        df["Price Before Discount"] = 0.0

    # Filter Sale/Return
    df = df[df["Event Sub Type"].str.lower().isin(["sale", "return"])]

    # Handle Returns (Negative Values)
    df["IsReturn"] = df["Event Sub Type"].str.lower().eq("return")
    df.loc[df["IsReturn"], "Item Quantity"] *= -1
    df.loc[df["IsReturn"], "Invoice Amount"] *= -1
    df.loc[df["IsReturn"], "Price Before Discount"] *= -1

    # Filter Valid Rows
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
    return pivot

@st.cache_data(show_spinner=False)
def process_multiple_sales_cached(files_bytes_list):
    all_raw = [load_single_sales_df_from_bytes(fb) for fb in files_bytes_list]
    combined = pd.concat(all_raw, ignore_index=True)
    return summarize_sales(combined)

# -------------------------------------------------------------------------
# Cost File Processing
# -------------------------------------------------------------------------

@st.cache_data(show_spinner=False)
def process_cost_file_cached(file_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    df = pd.read_excel(bio, sheet_name=0, engine="openpyxl")
    
    sku_col = find_col_by_keywords(df.columns, ["sku"])
    cost_col = find_col_by_keywords(df.columns, ["cost"])

    if sku_col is None or cost_col is None:
        return pd.DataFrame(columns=["SKU", "Cost Price"])

    out = df[[sku_col, cost_col]].copy()
    out.columns = ["SKU", "Cost Price"] # Rename for consistency
    out["SKU"] = out["SKU"].astype(str).str.strip()
    out["Cost Price"] = pd.to_numeric(out["Cost Price"], errors="coerce")
    out = out[out["SKU"].notna() & out["Cost Price"].notna()]
    return out[["SKU", "Cost Price"]]

def final_report_to_excel_bytes(df_mapping: pd.DataFrame):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_mapping.to_excel(writer, index=False, sheet_name="Final_Mapped_Report")
    return output.getvalue()

# -------------------------------------------------------------------------
# Main App
# -------------------------------------------------------------------------

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
            # 1. Process Files
            set_pivot = process_multiple_orders_cached([f.getvalue() for f in settlement_files])
            sale_pivot = process_multiple_sales_cached([f.getvalue() for f in sales_files])

            left_has_oi = "Order Item ID" in set_pivot.columns
            right_has_oi = "Order Item ID" in sale_pivot.columns

            # 2. Merge Logic
            if left_has_oi and right_has_oi:
                # Primary Match: Order Item ID
                mapping = sale_pivot.merge(
                    set_pivot[["Order Item ID", "Settlement_Qty", "Payment_Received"]],
                    on="Order Item ID",
                    how="left",
                )
            else:
                st.warning("Order Item ID not found in both. Falling back to Order ID match.")
                # Fallback Match: Order ID
                # Rename columns to ensure match key exists
                left_id = "Order ID" if "Order ID" in set_pivot.columns else "Order Item ID"
                right_id = "Order ID" if "Order ID" in sale_pivot.columns else "Order Item ID"
                
                set_tmp = set_pivot.rename(columns={left_id: "Order ID"})
                sale_tmp = sale_pivot.rename(columns={right_id: "Order ID"})
                
                mapping = sale_tmp.merge(
                    set_tmp[["Order ID", "Settlement_Qty", "Payment_Received"]],
                    on="Order ID",
                    how="left"
                )

            # 3. Cleanup & Fill NA
            if "Settlement_Qty" in mapping.columns:
                mapping["Settlement_Qty"] = mapping["Settlement_Qty"].fillna(0)
            else:
                mapping["Settlement_Qty"] = 0

            if "Payment_Received" in mapping.columns:
                mapping["Payment_Received"] = mapping["Payment_Received"].fillna(0.0)
            else:
                mapping["Payment_Received"] = 0.0

            # 4. Calculations
            mapping["Qty_Diff (Settlement - Sale)"] = mapping["Settlement_Qty"] - mapping["Item Quantity"]
            mapping["Amount_Diff (Settlement - Invoice)"] = mapping["Payment_Received"] - mapping["Invoice Amount"]

            if "Price Before Discount" in mapping.columns:
                mapping["Payment received agaist this Amount"] = mapping["Price Before Discount"]
            else:
                mapping["Payment received agaist this Amount"] = 0.0

            # 5. Cost & Royalty
            if cost_file is not None:
                cost_df = process_cost_file_cached(cost_file.getvalue())
                mapping = mapping.merge(cost_df, on="SKU", how="left")
                mapping["Cost Price"] = mapping["Cost Price"].fillna(0.0)
            else:
                mapping["Cost Price"] = 0.0

            # Cost logic: 50% for returns, 20% for 0 qty (lost/damaged placeholder)
            mapping["Cost Price Adjusted"] = mapping["Cost Price"]
            mask_return = mapping["Item Quantity"] < 0
            mapping.loc[mask_return, "Cost Price Adjusted"] = mapping.loc[mask_return, "Cost Price"] * 0.5
            
            mask_zero = mapping["Item Quantity"] == 0
            mapping.loc[mask_zero, "Cost Price Adjusted"] = mapping.loc[mask_zero, "Cost Price"] * 0.2
            
            mapping["Total Cost (Qty * Cost)"] = mapping["Item Quantity"] * mapping["Cost Price Adjusted"]

            # 6. Metrics
            total_rows = mapping.shape[0]
            unique_orders = mapping["Order ID"].nunique() if "Order ID" in mapping.columns else mapping.shape[0]
            unique_skus = mapping["SKU"].nunique() if "SKU" in mapping.columns else 0
            total_invoice = mapping["Invoice Amount"].sum()
            total_payment = mapping["Payment_Received"].sum()
            total_cost = mapping["Total Cost (Qty * Cost)"].sum()

            royalty_rules = {"MKUC": 0.10, "DKUC": 0.10, "DYK": 0.07, "MYK": 0.07, "KYK": 0.01, "MAC": 0.10}
            total_royalty = 0.0
            if "SKU" in mapping.columns:
                sku_series = mapping["SKU"].astype(str).fillna("")
                for prefix, rate in royalty_rules.items():
                    mask = sku_series.str.upper().str.startswith(prefix)
                    # Royalty on positive invoice value only
                    net_invoice = mapping.loc[mask & (mapping["Invoice Amount"] > 0), "Invoice Amount"].sum()
                    total_royalty += net_invoice * rate
            
            net_settlement_after = total_payment - total_cost - total_royalty

            # 7. Display Metrics
            c1, c2, c3 = st.columns(3)
            c1.metric("Rows", total_rows)
            c2.metric("Orders", unique_orders)
            c3.metric("SKUs", unique_skus)

            c4, c5, c6 = st.columns(3)
            c4.metric("Total Invoice", f"{total_invoice:,.2f}")
            c5.metric("Total Payment", f"{total_payment:,.2f}")
            c6.metric("Total Cost", f"{total_cost:,.2f}")
            
            c7, c8 = st.columns(2)
            c7.metric("Total Royalty", f"{total_royalty:,.2f}")
            c8.metric("Net Profit (Approx)", f"{net_settlement_after:,.2f}")

            st.markdown("---")

            # 8. Final Table View
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
        st.info("Upload Settlement and Sale Report files to start.")

if __name__ == "__main__":
    main()
