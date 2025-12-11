import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import re

# -----------------------
# Utility Functions
# -----------------------

def normalize_id(series):
    # Convert to string, clean up float artifacts (.0), remove existing quotes
    s = series.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.str.replace(r"^'+", "", regex=True)
    
    # Prefix with single quote for exact Excel matching
    is_nan = s.str.lower() == 'nan'
    s_final = "'" + s
    s_final[is_nan] = np.nan
    return s_final

def find_header_row(df_preview, keywords):
    # Dynamic header detection to handle merged cells like I2/I3
    for i, row in df_preview.iterrows():
        row_str = row.astype(str).str.lower().tolist()
        if any(k in str(x) for x in row_str for k in keywords):
            return i
    return 0

# -----------------------
# File Processing
# -----------------------

def process_settlement(file_bytes):
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)
    
    # 1. Find correct sheet
    sheet_name = next((s for s in xls.sheet_names if "order" in s.lower()), None)
    if not sheet_name:
        return pd.DataFrame()

    # 2. Dynamic Header Detection (Scans first 20 rows)
    bio.seek(0)
    df_preview = pd.read_excel(bio, sheet_name=sheet_name, header=None, nrows=20, engine="openpyxl")
    header_idx = find_header_row(df_preview, ["order item id", "order id", "settlement value", "bank"])
    
    # 3. Read Data
    bio.seek(0)
    df = pd.read_excel(bio, sheet_name=sheet_name, header=header_idx, engine="openpyxl")
    
    # 4. Get Order Item ID (Priority: Column I/Index 8 -> Name match)
    id_col = None
    
    # Check Column I (Index 8) first as requested
    if len(df.columns) > 8:
        col_i = df.columns[8]
        # Verify if it looks like an ID column or generic
        id_col = col_i

    # Fallback to name search if I is not valid or empty
    if id_col is None:
        for c in df.columns:
            if "order" in str(c).lower() and "item" in str(c).lower() and "id" in str(c).lower():
                id_col = c
                break

    if id_col is None:
        return pd.DataFrame() 

    df["Order Item ID"] = normalize_id(df[id_col])

    # 5. Get Bank Settlement Value
    pay_col = None
    for c in df.columns:
        if "bank" in str(c).lower() and "settlement" in str(c).lower():
            pay_col = c
            break
    if not pay_col:
        for c in df.columns:
            if "payment" in str(c).lower() or "amount" in str(c).lower():
                pay_col = c
                break
    
    if not pay_col and len(df.columns) > 3:
        pay_col = df.columns[3] # Fallback column

    if not pay_col:
        return pd.DataFrame()

    # 6. clean and return
    df = df[df["Order Item ID"].notna()]
    df["Settlement Value"] = pd.to_numeric(df[pay_col], errors='coerce').fillna(0.0)
    
    # Group by ID to handle duplicates (summing up payments for same item if split)
    return df.groupby("Order Item ID", as_index=False)["Settlement Value"].sum()

def process_sales(file_bytes):
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)
    
    # 1. Find Sales Report sheet
    sheet_name = next((s for s in xls.sheet_names if "sale" in s.lower() and "report" in s.lower()), None)
    if not sheet_name:
        sheet_name = next((s for s in xls.sheet_names if "sale" in s.lower()), xls.sheet_names[0])

    # 2. Dynamic Header
    bio.seek(0)
    df_preview = pd.read_excel(bio, sheet_name=sheet_name, header=None, nrows=20, engine="openpyxl")
    header_idx = find_header_row(df_preview, ["order item id", "sku", "invoice"])

    # 3. Read Data
    bio.seek(0)
    df = pd.read_excel(bio, sheet_name=sheet_name, header=header_idx, engine="openpyxl")

    # 4. Get Order Item ID (Priority: Column C/Index 2 -> Name match)
    id_col = None
    
    # Check Column C (Index 2) first as requested
    if len(df.columns) > 2:
        id_col = df.columns[2]
        
    # Fallback to name match
    if id_col is None:
        for c in df.columns:
            if "order" in str(c).lower() and "item" in str(c).lower() and "id" in str(c).lower():
                id_col = c
                break
        
    if id_col:
        df["Order Item ID"] = normalize_id(df[id_col])
    else:
        return pd.DataFrame()

    # 5. Map standard columns
    col_map = {}
    for c in df.columns:
        cl = str(c).lower()
        if "order date" in cl: col_map["Order Date"] = c
        elif "order id" in cl and "item" not in cl: col_map["Order ID"] = c
        elif "sku" in cl: col_map["SKU"] = c
        elif "qty" in cl or "quantity" in cl: col_map["Item Quantity"] = c
        elif "invoice" in cl and "amount" in cl: col_map["Invoice Amount"] = c
        elif "event" in cl and "sub" in cl: col_map["Event Sub Type"] = c
        elif "price" in cl and "before" in cl: col_map["Price Before Discount"] = c

    # Create standardized dataframe
    final_cols = ["Order Item ID"]
    for k, v in col_map.items():
        df[k] = df[v]
        final_cols.append(k)

    # 6. Handle Returns (Negative values)
    if "Event Sub Type" in df.columns:
        df = df[df["Event Sub Type"].astype(str).str.lower().isin(["sale", "return"])]
        is_return = df["Event Sub Type"].astype(str).str.lower() == "return"
        
        cols_to_negate = ["Item Quantity", "Invoice Amount", "Price Before Discount"]
        for col in cols_to_negate:
            if col in df.columns:
                df.loc[is_return, col] = df.loc[is_return, col].abs() * -1

    return df[final_cols].copy()

def process_cost(file_bytes):
    df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl")
    sku_col = next((c for c in df.columns if "sku" in str(c).lower()), None)
    cost_col = next((c for c in df.columns if "cost" in str(c).lower()), None)
    
    if sku_col and cost_col:
        df = df[[sku_col, cost_col]].copy()
        df.columns = ["SKU", "Cost Price"]
        df["SKU"] = df["SKU"].astype(str).str.strip()
        df["Cost Price"] = pd.to_numeric(df["Cost Price"], errors='coerce').fillna(0)
        return df
    return pd.DataFrame(columns=["SKU", "Cost Price"])

def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Final_Report')
    return output.getvalue()

# -----------------------
# Main App
# -----------------------

def main():
    st.set_page_config(page_title="Flipkart Reconciliation", layout="wide")
    st.title("📊 Flipkart Reconciliation Tool")

    st.sidebar.header("Upload Files")
    settlement_files = st.sidebar.file_uploader("Settlement Files (Order Sheet)", accept_multiple_files=True)
    sales_files = st.sidebar.file_uploader("Sales Report Files", accept_multiple_files=True)
    cost_file = st.sidebar.file_uploader("Cost File (Optional)", type=["xlsx", "xls"])

    if settlement_files and sales_files:
        try:
            # --- Process Settlement ---
            settlement_dfs = []
            for f in settlement_files:
                df = process_settlement(f.getvalue())
                if not df.empty:
                    settlement_dfs.append(df)
            
            if not settlement_dfs:
                st.error("Could not process Settlement files. Please check if 'Order Item ID' is in Column I or correct sheet exists.")
                return
                
            df_settlement = pd.concat(settlement_dfs, ignore_index=True)
            # Ensure unique ID per settlement row
            df_settlement = df_settlement.groupby("Order Item ID", as_index=False)["Settlement Value"].sum()

            # --- Process Sales ---
            sales_dfs = []
            for f in sales_files:
                df = process_sales(f.getvalue())
                if not df.empty:
                    sales_dfs.append(df)
            
            if not sales_dfs:
                st.error("Could not process Sales files. Please check if 'Order Item ID' is in Column C.")
                return

            df_sales = pd.concat(sales_dfs, ignore_index=True)

            # --- Merge Logic (Left Join on Sales) ---
            df_merged = pd.merge(df_sales, df_settlement, on="Order Item ID", how="left")
            df_merged["Settlement Value"] = df_merged["Settlement Value"].fillna(0)

            # --- Difference Calculation ---
            if "Invoice Amount" in df_merged.columns:
                df_merged["Difference"] = df_merged["Settlement Value"] - df_merged["Invoice Amount"]
            
            # --- Cost & Royalty Logic ---
            if cost_file:
                df_cost = process_cost(cost_file.getvalue())
                if "SKU" in df_merged.columns:
                    # Clean SKU prefix if any
                    df_merged["SKU_Clean"] = df_merged["SKU"].astype(str).str.strip().str.replace(r'^SKU:', '', regex=True).str.strip()
                    df_merged = pd.merge(df_merged, df_cost, left_on="SKU_Clean", right_on="SKU", how="left", suffixes=("", "_cost"))
                    df_merged.drop(columns=["SKU_cost", "SKU_Clean"], inplace=True)
                    df_merged["Cost Price"] = df_merged["Cost Price"].fillna(0)
                    
                    # Logic: Return = 50% cost, Zero Qty = 20% cost
                    cost_adj = df_merged["Cost Price"].copy()
                    if "Item Quantity" in df_merged.columns:
                        mask_ret = df_merged["Item Quantity"] < 0
                        mask_zero = df_merged["Item Quantity"] == 0
                        cost_adj.loc[mask_ret] *= 0.5
                        cost_adj.loc[mask_zero] *= 0.2
                        df_merged["Total Cost"] = df_merged["Item Quantity"] * cost_adj

            # Royalty
            royalty = 0.0
            royalty_map = {"MKUC": 0.10, "DKUC": 0.10, "DYK": 0.07, "MYK": 0.07, "KYK": 0.01, "MAC": 0.10}
            if "SKU" in df_merged.columns and "Invoice Amount" in df_merged.columns:
                sku_series = df_merged["SKU"].astype(str).str.strip().str.upper()
                inv_series = df_merged["Invoice Amount"].fillna(0)
                
                for prefix, rate in royalty_map.items():
                    # Royalty applies on positive invoice amounts
                    mask = sku_series.str.startswith(prefix) & (inv_series > 0)
                    royalty += (inv_series[mask] * rate).sum()

            # --- Dashboard & Download ---
            st.write("### Final Mapped Data")
            st.dataframe(df_merged)

            c1, c2, c3 = st.columns(3)
            total_inv = df_merged["Invoice Amount"].sum() if "Invoice Amount" in df_merged.columns else 0
            total_sett = df_merged["Settlement Value"].sum()
            
            c1.metric("Total Invoice Amount", f"{total_inv:,.2f}")
            c2.metric("Total Settlement Value", f"{total_sett:,.2f}")
            c3.metric("Total Royalty", f"{royalty:,.2f}")

            st.download_button(
                "Download Final Report",
                data=to_excel(df_merged),
                file_name="Flipkart_Reconciliation_Report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        except Exception as e:
            st.error(f"Error occurred: {e}")
    else:
        st.info("Please upload Settlement (Order sheet) and Sales Report (Sales sheet) files.")

if __name__ == "__main__":
    main()
