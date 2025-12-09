import streamlit as st
import pandas as pd
from io import BytesIO

# -----------------------
# Excel column name -> index (A=0, BG=58 etc.)
# -----------------------
def excel_col_to_idx(col_name: str) -> int:
    col_name = col_name.strip().upper()
    idx = 0
    for c in col_name:
        if "A" <= c <= "Z":
            idx = idx * 26 + (ord(c) - ord("A") + 1)
    return idx - 1


# ========================= SETTLEMENT FILE =========================
def process_orders_excel_from_bytes(file_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)

    if "Orders" not in xls.sheet_names:
        raise ValueError('Sheet named "Orders" not found.')

    bio.seek(0)
    orders = pd.read_excel(bio, sheet_name="Orders", engine="openpyxl")

    col_order_id = "Transaction Summary"   # H
    col_bank_value = "Unnamed: 3"          # D

    sku_idx = excel_col_to_idx("BG")
    qty_idx = excel_col_to_idx("BH")
    col_sku = orders.columns[sku_idx]
    col_qty = orders.columns[qty_idx]

    df = orders.copy()
    df = df[df[col_order_id].astype(str).str.startswith("OD", na=False)]

    df[col_bank_value] = pd.to_numeric(df[col_bank_value], errors="coerce")
    df[col_qty] = pd.to_numeric(df[col_qty], errors="coerce")

    df = df[
        df[col_order_id].notna()
        & df[col_bank_value].notna()
        & df[col_sku].notna()
        & df[col_qty].notna()
    ]

    return df[[col_order_id, col_sku, col_qty, col_bank_value]].rename(
        columns={
            col_order_id: "Order ID",
            col_sku: "Seller SKU",
            col_qty: "Settlement Qty",
            col_bank_value: "Payment Received",
        }
    )


def summarize_orders(df):
    pivot = (
        df.groupby(["Order ID", "Seller SKU"], as_index=False)
        .agg(
            Settlement_Qty=("Settlement Qty", "sum"),
            Payment_Received=("Payment Received", "sum"),
        )
    )
    summary = {
        "rows": df.shape[0],
        "orders": df["Order ID"].nunique(),
        "skus": df["Seller SKU"].nunique(),
        "qty": df["Settlement Qty"].sum(),
        "payment": df["Payment Received"].sum(),
    }
    return pivot, summary


@st.cache_data(show_spinner=False)
def process_multiple_orders_cached(files_bytes_list):
    all_raw = [process_orders_excel_from_bytes(f) for f in files_bytes_list]
    raw = pd.concat(all_raw, ignore_index=True)
    pivot, summary = summarize_orders(raw)
    return raw, pivot, summary


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

    # sheet detect (contains sale + report)
    target_sheet = None
    for s in xls.sheet_names:
        chk = s.lower().replace(" ", "")
        if "sale" in chk and "report" in chk:
            target_sheet = s
            break
    if not target_sheet:
        raise ValueError("Sale Report sheet not found")

    bio.seek(0)
    sales = pd.read_excel(bio, sheet_name=target_sheet, engine="openpyxl")

    # B: Order ID, F: SKU, N: Qty, H: Event
    order_idx = excel_col_to_idx("B")
    sku_idx = excel_col_to_idx("F")
    qty_idx = excel_col_to_idx("N")
    event_idx = excel_col_to_idx("H")

    col_order = sales.columns[order_idx]
    col_sku = sales.columns[sku_idx]
    col_qty = sales.columns[qty_idx]
    col_event = sales.columns[event_idx]

    # Order Date column
    col_order_date = [c for c in sales.columns if "order date" in c.lower()][0]

    # Final Invoice Amount (Price after discount+Shipping Charges)
    col_invoice = [
        c for c in sales.columns if "final invoice amount" in c.lower()
    ][0]

    # Price before discount
    col_price_before = None
    for c in sales.columns:
        if "price before discount" in c.lower():
            col_price_before = c
            break

    cols = [col_order_date, col_order, col_sku, col_qty, col_event, col_invoice]
    if col_price_before:
        cols.append(col_price_before)

    df = sales[cols].copy()

    df["Order Date"] = pd.to_datetime(df[col_order_date], errors="coerce")
    df["Order ID"] = df[col_order].astype(str).str.strip()
    df["SKU"] = df[col_sku].apply(clean_sku_text)
    df["Item Quantity"] = pd.to_numeric(df[col_qty], errors="coerce")
    df["Event"] = df[col_event].astype(str)

    # yahi se ORIGINAL long column se Invoice Amount bana rahe
    df["Invoice Amount"] = pd.to_numeric(df[col_invoice], errors="coerce")

    if col_price_before:
        df["Price Before Discount"] = pd.to_numeric(
            df[col_price_before], errors="coerce"
        )
    else:
        df["Price Before Discount"] = 0

    # saari Return rows hata do
    df = df[df["Event"].str.lower() != "return"]

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


def summarize_sales(df):
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
    all_raw = [load_single_sales_df_from_bytes(f) for f in files_bytes_list]
    raw = pd.concat(all_raw, ignore_index=True)
    pivot, summary = summarize_sales(raw)
    return raw, pivot, summary


# ========================= COST FILE =========================
@st.cache_data(show_spinner=False)
def process_cost_file_cached(file_bytes):
    bio = BytesIO(file_bytes)
    df = pd.read_excel(bio, engine="openpyxl")

    sku = [c for c in df.columns if "sku" in c.lower()][0]
    cost = [c for c in df.columns if "cost" in c.lower()][0]

    out = df[[sku, cost]].copy()
    out["SKU"] = out[sku].astype(str)
    out["Cost Price"] = pd.to_numeric(out[cost], errors="coerce")
    return out[["SKU", "Cost Price"]]


# ========================= EXPORT TO EXCEL =========================
def final_report_to_excel_bytes(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Final_Mapped_Report")
    return output.getvalue()


# ========================= STREAMLIT APP =========================
def main():
    st.set_page_config(
        page_title="Flipkart Reconciliation Tool",
        page_icon="📊",
        layout="wide",
    )
    st.title("📊 Flipkart Reconciliation Tool")

    # ---------- Sidebar uploads ----------
    st.sidebar.header("Upload Files")
    settlement_files = st.sidebar.file_uploader(
        "Settlement Excel file(s)",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
    )
    sales_files = st.sidebar.file_uploader(
        "Sale Report Excel file(s)",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
    )
    cost_file = st.sidebar.file_uploader(
        "SKU Cost Price file",
        type=["xlsx", "xls"],
    )

    st.markdown("---")

    if settlement_files and sales_files:
        try:
            # -------- Process files --------
            raw_set, pivot_set, set_summary = process_multiple_orders_cached(
                [f.getvalue() for f in settlement_files]
            )
            raw_sale, pivot_sale, sale_summary = process_multiple_sales_cached(
                [f.getvalue() for f in sales_files]
            )

            # -------- Mapping --------
            mapping = pivot_sale.merge(
                pivot_set[["Order ID", "Seller SKU", "Settlement_Qty", "Payment_Received"]],
                left_on=["Order ID", "SKU"],
                right_on=["Order ID", "Seller SKU"],
                how="left",
            )

            mapping = mapping.drop(columns=["Seller SKU"])
            mapping["Settlement_Qty"] = mapping["Settlement_Qty"].fillna(0)
            mapping["Payment_Received"] = mapping["Payment_Received"].fillna(0)

            mapping["Qty_Diff (Settlement - Sale)"] = (
                mapping["Settlement_Qty"] - mapping["Item Quantity"]
            )

            # Amount diff calculation (backend only – report me nahi dikhate)
            mapping["Amount_Diff (Settlement - Invoice)"] = (
                mapping["Payment_Received"] - mapping["Invoice Amount"]
            )

            # Payment received agaist this Amount = Price Before Discount
            mapping["Payment received agaist this Amount"] = mapping[
                "Price Before Discount"
            ]

            # Cost merge
            if cost_file:
                cost = process_cost_file_cached(cost_file.getvalue())
                mapping = mapping.merge(cost, on="SKU", how="left")
                mapping["Cost Price"] = mapping["Cost Price"].fillna(0)
                mapping["Total Cost (Qty * Cost)"] = (
                    mapping["Item Quantity"] * mapping["Cost Price"]
                )
            else:
                mapping["Cost Price"] = 0
                mapping["Total Cost (Qty * Cost)"] = 0

            # -------- Summary (upar detail) --------
            st.subheader("Summary")

            total_rows = mapping.shape[0]
            unique_orders = mapping["Order ID"].nunique()
            unique_skus = mapping["SKU"].nunique()
            total_invoice = mapping["Invoice Amount"].sum()
            total_payment = mapping["Payment_Received"].sum()
            total_qty_diff = mapping["Qty_Diff (Settlement - Sale)"].sum()
            total_cost = mapping["Total Cost (Qty * Cost)"].sum()

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Rows (Order + SKU)", total_rows)
            c2.metric("Unique Orders", unique_orders)
            c3.metric("Unique SKUs", unique_skus)
            c4.metric("Net Qty Diff", int(total_qty_diff))

            c5, c6, c7 = st.columns(3)
            c5.metric("Total Invoice Amount", f"{total_invoice:,.2f}")
            c6.metric("Total Settlement Payment", f"{total_payment:,.2f}")
            c7.metric("Total Cost (Qty * Cost)", f"{total_cost:,.2f}")

            st.markdown("---")

            # -------- Final view (J column removed) --------
            col_final = [
                "Order Date",
                "Order ID",
                "SKU",
                "Item Quantity",
                "Invoice Amount",
                "Payment received agaist this Amount",
                "Settlement_Qty",
                "Qty_Diff (Settlement - Sale)",
                "Payment_Received",
                "Cost Price",
                "Total Cost (Qty * Cost)",
            ]
            mapping_view = mapping[col_final]

            st.subheader("Final Mapped Report")
            st.dataframe(mapping_view, use_container_width=True)

            # -------- Download Excel --------
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
        st.info("Upload Settlement & Sales Report files from the sidebar to start reconciliation.")


if __name__ == "__main__":
    main()
