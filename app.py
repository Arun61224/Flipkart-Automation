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
        # thoda generic: koi bhi naam jisme sale + report dono ho
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

    df["Order ID"] = df[col_order].astype(str).strip()
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
