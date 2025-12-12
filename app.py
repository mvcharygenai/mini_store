"""
Mini Store Streamlit app using Databricks SQL Warehouse as backend.

Requirements:
- pip install streamlit pandas databricks-sql-connector

IMPORTANT:
- Replace HTTP_PATH with your Databricks SQL Warehouse HTTP Path (SQL endpoint).
- For production, keep tokens out of source code and use Streamlit secrets or environment variables.
"""

import streamlit as st
import pandas as pd
import uuid
from databricks import sql
from typing import Optional

# -------------------------
# Databricks connection info
# -------------------------
DATABRICKS_HOST = "dbc-3d597f5a-4384.cloud.databricks.com"  # provided host (without https://)
# Provided PAT token (you gave this). Consider moving to environment variable or Streamlit secrets.
DATABRICKS_TOKEN = "dapidc9ce50a9534bbf23f8dbb2aa84fcddf"

# --- IMPORTANT: replace this with your SQL Warehouse HTTP Path ---
# Example format (you can copy from your Databricks SQL endpoint): "/sql/1.0/warehouses/<warehouse-id>"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/7011f2b79b409286"  # <<< REPLACE THIS

# Unity catalog + schema (you previously said catalog "streamlit", schema "oltp")
CATALOG = "streamlit"
SCHEMA = "oltp"

# Helper to connect
def get_connection():
    if DATABRICKS_HTTP_PATH.startswith("<"):
        raise ValueError("HTTP_PATH is not set. Replace DATABRICKS_HTTP_PATH with your Databricks SQL Warehouse HTTP Path.")
    conn = sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
        catalog="streamlit",
        schema="oltp",
    )
    return conn

# -------------------------
# SQL: create database/schema and tables
# -------------------------
def initialize_db():
    """
    Creates schema (if necessary) and the Customers, Products, Orders tables.
    Uses STRING ids (UUIDs) to avoid relying on DB-specific autoincrement syntax.
    Each table has CREATED_DATE and LAST_UPDATE_DATE.
    """
    ddl_commands = [
        # Unity catalog: create catalog/schema if not exists is managed separately in many setups.
        # Create schema if doesn't exist
        f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}",
        # Customers
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.customers (
          id STRING PRIMARY KEY,
          name STRING,
          email STRING,
          phone STRING,
          address STRING,
          created_date TIMESTAMP,
          last_update_date TIMESTAMP
        )
        """,
        # Products
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.products (
          id STRING PRIMARY KEY,
          name STRING,
          description STRING,
          price DOUBLE,
          stock INT,
          created_date TIMESTAMP,
          last_update_date TIMESTAMP
        )
        """,
        # Orders
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.orders (
          id STRING PRIMARY KEY,
          customer_id STRING,
          product_id STRING,
          quantity INT,
          total_amount DOUBLE,
          order_date TIMESTAMP,
          created_date TIMESTAMP,
          last_update_date TIMESTAMP
        )
        """
    ]

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for sql_cmd in ddl_commands:
                cur.execute(sql_cmd)
    finally:
        conn.close()

# -------------------------
# CRUD functions
# -------------------------
# -- Customers
def create_customer(name: str, email: str, phone: str, address: str):
    conn = get_connection()
    try:
        cid = str(uuid.uuid4())
        insert_sql = f"""
        INSERT INTO {CATALOG}.{SCHEMA}.customers (id, name, email, phone, address, created_date, last_update_date)
        VALUES ('{cid}', '{escape(name)}', '{escape(email)}', '{escape(phone)}', '{escape(address)}', current_timestamp(), current_timestamp())
        """
        with conn.cursor() as cur:
            cur.execute(insert_sql)
        return cid
    finally:
        conn.close()

def list_customers() -> pd.DataFrame:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {CATALOG}.{SCHEMA}.customers ORDER BY created_date DESC")
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        return df
    finally:
        conn.close()

def update_customer(cid: str, name: str, email: str, phone: str, address: str):
    conn = get_connection()
    try:
        update_sql = f"""
        UPDATE {CATALOG}.{SCHEMA}.customers
        SET name = '{escape(name)}',
            email = '{escape(email)}',
            phone = '{escape(phone)}',
            address = '{escape(address)}',
            last_update_date = current_timestamp()
        WHERE id = '{cid}'
        """
        with conn.cursor() as cur:
            cur.execute(update_sql)
    finally:
        conn.close()

def delete_customer(cid: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {CATALOG}.{SCHEMA}.customers WHERE id = '{cid}'")
    finally:
        conn.close()

# -- Products
def create_product(name: str, description: str, price: float, stock: int):
    conn = get_connection()
    try:
        pid = str(uuid.uuid4())
        insert_sql = f"""
        INSERT INTO {CATALOG}.{SCHEMA}.products (id, name, description, price, stock, created_date, last_update_date)
        VALUES ('{pid}', '{escape(name)}', '{escape(description)}', {price}, {stock}, current_timestamp(), current_timestamp())
        """
        with conn.cursor() as cur:
            cur.execute(insert_sql)
        return pid
    finally:
        conn.close()

def list_products() -> pd.DataFrame:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {CATALOG}.{SCHEMA}.products ORDER BY created_date DESC")
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        return df
    finally:
        conn.close()

def update_product(pid: str, name: str, description: str, price: float, stock: int):
    conn = get_connection()
    try:
        update_sql = f"""
        UPDATE {CATALOG}.{SCHEMA}.products
        SET name = '{escape(name)}',
            description = '{escape(description)}',
            price = {price},
            stock = {stock},
            last_update_date = current_timestamp()
        WHERE id = '{pid}'
        """
        with conn.cursor() as cur:
            cur.execute(update_sql)
    finally:
        conn.close()

def delete_product(pid: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {CATALOG}.{SCHEMA}.products WHERE id = '{pid}'")
    finally:
        conn.close()

# -- Orders
def create_order(customer_id: str, product_id: str, quantity: int, total_amount: float, order_date: Optional[str] = None):
    conn = get_connection()
    try:
        oid = str(uuid.uuid4())
        order_date_sql = "current_timestamp()" if order_date is None else f"CAST('{escape(order_date)}' AS TIMESTAMP)"
        insert_sql = f"""
        INSERT INTO {CATALOG}.{SCHEMA}.orders
        (id, customer_id, product_id, quantity, total_amount, order_date, created_date, last_update_date)
        VALUES ('{oid}', '{customer_id}', '{product_id}', {quantity}, {total_amount}, {order_date_sql}, current_timestamp(), current_timestamp())
        """
        with conn.cursor() as cur:
            cur.execute(insert_sql)
        return oid
    finally:
        conn.close()

def list_orders() -> pd.DataFrame:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {CATALOG}.{SCHEMA}.orders ORDER BY created_date DESC")
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        return df
    finally:
        conn.close()

def update_order(oid: str, customer_id: str, product_id: str, quantity: int, total_amount: float, order_date: Optional[str] = None):
    conn = get_connection()
    try:
        order_date_sql = "current_timestamp()" if order_date is None else f"CAST('{escape(order_date)}' AS TIMESTAMP)"
        update_sql = f"""
        UPDATE {CATALOG}.{SCHEMA}.orders
        SET customer_id = '{customer_id}',
            product_id = '{product_id}',
            quantity = {quantity},
            total_amount = {total_amount},
            order_date = {order_date_sql},
            last_update_date = current_timestamp()
        WHERE id = '{oid}'
        """
        with conn.cursor() as cur:
            cur.execute(update_sql)
    finally:
        conn.close()

def delete_order(oid: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {CATALOG}.{SCHEMA}.orders WHERE id = '{oid}'")
    finally:
        conn.close()

# -------------------------
# Utilities
# -------------------------
def escape(s: Optional[str]) -> str:
    """Simple string escape for single quotes in SQL text values."""
    if s is None:
        return ""
    return s.replace("'", "''")

# -------------------------
# UI
# -------------------------
st.set_page_config(page_title="Mini Store (Databricks)", layout="wide")
st.title("Mini Store — Streamlit + Databricks")

# Attempt to initialize DB when app starts
with st.spinner("Initializing database and tables..."):
    try:
        initialize_db()
        st.success("Database initialized (schema & tables exist).")
    except Exception as e:
        st.error("Initialization error: " + str(e))
        st.stop()

tabs = st.tabs(["Customers", "Products", "Orders"])

# --------------
# Customers tab
# --------------
with tabs[0]:
    st.header("Customers")
    col1, col2 = st.columns([2, 3])

    with col1:
        st.subheader("Create new customer")
        with st.form("create_customer_form"):
            cname = st.text_input("Name")
            cemail = st.text_input("Email")
            cphone = st.text_input("Phone")
            caddress = st.text_area("Address", height=80)
            submitted = st.form_submit_button("Create")
            if submitted:
                try:
                    new_id = create_customer(cname, cemail, cphone, caddress)
                    st.success(f"Customer created (id={new_id})")
                except Exception as e:
                    st.error("Create failed: " + str(e))

    with col2:
        st.subheader("Customers list")
        try:
            customers_df = list_customers()
            if customers_df.empty:
                st.info("No customers yet.")
            else:
                # Show table
                st.dataframe(customers_df)
        except Exception as e:
            st.error("Failed to load customers: " + str(e))

    st.markdown("---")
    st.subheader("Edit or Delete Customer")
    try:
        customers_df = list_customers()
        choices = customers_df["id"].tolist() if not customers_df.empty else []
        selected_id = st.selectbox("Select customer to edit", options=[""] + choices)
        if selected_id:
            row = customers_df[customers_df["id"] == selected_id].iloc[0]
            with st.form("edit_customer_form"):
                ename = st.text_input("Name", value=row.get("name", ""))
                eemail = st.text_input("Email", value=row.get("email", ""))
                ephone = st.text_input("Phone", value=row.get("phone", ""))
                eaddress = st.text_area("Address", value=row.get("address", ""), height=80)
                btn_update = st.form_submit_button("Update")
                btn_delete = st.form_submit_button("Delete", type="secondary")
                if btn_update:
                    try:
                        update_customer(selected_id, ename, eemail, ephone, eaddress)
                        st.success("Customer updated.")
                    except Exception as e:
                        st.error("Update failed: " + str(e))
                if btn_delete:
                    try:
                        delete_customer(selected_id)
                        st.success("Customer deleted.")
                    except Exception as e:
                        st.error("Delete failed: " + str(e))
    except Exception as e:
        st.error("Error in edit section: " + str(e))

# --------------
# Products tab
# --------------
with tabs[1]:
    st.header("Products")
    col1, col2 = st.columns([2, 3])

    with col1:
        st.subheader("Create new product")
        with st.form("create_product_form"):
            pname = st.text_input("Name")
            pdesc = st.text_area("Description", height=80)
            pprice = st.number_input("Price", min_value=0.0, format="%.2f")
            pstock = st.number_input("Stock", min_value=0, step=1)
            submitted = st.form_submit_button("Create")
            if submitted:
                try:
                    pid = create_product(pname, pdesc, float(pprice), int(pstock))
                    st.success(f"Product created (id={pid})")
                except Exception as e:
                    st.error("Create product failed: " + str(e))

    with col2:
        st.subheader("Products list")
        try:
            products_df = list_products()
            if products_df.empty:
                st.info("No products yet.")
            else:
                st.dataframe(products_df)
        except Exception as e:
            st.error("Failed to load products: " + str(e))

    st.markdown("---")
    st.subheader("Edit or Delete Product")
    try:
        products_df = list_products()
        choices = products_df["id"].tolist() if not products_df.empty else []
        selected_pid = st.selectbox("Select product to edit", options=[""] + choices)
        if selected_pid:
            row = products_df[products_df["id"] == selected_pid].iloc[0]
            with st.form("edit_product_form"):
                ename = st.text_input("Name", value=row.get("name", ""))
                edesc = st.text_area("Description", value=row.get("description", ""), height=80)
                eprice = st.number_input("Price", value=float(row.get("price") or 0.0), format="%.2f")
                estock = st.number_input("Stock", value=int(row.get("stock") or 0), step=1)
                btn_update = st.form_submit_button("Update")
                btn_delete = st.form_submit_button("Delete", type="secondary")
                if btn_update:
                    try:
                        update_product(selected_pid, ename, edesc, float(eprice), int(estock))
                        st.success("Product updated.")
                    except Exception as e:
                        st.error("Update failed: " + str(e))
                if btn_delete:
                    try:
                        delete_product(selected_pid)
                        st.success("Product deleted.")
                    except Exception as e:
                        st.error("Delete failed: " + str(e))
    except Exception as e:
        st.error("Error in edit product section: " + str(e))

# --------------
# Orders tab
# --------------
with tabs[2]:
    st.header("Orders")
    col1, col2 = st.columns([2, 3])

    with col1:
        st.subheader("Create new order")
        try:
            customers_df = list_customers()
            products_df = list_products()

            customer_options = customers_df.apply(lambda r: (r["id"], r.get("name", "")), axis=1).tolist() if not customers_df.empty else []
            product_options = products_df.apply(lambda r: (r["id"], r.get("name", "")), axis=1).tolist() if not products_df.empty else []

            with st.form("create_order_form"):
                cust_choice = st.selectbox("Customer", options=[""] + [f"{id} | {name}" for id, name in customer_options])
                prod_choice = st.selectbox("Product", options=[""] + [f"{id} | {name}" for id, name in product_options])
                quantity = st.number_input("Quantity", min_value=1, step=1, value=1)
                # Calculate price preview
                total_preview = 0.0
                if prod_choice:
                    sel_pid = prod_choice.split(" | ")[0]
                    prod_row = products_df[products_df["id"] == sel_pid].iloc[0]
                    price = float(prod_row.get("price") or 0.0)
                    total_preview = price * int(quantity)
                st.markdown(f"**Total (preview):** {total_preview:.2f}")
                submitted = st.form_submit_button("Create Order")
                if submitted:
                    if not cust_choice or not prod_choice:
                        st.error("Select both a customer and a product.")
                    else:
                        sel_cid = cust_choice.split(" | ")[0]
                        sel_pid = prod_choice.split(" | ")[0]
                        try:
                            oid = create_order(sel_cid, sel_pid, int(quantity), float(total_preview))
                            st.success(f"Order created (id={oid})")
                        except Exception as e:
                            st.error("Create order failed: " + str(e))
        except Exception as e:
            st.error("Could not load customers/products for orders: " + str(e))

    with col2:
        st.subheader("Orders list")
        try:
            orders_df = list_orders()
            if orders_df.empty:
                st.info("No orders yet.")
            else:
                st.dataframe(orders_df)
        except Exception as e:
            st.error("Failed to load orders: " + str(e))

    st.markdown("---")
    st.subheader("Edit or Delete Order")
    try:
        orders_df = list_orders()
        customers_df = list_customers()
        products_df = list_products()
        order_choices = orders_df["id"].tolist() if not orders_df.empty else []
        selected_oid = st.selectbox("Select order to edit", options=[""] + order_choices)
        if selected_oid:
            row = orders_df[orders_df["id"] == selected_oid].iloc[0]
            with st.form("edit_order_form"):
                # Show customers & products dropdowns with ids
                cust_options = customers_df.apply(lambda r: (r["id"], r.get("name", "")), axis=1).tolist() if not customers_df.empty else []
                prod_options = products_df.apply(lambda r: (r["id"], r.get("name", "")), axis=1).tolist() if not products_df.empty else []

                selected_customer = st.selectbox("Customer", options=[f"{id} | {name}" for id, name in cust_options], index=0 if cust_options else -1)
                selected_product = st.selectbox("Product", options=[f"{id} | {name}" for id, name in prod_options], index=0 if prod_options else -1)
                quantity = st.number_input("Quantity", min_value=1, step=1, value=int(row.get("quantity") or 1))
                total_amount = st.number_input("Total amount", min_value=0.0, format="%.2f", value=float(row.get("total_amount") or 0.0))
                btn_update = st.form_submit_button("Update")
                btn_delete = st.form_submit_button("Delete", type="secondary")
                if btn_update:
                    try:
                        cid = selected_customer.split(" | ")[0]
                        pid = selected_product.split(" | ")[0]
                        update_order(selected_oid, cid, pid, int(quantity), float(total_amount))
                        st.success("Order updated.")
                    except Exception as e:
                        st.error("Update order failed: " + str(e))
                if btn_delete:
                    try:
                        delete_order(selected_oid)
                        st.success("Order deleted.")
                    except Exception as e:
                        st.error("Delete order failed: " + str(e))
    except Exception as e:
        st.error("Error in edit order section: " + str(e))

# Footer / debug
st.markdown("---")
st.caption("Mini Store app — every table has CREATED_DATE (set on insert) and LAST_UPDATE_DATE (set on update).")

