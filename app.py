# app.py
import streamlit as st
import pandas as pd
from database import SessionLocal, Customer, Product, Order, init_db
from sqlalchemy.exc import IntegrityError

st.set_page_config(page_title="Mini Store (SQL Server)", layout="wide")
init_db()
session = SessionLocal()

st.title("🛍️ Mini Store — Customers · Products · Orders")

menu = ["Customers", "Products", "Orders"]
choice = st.sidebar.radio("Menu", menu)

def refresh_session():
    global session
    session.close()
    session = SessionLocal()

# ---------------- Customers ----------------
if choice == "Customers":
    st.header("👤 Customers")

    with st.expander("Add new customer"):
        name = st.text_input("Customer Name", key="new_cust_name")
        email = st.text_input("Email", key="new_cust_email")
        if st.button("Add Customer", key="add_customer"):
            if not name.strip():
                st.error("Name is required")
            else:
                c = Customer(name=name.strip(), email=email.strip() or None)
                session.add(c)
                session.commit()
                st.success("Customer added")
                refresh_session()

    st.subheader("Existing customers")
    customers = session.query(Customer).order_by(Customer.id).all()
    df = pd.DataFrame([{"ID": c.id, "Name": c.name, "Email": c.email or ""} for c in customers])
    st.dataframe(df)

    st.markdown("---")
    st.subheader("Edit / Delete customer")
    cust_map = {f"{c.id} — {c.name}": c.id for c in customers}
    if cust_map:
        choice_key = st.selectbox("Choose customer", [""] + list(cust_map.keys()), key="edit_cust_select")
        if choice_key:
            cid = cust_map[choice_key]
            c = session.query(Customer).get(cid)
            col1, col2 = st.columns(2)
            with col1:
                new_name = st.text_input("Name", value=c.name, key=f"cust_name_{cid}")
            with col2:
                new_email = st.text_input("Email", value=c.email or "", key=f"cust_email_{cid}")
            if st.button("Save changes", key=f"save_cust_{cid}"):
                c.name = new_name.strip() or c.name
                c.email = new_email.strip() or None
                session.commit()
                st.success("Customer updated")
                refresh_session()
            if st.button("Delete customer", key=f"delete_cust_{cid}"):
                # optional: check if orders exist and block or cascade
                orders = session.query(Order).filter(Order.customer_id == cid).count()
                if orders:
                    if st.confirmation_dialog := False:
                        pass
                    # here we delete orders too for simplicity - alternative: prevent deletion
                    session.query(Order).filter(Order.customer_id == cid).delete()
                session.delete(c)
                session.commit()
                st.success("Customer deleted")
                refresh_session()
    else:
        st.info("No customers yet. Add one above.")

# ---------------- Products ----------------
elif choice == "Products":
    st.header("📦 Products")

    with st.expander("Add new product"):
        name = st.text_input("Product Name", key="new_prod_name")
        price = st.number_input("Price", min_value=0.0, format="%.2f", key="new_prod_price")
        if st.button("Add Product", key="add_product"):
            if not name.strip():
                st.error("Product name required")
            else:
                p = Product(name=name.strip(), price=float(price))
                session.add(p)
                session.commit()
                st.success("Product added")
                refresh_session()

    st.subheader("Product List")
    products = session.query(Product).order_by(Product.id).all()
    df = pd.DataFrame([{"ID": p.id, "Name": p.name, "Price": p.price} for p in products])
    st.dataframe(df)

    st.markdown("---")
    st.subheader("Edit / Delete product")
    prod_map = {f"{p.id} — {p.name}": p.id for p in products}
    if prod_map:
        choice_key = st.selectbox("Choose product", [""] + list(prod_map.keys()), key="edit_prod_select")
        if choice_key:
            pid = prod_map[choice_key]
            p = session.query(Product).get(pid)
            col1, col2 = st.columns(2)
            with col1:
                new_name = st.text_input("Name", value=p.name, key=f"prod_name_{pid}")
            with col2:
                new_price = st.number_input("Price", value=float(p.price), min_value=0.0, format="%.2f", key=f"prod_price_{pid}")
            if st.button("Save changes", key=f"save_prod_{pid}"):
                p.name = new_name.strip() or p.name
                p.price = float(new_price)
                session.commit()
                st.success("Product updated")
                refresh_session()
            if st.button("Delete product", key=f"delete_prod_{pid}"):
                # delete orders referencing it (or prevent deletion)
                session.query(Order).filter(Order.product_id == pid).delete()
                session.delete(p)
                session.commit()
                st.success("Product deleted")
                refresh_session()
    else:
        st.info("No products yet. Add one above.")

# ---------------- Orders ----------------
elif choice == "Orders":
    st.header("🧾 Orders")

    customers = session.query(Customer).order_by(Customer.id).all()
    products = session.query(Product).order_by(Product.id).all()

    if not customers or not products:
        st.warning("Add at least one customer and one product before placing orders.")
    else:
        with st.expander("Place new order"):
            cust_map = {f"{c.id} — {c.name}": c.id for c in customers}
            prod_map = {f"{p.id} — {p.name}": p.id for p in products}
            selected_c = st.selectbox("Customer", list(cust_map.keys()), key="new_order_cust")
            selected_p = st.selectbox("Product", list(prod_map.keys()), key="new_order_prod")
            quantity = st.number_input("Quantity", min_value=1, value=1, key="new_order_qty")
            if st.button("Place Order", key="place_order"):
                order = Order(
                    customer_id=cust_map[selected_c],
                    product_id=prod_map[selected_p],
                    quantity=int(quantity)
                )
                session.add(order)
                session.commit()
                st.success("Order placed")
                refresh_session()

    st.subheader("Order List")
    orders = session.query(Order).order_by(Order.id).all()
    df = pd.DataFrame([{
        "ID": o.id,
        "Customer": o.customer.name,
        "Product": o.product.name,
        "Quantity": o.quantity
    } for o in orders])
    st.dataframe(df)

    st.markdown("---")
    st.subheader("Edit / Delete order")
    order_map = {f"{o.id} — {o.customer.name} — {o.product.name}": o.id for o in orders}
    if order_map:
        choice_key = st.selectbox("Choose order", [""] + list(order_map.keys()), key="edit_order_select")
        if choice_key:
            oid = order_map[choice_key]
            o = session.query(Order).get(oid)
            col1, col2 = st.columns(2)
            with col1:
                new_qty = st.number_input("Quantity", min_value=1, value=int(o.quantity), key=f"order_qty_{oid}")
            with col2:
                # allow swapping product/customer
                cust_options = {f"{c.id} — {c.name}": c.id for c in customers}
                prod_options = {f"{p.id} — {p.name}": p.id for p in products}
                new_c = st.selectbox("Customer", list(cust_options.keys()), index=list(cust_options.values()).index(o.customer_id), key=f"order_cust_{oid}")
                new_p = st.selectbox("Product", list(prod_options.keys()), index=list(prod_options.values()).index(o.product_id), key=f"order_prod_{oid}")
            if st.button("Save changes", key=f"save_order_{oid}"):
                o.quantity = int(new_qty)
                o.customer_id = cust_options[new_c]
                o.product_id = prod_options[new_p]
                session.commit()
                st.success("Order updated")
                refresh_session()
            if st.button("Delete order", key=f"delete_order_{oid}"):
                session.delete(o)
                session.commit()
                st.success("Order deleted")
                refresh_session()
    else:
        st.info("No orders yet.")
