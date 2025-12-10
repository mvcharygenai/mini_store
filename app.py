# app.py
import streamlit as st
import pandas as pd
from database import SessionLocal, Customer, Product, Order, init_db

st.set_page_config(page_title="Mini Store (SQL Server)", layout="wide")

# Initialize database and session
init_db()
session = SessionLocal()

st.title("🛍️ Mini Store — Customers · Products · Orders")

menu = ["Customers", "Products", "Orders"]
choice = st.sidebar.radio("Menu", menu)

def refresh_session():
    """Refreshes SQLAlchemy session."""
    global session
    session.close()
    session = SessionLocal()

# ----------------------------------------
# CUSTOMERS PAGE
# ----------------------------------------
if choice == "Customers":
    st.header("👤 Customers")

    # Add new customer
    with st.expander("Add New Customer"):
        name = st.text_input("Customer Name", key="new_cust_name")
        email = st.text_input("Email", key="new_cust_email")

        if st.button("Add Customer", key="add_customer"):
            if not name.strip():
                st.error("Name is required.")
            else:
                c = Customer(name=name.strip(), email=email.strip() or None)
                session.add(c)
                session.commit()
                st.success("Customer added successfully!")
                refresh_session()

    # Customer list
    st.subheader("Customer List")
    customers = session.query(Customer).order_by(Customer.id).all()
    df = pd.DataFrame([{"ID": c.id, "Name": c.name, "Email": c.email or ""} for c in customers])
    st.dataframe(df, use_container_width=True)

    # Edit/Delete
    st.markdown("---")
    st.subheader("Edit / Delete Customer")

    cust_map = {f"{c.id} — {c.name}": c.id for c in customers}

    if cust_map:
        selected = st.selectbox("Choose customer", [""] + list(cust_map.keys()))

        if selected:
            cid = cust_map[selected]
            c = session.query(Customer).get(cid)

            col1, col2 = st.columns(2)
            with col1:
                new_name = st.text_input("Name", value=c.name)
            with col2:
                new_email = st.text_input("Email", value=c.email or "")

            if st.button("Save Changes"):
                c.name = new_name.strip() or c.name
                c.email = new_email.strip() or None
                session.commit()
                st.success("Customer updated!")
                refresh_session()

            if st.button("Delete Customer"):
                # Delete all orders for this customer
                session.query(Order).filter(Order.customer_id == cid).delete()
                session.delete(c)
                session.commit()
                st.success("Customer deleted!")
                refresh_session()
    else:
        st.info("No customers found.")

# ----------------------------------------
# PRODUCTS PAGE
# ----------------------------------------
elif choice == "Products":
    st.header("📦 Products")

    # Add new product
    with st.expander("Add New Product"):
        name = st.text_input("Product Name", key="new_prod_name")
        price = st.number_input("Price", min_value=0.0, format="%.2f", key="new_prod_price")

        if st.button("Add Product", key="add_product"):
            if not name.strip():
                st.error("Product name is required.")
            else:
                p = Product(name=name.strip(), price=float(price))
                session.add(p)
                session.commit()
                st.success("Product added successfully!")
                refresh_session()

    # Product list
    st.subheader("Product List")
    products = session.query(Product).order_by(Product.id).all()
    df = pd.DataFrame([{"ID": p.id, "Name": p.name, "Price": p.price} for p in products])
    st.dataframe(df, use_container_width=True)

    # Edit/Delete
    st.markdown("---")
    st.subheader("Edit / Delete Product")

    prod_map = {f"{p.id} — {p.name}": p.id for p in products}

    if prod_map:
        selected = st.selectbox("Choose product", [""] + list(prod_map.keys()))

        if selected:
            pid = prod_map[selected]
            p = session.query(Product).get(pid)

            col1, col2 = st.columns(2)
            with col1:
                new_name = st.text_input("Name", value=p.name)
            with col2:
                new_price = st.number_input("Price", value=float(p.price), min_value=0.0, format="%.2f")

            if st.button("Save Product"):
                p.name = new_name.strip()
                p.price = float(new_price)
                session.commit()
                st.success("Product updated!")
                refresh_session()

            if st.button("Delete Product"):
                # Delete orders referencing product
                session.query(Order).filter(Order.product_id == pid).delete()
                session.delete(p)
                session.commit()
                st.success("Product deleted!")
                refresh_session()
    else:
        st.info("No products found.")

# ----------------------------------------
# ORDERS PAGE
# ----------------------------------------
elif choice == "Orders":
    st.header("🧾 Orders")

    customers = session.query(Customer).order_by(Customer.id).all()
    products = session.query(Product).order_by(Product.id).all()

    if not customers or not products:
        st.warning("Please add at least one customer and one product before placing orders.")
    else:
        with st.expander("Place New Order"):
            cust_map = {f"{c.id} — {c.name}": c.id for c in customers}
            prod_map = {f"{p.id} — {p.name}": p.id for p in products}

            selected_cust = st.selectbox("Customer", list(cust_map.keys()))
            selected_prod = st.selectbox("Product", list(prod_map.keys()))
            quantity = st.number_input("Quantity", min_value=1, value=1)

            if st.button("Place Order"):
                order = Order(
                    customer_id=cust_map[selected_cust],
                    product_id=prod_map[selected_prod],
                    quantity=int(quantity)
                )
                session.add(order)
                session.commit()
                st.success("Order placed successfully!")
                refresh_session()

    # Order list
    st.subheader("Order List")
    orders = session.query(Order).order_by(Order.id).all()
    df = pd.DataFrame([
        {"ID": o.id, "Customer": o.customer.name, "Product": o.product.name, "Quantity": o.quantity}
        for o in orders
    ])
    st.dataframe(df, use_container_width=True)

    # Edit/Delete
    st.markdown("---")    
    st.subheader("Edit / Delete Order")

    order_map = {f"{o.id} — {o.customer.name} — {o.product.name}": o.id for o in orders}

    if order_map:
        selected = st.selectbox("Choose order", [""] + list(order_map.keys()))

        if selected:
            oid = order_map[selected]
            o = session.query(Order).get(oid)

            cust_map = {f"{c.id} — {c.name}": c.id for c in customers}
            prod_map = {f"{p.id} — {p.name}": p.id for p in products}

            col1, col2 = st.columns(2)
            with col1:
                new_cust = st.selectbox("Customer", list(cust_map.keys()),
                                        index=list(cust_map.values()).index(o.customer_id))
            with col2:
                new_prod = st.selectbox("Product", list(prod_map.keys()),
                                        index=list(prod_map.values()).index(o.product_id))

            new_qty = st.number_input("Quantity", min_value=1, value=o.quantity)

            if st.button("Save Order"):
                o.customer_id = cust_map[new_cust]
                o.product_id = prod_map[new_prod]
                o.quantity = int(new_qty)
                session.commit()
                st.success("Order updated!")
                refresh_session()

            if st.button("Delete Order"):
                session.delete(o)
                session.commit()
                st.success("Order deleted!")
                refresh_session()
    else:
        st.info("No orders found.")
