import sys
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import loader


def load_customer_data():
    try:
        print('Started processing customer records')
        # Call the API
        user_url = 'https://dummyjson.com/users'
        c_response = requests.get(user_url).json()

        customer_df = pd.json_normalize(c_response['users'])
        

        # filter record
        customer_df = customer_df[['id', 'firstName',
                'lastName', 'age', 'gender', 'email', 'phone', 'username', 'company.address.city',
                'company.address.state', 'company.address.stateCode', 'company.address.country']]
        
        # Rename Columns
        col_names = ['id', 'first_name', 'last_name', 'age', 'gender', 'email',
                    'phone', 'username', 'city', 'state', 'state_code', 'country']
        customer_df.columns = col_names

        customer_df['loading_datetime'] = datetime.now()

        print(customer_df.head(2))

        loader.write_to_db(data_df=customer_df, table_name='customer_details_raw'
                        ,mode='replace',schema='bronze_layer')
        print('Successfully saved customer record to DB')

    except Exception as err:
        print(f'Failed loading customer record with Error : {err}')
        raise




def load_cart_sales():
    try:
        print('Started processing cart records')
        # Call the API
        url = 'https://dummyjson.com/carts'
        response = requests.get(url).json()

        # Normalize nested structure into flat DataFrame
        df = pd.json_normalize(response['carts'])

        product_df = pd.json_normalize(
            response['carts'],
            record_path='products',
            meta=['id', 'userId', 'total'],
            meta_prefix='cart_'
        )
        product_df['loading_datetime'] = datetime.now()

        product_df.drop('thumbnail', axis=1, inplace=True)

        loader.write_to_db(data_df=product_df, table_name='carts_record_raw'
                        ,mode='replace',schema='bronze_layer')
        print('Successfully saved cart record to DB')
    except Exception as err:
        print(f'Failed loading cart record with Error : {err}')
        raise


def load_bronze():
    """Load already flattened bronze tables"""

    customers_query = "SELECT * FROM bronze_layer.customer_details_raw"
    carts_query = "SELECT * FROM bronze_layer.carts_record_raw"

    customers_df = loader.read_df(query=customers_query)
    carts_df = loader.read_df(carts_query)

    return customers_df, carts_df


def transform_to_silver():
    customers_df, carts_df = load_bronze()
    customers_clean = customers_df.copy()

    customers_clean = customers_clean.drop_duplicates(subset=["id"])

    customers_clean = customers_clean.rename(columns={
        "id": "userId"})


    customers_clean["first_name"] = customers_clean["first_name"].fillna("")
    customers_clean["last_name"] = customers_clean["last_name"].fillna("")

    print(customers_clean.head(3))

    carts_clean = carts_df.copy()

    carts_clean = carts_clean.drop_duplicates()

    carts_clean = carts_clean.rename(columns={"cart_id": "cart_id",
        "cart_userId": "userId",
        "cart_total": "cart_total",
        "id": "product_id",
        "title": "product_name",
        "price": "product_price",
        "quantity": "product_quantity",
        "total": "product_total"})

    # Handle nulls
    carts_clean = carts_clean.dropna(subset=["userId", "product_name"])

    # Fix datatypes
    numeric_cols = ["product_price", "product_quantity", "product_total", "cart_total"]
    for col in numeric_cols:
        if col in carts_clean.columns:
            carts_clean[col] = pd.to_numeric(carts_clean[col], errors="coerce")


    silver_df = carts_clean.merge(customers_clean,on="userId",how="left")

    silver_df["customer_name"] = (silver_df["first_name"] + " " + silver_df["last_name"]).str.strip()


    silver_df["unit_price"] = (silver_df["product_total"] /
        silver_df["product_quantity"].replace(0, pd.NA))

    silver_df = silver_df.drop_duplicates()

    silver_df = silver_df[["cart_id", "userId", "customer_name",
            "product_id", "product_name",
            "product_quantity", "product_price", "product_total",
            "cart_total","email", "phone", "city", "state", "country"]]
    
    loader.write_to_db(data_df=silver_df,table_name='cart_user_enriched'
                       ,mode='replace',schema='silver_layer')
    print('done')

def load_gold():
    t = 'silver_layer.cart_user_enriched'
    queries = {
        # Who are our best customers?
        'customer_sales_summary': f'''
            SELECT "userId", customer_name, city, country,
                   COUNT(DISTINCT cart_id) AS total_carts,
                   SUM(product_quantity)::int AS total_items,
                   ROUND(SUM(product_total)::numeric, 2) AS total_spent
            FROM {t}
            GROUP BY "userId", customer_name, city, country
            ORDER BY total_spent DESC''',
        # Which products sell most?
        'product_performance': f'''
            SELECT product_id, product_name,
                   SUM(product_quantity)::int AS units_sold,
                   ROUND(SUM(product_total)::numeric, 2) AS revenue,
                   COUNT(DISTINCT cart_id) AS carts_containing
            FROM {t}
            GROUP BY product_id, product_name
            ORDER BY revenue DESC''',
        # Where does revenue come from?
        'sales_by_location': f'''
            SELECT country, state,
                   COUNT(DISTINCT "userId") AS customers,
                   COUNT(DISTINCT cart_id) AS carts,
                   ROUND(SUM(product_total)::numeric, 2) AS revenue
            FROM {t}
            GROUP BY country, state
            ORDER BY revenue DESC''',
        # How big is a typical cart?
        'cart_summary': f'''
            SELECT cart_id, "userId",
                   COUNT(*) AS different_products,
                   SUM(product_quantity)::int AS total_items,
                   ROUND(SUM(product_total)::numeric, 2) AS cart_value
            FROM {t}
            GROUP BY cart_id, "userId"
            ORDER BY cart_value DESC''',
    }
    try:
        for name, q in queries.items():
            df = loader.read_df(q)
            loader.write_to_db(data_df=df, table_name=name,
                               mode='replace', schema='gold_layer')
            print(f'Gold table done: {name}')
    except Exception as err:
        print(f'Failed gold layer with Error : {err}')
        raise

if __name__ == '__main__':
    run_module = sys.argv[1]
    if run_module == 'cust':
        load_customer_data()
    elif run_module == 'cart':
        load_cart_sales()
    elif run_module == 'enrich':
        transform_to_silver()
    elif run_module == 'gold':
        load_gold()