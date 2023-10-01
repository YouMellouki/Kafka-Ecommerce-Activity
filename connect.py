import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import seaborn as sns



# Connect to the database
conn = psycopg2.connect(
    dbname='ecommerce_db',
    user='postgres',
    password='1234',
    host='localhost',
    port='5432'
)

db_uri = 'postgresql://postgres:1234@localhost:5432/ecommerce_db'
# Create a SQLAlchemy engine using the URI
engine = create_engine(db_uri)


# Querying data into a DataFrame
query = "SELECT * FROM activity_log;"
df = pd.read_sql_query(query, engine)
engine.dispose()

# Group by timestamp (you might need to adjust this based on your timestamp granularity)
activity_trends = df.groupby('timestamp').activity_type.value_counts().unstack().fillna(0)

activity_trends.plot(figsize=(15, 7))

most_viewed_products = df[df.activity_type == "product_view"].product_id.value_counts()

most_viewed_products.plot(kind='bar', figsize=(15, 7))

cart_additions = df[df.activity_type == "cart_addition"].shape[0]
purchases = df[df.activity_type == "purchase"].shape[0]

cart_abandonment_rate = (cart_additions - purchases) / cart_additions

print(f"Cart Abandonment Rate: {cart_abandonment_rate:.2%}")

total_views = df[df.activity_type == "product_view"].shape[0]

conversion_rate = purchases / total_views

print(f"Conversion Rate: {conversion_rate:.2%}")


# Example: Heatmap of user activity
activity_pivot = df.pivot_table(index='product_id', columns='activity_type', aggfunc='size', fill_value=0)
sns.heatmap(activity_pivot, annot=True, cmap="YlGnBu")