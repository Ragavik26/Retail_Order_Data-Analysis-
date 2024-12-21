# subprocess
import subprocess

# Run the Kaggle command
subprocess.run(
    ["kaggle", "datasets", "download", "ankitbansal06/retail-orders", "-f", "orders.csv"],
    check=True,
)

# Extract data from zipped file
import zipfile

unzipped_folder = "C:\\Users\\ADMIN\\Desktop\\project 1" # output destination path

with zipfile.ZipFile("orders.csv.zip", 'r') as zip_ref:
    zip_ref.extractall(unzipped_folder)
    print("File unzipped successfully!")

#Read file & trimming space
import pandas as pd
odf = pd.read_csv("orders.csv", skipinitialspace=True)

# Check duplicate values
print(odf.drop_duplicates()) # drops duplicate row 

#renaming columns
odf.columns = odf.columns.str.lower()
odf.columns = odf.columns.str.replace (' ','_')

# dropping rows with '0'
odf = pd.read_csv("orders.csv",na_values=['0']) #converting missing 0 values to NaN
odf.dropna(how='any',inplace=True) # drops rows with NaN

# handling missing values
odf = odf.replace(['Not Available','unknown'], "NaN")
#print(odf.head(40))

# New columns for discount, sale price, profit
odf.columns = odf.columns.str.lower().str.replace (' ','_') # due to column name error

odf['discount'] = odf["list_price"]*odf["discount_percent"] /100 # for discount
odf['sale_price'] = odf["list_price"]-odf["discount"] # for sales
odf['profit'] = odf['sale_price']-odf['cost_price'] # for profit
print(odf.columns.tolist())

#split table into two tables
primary_key = "order_id"

table1_column = [primary_key,'order_date', 'ship_mode', 'segment', 'country', 'city','state', 'postal_code', 'region']
table2_column = [primary_key,'category', 'sub_category','product_id', 'cost_price', 'list_price', 'quantity','discount_percent','discount','sale_price','profit']

order_data_1 = odf[table1_column]
order_data_2 = odf[table2_column]

order_data_1.to_csv('order_data_1.csv',index=False)
order_data_2.to_csv('order_data_2.csv',index=False)

odf1 = pd.read_csv("order_data_1.csv")
odf2 = pd.read_csv("order_data_2.csv")

# load tables into sql
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

host = "localhost"
user = "postgres"
database = "project_1"
# Database connection settings from environment variables
password = os.getenv("DB_PASSWORD")


engine = create_engine(f"postgresql://{user}:{password}@{host}/{database}")

odf1.to_sql('orders_data1',engine,if_exists="append",index=False)
odf2.to_sql('orders_data2',engine,if_exists="append",index=False)

print("sucessfully inserted")




