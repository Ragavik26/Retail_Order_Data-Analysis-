import streamlit as st
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Database connection settings from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
print(DB_PORT)

st.markdown(
    """
    <style>
    body {
        background-color: #121212;
        color: white;
    }
    .block-container {
        background-color: #121212;
        color: white;
    }
    .css-1aumxhk {
        color: white;
    }
    .stTextInput input {
        background-color: #333;
        color: white;
    }
    .stButton>button {
        background-color: grey;
        color: white;
        border-radius: 4px;
    }
    .stSelectbox>div {
        background-color: #333;
        color: white;
    }
    .stDataFrame {
        color: white;
        background-color: #333;
    }
    .stTitle {
        color: white;
    }
    .stSubheader {
        color: white;
    }
    
    .stSelectbox{
      color: white;
    }
    .stText {
        color: white;
    }
    .stCode {
        color: #03DAC6;
    }
    .stSlider>div {
        background-color: #333;
    }
    </style>
    """,
    unsafe_allow_html=True
)


# List of SQL queries
queries = [
    "1. Find top 10 highest revenue generating products",
    "2. Find the top 5 cities with the highest profit margins",
    "3. Calculate the total discount given for each category",
    "4. Find the average sale price per product category",
    "5. Find the region with the highest average sale price",
    "6. Find the total profit per category",
    "7. Identify the top 3 segments with the highest quantity of orders",
    "8. Determine the average discount percentage given per region",
    "9. Find the product category with the highest total profit",
    "10. Calculate the total revenue generated per year",
    "11. Calculate the total revenue generated each month",
    "12. Identify products with discounts greater than 20%",
    "13. Top-Selling Products",
    "14. Monthly Sales Analysis",
    "15. Region having more than 3000 quantity of sold products",
    "16. Find the 5 product categories with the highest discount",
    "17. Number of orders in each region",
    "18. Find the ship mode used based on subcategory",
    "19. Number of orders per day",
    "20. 5 high-selling products in each segment",
]

query_sql = [
    ''' 
    select product_id, sum(sale_price) as top_sale 
    from data2 
    group by product_id 
    order by top_sale desc 
    limit 10;
    ''',

    '''
    select data1.city, sum(data2.profit) as profit_margin 
    from data1 
    inner join data2 on data1.order_id = data2.order_id 
    group by data1.city 
    order by profit_margin desc 
    limit 5;
    ''',

    '''
    select category, sum(discount) as total_discount 
    from data2 
    group by data2.category 
    order by total_discount desc;
    ''',

    '''
    select sub_category, avg(sale_price) as avg_sc 
    from data2 
    group by data2.sub_category 
    order by avg_sc asc;
    ''',

    '''
    select data1.region, avg(data2.sale_price) as avg_reg 
    from data1 
    inner join data2 on data1.order_id = data2.order_id 
    group by data1.region 
    order by avg_reg desc 
    limit 1;
    ''',

    '''
    select category, sum(profit) as total_cp 
    from data2 
    group by data2.category 
    order by total_cp desc;
    ''',

    '''
    select data1.segment, sum(quantity) as total_qty
    from data1
    inner join data2 on data1.order_id = data2.order_id
    group by data1.segment 
    order by total_qty desc
    limit 3;
    ''',

    '''
    select data1.region, avg(data2.discount_percent) as region_dp 
    from data1 
    inner join data2 on data1.order_id = data2.order_id 
    group by data1.region 
    order by region_dp desc;
    ''',

    '''
    select sub_category, sum(profit) as total_scp 
    from data2 
    group by data2.sub_category 
    order by total_scp desc 
    limit 1;
    ''',

    '''
    select date_part('year', order_date) as year, sum(data2.profit) as total_pr 
    from data1 
    inner join data2 on data1.order_id = data2.order_id 
    group by year 
    order by total_pr asc;
    ''',

    '''
    select date_part('month', order_date) as month, sum(data2.profit) as total_pr 
    from data1 
    inner join data2 on data1.order_id = data2.order_id 
    group by month 
    order by month asc;
    ''',

    '''
    select discount_percent, 
           case 
               when discount_percent < 20 then 'low impact' 
               when discount_percent > 20 then 'high impact' 
               else 'No impact on growth' 
           end as impact_on_sales 
    from data2;
    ''',

    '''
    select product_id, sum(sale_price) as top_revenue 
    from data2 
    group by product_id 
    order by top_revenue desc;
    ''',

    '''
    select date_part('year', order_date) as year, 
           date_part('month', order_date) as month, 
           sum(data2.profit) as total_pr 
    from data1 
    inner join data2 on data1.order_id = data2.order_id 
    group by year, month 
    order by year, month asc;
    ''',

    '''
    select data1.region, count(data2.quantity) as reg_qty 
    from data1 
    inner join data2 on data1.order_id = data2.order_id 
    group by data1.region 
    having count(quantity) > 3000;
    ''',

    '''
    select sub_category, sum(discount) as total_discount, sum(list_price) as total_list_price 
    from data2 
    group by data2.sub_category 
    order by total_discount desc 
    limit 5;
    ''',

    '''
    select region, count(distinct order_id) as orders_region 
    from data1 
    group by region;
    ''',

    '''
    select sub_category, count(distinct data1.ship_mode) 
    from data2 
    inner join data1 on data1.order_id = data2.order_id 
    group by sub_category;
    ''',

    '''
    select order_date, count(order_id) as total_orders 
    from data1 
    group by order_date;
    ''',

    '''
    with cte as (
        select data1.segment, data2.product_id, sum(data2.sale_price) as sales
        from data1 
        inner join data2 on data1.order_id = data2.order_id 
        group by data1.segment, data2.product_id
    )
    select * 
    from (
        select *, row_number() over(partition by segment order by sales desc) as rn
        from cte
    ) a
    where rn <= 5;
    '''
]


# Streamlit UI
st.title("Retail Order Data Analysis")

def plot_histogram(df, column_name, title):
    """Helper function to plot a histogram."""
    plt.figure(figsize=(10, 6))
    sns.histplot(df[column_name], kde=True, bins=30, color='skyblue')
    plt.title(f"Histogram of {column_name}")
    plt.xlabel(column_name)
    plt.ylabel("Frequency")

    # Rotate the x-axis labels to prevent overlapping
    plt.xticks(rotation=90)

    st.pyplot(plt)

def plot_bar_chart(df, x_column, y_column, title):
    """Helper function to plot a bar chart."""
    plt.figure(figsize=(10, 6))
    sns.barplot(x=df[x_column], y=df[y_column], palette="viridis", ci=None)
    plt.title(title)
    plt.xlabel(x_column)
    plt.ylabel(y_column)

    # Rotate the x-axis labels to prevent overlapping
    plt.xticks(rotation=90)
    
    st.pyplot(plt)

def plot_line_chart(df, x_column, y_column, title):
    """Helper function to plot a line chart."""
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x=x_column, y=y_column)
    plt.title(title)
    plt.xlabel(x_column)
    plt.ylabel(y_column)

    # Rotate the x-axis labels to prevent overlapping
    plt.xticks(rotation=90)

    st.pyplot(plt)

# Visualization functions for each query (1-20)
def visualize_query_1(df):
    plot_bar_chart(df, 'product_id', 'top_sale', "Top 10 Highest Revenue Generating Products")

def visualize_query_2(df):
    plot_bar_chart(df, 'city', 'profit_margin', "Top 5 Cities with the Highest Profit Margins")

def visualize_query_3(df):
    plot_bar_chart(df, 'category', 'total_discount', "Total Discount Given for Each Category")

def visualize_query_4(df):
    plot_bar_chart(df, 'sub_category', 'avg_sc', "Average Sale Price per Product Category")

def visualize_query_5(df):
    plot_bar_chart(df, 'region', 'avg_reg', "Region with the Highest Average Sale Price")

def visualize_query_6(df):
    plot_bar_chart(df, 'category', 'total_cp', "Total Profit per Category")

def visualize_query_7(df):
    plot_bar_chart(df, 'sub_category', 'total_qty', "Top 3 Segments with the Highest Quantity of Orders")

def visualize_query_8(df):
    plot_bar_chart(df, 'region', 'region_dp', "Average Discount Percentage per Region")

def visualize_query_9(df):
    plot_bar_chart(df, 'sub_category', 'total_scp', "Product Category with the Highest Total Profit")

def visualize_query_10(df):
    plot_line_chart(df, 'year', 'total_pr', "Total Revenue Generated per Year")

def visualize_query_11(df):
    plot_line_chart(df, 'month', 'total_pr', "Total Revenue Generated Each Month")

def visualize_query_12(df):
    plot_bar_chart(df, 'discount_percent', 'impact_on_sales', "Discount Impact on Sales")

def visualize_query_13(df):
    plot_bar_chart(df, 'product_id', 'top_revenue', "Top-Selling Products")

def visualize_query_14(df):
    plot_line_chart(df, 'month', 'total_pr', "Monthly Sales Analysis")

def visualize_query_15(df):
    plot_bar_chart(df, 'region', 'reg_qty', "Regions with More than 3000 Quantity of Sold Products")

def visualize_query_16(df):
    plot_bar_chart(df, 'sub_category', 'total_discount', "Top 5 Product Categories with Highest Discount")

def visualize_query_17(df):
    plot_bar_chart(df, 'region', 'orders_region', "Number of Orders in Each Region")

def visualize_query_18(df):
    plot_bar_chart(df, 'sub_category', 'count', "Ship Mode Used Based on Subcategory")

def visualize_query_19(df):
    plot_line_chart(df, 'order_date', 'total_orders', "Number of Orders Per Day")

def visualize_query_20(df):
    plot_bar_chart(df, 'product_id', 'sales', "Top 5 High-Selling Products in Each Segment")

def visualize_query_based_on_results(df, query_name):
    """This function determines which visualization function to call based on the query name."""
    if query_name == queries[0]:
        visualize_query_1(df)
    elif query_name == queries[1]:
        visualize_query_2(df)
    elif query_name == queries[2]:
        visualize_query_3(df)
    elif query_name == queries[3]:
        visualize_query_4(df)
    elif query_name == queries[4]:
        visualize_query_5(df)
    elif query_name == queries[5]:
        visualize_query_6(df)
    elif query_name == queries[6]:
        visualize_query_7(df)
    elif query_name == queries[7]:
        visualize_query_8(df)
    elif query_name == queries[8]:
        visualize_query_9(df)
    elif query_name == queries[9]:
        visualize_query_10(df)
    elif query_name == queries[10]:
        visualize_query_11(df)
    elif query_name == queries[11]:
        visualize_query_12(df)
    elif query_name == queries[12]:
        visualize_query_13(df)
    elif query_name == queries[13]:
        visualize_query_14(df)
    elif query_name == queries[14]:
        visualize_query_15(df)
    elif query_name == queries[15]:
        visualize_query_16(df)
    elif query_name == queries[16]:
        visualize_query_17(df)
    elif query_name == queries[17]:
        visualize_query_18(df)
    elif query_name == queries[18]:
        visualize_query_19(df)
    elif query_name == queries[19]:
        visualize_query_20(df)


# Left side queries (1-10)
st.subheader("Query Set 1-10")
selected_query_left = st.selectbox("Select a Query (1-10)", queries[:10], key="query_left")
selected_sql_left = query_sql[queries.index(selected_query_left)] 

# Display the selected query in SQL format using st.code (with proper formatting)
if selected_query_left:
    st.subheader("Selected Query")
    st.code(selected_sql_left, language="sql")

# Execute the query and plot histogram when the button is clicked
if st.button("Run Query"):
    if selected_query_left:
        try:
            # Connect to the PostgreSQL database
            connection = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port="5432",
            )
            cursor = connection.cursor()

            # Execute the query
            cursor.execute(selected_sql_left)
            columns = [desc[0] for desc in cursor.description]  # Get column names
            data = cursor.fetchall()  # Fetch data

            # Display the query output
            df = pd.DataFrame(data, columns=columns)
            st.subheader("Query Output")
            st.dataframe(df)

            # Visualize the query results based on selected query
            visualize_query_based_on_results(df, selected_query_left)

        except Exception as e:
            st.error(f"An error occurred: {e}")
        
        finally:
            if connection:
                cursor.close()
                connection.close()
    else:
        st.error("Please select a query to run.")

# Right side queries (11-20)
st.subheader("Query Set 11-20")
selected_query_right = st.selectbox("Select a Query (11-20)", queries[10:], key="query_right")
selected_sql_right = query_sql[queries.index(selected_query_right)]

# Display the selected query in SQL format using st.code (with proper formatting)
if selected_query_right:
    st.subheader("Selected Query")
    st.code(selected_sql_right, language="sql")

# Execute the query and plot histogram when the button is clicked
if st.button("Run Right Query"):
    if selected_query_right:
        try:
            # Connect to the PostgreSQL database
            connection = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port="5432",
            )
            cursor = connection.cursor()

            # Execute the query
            cursor.execute(selected_sql_right)
            columns = [desc[0] for desc in cursor.description]  # Get column names
            data = cursor.fetchall()  # Fetch data

            # Display the query output
            df = pd.DataFrame(data, columns=columns)
            st.subheader("Query Output")
            st.dataframe(df)

            # Visualize the query results based on selected query
            visualize_query_based_on_results(df, selected_query_right)

        except Exception as e:
            st.error(f"An error occurred: {e}")
        
        finally:
            if connection:
                cursor.close()
                connection.close()
    else:
        st.error("Please select a query to run.")
