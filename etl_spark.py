import json
import requests
import datetime
import os

def get_from_date_end_date():
    from_date = "2023-01-01"
    to_date = "2023-01-05"
    from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
    to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d")
    response = {
        "from_date": from_date.strftime("%Y-%m-%d"),
        "to_date": to_date.strftime("%Y-%m-%d")
    }
    return response

def download_data():
    API_URL = f"https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"\
                  f"?date_received_max=<todate>&date_received_min=<fromdate>" \
                  f"&field=all&format=json"
    from_date,to_date = get_from_date_end_date().values()
    final_url = API_URL.replace("<todate>", to_date).replace("<fromdate>", from_date)
    print(final_url)
    data = requests.get(final_url, params={'User-agent': f'your bot '})
    finance_complaint_data = list(map(lambda x: x["_source"], filter(lambda y : "_source" in y.keys(),     json.loads(data.content))))
    return finance_complaint_data

warehouse_location="database"
spark = SparkSession \
        .builder \
        .appName("ComplaintsAnalytics") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()

table_name = "finance_complaint"
def get_data_frame():
    
    data = download_data()
    if data is None:
        print("Data Not Avaliable")
        return None
    else:
        df = spark.createDataFrame(data)
        return df
df = get_data_frame()

df.printSchema()

if spark.sql(f"show tables").filter(f"tableName='{table_name}'").count()>0:
    print(f"Table already exist")
    spark.sql(f"drop table {table_name}")

df.write.saveAsTable(table_name)

df.createOrReplaceTempView("financial_complaint")

spark.sql("select *from financial_complaint limit 10").show()

#Top 5 Companies with complaints
top_five_companies_with_complaints = spark.sql("""
  select company, count(*) as no_of_complaints from financial_complaint 
  group by company order by no_of_complaints desc limit 5
""")

top_five_companies_with_complaints.toPandas().plot(kind="pie", x="company", y="no_of_complaints")


df = spark.sql("select * from finance_complaint")

#Number of complaint for each product
product_complaints = spark.sql("""
    select product, count(*) as no_of_compalints from finance_complaint
    group by product order by no_of_compalints desc limit 5
""")

product_complaints.toPandas().plot(kind="pie", x="product", y="no_of_compalints")

#Number of unique product
spark.sql("""
 with res as (select product from finance_complaint group by product)
 select count(*) from res;
""").show()

 