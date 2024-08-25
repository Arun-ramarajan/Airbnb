from pymongo import MongoClient
import pandas as pd
pd.set_option("display.max_columns", None)

client = MongoClient("mongodb_link")

db = client['sample_airbnb']
col = db["listingsAndReviews"]
data = []
for i in col.find():
    data.append(i)

df = pd.DataFrame(data)

# dropping columns which have group of values
df.drop(['host','house_rules', 'address','availability', 'reviews', 'interaction', 'summary',	'space','description',	'neighborhood_overview', 'notes',	'transit','access',
         'last_scraped', 'calendar_last_scraped',	'first_review',	'last_review' ],axis=1, inplace=True)

df["images"]= df["images"].apply(lambda x: x["picture_url"])
df["review_scores"]= df["review_scores"].apply(lambda x: x.get("review_scores_rating",0))

df.isnull().sum()

# replacing null values 
df["bedrooms"].fillna(0,inplace= True)
df["beds"].fillna(0,inplace= True)
df["bathrooms"].fillna(0,inplace= True)
df["security_deposit"].fillna(0,inplace= True)
df["cleaning_fee"].fillna(0,inplace= True)
df["weekly_price"].fillna(0,inplace= True)
df["monthly_price"].fillna(0,inplace= True)
df["reviews_per_month"].fillna(0,inplace= True)

df.isnull().sum()

df.dtypes

# converting datatype 
df["minimum_nights"]= df["minimum_nights"].astype(int)
df["maximum_nights"]= df["maximum_nights"].astype(int)
df["bedrooms"]= df["bedrooms"].astype(int)
df["beds"]= df["beds"].astype(int)
df["bathrooms"]= df["bathrooms"].astype(str).astype(float).astype(int)
df["price"]= df["price"].astype(str).astype(float).astype(int)
df["security_deposit"]= df["guests_included"].astype(str).astype(float).astype(int)
df["cleaning_fee"]= df["cleaning_fee"].astype(str).astype(float).astype(int)
df["extra_people"]= df["extra_people"].astype(str).astype(float).astype(int)
df["guests_included"]= df["guests_included"].astype(str).astype(float).astype(int)
df["weekly_price"]= df["guests_included"].astype(str).astype(float).astype(int)
df["monthly_price"]= df["guests_included"].astype(str).astype(float).astype(int)

# treating the columns which had group of values
data1 = []

for i in col.find({},{"_id":1, "host":1, "address":1, "availability":1}):
    data1.append(i)

df1 = pd.DataFrame(data1)

columns= {'_id':[],'host_id':[], 'host_url':[], 'host_name':[], 'host_location':[],"host_response_time":[],
           'host_thumbnail_url':[], 'host_picture_url':[], 'host_neighbourhood':[], 
          'host_response_rate':[], 'host_is_superhost':[], 'host_has_profile_pic':[], 
          'host_identity_verified':[], 'host_listings_count':[], 'host_total_listings_count':[], 'host_verifications':[],
          'street':[], 'suburb':[], 'government_area':[], 'market':[], 'country':[], 'country_code':[],
            'location_type':[], "longitude":[], "latitude":[],"is_location_exact":[],
            'availability_30':[], 'availability_60':[], 'availability_90':[], 'availability_365':[]}

for i in df1["_id"]:
    columns["_id"].append(i)
for i in df1["host"]:
    columns["host_id"].append(i["host_id"])
    columns["host_url"].append(i["host_url"])
    columns["host_name"].append(i["host_name"])
    columns["host_location"].append(i["host_location"])
    columns["host_response_time"].append(i.get("host_response_time"))
    columns["host_thumbnail_url"].append(i["host_thumbnail_url"])
    columns["host_picture_url"].append(i["host_picture_url"])
    columns["host_neighbourhood"].append(i["host_neighbourhood"])
    columns["host_response_rate"].append(i.get("host_response_rate"))
    columns["host_is_superhost"].append(i["host_is_superhost"])
    columns["host_has_profile_pic"].append(i["host_has_profile_pic"])
    columns["host_identity_verified"].append(i["host_identity_verified"])
    columns["host_listings_count"].append(i["host_listings_count"])
    columns["host_total_listings_count"].append(i["host_total_listings_count"])
    columns["host_verifications"].append(i["host_verifications"])

for i in df1["address"]:
    columns["street"].append(i["street"])
    columns["suburb"].append(i["suburb"])
    columns["government_area"].append(i["government_area"])
    columns["market"].append(i["market"])
    columns["country"].append(i["country"])
    columns["country_code"].append(i["country_code"])
    columns["location_type"].append(i["location"]["type"])
    columns["longitude"].append(i["location"]["coordinates"][0])
    columns["latitude"].append(i["location"]["coordinates"][1])
    columns["is_location_exact"].append(i["location"]["is_location_exact"])

for i in df1["availability"]:
    columns["availability_30"].append(i["availability_30"])
    columns["availability_60"].append(i["availability_60"])
    columns["availability_90"].append(i["availability_90"])
    columns["availability_365"].append(i["availability_365"])

column = pd.DataFrame(columns)

column.isnull().sum()

# replacing null values
column["host_response_time"].fillna("Not Specified",inplace= True)
column["host_response_rate"].fillna("Not Specified",inplace= True)

column.isnull().sum()

# replacing the empty string
list_index = {col: [] for col in column.columns if col != '_id'}
for index, row in column.iterrows():
    for i in list(column.columns):
        if i != '_id' and row[i] == '':
            list_index[i].append(index)


for col in list_index:
    list_index[col] = len(list_index[col])

for i in list_index:
    if list_index[i] > 0:
        column[i]=column[i].apply(lambda i:(i.replace('',"Not Specified")))

list_index = {col: [] for col in column.columns if col != '_id'}
for index, row in column.iterrows():
    for i in list(column.columns):
        if i != '_id' and row[i] == '':
            list_index[i].append(index)


for col in list_index:
    list_index[col] = len(list_index[col])

# joining the dataframes
final_df = pd.merge(df, column, on='_id')

final_df.to_csv("Airbnb_preprocessed.csv", index=False)

