

from git import Repo
import os
import pandas as pd
import numpy as np
import mysql.connector
import streamlit as st
import io
import xlsxwriter
import ydata_profiling
import plotly.express as px
from streamlit_player import st_player
from streamlit_pandas_profiling import st_profile_report
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_extras.add_vertical_space import add_vertical_space

conn = mysql.connector.connect(
    host= "localhost",
    user = "root",
    password ='Sachin1507@'
)
cursor = conn.cursor()


#cloning from repository

repo_url="https://github.com/PhonePe/pulse.git"
clone_path= r"/Users/sachinpranav/Downloads/pulse"
#
if not os.path.exists(clone_path):
   os.makedirs(clone_path)
#
repo_path = os.path.join(clone_path,os.path.basename(repo_url).removesuffix('.git').title())
# Repo.clone_from(repo_url,repo_path)
#
directory = os.path.join(repo_path,'data')
print(directory)

#function to rename improper state names in proper name

def rename (directory):
	for root,dirs,files in os.walk(directory):
		if 'state' in dirs:
			state_dir = os.path.join(root,'state')
			for state_folder in os.listdir(state_dir):
				#renmae the state folder
			    old_path = os.path.join(state_dir,state_folder)
			new_path = os.path.join(state_dir,state_folder.title().replace('-',' ').replace('&','and'))
			os.rename(old_path,new_path)
	print("Renamed all sub-directories successfully")

#function to extract  all paths that has sub-directory in the name of 'state'

def extract_paths(directory):
	path_list = []
	for root,dirs,files in os.walk(directory):
		if os.path.basename(root) =='state':
			path_list.append(root.replace('\\','/'))
	return path_list





rename(directory)
state_directories = extract_paths(directory)
print(state_directories)

def add_region_column(df):
    state_groups = {
        'Northern Region': ['jammu-&-kashmir', 'himachal pradesh', 'punjab', 'chandigarh', 'uttarakhand', 'ladakh', 'delhi'],
        'Central Region': ['uttar-pradesh', 'madhya-pradesh', 'chhattisgarh'],
        'Western Region': ['rajasthan', 'gujarat', 'dadra-&-nagar-haveli-&-daman-&-diu', 'maharashtra'],
        'Eastern Region': ['bihar', 'jharkhand', 'odisha', 'west bengal', 'sikkim'],
        'Southern Region': ['andhra-pradesh', 'telangana', 'karnataka', 'kerala', 'tamil-nadu', 'puducherry', 'goa', 'lakshadweep','andaman-&-nicobar-islands'],
        'North Central Region': ['assam', 'meghalaya', 'manipur', 'nagaland', 'tripura', 'arunachal-pradesh', 'mizoram']
    }

    df['Region'] = df['State'].map({state: region for region, states in state_groups.items() for state in states})
    return df


#Creating datafrmes from cloned json files
 #1.Aggregate transactions

state_path = state_directories[0]
state_list = os.listdir(state_path)
agg_trans_dict = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Transaction_count': [], 'Transaction_amount': []}

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)
            # add_region_column(df);

            try:
                for transaction_data in df["data"]["districts"]:
                    name = transaction_data['entityName']
                    count = transaction_data['metric']['count']
                    amount = transaction_data['metric']['amount']

                    # Appending to agg_trans_dict
                    agg_trans_dict['State'].append(state)
                    agg_trans_dict['Year'].append(year)
                    agg_trans_dict['Quarter'].append(int(quarter.removesuffix(".json")))
                    agg_trans_dict['District'].append(name.title().replace('And','and'))
                    agg_trans_dict['Transaction_count'].append(count)
                    agg_trans_dict['Transaction_amount'].append(amount)

            except:
                pass

agg_trans_df = pd.DataFrame(agg_trans_dict)
# Call the add_region_column function to add the 'Region' column
agg_trans_df = add_region_column(agg_trans_df)


#2.Aggregate User

state_path = state_directories[1]
state_list = os.listdir(state_path)
print("PATH",state_path)
agg_user_dict = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Registered_users': []}

for state in state_list:
    year_path = state_path + '/' + state + '/'
    print("Year Path:", year_path)
    print("Files in Year Path:", os.listdir(year_path))
    year_list = [file for file in os.listdir(year_path) if not file.startswith('.DS_Store')]


    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = [file for file in os.listdir(quarter_path) if not file.startswith('.DS_Store')]

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for user_data in df["data"]["districts"]:
                    name = user_data['name']
                    registered_users = user_data['registeredUsers']


                    # Appending to agg_user_dict
                    agg_user_dict['State'].append(state)
                    agg_user_dict['Year'].append(year)
                    agg_user_dict['Quarter'].append(int(quarter.removesuffix(".json")))
                    agg_user_dict['District'].append(name)
                    agg_user_dict['Registered_users'].append(registered_users)


            except:
                pass

agg_user_df = pd.DataFrame(agg_user_dict)
agg_user_df = add_region_column(agg_user_df)


#3.Map transaction

state_path = state_directories[2]
state_list = os.listdir(state_path)
map_trans_dict = {'State': [], 'Year': [], 'Quarter': [],'District': [],'Transaction_count': [], 'Transaction_amount': []}

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for transaction_data in df["data"]["hoverDataList"]:
                    district = transaction_data['name']
                    count = transaction_data['metric'][0]['count']
                    amount = transaction_data['metric'][0]['amount']

                    # Appending to map_trans_dict
                    map_trans_dict['State'].append(state)
                    map_trans_dict['Year'].append(year)
                    map_trans_dict['Quarter'].append(int(quarter.removesuffix(".json")))
                    map_trans_dict['District'].append(district.removesuffix('district').title().replace('And', 'and'))

                    map_trans_dict['Transaction_count'].append(count)
                    map_trans_dict['Transaction_amount'].append(amount)

            except:
                pass

map_trans_df = pd.DataFrame(map_trans_dict)
map_trans_df = add_region_column(map_trans_df)


# #4.Map User
state_path = state_directories[3]
state_list = os.listdir(state_path)
map_user_dict = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Registered_users': [], 'App_opens': []}
print("MAP USER PATH",state_path)

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for district, user_data in df["data"]["hoverData"].items():
                    reg_user_count = user_data['registeredUsers']
                    app_open_count = user_data['appOpens']

                    # Appending to map_user_dict
                    map_user_dict['State'].append(state)
                    map_user_dict['Year'].append(year)
                    map_user_dict['Quarter'].append(int(quarter.removesuffix(".json")))
                    map_user_dict['District'].append(district.removesuffix('district').title().replace('And', 'and'))
                    map_user_dict['Registered_users'].append(reg_user_count)
                    map_user_dict['App_opens'].append(app_open_count)

            except:
                pass

map_user_df = pd.DataFrame(map_user_dict)
map_user_df = add_region_column(map_user_df)

# 5.Top Transaction District-wise
#
state_path = state_directories[4]
state_list = os.listdir(state_path)
top_trans_dist_dict = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_name': [],'Transaction_count': [], 'Transaction_amount': [],'Transaction_type': []}

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)
            try:
                for district_data in df["data"]["transactionData"]:
                    transaction_name = district_data['name']
                    transaction_type = district_data['paymentInstruments'][0]['type']
                    transaction_count = district_data['paymentInstruments'][0]['count']
                    transaction_amount = district_data['paymentInstruments'][0]['amount']

                    # Appending to top_trans_dist_dict
                    top_trans_dist_dict['State'].append(state)
                    top_trans_dist_dict['Year'].append(year)
                    top_trans_dist_dict['Quarter'].append(int(quarter.removesuffix(".json")))
                    top_trans_dist_dict['Transaction_name'].append(transaction_name)
                    top_trans_dist_dict['Transaction_count'].append(transaction_count)
                    top_trans_dist_dict['Transaction_amount'].append(transaction_amount)
                    top_trans_dist_dict['Transaction_type'].append(transaction_type)

            except:
                pass

top_trans_dist_df = pd.DataFrame(top_trans_dist_dict)
top_trans_dist_df = add_region_column(top_trans_dist_df)

#6.Top Transaction District Wise

state_path = state_directories[5]
state_list = os.listdir(state_path)
top_trans_dist_dict = {'State': [], 'Year': [], 'Quarter': [], 'Brand': [], 'Transaction_count': [], 'Percentage': []}

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for district_data in df["data"]["usersByDevice"]:
                    brand = district_data['brand']
                    count = district_data['count']
                    percent = district_data['percentage']

                    # Appending to top_trans_dist_dict
                    top_trans_dist_dict['State'].append(state)
                    top_trans_dist_dict['Year'].append(year)
                    top_trans_dist_dict['Quarter'].append(int(quarter.removesuffix(".json")))
                    top_trans_dist_dict['Brand'].append(brand)
                    top_trans_dist_dict['Transaction_count'].append(count)
                    top_trans_dist_dict['Percentage'].append(percent)

            except:
                pass

top_trans_dist_dict_df = pd.DataFrame(top_trans_dist_dict)
top_trans_dist_dict_df = add_region_column(top_trans_dist_dict_df)

# #7.Top  User District-wise
#
state_path = state_directories[1]
state_list = os.listdir(state_path)
top_User_dist_dict = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Registered_users': []}

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for district_data in df["data"]["districts"]:
                    name = district_data['name']
                    registered_users = district_data['registeredUsers']

                    # Appending to top_trans_pin_dict
                    top_User_dist_dict['State'].append(state)
                    top_User_dist_dict['Year'].append(year)
                    top_User_dist_dict['Quarter'].append(int(quarter.removesuffix(".json")))
                    top_User_dist_dict['District'].append(name.title().replace('And', 'and'))
                    top_User_dist_dict['Registered_users'].append(registered_users)

            except:
                pass

top_User_dist_df = pd.DataFrame(top_User_dist_dict)
top_User_dist_df = add_region_column(top_User_dist_df )




# #8.Top User Pincode-Wise
#
state_path = state_directories[1]
state_list = os.listdir(state_path)
top_user_pin_dict = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Registered_users': []}

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)

    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)

        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)

            try:
                for district_data in df["data"]["pincodes"]:
                    name = district_data['name']
                    count = district_data['registeredUsers']

                    # Appending to top_user_pin_dict
                    top_user_pin_dict['State'].append(state)
                    top_user_pin_dict['Year'].append(year)
                    top_user_pin_dict['Quarter'].append(int(quarter.removesuffix(".json")))
                    top_user_pin_dict['Pincode'].append(name)
                    top_user_pin_dict['Registered_users'].append(count)

            except:
                pass

top_user_pin_df = pd.DataFrame(top_user_pin_dict)
top_user_pin_df= add_region_column(top_user_pin_df)

# #List of Data Frames Created So Far
#
df_list = [df for df in globals() if isinstance(globals()[df],pd.core.frame.DataFrame) and df.endswith('_df')]
# df_list



#Column wise null_count and duplicated_rows_count

for df_name in df_list:
    df = globals()[df_name]
    print(f'{df_name}:')
    print(f"Null count:\n{df.isnull().sum()}")
    print(f"Duplicated rows count:\n{df.duplicated().sum()}")
    print(df.shape)
    print("\n", 25 * "_", "\n")

# Understanding the data frames

for df_name in df_list:
	df = globals()[df_name]
	print(df_name + ':\n' )
	df.info()
	print(" \n",45 * "_", "\n")


# Dropping rows with the null columns

# top_trans_dist_pin_df.dropna(axis = "index",inplace = True)
# top_trans_dist_pin_df.isnull().sum()

#Changing datatype across all dataframes

# for df_name in df_list:
# 	df = globals()[df_name]
# 	df['Year'] = df['Year'].astype('int')

#n Outlier count across all data frames



def count_outliers(df):
    outliers = {}
    for col in df.select_dtypes(include=[np.number]).columns:
        if col in ['Transaction_count', 'Transaction_amount']:
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            upper_level = q3 + (1.5 * iqr)
            lower_level = q1 - (1.5 * iqr)
            outliers[col] = len(df[(df[col] > upper_level) | (df[col] < lower_level)])
    return outliers

print("OUTLIER ACROSS ALL DATA FRAMES:\n")
for df_name in df_list:
    df = globals()[df_name]
    outliers = count_outliers(df)
    if len(outliers) == 0:
        pass
    else:
        print(df_name,":\n\n" , outliers,"\n")
        print("\n",55 * "_", "\n")

def unique_value_count(df, exclude_cols=[]):
    for col in df.columns:
        if col in exclude_cols:
            continue
        unique_vals = df[col].nunique()
        print(f"{col}: {unique_vals} unique_vals")
        if unique_vals < 10:
            print(df[col].unique())

print('UNIQUE VALUE COUNT FOR ALL DATA FRAMES; \n')

for df_name in df_list:
    df = globals()[df_name]
    print(df_name, ":\n")
    unique_value_count(df, exclude_cols = ['State','Year','Quarter','Percentage'])
    print("\n",55 * "_", "\n")

#Creating Csv files

def save_dfs_as_csv(df_list):
    subfolder = "Miscellaneous"
    if not os.path.exists(subfolder):
        os.makedirs(subfolder)

    for df_name in df_list:
        df = globals()[df_name]
        file_path = os.path.join(subfolder, df_name.replace('_df',"")+ '.csv')
        df.to_csv (file_path,index = False)

save_dfs_as_csv(df_list)

#Data Base creation

cursor.execute("DROP DATABASE IF EXISTS phonepe_pulse")
cursor.execute("CREATE DATABASE phonepe_pulse")
cursor.execute("USE phonepe_pulse")
print("hello")

#Creating Tables

cursor.execute('''CREATE TABLE agg_trans(
                    State VARCHAR(255),
                    year YEAR,
                    Quarter INTEGER,
                    District VARCHAR(255),
                    Transaction_count INTEGER,
                    Transaction_amount FLOAT,
                    Region VARCHAR(255),
                    PRIMARY KEY (State(255), Year,Quarter,District(255),Region(255))
                    )''')

cursor.execute('''CREATE TABLE agg_user(
                    State VARCHAR(255),
                    year YEAR,
                    Quarter INTEGER,
                    District VARCHAR(255),
                    Registered_users INTEGER,
                    Region VARCHAR(255),
                    PRIMARY KEY (State(255), Year,Quarter,District(255),Region(255))
                    )''')

cursor.execute('''CREATE TABLE map_trans(
                    State VARCHAR(255),
                    year YEAR,
                    Quarter INTEGER,
                    District VARCHAR(255),
                    Transaction_count INTEGER,
                    Transaction_amount FLOAT,
                    Region VARCHAR(255),
                    PRIMARY KEY(State(255), Year,Quarter,District(255),Region(255))
                    )''')

cursor.execute('''CREATE TABLE map_user(
                    State VARCHAR(255),
                    year YEAR,
                    Quarter INTEGER,
                    District VARCHAR(255),
                    Registered_users INTEGER,
                    App_opens FLOAT,
                    Region VARCHAR(255),
                    PRIMARY KEY (State(255), Year,Quarter,District(255),Region(255))
                    )''')

cursor.execute('''CREATE TABLE top_trans_dist(
                    State VARCHAR(255),
                    year YEAR,
                    Quarter INTEGER,
                    Transaction_name VARCHAR(255),
                    Transaction_count INTEGER,
                    Transaction_amount FLOAT,
                    Transaction_type VARCHAR(255),
                    Region VARCHAR(255),
                    PRIMARY KEY (State(255), Year,Quarter,Transaction_name(255),Region(255))
                    )''')

cursor.execute('''CREATE TABLE top_trans_dist_dict(
                    State VARCHAR(255),
                    year YEAR,
                    Quarter INTEGER,
                    Brand VARCHAR(255),
                    Transaction_count INTEGER,
                    Percentage FLOAT,
                    Region VARCHAR(255),
                    PRIMARY KEY (State(255), Year,Quarter,Brand(255),Region(255))
                    )''')

cursor.execute('''CREATE TABLE top_user_dist_dict(
                    State VARCHAR(255),
                    year YEAR,
                    Quarter INTEGER,
                    District VARCHAR(255),
                    Registered_users INTEGER,
                    Region VARCHAR(255),
                    PRIMARY KEY (State(255), Year,Quarter,District (255),Region(255))
                    )''')

cursor.execute('''CREATE TABLE top_user_pin_dict(
                    State VARCHAR(255),
                    year YEAR,
                    Quarter INTEGER,
                    Pincode VARCHAR(255),
                    Registered_users INTEGER,
                    Region VARCHAR(255),
                    PRIMARY KEY (State(255), Year,Quarter,Pincode(255),Region(255))
                    )''')

print("sachin")

# Pushing data into Mysql

def push_data_into_mysql(conn, cursor, dfs, table_columns):
    print("KEYSS",dfs.keys())
    for table_name in dfs.keys():
        df = dfs[table_name]
        columns = table_columns[table_name]

        # Fill NaN values with appropriate defaults (e.g., empty string or NULL)
        df = df.fillna('')  # Replace NaN values with empty string

        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        print(f"Table Name: {table_name}")
        print("Columns:", columns)
        print("Data:")
        print(df)
        print()

        for _, row in df.iterrows():
            data = tuple(row[column] for column in columns)
            cursor.execute(query, data)

        conn.commit()

    print("DATA PUSHED SUCCESSFULLY")


# Mapping

dfs = {
    'agg_trans' : agg_trans_df,
    'agg_user'  : agg_user_df,
    'map_trans' : map_trans_df,
    "map_user"  : map_user_df,
    'top_trans_dist' : top_trans_dist_df,
    'top_trans_dist_dict'  : top_trans_dist_dict_df,
    'top_user_dist_dict' : top_User_dist_df,
    'top_user_pin_dict'   : top_user_pin_df
}

# Mapping associated with the columns

table_columns = {
    'agg_trans' : list(agg_trans_df.columns),
    'agg_user'  : list(agg_user_df.columns),
    'map_trans' : list(map_trans_df.columns),
    "map_user"  : list(map_user_df.columns),
    'top_trans_dist' : list(top_trans_dist_df.columns),
    'top_trans_dist_dict'  : list(top_trans_dist_dict_df.columns),
    'top_user_dist_dict' : list(top_User_dist_df.columns),
    'top_user_pin_dict'   : list(top_user_pin_df.columns)
}

push_data_into_mysql(conn, cursor, dfs, table_columns)
print("TOP USER DISCTTT",top_User_dist_df.columns)
#
def get_dataframe(table_name):
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=[i[0] for i in cursor.description])
    df['year'] = df['year'].astype(str)
    return df




table_names = [
    'agg_trans', 'agg_user', 'map_trans',
    'map_user', 'top_trans_dist', 'top_trans_dist_dict',
    'top_user_dist_dict', 'top_user_pin_dict'
]

for table_name in table_names:
    var_name = f"{table_name}_df"
    globals()[var_name] = get_dataframe(table_name)

cursor.close()
conn.close()

if 'options' not in st.session_state:
    st.session_state['options'] = {
        'Aggregate Transaction': 'agg_trans_df',
        'Aggregate User': 'agg_user_df',
        'Map Transaction': 'map_trans_df',
        'Map User': 'map_user_df',
        'Top Transaction Districtwise': 'top_trans_dist_df',
        'Top Transaction Pincodewise': 'top_trans_dist_dict_df',
        'Top User Districtwise': 'top_User_dist_df',
        'Top User Pincodewise': 'top_user_pin_df'
    }

df_names = [
    var_name for var_name in globals()
    if isinstance(globals()[var_name], pd.core.frame.DataFrame) and var_name.endswith('_df')
]

if 'df_list' not in st.session_state:
    st.session_state['df_list'] = []

    for var_name in df_names:
        st.session_state[var_name] = globals()[var_name]
        st.session_state['df_list'].append(var_name)


# StreamlitApp
st.set_page_config(
                    page_title = 'PhonePe Data Visualization', layout = 'wide',
                    )
st.title(':black[PhonePe Data Visualization]')

add_vertical_space(2)

phonepe_description = """PhonePe has launched PhonePe Pulse, a data analytics platform that provides insights into
                        how Indians are using digital payments. With over 30 crore registered users and 2000 crore 
                        transactions, PhonePe, India's largest digital payments platform with 46% UPI market share,
                        has a unique ring-side view into the Indian digital payments story. Through this app, you 
                        can now easily access and visualize the data provided by PhonePe Pulse, gaining deep 
                        insights and interesting trends into how India transacts with digital payments."""

st.write(phonepe_description)

add_vertical_space(2)

st_player(url = "https://www.youtube.com/watch?v=c_1H6vivsiA", height = 480)

add_vertical_space(2)
#
col1, col2, col3 = st.columns(3)

total_reg_users = top_User_dist_df['Registered_users'].sum()
print("TOTAL REG USER",total_reg_users)
col1.metric(
    label='Total Registered Users',
    value='{:.2f} Cr'.format(total_reg_users / 100000000),
    delta='Forward Trend'
)

total_app_opens = map_user_df['App_opens'].sum()
col2.metric(
    label='Total App Opens', value='{:.2f} Cr'.format(total_app_opens / 100000000),
    delta='Forward Trend'
)

col3.metric(label='Total Transaction Count', value='2000 Cr +', delta='Forward Trend')

style_metric_cards()

add_vertical_space(2)



add_vertical_space(2)

col, buff = st.columns([2, 4])

option = col.selectbox(
    label='Data',
    options=list(st.session_state['options'].keys()),
    key='df'
)

tab1, tab2 = st.tabs(['Report and Dataset', 'Download Dataset'])

with tab1:
    column1, column2, buffer = st.columns([2, 2, 4])

    show_profile = column1.button(label='Show Detailed Report', key='show')
    show_df = column2.button(label='Show Dataset', key='show_df')

    if show_profile:
        df_name = st.session_state['options'][option]
        df = globals()[df_name]
        pr = df.profile_report()
        st_profile_report(pr)

    if show_df:
        st.data_editor(
            data=globals()[st.session_state['options'][option]],
            use_container_width=True
        )

with tab2:
    col1, col2, col3 = st.columns(3)

    df_name = st.session_state['options'][option]
    df = globals()[df_name]

    csv = df.to_csv()
    json = df.to_json(orient='records')
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, engine='xlsxwriter', index=False)
    excel_bytes = excel_buffer.getvalue()

    col1.download_button(
        "Download CSV file", data=csv,
        file_name=f'{option}.csv',
        mime='text/csv', key='csv'
    )
    col2.download_button(
        "Download JSON file", data=json,
        file_name=f'{option}.json',
        mime='application/json', key='json'
    )
    col3.download_button("Download Excel file", data=excel_bytes,
                         file_name=f'{option}.xlsx',
                         mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                         key='excel'
                         )

#TRANSACTION OVERVIEW

agg_trans = trans_df = trans_df_2 = st.session_state["agg_trans_df"]
map_df = st.session_state["map_trans_df"]

states = agg_trans["State"].unique()
years = agg_trans["year"].unique()
quarters = agg_trans["Quarter"].unique()

if 'states' not in st.session_state:
    st.session_state["states"] = states
if 'years' not in st.session_state:
    st.session_state["years"] = years
if 'quarters' not in st.session_state:
    st.session_state["quarters"] = quarters


# App



st.title(':red[Transaction]')
add_vertical_space(3)


#1


st.subheader(':blue[Transaction amount breakdown]')


col1, col2, col3 = st.columns([5, 3, 1])

state1 = col1.selectbox("State", states, key='state1')
year1 = col2.selectbox("Year", years, key='year1')
quarter_options = ["All"] + list(map(str, quarters))
quarter1 = col3.selectbox("Quarter", quarter_options, key='quarter1')

trans_df = trans_df[(trans_df["State"] == state1) & (trans_df["year"] == year1)]

if quarter1 != 'All':
    trans_df = trans_df[(trans_df["Quarter"] == int(quarter1))]

trans_df = trans_df.sort_values("Transaction_amount", ascending=False).reset_index(drop = True)

suffix1 = " quarters" if quarter1 == 'All' else "st" if quarter1 == '1' else "nd" if quarter1 == '2' else "rd" if quarter1 == '3' else "th"

title1 = f"Transaction details of {state1} for {quarter1.lower()}{suffix1} {'' if quarter1 == 'All' else 'quarter'} of {year1}"

fig1 = px.bar(
             trans_df, x="Transaction_count", y="Transaction_amount",
             color="Transaction_count",
             color_discrete_sequence=px.colors.qualitative.Plotly,
             title=title1,
             labels=dict(Transaction_amount='Transaction Amount', Transaction_count='Transaction Count'),
             hover_data={'Quarter': True}
             )

fig1.update_layout(
                   showlegend=False,
                   title={
                       'x': 0.5,
                       'xanchor': 'center',
                       'y': 0.9,
                       'yanchor': 'top'
                       },
                   width = 900, height = 500
                   )

fig1.update_traces(marker = dict(line = dict(width = 1, color = 'DarkSlateGrey')))

st.plotly_chart(fig1)

expander1 = st.expander(label = 'Detailed view')
expander1.write(trans_df.loc[:, ['Quarter', 'Transaction_count', 'Transaction_amount']].reset_index(drop=True))


#2


st.subheader(':red[Transaction Hotspots - Districts]')


year_col, quarter_col, buff = st.columns([1,1,4])

year2 = year_col.selectbox("Year", years, key = 'year2')
quarter2 = quarter_col.selectbox("Quarter", quarter_options, key = 'quarter2')

map_df = map_df[map_df["year"] == year2]

if quarter2 != 'All':
    map_df = map_df[(map_df["Quarter"] == int(quarter2))]
expander2 = st.expander(label = 'Detailed view')
expander2.write(map_df.loc[:, ['State', 'District', 'Quarter', 'Transaction_amount']].reset_index(drop=True))


#3


st.subheader(":red[Breakdown by transaction count proportion]")


state_pie, year_pie, quarter_pie = st.columns([5, 3, 1])

state3 = state_pie.selectbox('State', options = states, key = 'state3')
year3 = year_pie.selectbox('Year', options = years, key = 'year3')
quarter3 = quarter_pie.selectbox('Quarter', options = quarter_options, key = 'quarter3')

filtered_trans = trans_df_2[(trans_df_2.State == state3) & (trans_df_2.year == year3)]

if quarter3 != 'All':
    filtered_trans = filtered_trans[filtered_trans.Quarter == int(quarter3)]

fig3 = px.pie(
              filtered_trans, names = 'Transaction_amount',
              values = 'Transaction_count', hole = .65
              )

fig3.update_layout(width = 900, height = 500)

st.plotly_chart(fig3)

expander3 = st.expander(label = 'Detailed view')
expander3.write(filtered_trans.loc[:, ['Quarter', 'Transaction_amount', 'Transaction_count']].reset_index(drop = True))

#USER VALUES IN STREAMLIT

agg_user_df1 = st.session_state["top_trans_dist_dict_df"]
map_user_df1 = st.session_state["map_user_df"]
top_user_dist_df1 = st.session_state["top_User_dist_df"]

# App
st.title(':red[Users]')
add_vertical_space(3)

st.subheader(':red[Transaction Count and Percentage by Brand]')

col1, col2, col3 = st.columns([5, 3, 1])

state_options = ['All'] + [state for state in st.session_state['states']]
quarter_options = ["All"] + list(map(str, st.session_state['quarters']))

state11 = col1.selectbox('State', options=state_options, key='state11')
year11 = col2.selectbox('Year', options=st.session_state['years'], key='year11')
quarter11 = col3.selectbox("Quarter", options=quarter_options, key='quarter11')

if state11 == "All":

    agg_user_df_filtered = agg_user_df1[(agg_user_df1['year'] == year11)]

    if quarter11 != 'All':
        agg_user_df_filtered = agg_user_df_filtered[agg_user_df_filtered['Quarter'] == int(quarter11)]

    suffix1 = " quarters" if quarter11 == 'All' else "st" if quarter11 == '1' else "nd" if quarter11 == '2' else "rd" if quarter11 == '3' else "th"

    title1 = f"Transaction Count and Percentage across all states for {quarter11.lower()}{suffix1} {'' if quarter11 == 'All' else 'quarter'} of {year11}"

else:

    agg_user_df_filtered = agg_user_df1[(agg_user_df1['State'] == state11) & (agg_user_df1['year'] == year11)]

    if quarter11 != 'All':
        agg_user_df_filtered = agg_user_df_filtered[agg_user_df_filtered['Quarter'] == int(quarter11)]

    suffix1 = " quarters" if quarter11 == 'All' else "st" if quarter11 == '1' else "nd" if quarter11 == '2' else "rd" if quarter11 == '3' else "th"

    title1 = f"Transaction Count and Percentage in {state11} for {quarter11.lower()}{suffix1} {'' if quarter11 == 'All' else 'quarter'} of {year11}"

fig1 = px.treemap(
    agg_user_df_filtered,
    path=['Brand'],
    values='Transaction_count',
    color='Percentage',
    color_continuous_scale='ylorbr',
    hover_data={'Percentage': ':.2%'},
    hover_name='Region'
)

fig1.update_layout(
    width=975, height=600,
    coloraxis_colorbar=dict(tickformat='.1%', len=0.85),
    margin=dict(l=20, r=20, t=0, b=20),
    title={
        "text": title1,
        'x': 0.45,
        'xanchor': 'center',
        'y': 0.007,
        'yanchor': 'bottom'
    }
)

fig1.update_traces(
    hovertemplate=
    '<b>%{label}</b><br>Transaction Count: %{value}<br>Percentage: %{color:.2%}<extra></extra>'
)

st.plotly_chart(fig1)

expander1 = st.expander(label='Detailed view')
expander1.write(agg_user_df_filtered.loc[:, ['State', 'Quarter', 'Brand', 'Percentage']])

add_vertical_space(2)

# 2
st.subheader(':blue[Top Districts by Registered Users]')

col7, col8, buff1 = st.columns([5, 2, 5])

state33 = col7.selectbox('State', options=state_options, key='state33')
year33 = col8.selectbox('Year', options=st.session_state['years'], key='year33')

if state33 == "All":

    top_user_dist_df_filtered = top_user_dist_df1[
        top_user_dist_df1['Year'] == year33
        ].groupby('District').sum().reset_index()

    top_user_dist_df_filtered = top_user_dist_df_filtered.sort_values(
        by='Registered_users',
        ascending=False
    ).head(10)

    title3 = f'Top 10 districts across all states by registered users in {year33}'

else:

    top_user_dist_df_filtered = top_user_dist_df1[
        (top_user_dist_df1['State'] == state33)
        &
        (top_user_dist_df1['Year'] == year33)
        ].groupby('District').sum().reset_index()

    top_user_dist_df_filtered = top_user_dist_df_filtered.sort_values(
        by='Registered_users',
        ascending=False
    ).head(10)

    title3 = f'Top districts in {state33} by registered users in {year33}'

fig3 = px.bar(
    top_user_dist_df_filtered,
    x='Registered_users',
    y='District',
    color='Registered_users',
    color_continuous_scale='Greens',
    orientation='h', labels={'Registered_users': 'Registered Users'},
    hover_name='District',
    hover_data=['Registered_users']
)

fig3.update_traces(hovertemplate='<b>%{hovertext}</b><br>Registered users: %{x:,}<br>')

fig3.update_layout(
    height=500, width=950,
    yaxis=dict(autorange="reversed"),
    title={
        'text': title3,
        'x': 0.5,
        'xanchor': 'center',
        'y': 0.007,
        'yanchor': 'bottom'
    }
)

st.plotly_chart(fig3)

expander3 = st.expander(label='Detailed view')
expander3.write(top_user_dist_df_filtered.loc[:, ['District', 'Registered_users']].reset_index(drop=True))

add_vertical_space(2)

# 4


st.subheader(':blue[Number of app opens by District]')

col9, col10, buff2 = st.columns([2, 2, 7])

year_options = [year for year in st.session_state['years'] if year != '2018']

year4 = col9.selectbox('Year', options=year_options, key='year4')

if year4 == '2019':
    quarter_options.remove('1')

quarter4 = col10.selectbox("Quarter", options=quarter_options, key='quarter4')

map_user_df_filtered = map_user_df1[(map_user_df1["year"] == year4)]
expander4 = st.expander(label='Detailed view')
expander4.write(map_user_df_filtered.loc[:, ['District', 'Quarter', 'App_opens']].reset_index(drop=True))




















