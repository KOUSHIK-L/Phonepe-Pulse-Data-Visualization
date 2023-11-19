# importing necessary libraries
import git
import requests
import json
import os
import pandas as pd 
import numpy as np 
import pymysql
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
from PIL import Image

# Establishing Python-MySQL Connection
mydb = pymysql.connect(host="127.0.0.1", user="root", password="Koushik@29")
sql = mydb.cursor()

# Phonepe data collection from the Github repository
def Data_Collection():
    try:
        git.Repo.clone_from("https://github.com/PhonePe/pulse.git",'PhonePe_Data')
    except:
        pass


# Defining a functions for extracting data from the downloaded local repository
def aggregate_transaction():
    path = "D:\\PhonePe_Data\\data\\aggregated\\transaction\\country\\india\\state\\"
    agg_state = os.listdir(path)

    data = {'state': [], 'year': [], 'quarter': [], 'trans_type': [], 'trans_count': [], 'trans_amount': []}

    for a in agg_state:
        path_a = os.path.join(path, a)
        agg_year = os.listdir(path_a)

        for b in agg_year:
            path_b = os.path.join(path_a, b)
            agg_json = os.listdir(path_b)

            for c in agg_json:
                path_c = os.path.join(path_b, c)
                with open(path_c, 'r') as file:
                    dfile = json.load(file)

                for d in dfile['data']['transactionData']:
                    name = d['name']
                    count = d['paymentInstruments'][0]['count']
                    amount = d['paymentInstruments'][0]['amount']
                    data['state'].append(a)
                    data['year'].append(b)
                    data['quarter'].append('Q' + str(c[0]))
                    data['trans_type'].append(name)
                    data['trans_count'].append(count)
                    data['trans_amount'].append(amount)

    return data


def aggregate_user():
    path = "D:\\PhonePe_Data\\data\\aggregated\\user\\country\\india\\state\\"
    agg_state = os.listdir(path)

    data = {'state': [], 'year': [], 'quarter': [], 'user_brand': [], 'user_count': [], 'user_percent': []}

    for a in agg_state:
        path_a = os.path.join(path, a)
        agg_year = os.listdir(path_a)

        for b in agg_year:
            path_b = os.path.join(path_a, b)
            agg_json = os.listdir(path_b)

            for c in agg_json:
                path_c = os.path.join(path_b, c)
                with open(path_c, 'r') as file:
                    dfile = json.load(file)
                    try:
                        for d in dfile['data']['usersByDevice']:
                            brand = d['brand']
                            count = d['count']
                            percentage = d['percentage'] * 100

                            data['state'].append(a)
                            data['year'].append(b)
                            data['quarter'].append('Q' + str(c[0]))
                            data['user_brand'].append(brand)
                            data['user_count'].append(count)
                            data['user_percent'].append(percentage)
                    except:
                        pass

    return data


def map_transaction():
    path = "D:\\PhonePe_Data\\data\\map\\transaction\\hover\\country\\india\\state\\"
    agg_state = os.listdir(path)

    data = {'state': [], 'year': [], 'quarter': [], 'district': [], 'trans_count': [], 'trans_amount': []}

    for a in agg_state:
        path_a = os.path.join(path, a)
        agg_year = os.listdir(path_a)

        for b in agg_year:
            path_b = os.path.join(path_a, b)
            agg_json = os.listdir(path_b)

            for c in agg_json:
                path_c = os.path.join(path_b, c)
                with open(path_c, 'r') as file:
                    dfile = json.load(file)

                for d in dfile['data']['hoverDataList']:
                    district = d['name'].split(' district')[0]
                    count = d['metric'][0]['count']
                    amount = d['metric'][0]['amount']
                    data['state'].append(a)
                    data['year'].append(b)
                    data['quarter'].append('Q' + str(c[0]))
                    data['district'].append(district)
                    data['trans_count'].append(count)
                    data['trans_amount'].append(amount)

    return data


def map_user():
    path = "D:\\PhonePe_Data\\data\\map\\user\\hover\\country\\india\\state\\"
    agg_state = os.listdir(path)

    data = {'state': [], 'year': [], 'quarter': [], 'district': [], 'reg_users': [], 'app_opens': []}

    for a in agg_state:
        path_a = os.path.join(path, a)
        agg_year = os.listdir(path_a)

        for b in agg_year:
            path_b = os.path.join(path_a, b)
            agg_json = os.listdir(path_b)

            for c in agg_json:
                path_c = os.path.join(path_b, c)
                with open(path_c, 'r') as file:
                    dfile = json.load(file)

                for dk,dv in dfile['data']['hoverData'].items():
                    district = dk.split(' district')[0]
                    reg_user = dv['registeredUsers']
                    app_opens = dv['appOpens']
                    
                    data['state'].append(a)
                    data['year'].append(b)
                    data['quarter'].append('Q' + str(c[0]))
                    data['district'].append(district)
                    data['reg_users'].append(reg_user)
                    data['app_opens'].append(app_opens)

    return data


def top_transaction():
    path = "D:\\PhonePe_Data\\data\\top\\transaction\\country\\india\\state\\"
    agg_state = os.listdir(path)

    data = {'state':[],'year':[],'quarter':[],'pincode':[],'transaction_count':[], 'transaction_amount':[]}

    for a in agg_state:
        path_a = os.path.join(path, a)
        agg_year = os.listdir(path_a)

        for b in agg_year:
            path_b = os.path.join(path_a, b)
            agg_json = os.listdir(path_b)

            for c in agg_json:
                path_c = os.path.join(path_b, c)
                with open(path_c, 'r') as file:
                    dfile = json.load(file)

                for d in dfile['data']['pincodes']:
                    pincode = d['entityName']
                    trans_count = d['metric']['count']
                    trans_amount = d['metric']['amount']                 

                    data['state'].append(a)
                    data['year'].append(b)
                    data['quarter'].append('Q' + str(c[0])) 
                    data['pincode'].append(pincode)
                    data['transaction_count'].append(trans_count)
                    data['transaction_amount'].append(trans_amount)                       
    return data


def top_user():
    path = "D:\\PhonePe_Data\\data\\top\\user\\country\\india\\state\\"
    agg_state = os.listdir(path)

    data = {'state':[],'year':[],'quarter':[],'pincode':[],'registered_users':[]}

    for a in agg_state:
        path_a = os.path.join(path, a)
        agg_year = os.listdir(path_a)

        for b in agg_year:
            path_b = os.path.join(path_a, b)
            agg_json = os.listdir(path_b)

            for c in agg_json:
                path_c = os.path.join(path_b, c)
                with open(path_c, 'r') as file:
                    dfile = json.load(file)

                for d in dfile['data']['pincodes']:
                    pincode = d['name']
                    reg_user = d['registeredUsers']          

                    data['state'].append(a)
                    data['year'].append(b)
                    data['quarter'].append('Q' + str(c[0])) 
                    data['pincode'].append(pincode)
                    data['registered_users'].append(reg_user)

    return data


def update_state_names(df, geojson_data):
    # Getting state names from the GeoJSON data in alphabetical order
    state_names = [f['properties']['ST_NM'] for f in geojson_data['features']]
    states = sorted(state_names)
    # Create a mapping dictionary for old and new state names
    state_mapping = dict(zip(df['state'].sort_values().unique(), states))
    # Updating the 'state' column with new state names
    df['state'] = df['state'].map(state_mapping)
    return df

# Fetching GeoJSON data from the URL
geojson_data = 'https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson'
response = requests.get(geojson_data)
geojson_data = json.loads(response.content)

# convarting all datas using custom functions into data frame
at0 = pd.DataFrame(aggregate_transaction())
au0 = pd.DataFrame(aggregate_user())
mt0 = pd.DataFrame(map_transaction())
mu0 = pd.DataFrame(map_user())
tt0 = pd.DataFrame(top_transaction())
tu0 = pd.DataFrame(top_user())

# converting state names accordingly to the geojson name format
at = update_state_names(at0, geojson_data)
au = update_state_names(au0, geojson_data)
mt = update_state_names(mt0, geojson_data)
mu = update_state_names(mu0, geojson_data)
tt = update_state_names(tt0, geojson_data)
tu = update_state_names(tu0, geojson_data)

# to export all the created datfames to MySQL database 
sql.execute("CREATE DATABASE IF NOT EXISTS phonepe_pulse")
sql.execute("USE phonepe_pulse")
sql = mydb.cursor()

# Function for creating tables and insering values into the tables 
def Data_Transform():
    try:
        sql.execute("CREATE TABLE Aggregate_Transaction(State VARCHAR(50), Year INT, Quarter CHAR(2), Transaction_type VARCHAR(30), Transaction_count INT, Transaction_amount BIGINT)")
        insert1 = "INSERT INTO Aggregate_Transaction(State, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount) VALUES (%s,%s,%s,%s,%s,%s)"  
        for i in range(0,len(at)):
            sql.execute(insert1, tuple(at.iloc[i]))
            mydb.commit()

        sql.execute("CREATE TABLE Aggregate_User(State VARCHAR(50), Year INT, Quarter CHAR(2), User_Brand VARCHAR(15), User_Count INT, User_Percentage FLOAT)")
        insert2 = "INSERT INTO Aggregate_User(State, Year, Quarter, User_Brand, User_Count, User_Percentage) VALUES (%s,%s,%s,%s,%s,%s)"  
        for i in range(len(au)):
            sql.execute(insert2,tuple(au.iloc[i]))
            mydb.commit()

        sql.execute("CREATE TABLE Map_Transaction(State VARCHAR(50), Year INT, Quarter CHAR(2), District VARCHAR(50), Transaction_count INT, Transaction_amount BIGINT)")
        insert3 = "INSERT INTO Map_Transaction(State, Year, Quarter, District, Transaction_count, Transaction_amount) VALUES (%s,%s,%s,%s,%s,%s)"  
        for i in range(len(mt)):
            sql.execute(insert3,tuple(mt.iloc[i]))
            mydb.commit()

        sql.execute("CREATE TABLE Map_User(State VARCHAR(50), Year INT, Quarter CHAR(2), District VARCHAR(50), Registered_users INT, App_opens INT)")
        insert4 = "INSERT INTO Map_User(State, Year, Quarter, District, Registered_users, App_opens) VALUES (%s,%s,%s,%s,%s,%s)"
        for i in range(len(mu)):
            sql.execute(insert4,tuple(mu.iloc[i]))
            mydb.commit()

        sql.execute("CREATE TABLE Top_Transaction(State VARCHAR(50), Year INT, Quarter CHAR(2), Pincode VARCHAR(20), Transaction_count INT, Transaction_amount BIGINT)")
        insert5 = "INSERT INTO Top_Transaction(State, Year, Quarter, Pincode, Transaction_count, Transaction_amount) VALUES (%s,%s,%s,%s,%s,%s)"
        for i in range(len(tt)):
            sql.execute(insert5,tuple(tt.iloc[i]))
            mydb.commit()

        sql.execute("CREATE TABLE Top_User(State VARCHAR(50), Year INT, Quarter CHAR(2), Pincode VARCHAR(20), Registered_users INT)")
        insert6 = "INSERT INTO Top_User(State, Year, Quarter, Pincode, Registered_users) VALUES (%s,%s,%s,%s,%s)"
        for i in range(len(tu)):
            sql.execute(insert6,tuple(tu.iloc[i]))
            mydb.commit()
    except:
        pass



# Functions to get list of items from the extracted data
def state_list():
    df = pd.read_sql_query('SELECT DISTINCT state FROM map_user ORDER BY state', mydb)
    state_list = list(df['state'])
    state_list.insert(0,' ')
    return state_list

def district_lists(state_name):
    df = pd.read_sql_query(f"SELECT DISTINCT district,state FROM map_user WHERE state='{state_name}'ORDER BY district", mydb)
    districts = list(df['district'])
    districts.insert(0,' ')
    return districts

def user_brand():
    df = pd.read_sql_query('SELECT DISTINCT user_brand FROM aggregate_user ORDER BY user_brand',mydb)
    brands = list(df['user_brand'])
    brands.insert(0,' ')
    return brands

# Fetching GeoJSON data from the URL
url = 'https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson'

# Creating a calss for all the plots used from Plotly Library
class Plotly():
    # Creating the choropleth map using Plotly Express
    def choropleth_plot(data, location_value, color_value, title_name,title_x):
        fig = px.choropleth(data, geojson=url , featureidkey='properties.ST_NM', locations=location_value,
                            color=color_value, color_continuous_scale='sunset', title=title_name, width=600, height=800)
        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(title_x=title_x, title_y=0.85, title_font=dict(size=28), title_font_color='#6739B7')
        st.plotly_chart(fig,theme="streamlit", use_container_width=True)
    
    # Pie plt using Plotly express
    def pie_plot(df, x, y, title,title_x=0.15):
        fig = px.pie(df, names=x, values=y, title=title,color_discrete_sequence=['#3C9D4E', '#7031AC', '#C94D6D', '#E4BF58', '#FD6787','#4174C9'])
        fig.update_layout(title_x=title_x, title=dict(font=dict(size=20, family='serif', color='black')))
        st.plotly_chart(fig, theme="streamlit", use_container_width=True)

    # Bar plot using Plotly express 
    def bar_plot(df,x,y,title,xaxis,yaxis,title_x):
        fig = px.bar(df, x=x,y=y,title=title,text_auto='.2s',height=600,width=600)
        fig.update_traces(marker_color= '#6739B7', textfont_size = 14, textangle = 0, textposition = "outside")
        fig.update_layout(autosize=False, xaxis_title=xaxis,yaxis_title=yaxis,title_x=title_x,title_y=0.93, 
        title=dict(font=dict(size=22, family='serif', color='black')),
        xaxis= dict(title_font=dict(size=18, family='serif', color='black'), tickfont=dict(size=14, family='serif')), 
        yaxis= dict(title_font=dict(size=16, family='serif', color='black'), tickfont=dict(size=14, family='serif')),)
        st.plotly_chart(fig,  use_container_width=True)
    
    # Horizontal Bar plot using Plotly express 
    def horizontal_bar_plot(df,x,y,title,xaxis,yaxis,title_x,width,height):
        fig = px.bar(df, x=x,y=y,title=title,text_auto='.2s',width=width,height=height)
        fig.update_traces(marker_color= '#6739B7', textfont_size = 14, textangle = 0, textposition = "outside")
        fig.update_layout(xaxis_title=xaxis,yaxis_title=yaxis,title_x=title_x,title_y=0.97, 
        title=dict(font=dict(size=22, family='serif', color='black')),
        xaxis= dict(title_font=dict(size=18, family='serif', color='black'), tickfont=dict(size=14, family='serif')), 
        yaxis= dict(title_font=dict(size=18, family='serif', color='black'), tickfont=dict(size=14, family='serif')),)
        st.plotly_chart(fig, theme="streamlit", use_container_width=True)


# Configuring Stramlit page
st.set_page_config(page_title='PhonePe Pulse', layout="wide")
# Theme of Streamlit Page
base = "light"
primaryColor = "#6739b7"
font = "serif"
secondaryColor = "#F0F2F6"
textColor = "#31333F"
backgroundColor = "#FFFFFF"


# Streamlit Page Title Markdown
st.markdown(f'<h1 style="text-align: center; color: #6739B7">PhonePe Pulse Data Visualization and Exploration</h1>', unsafe_allow_html=True)    
st.write('')
st.write('')

# creating an option menu for various data exploration steps
select = option_menu(None, options=['About Data','Data Analysis','Exit Data'],orientation='horizontal')
# About Data  
if select=="About Data":
    image1 = Image.open("D:\PhonePe_Logo.svg.png")
    cm1,cm2,cm3 = st.columns(3)  
    with cm1:
        st.write('')
    with cm2:
        st.image(image1, caption ='Source Credit - https://en.wikipedia.org/wiki/PhonePe', width=350)
    with cm3:
        st.write('')
    st.write('')
    st.write('''PhonePe is an Indian digital payments and financial services company headquartered in Bengaluru, Karnataka, India. 
             The PhonePe app, based on the Unified Payments Interface (UPI), by using PhonePe, users can send and receive money, recharge mobile, DTH, data cards, make utility payments, pay at shops, invest in tax saving funds, buy insurance, mutual funds, and digital gold.
             PhonePe Pulse is the window to the world of how India transacts with interesting trends, deep insights and in-depth analysis based on the data put together by the PhonePe team.''') 
    st.write('')
    st.subheader(':violet[Problem Statement]')
    st.write('The Phonepe pulse Github repository contains a wealth of data related to various metrics and statistics. The challenge is to extract and process this data to derive insights and information that can be visually presented in a user-friendly manner.')
    st.write('Here\'s how we approach this problem:')
    
    # Add interactive bullet points
    st.markdown("- :red[**Data Extraction:**] The PhonePe data is cloned from the GitHub repository and stored locally as JSON format files.")
    st.markdown("- :red[**Data Transformation:**] Unstructured data is converted into a structured form and stored in a MySQL database using Python, Pandas, and SQL.")
    st.markdown("- :red[**Data Exploration:**] The stored data is retrieved from the database, analyzed, and explored using the Plotly Python library for visualization. The results are presented through a Streamlit application.")
    st.write(' ')
    
    st.header(':violet[PhonePe Pulse - Exploring Indian Online Money Transactions]')
    st.write('')
    st.subheader('Data Explorations include:')
    # Add interactive bullet points
    st.markdown("- :red[**Aggregated Transaction:**] State-wise Transaction Amount and Transaction Count based on different years, quarters, and transaction types.")
    st.markdown("- :red[**Aggregated User:**] PhonePe Users State-wise based on different years, quarters, and user phone brand.")
    st.markdown("- :red[**Map Transaction:**] Total Transaction amount and count district-wise for all years and quarters of all states.")
    st.markdown("- :red[**Map User:**] Total number of Registered users and app opens state and district-wise for all years and quarters.")
    st.markdown("- :red[**Top Transaction:**] Top 10 highest transaction Amount and count of all state's top districts and pincodes for all years and quarters.")
    st.markdown("- :red[**Top User:**] Top 10 highest Registered Users of all state's top districts and pincodes for all years and quarters.")
    st.subheader('Guide through for this analysis')
    st.write('The data is categorized into:')
    st.markdown(":red[**- Transaction**]")
    st.markdown(":red[**- Users**]")
    
    st.write('For each category, there are sub-categories such as:')
    
    sc1, sc2, sc3 = st.columns(3)
    with sc1:
        # Add interactive bullet points
        st.markdown("#### :violet[Location-based Analysis]")
        st.markdown("- Overall Country-wise - India")
        st.markdown("- State-wise")
        st.markdown("- District-wise")
        st.markdown("- Pincode-wise")
        st.markdown("- Top Categories of each")
        
    with sc2:
        # Add interactive bullet points
        st.markdown("#### :violet[Time-based Analysis]")
        st.markdown("- Year-wise")
        st.markdown("- Quarter-wise")
        st.markdown("- Top Categories of each")

    with sc3:
        # Add interactive bullet points
        st.markdown("#### :violet[Type-based Analysis]")
        st.markdown("- Transaction Type-wise")
        st.markdown("- User Brand-wise")



#  Data Anlysois and Exploration 
elif select=="Data Analysis":
    main = st.selectbox('Select any Process',('','Data Extraction','Data Transform','Data Exploration'))
    if main=='Data Extraction':
        st.subheader(':violet[Data Collection and Extraction:]')
        st.markdown('- In the data extraction phase, Python is employed to clone data from the PhonePe Pulse GitHub repository. This raw data is then locally stored in JSON format.') 
        st.markdown('- As part of this step, the unstructured data is transformed into a structured format in preparation for preprocessing.')    
        if st.button("**Extract Data**"):
            Data_Collection()
            st.success('**Extracted!**')
    
    elif main=='Data Transform': 
        st.subheader(':violet[Data Transformation:]')
        st.markdown('- During the data transformation phase, the collected data undergoes essential preprocessing processes, such as data cleaning and handling missing values.') 
        st.markdown('- Following this, the processesd and structured data is inserted into a MySQL Database, serving as a Ware House of datas for further in-depth analysis and visualization.')
        if st.button('**Transform Data**'):
            Data_Transform()
            st.success('**Transformed!**')
    
    elif main=='Data Exploration':
        transaction, users = st.tabs(['Trasactions','Users'])
        with transaction:
            location, time, type = st.tabs(['Location based Anlysis','Time based Anlysis','Type based Anlysis'])
            with location:
                tab_india, tab_state, tab_district, tab_pincode =st.tabs(['India','States','Districts','Pincodes'])
                with tab_india:
                    year_select = st.select_slider('Select a Year', options = ['All Years','2018','2019','2020','2021','2022','2023'], key='sst_i')
                    if year_select == "All Years":
                        df1 = pd.read_sql_query(f"SELECT state, SUM(transaction_amount) AS Transaction_amount, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction GROUP BY state",mydb)
                        Plotly.choropleth_plot(df1,'state','Transaction_amount','Overall State wise Transaction Amount (INR) by All Years', 0.2)
                        Plotly.choropleth_plot(df1,'state','Transaction_count','Overall State wise Transaction Count by All Years',0.25)
                    if year_select != "All Years":
                        df2 = pd.read_sql_query(f"SELECT state, SUM(transaction_amount) AS Transaction_amount, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction WHERE year={year_select} GROUP BY state",mydb)
                        Plotly.choropleth_plot(df2,'state','Transaction_amount',f"Overall State wise Transaction Amount (INR) by {year_select}", 0.2)
                        Plotly.choropleth_plot(df2,'state','Transaction_count',f"Overall State wise Transaction Count by {year_select}",0.25)
                    
                with tab_state:
                    overall_t_state, indepth_t_state, top_t_state = st.tabs(['Overall Analysis','Indepth Analysis','Top Categories'])
                    with overall_t_state:
                        state = st.selectbox('Select State', state_list(),key='ost_s')
                        if st.button('Search by State', key='ost_ss'):                           
                            df1 = pd.read_sql_query(f"SELECT state, year, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction WHERE state='{state}' GROUP BY year ORDER BY Transaction_amount",mydb)
                            df2 = pd.read_sql_query(f"SELECT state, year, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction WHERE state='{state}' GROUP BY year ORDER BY Transaction_count",mydb)
                            Plotly.bar_plot(df1,'year','Transaction_amount',f"{state} Year wise Transaction Amount (INR)", ' Years ', 'Transaction Amount (INR)',0.3)
                            Plotly.bar_plot(df2,'year','Transaction_count',f"{state} Year wise Transaction Count", ' Years ', 'Transaction Count',0.35)
                            col1,col2 = st.columns(2)
                            with col1:
                                Plotly.pie_plot(df1,df1['year'],df1['Transaction_amount'],f"{state} Year wise Transaction Amount")
                            with col2:
                                Plotly.pie_plot(df2,df2['year'],df2['Transaction_count'],f"{state} Year wise Transaction Count")
                    
                    with indepth_t_state:
                        col1,col2,col3,col4 = st.columns(4)
                            # Adding buttons to choose which data to display
                        with col1:
                            state = st.selectbox('Select State', state_list(),key='ist_s')
                        with col2:
                            year = st.selectbox('Select Year', ('', '2018', '2019', '2020', '2021', '2022', '2023'), key='ist_y')
                            show_by_year = st.button("Show by Year",key='ist_sy')
                        with col3:
                            quarter = st.selectbox('Select Quarter', ('', 'Q1', 'Q2', 'Q3', 'Q4'), key='ist_q')
                            show_by_quarter = st.button("Show by Quarter",key='ist_sq')
                        with col4:
                            transaction = st.selectbox('Select Transaction type', ('', 'Recharge & bill payments', 'Peer-to-peer payments', 'Merchant payments', 'Financial Services', 'Others'), key='ist_t')
                            show_by_transaction = st.button("Show by Transaction Type", key='ist_st')

                        if show_by_year:
                            if state and year:
                                df1 = pd.read_sql_query(f"SELECT state, year, quarter, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction WHERE state='{state}' and year='{year}' GROUP BY quarter ORDER BY Transaction_amount", mydb)
                                df2 = pd.read_sql_query(f"SELECT state, year, quarter, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction WHERE state='{state}' and year='{year}' GROUP BY quarter ORDER by Transaction_count", mydb)
                            Plotly.bar_plot(df1, 'quarter', 'Transaction_amount', f"{state} Transaction Amount (INR) in {year}", f" Quarters of {year}", 'Transaction Amount (INR)', 0.3)
                            Plotly.bar_plot(df2, 'quarter', 'Transaction_count', f"{state} Transaction Count in {year}", f" Quarters of {year}", 'Transaction Count', 0.34)    
                            col1, col2 = st.columns(2)
                            with col1:
                                Plotly.pie_plot(df1, df1['quarter'], df1['Transaction_amount'], f"{state} Transaction Amount in {year}")
                            with col2:
                                Plotly.pie_plot(df2, df2['quarter'], df2['Transaction_count'], f"{state} Transaction Count in {year}")
                            
                        if show_by_quarter:
                            if state and year and quarter:
                                df3 = pd.read_sql_query(f"SELECT state, year, quarter, transaction_type, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction WHERE state='{state}' and year='{year}' and quarter='{quarter}' GROUP BY transaction_type ORDER BY Transaction_amount", mydb)
                                df4 = pd.read_sql_query(f"SELECT state, year, quarter, transaction_type, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction WHERE state='{state}' and year='{year}' and quarter='{quarter}' GROUP BY transaction_type ORDER by Transaction_count", mydb)
                            Plotly.horizontal_bar_plot(df3, 'Transaction_amount', 'transaction_type', f"{state} Transaction Amount (INR) in {year}-{quarter}", 'Transaction Amount (INR)'," Transaction Types ", 0.3,300,500)
                            Plotly.horizontal_bar_plot(df4, 'Transaction_count', 'transaction_type', f"{state} Transaction Count in {year}-{quarter}", 'Transaction Count', " Transaction Types ", 0.35,300,500)
                            col1, col2 = st.columns(2)
                            with col1:
                                Plotly.pie_plot(df3, df3['transaction_type'], df3['Transaction_amount'], f"{state} Transaction Amount in {year}-{quarter}")                      
                            with col2:
                                Plotly.pie_plot(df4, df4['transaction_type'], df4['Transaction_count'], f"{state} Transaction Count in {year}-{quarter}")                          

                        if show_by_transaction:
                            if state and year and transaction:
                                df5 = pd.read_sql_query(f"SELECT year, transaction_type, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction WHERE transaction_type='{transaction}' GROUP BY year ORDER BY Transaction_amount", mydb)
                                df6 = pd.read_sql_query(f"SELECT year, transaction_type, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction WHERE transaction_type='{transaction}' GROUP BY year ORDER by Transaction_Count", mydb)
                                Plotly.bar_plot(df5, 'year', 'Transaction_amount', f"{state} Transaction Amount (INR) in {year} by {transaction}", f"{transaction} type Year wise", 'Transaction Amount (INR)',  0.3)
                                Plotly.bar_plot(df6, 'year', 'Transaction_count', f"{state} Transaction Count in {year} by {transaction}", f"{transaction} type Year wise", 'Transaction Count',  0.32)
                            col1, col2 = st.columns(2)
                            with col1:
                                Plotly.pie_plot(df5, df5['year'], df5['Transaction_amount'], f"Transaction Amount by {transaction}")
                            with col2:
                                Plotly.pie_plot(df6, df6['year'], df6['Transaction_count'], f"Transaction Count by {transaction}")
                    
                    with top_t_state:
                        col1,col2 = st.columns(2)
                        with col1:
                            year = st.selectbox('Select Year', ('', '2018', '2019', '2020', '2021', '2022', '2023'), key='tts_y')
                            show_by_year = st.button("Show by Year",key='tts_sy')
                        with col2:
                            quarter = st.selectbox('Select Quarter', ('', 'Q1', 'Q2', 'Q3', 'Q4'), key='tts_q')
                            show_by_quarter = st.button("Show by Quarter",key='tts_sq')

                        if st.button('Show by State', key='tts_ss'):
                            df1 = pd.read_sql_query(f"SELECT state, SUM(transaction_amount) AS Transaction_amount FROM Top_transaction GROUP BY state ORDER BY Transaction_amount DESC LIMIT 10",mydb)
                            df2 = pd.read_sql_query(f"SELECT state, SUM(transaction_count) AS Transaction_count FROM Top_transaction GROUP BY state ORDER BY Transaction_count DESC LIMIT 10",mydb)
                            Plotly.bar_plot(df1, 'state', 'Transaction_amount', "Top States with Highest Transaction Amount", 'Top States name', 'Transaction Amount', 0.3)
                            Plotly.bar_plot(df2, 'state', 'Transaction_count', "Top States with Highest Transaction Count", 'Transaction Count', 'Top States name',  0.3)                    
                        if year and show_by_year:
                            df3 = pd.read_sql_query(f"SELECT state, SUM(transaction_amount) AS Transaction_amount FROM Top_transaction WHERE year='{year}' GROUP BY state ORDER BY Transaction_amount DESC LIMIT 10",mydb)
                            df4 = pd.read_sql_query(f"SELECT state, SUM(transaction_count) AS Transaction_count FROM Top_transaction WHERE year='{year}' GROUP BY state ORDER BY Transaction_count DESC LIMIT 10",mydb)
                            Plotly.bar_plot(df3, 'state', 'Transaction_amount', f"Top States with Highest Transaction Amount in {year}", 'Top States name', 'Transaction Amount', 0.27)
                            Plotly.bar_plot(df4, 'state', 'Transaction_count', f"Top States with Highest Transaction Count in {year}", 'Transaction Count', 'Top States name', 0.29)
                        if quarter and show_by_quarter:
                            df5 = pd.read_sql_query(f"SELECT state, SUM(transaction_amount) AS Transaction_amount FROM Top_transaction WHERE year='{year}' and quarter='{quarter}' GROUP BY state ORDER BY Transaction_amount DESC LIMIT 10",mydb)
                            df6 = pd.read_sql_query(f"SELECT state, SUM(transaction_count) AS Transaction_count FROM Top_transaction WHERE year='{year}' and quarter='{quarter}' GROUP BY state ORDER BY Transaction_count DESC LIMIT 10",mydb)
                            Plotly.bar_plot(df5, 'state', 'Transaction_amount', f"Top States with Highest Transaction Amount in {year}-{quarter}", 'Top States name', 'Transaction Amount', 0.26)
                            Plotly.bar_plot(df6, 'state', 'Transaction_count', f"Top States with Highest Transaction Count in {year}-{quarter}", 'Transaction Count', 'Top States name', 0.28)
                            
                with tab_district:
                    overall_t_district , indepth_t_district, top_t_district = st.tabs(['Overall Analysis','Indepth Analysis','Top Categories'])
                    with overall_t_district:
                        col1,col2 = st.columns(2)
                        with col1:
                            state = st.selectbox('Select State', state_list(),key='otd_s')
                            show_by_state = st.button("Show by State", key='otd_ss')
                        with col2:
                            district = st.selectbox('Slelct District', district_lists(state), key='otd_d')
                            show_by_district = st.button("Show by District", key='otd_sd')
                        if state and show_by_state:    
                            df1 = pd.read_sql_query(f"SELECT district, state, SUM(transaction_amount) AS Transaction_amount FROM map_transaction WHERE state='{state}' GROUP BY district ORDER BY Transaction_amount",mydb)
                            df2 = pd.read_sql_query(f"SELECT district, state, SUM(transaction_count) AS Transaction_count FROM map_transaction WHERE state='{state}' GROUP BY district ORDER BY Transaction_count",mydb)
                            Plotly.horizontal_bar_plot(df1,'Transaction_amount','district', 'District wise Transaction Amount (INR)', 'Transaction Amount (INR)',f"Districts of {state}",0.3,600,1000)
                            Plotly.horizontal_bar_plot(df2,'Transaction_count','district','District wise Transaction Count','Transaction Count', f"District of {state}",0.32, 600,1000)

                        if state and district and show_by_district:
                            df1 = pd.read_sql_query(f"SELECT district, state, year, SUM(transaction_amount) AS Transaction_amount FROM map_transaction WHERE state='{state}' and district='{district}' GROUP BY year ORDER BY Transaction_amount",mydb)
                            df2 = pd.read_sql_query(f"SELECT district, state, year, SUM(transaction_count) AS Transaction_count FROM map_transaction WHERE state='{state}' and district='{district}' GROUP BY year ORDER BY Transaction_count",mydb)
                            Plotly.bar_plot(df1, 'year', 'Transaction_amount', f"{state}-{district} Transaction Amount by All Years", ' Year ', 'Transaction Amount (INR)',  0.3)
                            Plotly.bar_plot(df2, 'year', 'Transaction_count', f"{state}-{district} Transaction Count by All Years", ' Year ', 'Transaction Count',  0.3)   
                            col1, col2 = st.columns(2)
                            with col1:
                                Plotly.pie_plot(df1, df1['year'], df1['Transaction_amount'], f"{state}-{district} Transaction Amount")                                                           
                            with col2:
                                Plotly.pie_plot(df2, df2['year'], df2['Transaction_count'], f"{state}-{district} Transaction Count")
                    
                    with indepth_t_district:
                        col1,col2 = st.columns(2)
                        with col1:
                            state = st.selectbox('Select State', state_list(),key='itd_s')
                            year = st.selectbox('Select Year', ('', '2018', '2019', '2020', '2021', '2022', '2023'), key='itd_y')
                            show_by_year = st.button("Show by Year", key='itd_sy')
                        with col2:
                            district = st.selectbox('Slelct District', district_lists(state), key='itd_d')
                            quarter = st.selectbox('Select Quarter', ('', 'Q1', 'Q2', 'Q3', 'Q4'), key='itd_q')
                            show_by_quarter = st.button("Show by Quarter", key='itd_sq')
                        if state and district and show_by_year:
                            df1 = pd.read_sql_query(f"SELECT district, state, quarter, SUM(transaction_amount) AS Transaction_amount FROM map_transaction WHERE state='{state}' AND district='{district}' AND year='{year}' GROUP BY quarter ORDER BY Transaction_amount",mydb)
                            df2 = pd.read_sql_query(f"SELECT district, state, quarter, SUM(transaction_count) AS Transaction_count FROM map_transaction WHERE state='{state}' AND district='{district}' AND year='{year}' GROUP BY quarter ORDER BY Transaction_count",mydb)
                            Plotly.bar_plot(df1, 'quarter', 'Transaction_amount', f"{state}-{district} Transaction Amount by {year}", f"Quarters of {year}", 'Transaction Amount (INR)',  0.3)
                            Plotly.bar_plot(df2, 'quarter', 'Transaction_count', f"{state}-{district} Transaction Count by {year}", f"Quarters of {year}", 'Transaction Count',  0.32)   
                            col1, col2 = st.columns(2)
                            with col1:
                                Plotly.pie_plot(df1, df1['quarter'], df1['Transaction_amount'], f"{state}-{district} Transaction Amount by {year}")                                                           
                            with col2:
                                Plotly.pie_plot(df2, df2['quarter'], df2['Transaction_count'], f"{state}-{district} Transaction Count by {year}")
                        if state and district and show_by_quarter:
                            df1 = pd.read_sql_query(f"SELECT district, state, year, SUM(transaction_amount) AS Transaction_amount FROM map_transaction WHERE state='{state}' AND district='{district}' AND quarter='{quarter}' GROUP BY year ORDER BY Transaction_amount",mydb)
                            df2 = pd.read_sql_query(f"SELECT district, state, year, SUM(transaction_count) AS Transaction_count FROM map_transaction WHERE state='{state}' AND district='{district}' AND quarter='{quarter}' GROUP BY year ORDER BY Transaction_count",mydb)
                            Plotly.bar_plot(df1, 'year', 'Transaction_amount', f"{state}-{district} Transaction Amount by All {quarter}", f"Years of all {quarter}", 'Transaction Amount (INR)',  0.26)
                            Plotly.bar_plot(df2, 'year', 'Transaction_count', f"{state}-{district} Transaction Count by All {quarter}", f"Years of all {quarter}", 'Transaction Count',  0.28)  
                            col1, col2 = st.columns(2)
                            with col1:
                                Plotly.pie_plot(df1, df1['year'], df1['Transaction_amount'], f"{state}-{district} Transaction Amount by All {quarter}")                                                           
                            with col2:
                                Plotly.pie_plot(df2, df2['year'], df2['Transaction_count'], f"{state}-{district} Transaction Count by All {quarter}")
                    
                    with top_t_district:
                        col1,col2,col3 = st.columns(3)
                        with col1:
                            state = st.selectbox('Select State', state_list(),key='ttd_d')
                            show_by_state = st.button("Show by State",key='ttd_ss')
                        with col2:
                            year = st.selectbox('Select Year', ('', '2018', '2019', '2020', '2021', '2022', '2023'), key='ttd_y')
                            show_by_year = st.button("Show by Year",key='ttd_sy')
                        with col3:
                            quarter = st.selectbox('Select Quarter', ('', 'Q1', 'Q2', 'Q3', 'Q4'), key='ttd_q')
                            show_by_quarter = st.button("Show by Quarter",key='ttd_sq')

                        if state and show_by_state:
                            df1 = pd.read_sql_query(f"SELECT district, SUM(transaction_amount) AS Transaction_amount FROM Map_transaction WHERE state='{state}' GROUP BY district ORDER BY Transaction_amount DESC LIMIT 10",mydb)
                            df2 = pd.read_sql_query(f"SELECT district, SUM(transaction_count) AS Transaction_count FROM Map_transaction WHERE state='{state}' GROUP BY district ORDER BY Transaction_count DESC LIMIT 10",mydb)
                            Plotly.bar_plot(df1, 'district', 'Transaction_amount', f"Top Districts of {state} with Highest Transaction Amount", 'Top Districts name', 'Transaction Amount', 0.3)
                            Plotly.bar_plot(df2, 'district', 'Transaction_count', f"Top District of {state}s with Highest Transaction Count", 'Top Districts name', 'Transaction Count',  0.32)                    
                        if year and show_by_year:
                            df3 = pd.read_sql_query(f"SELECT district, SUM(transaction_amount) AS Transaction_amount FROM Map_transaction WHERE year='{year}' and state='{state}' GROUP BY district ORDER BY Transaction_amount DESC LIMIT 10",mydb)
                            df4 = pd.read_sql_query(f"SELECT district, SUM(transaction_count) AS Transaction_count FROM Map_transaction WHERE year='{year}' and state='{state}' GROUP BY district ORDER BY Transaction_count DESC LIMIT 10",mydb)
                            Plotly.bar_plot(df3, 'district', 'Transaction_amount', f"Top Districts of {state} with Highest Transaction Amount in {year}", 'Top Districts name', 'Transaction Amount', 0.28)
                            Plotly.bar_plot(df4, 'district', 'Transaction_count', f"Top Districts of {state} with Highest Transaction Count in {year}", 'Top Districts name', 'Transaction Count', 0.3)
                        if quarter and show_by_quarter:
                            df5 = pd.read_sql_query(f"SELECT district, SUM(transaction_amount) AS Transaction_amount FROM Map_transaction WHERE year='{year}' and state='{state}' and quarter='{quarter}' GROUP BY district ORDER BY Transaction_amount DESC LIMIT 10",mydb)
                            df6 = pd.read_sql_query(f"SELECT district, SUM(transaction_count) AS Transaction_count FROM Map_transaction WHERE year='{year}' and state='{state}' and quarter='{quarter}' GROUP BY district ORDER BY Transaction_count DESC LIMIT 10",mydb)
                            Plotly.bar_plot(df5, 'district', 'Transaction_amount', f"Top Districts of {state} with Highest Transaction Amount in {year}-{quarter}", 'Top Districts name', 'Transaction Amount', 0.26)
                            Plotly.bar_plot(df6, 'district', 'Transaction_count', f"Top Districts of {state} with Highest Transaction Count in {year}-{quarter}", 'Top Districts name', 'Transaction Count', 0.28)


                with tab_pincode: 
                    state = st.selectbox('Select State', state_list(),key='tp_s') 
                    show_by_state = st.button("Show by State", key='tp_ss')
                    if state and show_by_state:                            
                        df1 = pd.read_sql_query(f"SELECT pincode, state, SUM(transaction_amount) AS Transaction_amount, SUM(transaction_count) AS Transaction_count FROM top_transaction WHERE state='{state}' GROUP BY pincode ORDER BY Transaction_amount DESC",mydb)
                        st.subheader(f"Transaction Amount of Pincodes of {state}")
                        st.write(' ')
                        st.bar_chart(data=df1, x='pincode', y='Transaction_amount', width=800, height=800, use_container_width=True)
                        st.subheader(f"Transaction Count of Pincodes of {state}")
                        st.write(' ')
                        st.bar_chart(data=df1, x='pincode', y='Transaction_count', width=800, height=800, use_container_width=True)
                        

            with time:
                year_t_tab , quarter_t_tab, top_t_tab = st.tabs(['Year','Quarter','Top Categories'])
                with year_t_tab:
                    if st.button('Show by all Years'):
                        df1 = pd.read_sql_query(f"SELECT year, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction GROUP BY year ORDER BY Transaction_amount",mydb)
                        df2 = pd.read_sql_query(f"SELECT year, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction GROUP BY year ORDER BY Transaction_count",mydb)
                        Plotly.bar_plot(df1,'year','Transaction_amount', "Year wise Transaction Amount (INR) by all States", ' Years ', 'Transaction Amount (INR)',0.3)
                        Plotly.bar_plot(df2,'year','Transaction_count','Year wise Transaction Count by all States', ' Years ', 'Transaction Count',0.35)
                        col1,col2 = st.columns(2)
                        with col1:
                            Plotly.pie_plot(df1,df1['year'],df1['Transaction_amount'],'Year wise Transaction Amount ')
                        with col2:
                            Plotly.pie_plot(df2,df2['year'],df2['Transaction_count'],'Year wise Transaction Count')
            
                    year = st.selectbox('Select year', ('', '2018', '2019', '2020', '2021', '2022', '2023'), key='tty_y')
                    show_by_year = st.button('Show by Year', key='tty_sy')
                    if year and show_by_year:
                        df3 = pd.read_sql_query(f"SELECT quarter, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction WHERE year='{year}' GROUP BY quarter ORDER BY Transaction_amount",mydb)
                        df4 = pd.read_sql_query(f"SELECT quarter, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction WHERE year='{year}' GROUP BY quarter ORDER BY Transaction_count",mydb)
                        Plotly.bar_plot(df3,'quarter','Transaction_amount', f"Quarter wise Transaction Amount (INR) by all States in {year}", f"Quarters of {year}" , 'Transaction Amount (INR)',0.28)
                        Plotly.bar_plot(df4,'quarter','Transaction_count',f"Quarter wise Transaction Count by all States in {year}", f"Quarters of {year}" , 'Transaction Count',0.3)    
                        col1,col2 = st.columns(2)
                        with col1:
                            Plotly.pie_plot(df3,df3['quarter'],df3['Transaction_amount'],f"Quarter wise Transaction Amount in {year}")
                        with col2:
                            Plotly.pie_plot(df4,df4['quarter'],df4['Transaction_count'],f"Quarter wise Transaction Count in {year}")
                
                
                with quarter_t_tab:
                    df1 = pd.read_sql_query(f"SELECT quarter, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction GROUP BY quarter ORDER BY Transaction_amount",mydb)
                    df2 = pd.read_sql_query(f"SELECT quarter, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction GROUP BY quarter ORDER BY Transaction_count",mydb)
                    Plotly.bar_plot(df1,'quarter','Transaction_amount', "Quarter wise Transaction Amount (INR) in All Years", ' Quarters ', 'Transaction Amount (INR)',0.3)
                    Plotly.bar_plot(df2,'quarter','Transaction_count','Quarter wise Transaction Count in All Years', ' Quarters ', 'Transaction Count',0.32) 
                    col1,col2 = st.columns(2)
                    with col1:
                        Plotly.pie_plot(df1,df1['quarter'],df1['Transaction_amount'],'Quarter wise Transaction Amount')
                    with col2:
                        Plotly.pie_plot(df2,df2['quarter'],df2['Transaction_count'],'Quarter wise Transaction Count')
                
                with top_t_tab:
                    col1,col2 = st.columns(2)
                    with col1:
                        year = st.selectbox('Select Year', ('', '2018', '2019', '2020', '2021', '2022', '2023'), key='ttt_y')
                        show_by_year = st.button("Show by Year", key='ttt_sy') 
                    with col2:
                        quarter = st.selectbox('Select Quarter', ('', 'Q1', 'Q2', 'Q3', 'Q4'), key='ttt_q')
                        show_by_quarter = st.button("Show by Quarter", key='ttt_sq')
                    if year and show_by_year:
                        df1 = pd.read_sql_query(f"SELECT state, SUM(transaction_amount) AS Transaction_amount FROM Top_transaction WHERE year='{year}' GROUP BY state ORDER BY Transaction_amount DESC LIMIT 10",mydb)
                        df2 = pd.read_sql_query(f"SELECT state, SUM(transaction_count) AS Transaction_count FROM top_transaction WHERE year='{year}' GROUP BY state ORDER BY Transaction_count DESC LIMIT 10",mydb)
                        Plotly.bar_plot(df1, 'state', 'Transaction_amount', f"Top States Transaction Amount in {year}", 'Top States name', 'Transaction Amount',  0.38)
                        Plotly.bar_plot(df2, 'state', 'Transaction_count', f"Top States Transaction Count in {year}", 'Top States name', 'Transaction Count',  0.38)
    
                    if year and quarter and show_by_quarter:
                        df1 = pd.read_sql_query(f"SELECT state, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction WHERE year='{year}' and quarter='{quarter}' GROUP BY state ORDER BY Transaction_amount DESC LIMIT 10",mydb)
                        df2 = pd.read_sql_query(f"SELECT state, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction WHERE year='{year}' and quarter='{quarter}' GROUP BY state ORDER BY Transaction_count DESC LIMIT 10",mydb)
                        Plotly.bar_plot(df1, 'state', 'Transaction_amount', f"Top States Transaction Amount in {year}-{quarter}", 'Top States name', 'Transaction Amount',  0.37)
                        Plotly.bar_plot(df2, 'state', 'Transaction_count', f"Top States Transaction Count in {year}-{quarter}", 'Top States name', 'Transaction Count',  0.37)

                        
            with type:
                overall_t_type, indepth_t_type, top_t_type= st.tabs(['Overall Analysis','Indepth Analysis','Top Categories'])
                with overall_t_type:
                    df1 = pd.read_sql_query(f"SELECT transaction_type, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction GROUP BY transaction_type ORDER BY Transaction_amount", mydb)
                    df2 = pd.read_sql_query(f"SELECT transaction_type, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction GROUP BY transaction_type ORDER by Transaction_Count", mydb)
                    
                    Plotly.horizontal_bar_plot(df1,'Transaction_amount','transaction_type', "Overall Transaction Amount (INR) in all Years by Each Transaction Type", 'Transaction Amount (INR)', 'Transaction Type', 0.15,600,700)
                    Plotly.horizontal_bar_plot(df2, 'Transaction_count', 'transaction_type', "Overall Transaction Count in all Years by Each Transaction Type", 'Transaction Count', 'Transaction Type', 0.2,600,700)
                    st.write(' ')
                    col1, col2 = st.columns(2)
                    with col1:    
                        Plotly.pie_plot(df1, df1['transaction_type'], df1['Transaction_amount'], ' Transaction Amount by Each Transaction Type')                      
                    with col2:
                        Plotly.pie_plot(df2, df2['transaction_type'], df2['Transaction_count'], ' Transaction Count by Each Transaction Type')
            
                with indepth_t_type:
                    col1,col2,col3 = st.columns(3)
                    with col1:
                        year = st.selectbox('Select Year', ('', '2018', '2019', '2020', '2021', '2022', '2023'), key='itt_y')
                        show_by_year = st.button("Show by Year", key='itt_sy') 
                    with col2:
                        quarter = st.selectbox('Select Quarter', ('', 'Q1', 'Q2', 'Q3', 'Q4'), key='itt_q')
                        show_by_quarter = st.button("Show by Quarter", key='itt_sq')
                    with col3:
                        transaction = st.selectbox('Select Transaction type', ('', 'Recharge & bill payments', 'Peer-to-peer payments', 'Merchant payments', 'Financial Services', 'Others'), key='itt_t')
                        show_by_transaction = st.button("Show by Transaction Type",key='itt_st')

                    if year and show_by_year:
                        df1 = pd.read_sql_query(f"SELECT year, transaction_type, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction WHERE year='{year}' GROUP BY transaction_type ORDER BY Transaction_amount", mydb)
                        df2 = pd.read_sql_query(f"SELECT year, transaction_type, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction  WHERE year='{year}' GROUP BY transaction_type ORDER by Transaction_Count", mydb)
                        
                        Plotly.horizontal_bar_plot(df1,'Transaction_amount','transaction_type', f"Overall Transaction Amount (INR) in {year} by Each Transaction Type", 'Transaction Amount (INR)', 'Transaction Type', 0.15,600,700)
                        Plotly.horizontal_bar_plot(df2, 'Transaction_count', 'transaction_type', f"Overall Transaction Count in {year} by Each Transaction Type", 'Transaction Count', 'Transaction Type', 0.2,600,700)
                        st.write(' ')
                        col1, col2 = st.columns(2)
                        with col1:    
                            Plotly.pie_plot(df1, df1['transaction_type'], df1['Transaction_amount'], f" Transaction Amount in {year}")                      
                        with col2:
                            Plotly.pie_plot(df2, df2['transaction_type'], df2['Transaction_count'], f" Transaction Count in {year}")
                    
                    if year and quarter and show_by_quarter:
                        df3 = pd.read_sql_query(f"SELECT transaction_type, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction WHERE year='{year}' and quarter='{quarter}' GROUP BY transaction_type ORDER BY Transaction_amount", mydb)
                        df4 = pd.read_sql_query(f"SELECT transaction_type, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction  WHERE year='{year}' and quarter='{quarter}' GROUP BY transaction_type ORDER by Transaction_Count", mydb)
                        
                        Plotly.horizontal_bar_plot(df3,'Transaction_amount','transaction_type', f"Overall Transaction Amount (INR) in {year}-{quarter} by Each Transaction Type", 'Transaction Amount (INR)', 'Transaction Type', 0.15,600,700)
                        Plotly.horizontal_bar_plot(df4, 'Transaction_count', 'transaction_type', f"Overall Transaction Count in {year}-{quarter} by Each Transaction Type", 'Transaction Count', 'Transaction Type', 0.2,600,700)
                        st.write(' ')
                        col1, col2 = st.columns(2)
                        with col1:    
                            Plotly.pie_plot(df3, df3['transaction_type'], df3['Transaction_amount'], f" Transaction Amount in {year}-{quarter} ")                      
                        with col2:
                            Plotly.pie_plot(df4, df4['transaction_type'], df4['Transaction_count'], f" Transaction Count in {year}-{quarter} ")
                    
                    if year and show_by_transaction:
                        df5 = pd.read_sql_query(f"SELECT year, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction WHERE transaction_type='{transaction}' GROUP BY year ORDER BY Transaction_amount", mydb)
                        df6 = pd.read_sql_query(f"SELECT year, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction  WHERE transaction_type='{transaction}' GROUP BY year ORDER by Transaction_Count", mydb)
                        
                        Plotly.bar_plot(df5,'year', 'Transaction_amount', f"Overall Transaction Amount (INR) in all Years by {transaction}", 'Transaction Amount (INR)', ' Year ', 0.21)
                        Plotly.bar_plot(df6, 'year', 'Transaction_count', f"Overall Transaction Count in all Years by {transaction}", 'Transaction Count', ' Year ', 0.23)
                        st.write(' ')
                        col1, col2 = st.columns(2)
                        with col1:    
                            Plotly.pie_plot(df5, df5['year'], df5['Transaction_amount'],  f"Transaction Amount by {transaction}")                      
                        with col2:
                            Plotly.pie_plot(df6, df6['year'], df6['Transaction_count'],  f"Transaction Count by  {transaction}")

                with top_t_type:
                    col1,col2,col3 = st.columns(3)
                    with col1:
                        transaction = st.selectbox('Select Transaction type', ('', 'Recharge & bill payments', 'Peer-to-peer payments', 'Merchant payments', 'Financial Services', 'Others'), key='ttt_t')
                        show_by_transaction = st.button("Show by Transaction Type",key='ttt_st')
                    with col2:
                        year = st.selectbox('Select Year', ('', '2018', '2019', '2020', '2021', '2022', '2023'), key='tt_y')
                        show_by_year = st.button("Show by Year", key='tt_sy')     
                    with col3:
                        quarter = st.selectbox('Select Quarter', ('', 'Q1', 'Q2', 'Q3', 'Q4'), key='tt_q')
                        show_by_quarter = st.button("Show by Quarter", key='tt_sq')
                    if transaction and show_by_transaction:
                        df1 = pd.read_sql_query(f"SELECT state, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction WHERE transaction_type='{transaction}' GROUP BY state ORDER BY Transaction_amount DESC LIMIT 10", mydb)
                        df2 = pd.read_sql_query(f"SELECT state, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction  WHERE transaction_type='{transaction}' GROUP BY state ORDER by Transaction_Count DESC LIMIT 10", mydb)
                        
                        Plotly.horizontal_bar_plot(df1,'Transaction_amount','state', f"Top Highest Transaction Amount (INR) in all Years by {transaction}", 'Transaction Amount (INR)', ' Top States name ', 0.2, 600,800)
                        Plotly.horizontal_bar_plot(df2, 'Transaction_count', 'state', f"Top Highest Transaction Count in all Years by {transaction}", 'Transaction Count', ' Top States name ', 0.2, 600,800)
                        
                        
                    if transaction and year and show_by_year:
                        df1 = pd.read_sql_query(f"SELECT state, year, transaction_type, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction WHERE transaction_type='{transaction}' and year='{year}' GROUP BY state ORDER BY Transaction_amount DESC LIMIT 10", mydb)
                        df2 = pd.read_sql_query(f"SELECT state, year, transaction_type, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction  WHERE transaction_type='{transaction}' and year='{year}' GROUP BY state ORDER by Transaction_Count DESC LIMIT 10", mydb)
                        
                        Plotly.horizontal_bar_plot(df1,'Transaction_amount','state', f"Top Highest Transaction Amount (INR) in {year} by {transaction}", 'Transaction Amount (INR)', ' Top States name ', 0.2, 600,800)
                        Plotly.horizontal_bar_plot(df2, 'Transaction_count', 'state', f"Top Highest Transaction Count in {year} by {transaction}", 'Transaction Count', ' Top States name ', 0.2, 600,800)
                        
                    if transaction and year and quarter and show_by_quarter:
                        df1 = pd.read_sql_query(f"SELECT state, year, quarter, transaction_type, SUM(transaction_amount) AS Transaction_amount FROM Aggregate_Transaction WHERE transaction_type='{transaction}' and year='{year}' and quarter='{quarter}' GROUP BY state ORDER BY Transaction_amount DESC LIMIT 10", mydb)
                        df2 = pd.read_sql_query(f"SELECT state, year, quarter, transaction_type, SUM(transaction_count) AS Transaction_count FROM Aggregate_Transaction  WHERE transaction_type='{transaction}' and year='{year}'  and quarter='{quarter}' GROUP BY state ORDER by Transaction_Count DESC LIMIT 10", mydb)
                        
                        Plotly.horizontal_bar_plot(df1,'Transaction_amount','state', f"Top Highest Transaction Amount (INR) in {year}-{quarter} by {transaction}", 'Transaction Amount (INR)', ' Top States name ', 0.18, 600,800)
                        Plotly.horizontal_bar_plot(df2, 'Transaction_count', 'state', f"Top Highest Transaction Count in {year}-{quarter} by {transaction}", 'Transaction Count', ' Top States name ', 0.2, 600,800)



    # User wise Analysis
        with users:
            location_user, time_user, type_user = st.tabs(['Location based Anlysis','Time based Anlysis','Type based Anlysis'])
            with location_user:
                tab_u_india, tab_u_state, tab_u_district, tab_u_pincode =st.tabs(['India','States','Districts','Pincodes'])
                with tab_u_india:
                    year_select = st.select_slider('Select a Year', options = ['All Years','2018','2019','2020','2021','2022'], key='ssu_i')
                    if year_select == 'All Years':
                        df1 = pd.read_sql_query(f"SELECT state, SUM(user_count) AS User_Count FROM Aggregate_User GROUP BY state",mydb)
                        Plotly.choropleth_plot(df1,'state','User_Count','Overall State wise User Count for All Years', 0.25)
                    if year_select != 'All Years':
                        df2 = pd.read_sql_query(f"SELECT state, SUM(user_count) AS User_Count FROM Aggregate_User WHERE year={year_select} GROUP BY state",mydb)
                        Plotly.choropleth_plot(df2,'state','User_Count',f"Overall State wise User Count for {year_select}", 0.25)

                with tab_u_state:
                    overall_u_state,indepth_u_state,top_u_state = st.tabs(['Overall Analysis','Indepth Analysis','Top Categories'])
                    with overall_u_state:
                        state = st.selectbox('Select State', state_list(),key='ous_s')
                        if st.button('Show by State', key='ous_ss'):                           
                            df1 = pd.read_sql_query(f"SELECT state, year, SUM(user_count) AS User_Count FROM Aggregate_User WHERE state='{state}' GROUP BY year ORDER BY user_count",mydb)
                            Plotly.bar_plot(df1,'year','User_Count',f"{state} Year wise User Count", '  Year  ', ' User Count ',0.35)
                            Plotly.pie_plot(df1,df1['year'],df1['User_Count'],f"{state} Year wise User Count",0.3)
                    
                    with indepth_u_state:
                        col1,col2,col3,col4 = st.columns(4)
                            # Adding buttons to choose which data to display
                        with col1:
                            state = st.selectbox('Select State', state_list(),key='ius_s')
                        with col2:
                            year = st.selectbox('Select Year', ('', '2018', '2019', '2020', '2021', '2022', '2023'), key='ius_y')
                            show_by_year = st.button("Show by Year",key='ius_sy')
                        with col3:
                            quarter = st.selectbox('Select Quarter', ('', 'Q1', 'Q2', 'Q3', 'Q4'), key='ius_q')
                            show_by_quarter = st.button("Show by Quarter",key='ius_sq')
                        with col4:
                            brand = st.selectbox('Select User Brand', user_brand(), key='ius_b')
                            show_by_brand = st.button("Show by User Brand", key='ius_sb')

                        if state and year and show_by_year:
                            df1 = pd.read_sql_query(f"SELECT quarter, SUM(user_count) AS User_Count FROM Aggregate_User WHERE state='{state}' and year='{year}' GROUP BY quarter ORDER BY User_Count", mydb)
                            Plotly.bar_plot(df1, 'quarter', 'User_Count', f"{state} User Count in {year}", f" Quarters of {year}", 'User Count', 0.35)
                            Plotly.pie_plot(df1, df1['quarter'], df1['User_Count'], f"{state} User Count in {year}",0.35) 

                        if state and year and quarter and show_by_quarter:
                            df2 = pd.read_sql_query(f"SELECT user_brand, SUM(user_count) AS User_Count FROM Aggregate_user WHERE state='{state}' and year='{year}' and quarter='{quarter}' GROUP BY user_brand ORDER BY User_Count", mydb)
                            Plotly.bar_plot(df2, 'User_Count', 'user_brand', f"{state} User Count in {year}-{quarter} by all User brands", ' User Count ', " User Brand ", 0.3)
                            Plotly.pie_plot(df2, df2['user_brand'], df2['User_Count'], f"{state} User Count in {year}-{quarter} by all User brands",0.3)   
                            
                        if state and year and quarter and brand and show_by_brand:
                            df3 = pd.read_sql_query(f"SELECT year, user_brand, SUM(user_count) AS User_Count FROM Aggregate_User WHERE user_brand='{brand}' GROUP BY year ORDER BY User_Count", mydb)
                            Plotly.bar_plot(df3, 'year', 'User_Count', f"{state} User Count in {year} by {brand}", f"{brand} type Year wise", 'User Count',  0.35)
                            Plotly.pie_plot(df3, df3['year'], df3['User_Count'], f"{state} User Count in {year} by {brand}",0.32)

                    with top_u_state:
                        df1 = pd.read_sql_query(f"SELECT state, SUM(user_count) AS User_Count FROM Aggregate_user GROUP BY state ORDER BY User_Count DESC LIMIT 10",mydb)
                        Plotly.horizontal_bar_plot(df1, 'User_Count','state', "Top Highest States in User Count by All Years", 'User Count','Top States name', 0.3,600,800)
                        
                with tab_u_district:
                    overall_u_district, indepth_u_district, top_u_district= st.tabs(['Overall Analysis','Indepth Analysis','Top Categories'])
                    with overall_u_district:
                        col1,col2 = st.columns(2)
                        with col1:
                            state = st.selectbox('Select State', state_list(),key='oud_s')
                            show_by_state = st.button("Show by State", key='oud_ss')
                        with col2:
                            district = st.selectbox('Slelct District', district_lists(state), key='ooud_d')
                            show_by_district = st.button("Show by District", key='ooud_sd')
                        if state and show_by_state:    
                            df1 = pd.read_sql_query(f"SELECT district, state, SUM(registered_users) AS Registered_users FROM Map_user WHERE state='{state}' GROUP BY district ORDER BY Registered_users",mydb)
                            df2 = pd.read_sql_query(f"SELECT district, state, SUM(app_opens) AS App_opens FROM Map_user WHERE state='{state}' GROUP BY district ORDER BY App_opens",mydb)
                            Plotly.horizontal_bar_plot(df1,'Registered_users','district', f"{state} District wise Registered Users", 'Registered Users', f"Districts of {state}",0.35, 600,1200)
                            Plotly.horizontal_bar_plot(df2,'App_opens','district',f"{state}District wise App Opens",'App Opens', f"Districts of {state}",0.30, 600,1200)

                        if state and district and show_by_district:
                            df1 = pd.read_sql_query(f"SELECT district, state, year, SUM(registered_users) AS Registered_users FROM Map_user WHERE state='{state}' and district='{district}' GROUP BY year ORDER BY Registered_users",mydb)
                            df2 = pd.read_sql_query(f"SELECT district, state, year, SUM(app_opens) AS App_opens FROM Map_user WHERE state='{state}' and district='{district}' GROUP BY year ORDER BY App_opens",mydb)
                            Plotly.bar_plot(df1, 'year', 'Registered_users', f"{state}-{district} Registered Users by All Years", ' Year ', 'Registered Users',  0.28)
                            Plotly.bar_plot(df2, 'year', 'App_opens', f"{state}-{district} App Opens by All Years", ' Year ', 'App Opens',  0.3)    
                            col1, col2 = st.columns(2)
                            with col1:
                                Plotly.pie_plot(df1, df1['year'], df1['Registered_users'], f"{district} Registered Users by All Years",0.25)                                                           
                            with col2:
                                Plotly.pie_plot(df2, df2['year'], df2['App_opens'], f"{district} App Opens by All Years",0.25)
                    
                    with indepth_u_district:
                        col1,col2 = st.columns(2)
                        with col1:
                            state = st.selectbox('Select State', state_list(),key='iud_s')
                            year = st.selectbox('Select Year', ('', '2018', '2019', '2020', '2021', '2022', '2023'), key='iud_y')
                            show_by_year = st.button("Show by Year", key='iud_sy')
                        with col2:
                            district = st.selectbox('Slelct District', district_lists(state), key='iud_d')
                            quarter = st.selectbox('Select Quarter', ('', 'Q1', 'Q2', 'Q3', 'Q4'), key='iud_q')
                            show_by_quarter = st.button("Show by Quarter", key='iud_sq')
                        if state and district and show_by_year:
                            df1 = pd.read_sql_query(f"SELECT district, state, quarter, SUM(registered_users) AS Registered_users FROM Map_user WHERE state='{state}' AND district='{district}' AND year='{year}' GROUP BY quarter ORDER BY Registered_users",mydb)
                            df2 = pd.read_sql_query(f"SELECT district, state, quarter, SUM(app_opens) AS App_opens FROM Map_user WHERE state='{state}' AND district='{district}' AND year='{year}' GROUP BY quarter ORDER BY App_opens",mydb)
                            Plotly.bar_plot(df1, 'quarter', 'Registered_users', f"{state}-{district} Registered Users by {year}", f"Quarters of {year}", 'Registered Users',  0.27)
                            Plotly.bar_plot(df2, 'quarter', 'App_opens', f"{state}-{district} App Opens by {year}", f"Quarters of {year}", 'App Opens',  0.3)
                            col1, col2 = st.columns(2)
                            with col1:
                                Plotly.pie_plot(df1, df1['quarter'], df1['Registered_users'], f"{district} Registered Users by {year}",0.25)                                                           
                            with col2:
                                Plotly.pie_plot(df2, df2['quarter'], df2['App_opens'], f"{district} App Opens by {year}",0.25)
                        if state and district and show_by_quarter:
                            df1 = pd.read_sql_query(f"SELECT district, state, year, SUM(registered_users) AS Registered_users FROM Map_user WHERE state='{state}' AND district='{district}' AND quarter='{quarter}' GROUP BY year ORDER BY Registered_users",mydb)
                            df2 = pd.read_sql_query(f"SELECT district, state, year, SUM(app_opens) AS App_opens FROM Map_user WHERE state='{state}' AND district='{district}' AND quarter='{quarter}' GROUP BY year ORDER BY App_opens",mydb)
                            Plotly.bar_plot(df1, 'year', 'Registered_users', f"{state}-{district} Registered Users by All {quarter}", f"{quarter} of all Years", 'Registered Users',  0.26)
                            Plotly.bar_plot(df2, 'year', 'App_opens', f"{state}-{district} App Opens by All {quarter}", f"{quarter} of all Years", 'App Opens',  0.3)
                            col1, col2 = st.columns(2)
                            with col1:
                                Plotly.pie_plot(df1, df1['year'], df1['Registered_users'], f"{district} Registered Users by All {quarter}",0.22)                                                           
                            with col2:
                                Plotly.pie_plot(df2, df2['year'], df2['App_opens'], f"{district} App Opens by All {quarter}",0.22)
                    
                    with top_u_district:
                        state = st.selectbox('Select State', state_list(),key='tud_s')
                        show_by_state = st.button("Show by State", key='tud_ss')
                        if state and show_by_state:
                            df1 = pd.read_sql_query(f"SELECT state, district, SUM(registered_users) AS Registered_users FROM Map_user WHERE state='{state}' GROUP BY district ORDER BY Registered_users DESC LIMIT 10",mydb)
                            df2 = pd.read_sql_query(f"SELECT state, district, SUM(app_opens) AS App_opens FROM Map_user WHERE state='{state}' GROUP BY district ORDER BY App_opens DESC LIMIT 10",mydb)
                            Plotly.horizontal_bar_plot(df1,'Registered_users', 'district', f"Top Districts of {state}\'s Registered Users", 'Registered Users', f"Top Districts of {state}", 0.32,600,600)
                            Plotly.horizontal_bar_plot(df2, 'App_opens','district', f"Top Districts of {state}\'s App Opens",'App Opens', f"Top Districts of {state}", 0.35,600,600)
                            
                with tab_u_pincode: 
                    state = st.selectbox('Select State', state_list(),key='up_s') 
                    show_by_state = st.button("Show by State", key='up_ss')
                    if state and show_by_state:                            
                        df1 = pd.read_sql_query(f"SELECT pincode, SUM(registered_users) AS Resitered_Users FROM top_user WHERE state='{state}' GROUP BY pincode ORDER BY Resitered_Users DESC",mydb)
                        st.subheader(f"Resitered Users by Top Pincodes of {state}")
                        st.bar_chart(data=df1, x='pincode', y='Resitered_Users', width=800, height=800, use_container_width=True)
                        
            with time_user:
                tab_u_year , tab_u_quarter, tab_u_top = st.tabs(['Year','Quarter','Top Categories'])
                with tab_u_year:
                    df1 = pd.read_sql_query(f"SELECT year, SUM(user_count) as User_Count FROM Aggregate_user GROUP BY year ORDER BY User_Count",mydb)
                    Plotly.bar_plot(df1,'year','User_Count', "Year wise Total User Count", ' Year ', 'User Count',0.4)
                    Plotly.pie_plot(df1,df1['year'],df1['User_Count'],"Year wise Total User Count",0.4)


                with tab_u_quarter:
                    df1 = pd.read_sql_query(f"SELECT quarter, SUM(user_count) as User_Count FROM Aggregate_user GROUP BY quarter ORDER BY User_Count",mydb)
                    Plotly.bar_plot(df1,'quarter','User_Count', "Quarter wise User Count of All Years", ' Quarters ', 'User Count',0.35)
                    Plotly.pie_plot(df1,df1['quarter'],df1['User_Count'],"Quarter wise User Count of All Years", 0.35)
                
                with tab_u_top:
                    col1,col2 = st.columns(2)
                    with col1:
                        year = st.selectbox('Select Year', ('', '2018', '2019', '2020', '2021', '2022', '2023'), key='tu_y')
                        show_by_year = st.button("Show by Year", key='tu_sy') 
                    with col2:
                        quarter = st.selectbox('Select Quarter', ('', 'Q1', 'Q2', 'Q3', 'Q4'), key='tu_q')
                        show_by_quarter = st.button("Show by Quarter", key='tu_sq')
                    if year and show_by_year:
                        df1 = pd.read_sql_query(f"SELECT state, SUM(user_count) AS User_count FROM Aggregate_user WHERE year='{year}' GROUP BY state ORDER BY User_count DESC LIMIT 10",mydb)
                        Plotly.horizontal_bar_plot(df1, 'User_count', 'state', f"Top Highest User Count States in {year}", 'User Count', 'Top States name', 0.35,600,600)
                    if year and quarter and show_by_quarter:
                        df2= pd.read_sql_query(f"SELECT state, SUM(user_count) AS User_count FROM Aggregate_user WHERE year='{year}' and quarter='{quarter}' GROUP BY state ORDER BY User_count DESC LIMIT 10",mydb)
                        Plotly.horizontal_bar_plot(df2, 'User_count', 'state', f"Top Highest User Count States in {year}-{quarter}", 'User Count','Top States name', 0.35,600,600)
                        

            with type_user:
                overall_u_type, indepth_u_type, top_u_type= st.tabs(['Overall Analysis','Indepth Analysis','Top Categories'])
                with overall_u_type:
                    df1 = pd.read_sql_query(f"SELECT user_brand, SUM(user_count) as User_Count FROM Aggregate_user GROUP BY user_brand ORDER BY User_Count", mydb)
                    Plotly.horizontal_bar_plot(df1,'User_Count','user_brand', "Overall User Count in all Years by Each User Brand", 'User Count', 'User Brand', 0.28,600,900)
                    
                with indepth_u_type:
                    col1,col2,col3 = st.columns(3)
                    with col1:
                        year = st.selectbox('Select Year', ('', '2018', '2019', '2020', '2021', '2022', '2023'), key='iut_y')
                        show_by_year = st.button("Show by Year", key='iut_sy') 
                    with col2:
                        quarter = st.selectbox('Select Quarter', ('', 'Q1', 'Q2', 'Q3', 'Q4'), key='iut_q')
                        show_by_quarter = st.button("Show by Quarter", key='iut_sq')
                    with col3:
                        brand = st.selectbox('Select User Brand', user_brand() , key='iut_b')
                        show_by_brand = st.button("Show by User Brand",key='iut_sb')

                    if year and show_by_year:
                        df1 = pd.read_sql_query(f"SELECT user_brand, SUM(user_count) as User_Count FROM Aggregate_user WHERE year='{year}' GROUP BY user_brand ORDER BY User_Count", mydb)
                        Plotly.horizontal_bar_plot(df1,'User_Count','user_brand', f"Overall User Count in {year} by Each User Brand", 'User Count', 'User Brand', 0.27,600,900)
                        
                    if year and quarter and show_by_quarter:
                        df2 = pd.read_sql_query(f"SELECT user_brand, SUM(user_count) as User_Count FROM Aggregate_user WHERE year='{year}' and quarter='{quarter}' GROUP BY user_brand ORDER BY User_Count", mydb)
                        Plotly.horizontal_bar_plot(df2,'User_Count','user_brand', f"Overall User Count in {year}-{quarter} by Each User Brand", 'User Count', 'User Brand', 0.27,600,900)
                        
                    if year and show_by_brand:
                        df3 = pd.read_sql_query(f"SELECT year,  SUM(user_count) as User_Count FROM Aggregate_user WHERE user_brand='{brand}' GROUP BY year ORDER BY User_Count", mydb)
                        Plotly.bar_plot(df3,'year', 'User_Count', f"Overall User Count in all Years by {brand}", 'User Count', ' Year ', 0.3)

                with top_u_type:
                    col1,col2,col3 = st.columns(3)
                    with col1:
                        brand = st.selectbox('Select User Brand', user_brand(), key='tut_b')
                        show_by_brand = st.button("Show by User Brand",key='tut_sb')
                    with col2:
                        year = st.selectbox('Select Year', ('', '2018', '2019', '2020', '2021', '2022', '2023'), key='tut_y')
                        show_by_year = st.button("Show by Year", key='tut_sy')     
                    with col3:
                        quarter = st.selectbox('Select Quarter', ('', 'Q1', 'Q2', 'Q3', 'Q4'), key='tut_q')
                        show_by_quarter = st.button("Show by Quarter", key='tut_sq')
                    if brand and show_by_brand:
                        df1 = pd.read_sql_query(f"SELECT state, user_brand, SUM(user_count) as User_Count FROM Aggregate_user WHERE user_brand='{brand}' GROUP BY state ORDER BY User_Count DESC LIMIT 10", mydb)
                        Plotly.horizontal_bar_plot(df1,'User_Count','state', f"Top Highest User Count States in all Years by {brand}", 'User Count', ' Top States name ', 0.28, 600,800)
                        
                    if brand and year and show_by_year:
                        df2 = pd.read_sql_query(f"SELECT state, year, user_brand, SUM(user_count) as User_Count FROM Aggregate_user WHERE user_brand='{brand}' and year='{year}' GROUP BY state ORDER BY User_Count DESC LIMIT 10", mydb)
                        Plotly.horizontal_bar_plot(df2,'User_Count','state', f"Top Highest User Count States in {year} by {brand}", 'User Count', ' Top States name ', 0.28, 600,800)
                        
                    if brand and year and quarter and show_by_quarter:
                        df3 = pd.read_sql_query(f"SELECT state, year, quarter, user_brand, SUM(user_count) as User_Count FROM Aggregate_user WHERE user_brand='{brand}' and year='{year}' and quarter='{quarter}' GROUP BY state ORDER BY User_Count DESC LIMIT 10", mydb)
                        Plotly.horizontal_bar_plot(df3,'User_Count','state', f"Top Highest User Count States in {year}-{quarter} by {brand}", 'User Count', ' Top States name ', 0.25, 600,800)


elif select=='Exit Data':
    st.markdown("<h1 style='text-align: center; color: #6739B7;'>Thank You!!!</h1>", unsafe_allow_html=True)
