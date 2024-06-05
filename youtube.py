import streamlit as st
from PIL import Image
from streamlit_option_menu import option_menu
import plotly as px
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import pandas as pd
import re
import mysql.connector
from sqlalchemy import create_engine
from tabulate import tabulate




#API key connection to interact with youtube API
api_service_name = "youtube"
api_version = "v3"
api_Key="AIzaSyCcWiQkZYP4bg2FF1eh7F4QWKEJb3roICM"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_Key)

#mycurser and engine created to intreact with MYSQL Database
mydb = mysql.connector.connect(host="localhost",user="root",password="")
mycursor = mydb.cursor(buffered=True)
engine = create_engine("mysql+mysqlconnector://root:@localhost/youtube")

#to create and use the database in MYSQL database 
mycursor.execute('create database if not exists youtube')
mycursor.execute('use youtube')

#setting up streamlit page and adding name to it
icon=Image.open("image.jpeg")
st.set_page_config(page_title='YouTube Data Harvesting and Warehousing',
                    page_icon=icon,
                    layout='wide',
                    initial_sidebar_state='expanded',
)

#setting up streamlit sidebar menu with optins
with st.sidebar:
    selected =option_menu("Main Menu",
                        ["Home","Data collection and upload","MYSQL Database","Analysis using SQL"],
                        icons=["house","cloud-upload","database", "filetype-sql"],
                        menu_icon="menu-up",
                        orientation="vertical")
    
    # Setting up the option "Home" in streamlit page
if selected == "Home":
    st.title(':red[You]Tube :blue[Data Harvesting & Warehousing using SQL]')
    st.subheader(':blue[Domain :] Social Media')
    st.subheader(':blue[Overview :]')
    st.markdown('''Build a simple dashboard or UI using Streamlit and 
                retrieve YouTube channel data with the help of the YouTube API.
                Stored the data in an SQL database(warehousing) managed by XAMPP control panel,
                enabling querying of the data using SQL.Visualize the data within the Streamlit app to uncover insights,
                trends with the YouTube channel data''')
    st.subheader(':blue[Skill Take Away :]')
    st.markdown(''' Python scripting,Data Collection,API integration,Data Management using SQL,Streamlit''')
    st.subheader(':blue[About :]')
    st.markdown('''Hello! I'm Tamil, a BBA graduate with a keen interest in data science and analytics.
                Currently on an exciting journey into the world of data science,
                this is my first project title as YouTube data harvesting and warehousing using SQL, 
                where I explored the vast realm of YouTube data to extract meaningful insights.
                This experience ignited my passion for data-driven decision-making and deepened my understanding of
                data extraction techniques and Database management.''')

# get channel details

def get_channel_info(channel_id): 
    request =youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id         
        )
    response = request.execute()
    
    for item in response['items']:
        channel_data={
                "channel_name":item['snippet']['title'],
                "channel_id":item['id'],
                "channel_Description":item['snippet']['description'],
                "channel_Thumbnail":item['snippet']['thumbnails']['default']['url'],
                "playlist_id":item['contentDetails']['relatedPlaylists']['uploads'],
                "subscription_Count":item['statistics']['subscriberCount'],
                "channel_Views":item['statistics']['viewCount'],
                "video_Count":item['statistics']['videoCount']    
        }
        
    return (channel_data)



#Get video ids
def get_video_ids(channel_id):
    video_id=[]
    request = youtube.channels().list(
                    part="snippet,statistics,contentDetails",
                    id=channel_id
                    )
    response = request.execute()

    Playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None
    while True:
        request = youtube.playlistItems().list(
                        part="snippet",
                        playlistId=Playlist_id,
                        maxResults=50,
                        pageToken=next_page_token
                        )
        response1= request.execute()       

        for i in range(len(response1['items'])):
            video_id.append(response1['items'][i]["snippet"]["resourceId"]['videoId'])

        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
            
    return (video_id)

# convert duration in video data collection


def iso8601_to_seconds(Duration):

    pattern = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S?)")
    match=pattern.match(Duration)
    if match:
        hours = int (match.group(1)) if match.group(1) else 0
        minutes = int (match.group(2)) if match.group(2) else 0
        seconds = int (match.group(3)) if match.group(3) else 0

        total_seconds = hours * 3600 + minutes * 60 + seconds
        
        return  total_seconds
    else: 
        return None
    
    # get video data

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
                part="snippet,contentDetails,statistics,status",
                id=video_id,
                maxResults=50
            )
        response = request.execute()


        for item in response['items']:
            tags = item['snippet'].get('tags', [])
            tags_str = ','.join(tags)  # Convert list to comma-separated string

            
            data=dict(
                    channel_id=item['snippet']['channelId'],
                    channel_name=item['snippet']['channelTitle'],    
                    video_id=video_id,
                    video_name=item['snippet']['title'],
                    video_Description=item['snippet']['description'],
                    Tags=tags_str,
                    PublishedAt=item['snippet']['publishedAt'],
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Caption_Status=item['contentDetails']['caption'],
                    Duration=iso8601_to_seconds(item['contentDetails'].get('duration')),
                    View_Count=item['statistics']['viewCount'], 
                    Like_Count=item.get('likeCount'),                               #.get('likeCount') #['statistics']['likeCount']
                    Comment_Count=item['statistics']['commentCount'],#.get ("commentcount")#['statistics']['commentCount']
                    Favorite_Count=item['statistics']['favoriteCount']
            )
            video_data.append(data)

    return (video_data)

# get comment informetion 

def get_comment_info(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50,
                
            )
            response = request.execute()
            for item in response['items']:
                data =dict(
                    channel_id=item['snippet']['channelId'],
                    video_id=item['snippet']['videoId'],
                    comment_id=item['snippet']['topLevelComment']['id'],
                    comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_PublishedAt=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comment_data.append(data)
                
    except:
        pass

    return (comment_data)

# get playlist details
def get_playlist_info(channel_id):
    All_data=[]
    next_page_token=None
    try:
        while True:
            
            request = youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
            response = request.execute()
            for item in response['items']:

                data=dict(
                    channel_name=item['snippet']['channelTitle'],
                    playlist_id=item['id'],
                    channel_id=item['snippet']['channelId'],
                    playlist_name=item['snippet']['title'],
                    video_count=item['contentDetails']['itemCount']
                )
                All_data.append(data)
            next_page_token=response.get('nextPageToken')
            if next_page_token is None:
                    break
    except HttpError as e:
        error_message = f"Error retrieving playlists: {e}"   # Handle specific YouTube API errors
        st.error(error_message)
    return (All_data)

#setting up the option "Data collection and upload" in streamlit page
if selected == "Data collection and upload":
    st.subheader(':blue[Data collection and upload]')
    st.markdown('''
                - Provide channel ID in the input field.
                - Clicking the 'View Details' button will display an overview of youtube channel.
                - Clicking 'Upload to MYSQL database' will store the extracted channel information,
                Playlists,Videos,Comments in MYSQL Database''')
    st.markdown('''
                :red[note:] ***you can get the channel ID :***
                open youtube - go to any channel - go to about - share cahnnel - copy the channel ID''')
    
    channel_ID = st.text_input("***Enter the channel ID in the below box :***")
    
    if st.button("View details"): # Shows the channel information from Youtube
        with st.spinner('Extraction in progress...'):
            try:
                extracted_details = get_channel_info(channel_id=channel_ID)
                st.write('**:blue[Channel Thumbnail]** :')
                st.image(extracted_details.get('channel_Thumbnail'))
                st.write('**:blue[Channel Name]** :', extracted_details['channel_name'])
                st.write('**:blue[Description]** :', extracted_details['channel_Description'])
                st.write('**:blue[Total_Videos]** :', extracted_details['video_Count'])
                st.write('**:blue[Subscriber Count]** :', extracted_details['subscription_Count'])
                st.write('**:blue[Total Views]** :', extracted_details['channel_Views'])
            except HttpError as e:
                if e.resp.status == 403 and e.error_details[0]["reason"] == 'quotaExceeded':
                    st.error(" API Quota exceeded. Please try again later.")
            except:
                st.error("Please ensure to give valid channel ID")

    
    if st.button("Upload to MYSQL database"): # upload the youtube retrieved data into MYSQL database

        with st.spinner('Upload in progress...'):
            try:
                #to create a channel table in sql database
                mycursor.execute('''create table  if not exists channel(channel_name VARCHAR(100),
                                                                        channel_id VARCHAR(50)  PRIMARY KEY,
                                                                        channel_Description TEXT ,
                                                                        channel_Thumbnail VARCHAR(50),
                                                                        playlist_id VARCHAR(50),
                                                                        subscription_Count INT(10),
                                                                        channel_Views INT(10),
                                                                        video_count INT(10))''')
                
                #to create videos table in sql database
                mycursor.execute('''create table  if not exists videos (channel_id VARCHAR(100),
                                                                        channel_name VARCHAR(100),
                                                                        video_id VARCHAR(100) PRIMARY KEY,
                                                                        video_Name VARCHAR(50),
                                                                        video_Description TEXT ,
                                                                        Tags TEXT,
                                                                        PublishedAt DATETIME,
                                                                        Thumbnail VARCHAR(200),
                                                                        Caption_Status VARCHAR(200),
                                                                        Duration VARCHAR(50),
                                                                        View_Count BIGINT,
                                                                        Like_Count BIGINT, 
                                                                        Comment_Count INT(50), 
                                                                        Favorite_Count INT(50) )''')
                                                    
                #to create comments table in sql database
                mycursor.execute('''create table  if not exists comments(channel_id  VARCHAR(100),
                                                                        video_id VARCHAR(100),
                                                                        comment_id VARCHAR(100),
                                                                        comment_Text VARCHAR(100),
                                                                        comment_Author VARCHAR(100),
                                                                        comment_PublishedAt DATETIME)''')
                
                #to create a playlist table in sql database
                mycursor.execute('''create table  if not exists playlist(channel_name VARCHAR(100),
                                                                        playlist_id VARCHAR(100) PRIMARY KEY,
                                                                        channel_ID VARCHAR(100),
                                                                        playlist_name VARCHAR(100),
                                                                        video_count INT (10))''')
                
                #Transform corresponding data's into pandas dataframe
                df=pd.DataFrame(get_channel_info(channel_id=channel_ID),index=[0])
                df1=pd.DataFrame(get_video_info(video_ids= get_video_ids(channel_id=channel_ID)))
                df2=pd.DataFrame(get_comment_info(video_ids=get_video_ids(channel_id=channel_ID)))
                df3=pd.DataFrame(get_playlist_info(channel_id=channel_ID))
                
                #load the dataframe into tabel in SQL Database
                df.to_sql('channel',engine,if_exists='append',index=False)
                df1.to_sql('videos',engine,if_exists='append',index=False)
                df2.to_sql('comments',engine,if_exists='append',index=False)
                df3.to_sql('playlist',engine,if_exists='append',index=False)
                mydb.commit()
                st.success('channel information,playlists,videos,comments are uploaded successfully')
            except :
                st.error('channel already uploaded or exist in MYSQL Database')           


# Function to retrieve channel name from SQL DB
def fetch_channel_names():
    mycursor.execute("SELECT channel_name FROM channel")
    channel_names = [row[0] for row in mycursor.fetchall()]
    return channel_names

# Function to Fetch all the related data from SQL DB
def load_channel_data(channel_name):
    # Fetch channel data
    mycursor.execute("SELECT * FROM channel WHERE channel_name = %s", (channel_name,))
    out = mycursor.fetchall()
    channel_df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description]).reset_index(drop=True)
    channel_df.index += 1

    # Fetch videos data
    mycursor.execute("SELECT * FROM videos WHERE channel_id = %s", (channel_df['channel_id'].iloc[0],))
    out = mycursor.fetchall()
    videos_df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description]).reset_index(drop=True)
    videos_df.index += 1

    # Fetch comments data
    mycursor.execute("SELECT * FROM comments WHERE video_id IN (SELECT video_id FROM videos WHERE channel_id = %s)", 
                    (channel_df['channel_id'].iloc[0],))
    out = mycursor.fetchall()
    comments_df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description]).reset_index(drop=True)
    comments_df.index += 1

    # Fetch playlists data
    mycursor.execute("SELECT * FROM playlist WHERE channel_id = %s", (channel_df['channel_id'].iloc[0],))
    out = mycursor.fetchall()
    playlists_df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description]).reset_index(drop=True)
    playlists_df.index += 1

    return channel_df, videos_df, comments_df, playlists_df

# Function to fetch all channel names from the database
def fetch_channel_names():
    mycursor.execute("SELECT channel_name FROM channel")
    channels = mycursor.fetchall()
    return [channel[0] for channel in channels]

# Setting up the option "MYSQL Database" in streamlit page
if selected == "MYSQL Database":
    st.subheader(':blue[MYSQL Database]')
    st.markdown('''__You can view the channel details along with the playlist, videos, comments in table format 
                    which is stored in MYSQL database__''')
    try:
        # Fetch all channel names for the selectbox
        channel_names = fetch_channel_names()
        selected_channel = st.selectbox(':red[Select Channel]', channel_names)

        if selected_channel:
            channel_info, videos_info, comments_info, playlist_info = load_channel_data(selected_channel)

            st.subheader(':blue[Channel Table]')
            st.write(channel_info)

            st.subheader(':blue[Videos Table]')
            st.write(videos_info)

            st.subheader(':blue[Comments Table]')
            st.write(comments_info)

            st.subheader(':blue[Playlists Table]')
            st.write(playlist_info)
    except Exception as e:
        st.error(f'Database is empty or error occurred: {e}')


#SQL Query Output need to displayed as table in Streamlit Application:
# Function to excute Query of 1st Question 
def sql_question_1():
    mycursor.execute('''SELECT channel.channel_name,videos.video_name
                        FROM videos 
                        JOIN channel ON channel.Channel_id = videos.Channel_id
                        ORDER BY channel_name''')
    out=mycursor.fetchall()
    Q1= pd.DataFrame(out, columns=['Channel Name','Videos Name']).reset_index(drop=True)
    Q1.index +=1
    st.dataframe(Q1)

# Function to excute Query of 2nd Question 
def sql_question_2():
    mycursor.execute('''SELECT channel.channel_Name, COUNT(videos.channel_id) AS video_count
FROM channel
INNER JOIN videos ON channel.channel_id= videos.channel_id
GROUP BY channel.channel_Name
ORDER BY video_count DESC
LIMIT 11;
''')
    out=mycursor.fetchall()
    Q2= pd.DataFrame(out, columns=['Channel Name','Total Videos']).reset_index(drop=True)
    Q2.index +=1
    st.dataframe(Q2)

# Function to excute Query of 3rd Question 
def sql_question_3():
    mycursor.execute('''SELECT channel.Channel_name,videos.Video_name, videos.View_Count as Total_Views
                        FROM videos
                        JOIN channel ON channel.Channel_id = videos.Channel_id
                        ORDER BY videos.View_Count DESC
                        ''')
    out=mycursor.fetchall()
    Q3= pd.DataFrame(out, columns=['Channel Name','Videos Name','Total Views']).reset_index(drop=True)
    Q3.index +=1
    st.dataframe(Q3)

# Function to excute Query of  4th Question 
def sql_question_4():
    mycursor.execute('''SELECT videos.video_name,videos.comment_count as Total_Comments
                    FROM videos
                    ORDER BY videos.comment_count DESC''')
    out=mycursor.fetchall()
    Q4= pd.DataFrame(out, columns=['Videos Name','Total Comments']).reset_index(drop=True)
    Q4.index +=1
    st.dataframe(Q4)

# Function to excute Query of 5th Question 5
def sql_question_5():
    mycursor.execute('''SELECT channel.channel_name,videos.video_name,videos.like_count as Highest_likes FROM videos 
                    JOIN channel ON videos.channel_id=channel.channel_id
                    WHERE like_count=(SELECT MAX(videos.like_count) FROM videos v WHERE videos.channel_id=v.channel_id
                    GROUP BY channel_id)
                    ORDER BY Highest_likes DESC''')
    out=mycursor.fetchall()
    Q5= pd.DataFrame(out, columns=['Channel Name','Videos Name','Likes']).reset_index(drop=True)
    Q5.index +=1
    st.dataframe(Q5)    

# Function to excute Query of 6th Question 
def sql_question_6():
    mycursor.execute('''SELECT videos.video_name,videos.like_count as Likes
                    FROM videos
                    ORDER BY videos.like_count DESC''')
    out=mycursor.fetchall()
    Q6= pd.DataFrame(out, columns=['Videos Name','Likes']).reset_index(drop=True)
    Q6.index +=1
    st.dataframe(Q6)

# Function to excute Query of 7th Question 
def sql_question_7():
    mycursor.execute('''SELECT channel.channel_name,channel.channel_views as Total_views
                    FROM channel
                    ORDER BY channel.channel_views DESC  ''')
    out=mycursor.fetchall()
    Q7= pd.DataFrame(out, columns=['Channel Name','Total views']).reset_index(drop=True)
    Q7.index +=1
    st.dataframe(Q7)

# Function to excute Query of 8th Question 
def sql_question_8():
    mycursor.execute('''SELECT DISTINCT channel.channel_name
                    FROM channel
                    JOIN videos ON  videos.channel_id=channel.channel_id
                    WHERE YEAR(videos.PublishedAt) = 2022 ''')
    out=mycursor.fetchall()
    Q8= pd.DataFrame(out, columns=['Channel Name']).reset_index(drop=True)
    Q8.index +=1
    st.dataframe(Q8)

# Function to excute Query of 9th  Question 
def sql_question_9():
    mycursor.execute('''SELECT channel.channel_name,
                    TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(videos.Duration)))), "%H:%i:%s") AS Duration
                    FROM videos
                    JOIN channel ON videos.channel_id=channel.channel_id
                    GROUP BY channel_name ''')
    out=mycursor.fetchall()
    Q9= pd.DataFrame(out, columns=['Chanel Name','Duration']).reset_index(drop=True)
    Q9.index +=1
    st.dataframe(Q9)

# Function to excute Query of 10th Question 
def sql_question_10():
    mycursor.execute('''SELECT channel.channel_name,videos.video_name,videos.comment_count as Total_Comments
                    FROM videos
                    JOIN channel ON channel.channel_id=videos.channel_id
                    ORDER BY videos.comment_count DESC''')
    out=mycursor.fetchall()
    Q10= pd.DataFrame(out, columns=['Channel Name','Videos Name','Comments']).reset_index(drop=True)
    Q10.index +=1
    st.dataframe(Q10)

# Setting up the option "Analysis using SQL" in streamlit page 
if selected == 'Analysis using SQL':
    st.subheader(':blue[Analysis using SQL]')
    st.markdown('''You can analyze the collection of YouTube channel data stored in a MySQL database.
                Based on selecting the listed questions below, the output will be displayed in a table format''')
    Questions = ['Select your Question',
        '1.What are the names of all the videos and their corresponding channels?',
        '2.Which channels have the most number of videos, and how many videos do they have?',
        '3.What are the top 10 most viewed videos and their respective channels?',
        '4.How many comments were made on each video, and what are their corresponding video names?',
        '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7.What is the total number of views for each channel, and what are their corresponding channel names?',
        '8.What are the names of all the channels that have published videos in the year 2022?',
        '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10.Which videos have the highest number of comments, and what are their corresponding channel names?' ]
    
    Selected_Question = st.selectbox(' ',options=Questions)
    if Selected_Question =='1.What are the names of all the videos and their corresponding channels?':
        sql_question_1()
    if Selected_Question =='2.Which channels have the most number of videos, and how many videos do they have?':
        sql_question_2()
    if Selected_Question =='3.What are the top 10 most viewed videos and their respective channels?': 
        sql_question_3()
    if Selected_Question =='4.How many comments were made on each video, and what are their corresponding video names?':
        sql_question_4()  
    if Selected_Question =='5.Which videos have the highest number of likes, and what are their corresponding channel names?':
        sql_question_5() 
    if Selected_Question =='6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        st.write('**:red[Note]:- Dislike property was made private as of December 13, 2021.**')
        sql_question_6()   
    if Selected_Question =='7.What is the total number of views for each channel, and what are their corresponding channel names?':
        sql_question_7()
    if Selected_Question =='8.What are the names of all the channels that have published videos in the year 2022?':
        sql_question_8()
    if Selected_Question =='9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        sql_question_9()
    if Selected_Question =='10.Which videos have the highest number of comments, and what are their corresponding channel names?':
        sql_question_10()