#Importing Packages & libraries
from googleapiclient.discovery import build
from collections import defaultdict
import pymongo
import certifi
import  psycopg2
import pandas as pd
import streamlit as st
import requests
import streamlit_lottie as st_lottie

#API Connection
def Api_connect():
    Api_key ='AIzaSyDBuNYz7y-AQfdMDbdKrRahNgoAypDEwQw'
    api_service_name='youtube'
    api_version='v3'

    youtube = build(api_service_name,api_version,developerKey=Api_key)

    return youtube

youtube=  Api_connect()


#Get Channels Information
def get_channel_stats(channel_ids):
    all_data = []
    request= youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_ids)
    response = request.execute(num_retries=10)

    for i in range(len(response['items'])):
        channel_data =dict(Channel_ID= response['items'][i]['id'],
                           Channel_Name= response['items'][i]['snippet']['title'],
                           Channel_Description=response['items'][i]['snippet']['description'],
                           Channel_Pat= response['items'][i]['snippet']['publishedAt'],
                           Playlist_iD= response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                           Views=  response['items'][i]['statistics']['viewCount'],
                           Subscriber= response['items'][i]['statistics']['subscriberCount'],
                           Video_Count= response['items'][i]['statistics']['videoCount'])
        all_data.append(channel_data)

    return all_data

#Get Videos Ids
def get_channel_videos(channel_ids):
    video_ids = []
    response1 = youtube.channels().list(id=channel_ids,
                                  part='contentDetails').execute(num_retries=10)
    playlist_id = response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
                                           part = 'snippet',
                                           playlistId = playlist_id,
                                           pageToken = next_page_token).execute(num_retries=10)

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

#get video information
def get_video_info(video_Ids):

    video_data = []

    for video_id in video_Ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id= video_id
        )
        response = request.execute()

        for items in response['items']:
            data = dict(Channel_Name = items['snippet']['channelTitle'],
                        Channel_Id = items['snippet']['channelId'],
                        Video_Id = items['id'],
                        Title = items['snippet']['title'],
                        Tags = items['snippet'].get('tags'),
                        Thumbnail = items['snippet']['thumbnails']['default']['url'],
                        Description = items['snippet']['description'],
                        Published_Date = items['snippet']['publishedAt'],
                        Duration = items['contentDetails']['duration'],
                        Views = items['statistics']['viewCount'],
                        Likes = items['statistics'].get('likeCount'),
                        Comments = items['statistics'].get('commentCount'),
                        Favorite_Count = items['statistics']['favoriteCount'],
                        Definition = items['contentDetails']['definition'],
                        Caption_Status = items['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data

#get comment information
def get_comment_info(video_Ids):
    Comment_data = []
    try:
        for video_id in video_Ids:

            request = youtube.commentThreads().list(
                part = "snippet",
                videoId = video_id,
                maxResults = 50
            )
            response = request.execute()

            for item in response["items"]:
                data = dict(Comment_Id = item["snippet"]["topLevelComment"]["id"],
                          Video_Id = item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                          Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                          Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                          Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                Comment_data.append(data)
    except:
        pass

    return Comment_data

#get playlist information
def get_playlist_details(channel_id):
        next_page_token=None
        playlist_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount'])
                        playlist_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break

        return playlist_data

#Upload to mongoDB    
ca=certifi.where()
client= pymongo.MongoClient('mongodb+srv://manavsaxena119:100manav@cluster0.qkwgndk.mongodb.net/?retryWrites=true&w=majority',tlsCAFile=ca)
db=client['youtube_data']

def channel_details(channel_id):
    ch_details = get_channel_stats(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_Ids = get_channel_videos(channel_id)
    vi_details = get_video_info(vi_Ids)
    com_details = get_comment_info(vi_Ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,
                      "comment_information":com_details})

    return "upload completed successfully"


#table creation for channel , playlist , videos , comments
def channels_table():    
    mydb= psycopg2.connect(host='localhost',
                           user='postgres',
                           password='postgres',
                           database='youtube_data',
                           port='5432')
    cursor=mydb.cursor()

    drop_query = "drop table if exists channels"
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_ID varchar(80) primary key,
                                                        Subscriber bigint,
                                                        Views bigint,
                                                        Video_Count int,
                                                        Channel_Description text,
                                                        Playlist_iD varchar(50))'''

    cursor.execute(create_query)
    mydb.commit()

    ch_list= []
    db=client['youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
      ch_list.append(ch_data['channel_information'][0])
    df=pd.DataFrame(ch_list)    

    for index,row in df.iterrows():
         insert_query ='''insert into channels(Channel_Name,
                                              Channel_ID,
                                              Subscriber,
                                              Views,
                                              Video_Count,
                                              Channel_Description,
                                              Playlist_iD)

                                              values(%s,%s,%s,%s,%s,%s,%s)'''

         values =(row['Channel_Name'],
                  row['Channel_ID'],
                  row['Subscriber'],
                  row['Views'],
                  row['Video_Count'],
                  row['Channel_Description'],
                  row['Playlist_iD'])
                    
         cursor.execute(insert_query,values)
         mydb.commit() 

     
     

def playlists_table():
    mydb= psycopg2.connect(host='localhost',
                               user='postgres',
                               password='postgres',
                               database='youtube_data',
                               port='5432')
    cursor = mydb.cursor()

    drop_query = "drop table if exists playlists"
    cursor.execute(drop_query)
    mydb.commit()

    
    create_query = '''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                            Title varchar(150),
                                                            Channel_Id varchar(100),
                                                            Channel_Name varchar(100),
                                                            PublishedAt timestamp,
                                                            Video_Count int
                                                            )'''
    cursor.execute(create_query)
    mydb.commit()


    pl_list = []
    db = client["youtube_data"]
    coll1 =db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
          pl_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        insert_query = '''INSERT into playlists(Playlist_Id,
                                                    Title,
                                                    Channel_Id,
                                                    Channel_Name,
                                                    PublishedAt,
                                                    Video_Count)
                                        VALUES(%s,%s,%s,%s,%s,%s)'''
        values =(
                row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Video_Count'])

        cursor.execute(insert_query,values)
        mydb.commit()

def videos_table():   
    mydb= psycopg2.connect(host='localhost',
                               user='postgres',
                               password='postgres',
                               database='youtube_data',
                               port='5432')
    cursor=mydb.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(30) ,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favorite_Count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(50)
                                                          )'''



    cursor.execute(create_query)
    mydb.commit()


    vi_list= []
    db=client['youtube_data']
    coll1=db['channel_details']
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
          vi_list.append(vi_data['video_information'][i])
    df2=pd.DataFrame(vi_list)


    for index,row in df2.iterrows():
         insert_query ='''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                Views,
                                                Likes,
                                                Comments,
                                                Favorite_Count,
                                                Definition,
                                                Caption_Status)

                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''


         values =(
                row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_Status'])


         cursor.execute(insert_query,values)
         mydb.commit()

def comments_table():

    mydb= psycopg2.connect(host='localhost',
                               user='postgres',
                               password='postgres',
                               database='youtube_data',
                               port='5432')
    cursor = mydb.cursor()

    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()


    create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                                                           Video_Id varchar(80),
                                                           Comment_Text text,
                                                           Comment_Author varchar(150),
                                                           Comment_Published timestamp)'''
    cursor.execute(create_query)
    mydb.commit()



    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
          com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)


    for index, row in df3.iterrows():
            insert_query = '''insert into comments (Comment_Id,
                                                      Video_Id ,
                                                      Comment_Text,
                                                      Comment_Author,
                                                      Comment_Published
                                                      )
                                              VALUES (%s,%s,%s,%s,%s)'''

            values = (row['Comment_Id'],
                     row['Video_Id'],
                     row['Comment_Text'],
                     row['Comment_Author'],
                     row['Comment_Published']
                     )

            cursor.execute(insert_query,values)
            mydb.commit()    
            
def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    
    return 'Tables created successfully'

#DataFrame for streamlit
def show_channels_table():
    ch_list= []
    db=client['youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
      ch_list.append(ch_data['channel_information'])
    df=st.dataframe(ch_list)
    
    
    return df

def show_playlists_table():
    pl_list = []
    db = client["Youtube_data"]
    coll1 =db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
          pl_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(pl_list)
    
    
    return df1

def show_videos_table():
    vi_list= []
    db=client['youtube_data']
    coll1=db['channel_details']
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
          vi_list.append(vi_data['video_information'][i])
    df2=st.dataframe(vi_list)
    
    return df2

def show_comments_table():    
    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
          com_list.append(com_data["comment_information"][i])
    df3 = st.dataframe(com_list)
    
    return df3


#streamlit part
st.title(':black[YOUTUBE DATA HARVESTING AND WAREHOUSING...]')

right_column, left_column = st.columns(2)
with right_column:
    st.image('Videos/Captures/youtube-logo-png-31812.png')


with left_column:
    channel_id = st.text_input("Enter the Channel id")

    if st.button('collect and store data'):
        ch_ids=[]
        db=client['youtube_data']
        coll1=db['channel_details']
        for ch_data in coll1.find({},{'_id':0,  'channel_information':1}):
            ch_ids.append(ch_data['channel_information'][0]['Channel_ID'])

        if channel_id in ch_ids:
            st.success('channel details of the given channel is already exists')

        else:
            insert= channel_details(channel_id)
            st.success(insert)
            
    all_channels= []
    db=client['youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        all_channels.append(ch_data['channel_information'][0]['Channel_Name'])

    
    unique_channel=st.selectbox('select the channel',all_channels)        
                
        
    if st.button('migrate to sql'):
        Table=tables()
        st.success(Table)

    show_table= st.radio('SELECT THE TABLE ',('CHANNELS','PLAYLISTS','VIDEOS','COMMENTS'))

    if show_table=='CHANNELS':
      show_channels_table()

    elif show_table=='PLAYLISTS':
       show_playlists_table()

    elif show_table=='VIDEOS':
       show_videos_table()

    elif show_table=='COMMENTS':
       show_comments_table()

    mydb= psycopg2.connect(host='localhost',
                               user='postgres',
                               password='postgres',
                               database='youtube_data',
                               port='5432')
    cursor = mydb.cursor()

    question = st.selectbox('Please Select Your Question',('1. All the videos and the Channel Name',
                                                           '2. Channels with most number of videos',
                                                           '3. 10 most viewed videos',
                                                           '4. Comments in each video',
                                                           '5. Videos with highest likes',
                                                           '6. likes of all videos',
                                                           '7. views of each channel',
                                                           '8. videos published in the year 2022',
                                                           '9. average duration of all videos in each channel',
                                                           '10. videos with highest number of comments'))

    
    
if question=='1. All the videos and the Channel Name':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1, columns=["Video Title","Channel Name"])
    st.write(df)


elif question == '2. Channels with most number of videos':
    query2 = "select Channel_Name as ChannelName,Video_Count as NO_Videos from channels order by Video_Count desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2, columns=["Channel Name","No Of Videos"])
    st.write(df2) 
    
    
elif question == '3. 10 most viewed videos':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df3=pd.DataFrame(t3, columns = ["views","channel Name","video title"])
    st.write(df3)

    
elif question == '4. Comments in each video':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4, columns=["No Of Comments", "Video Title"])
    st.write(df4)
    
    
elif question == '5. Videos with highest likes':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df5=pd.DataFrame(t5, columns=["video Title","channel Name","like count"])
    st.write(df5)
    
    
elif question == '6. likes of all videos':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    df6=pd.DataFrame(t6, columns=["like count","video title"])
    st.write(df6)
    
    
elif question == '7. views of each channel':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7, columns=["channel name","total views"])
    st.write(df7)
    
    
elif question == '8. videos published in the year 2022':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"])
    st.write(df8)


elif question == '9. average duration of all videos in each channel':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    df9=pd.DataFrame(T9)
    st.write(df9)
    
    
elif question == '10. videos with highest number of comments':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments'])
    st.write(df10)
    


        


