import googleapiclient.discovery
import pymongo
import mysql.connector
import datetime
import streamlit as st
import pandas as pd
from datetime import datetime

# YouTube API key
api_key = 'Your_APi_Key'

# Function to get channel information from YouTube API
def channel_information(c_id):
    youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=api_key)
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=c_id
    )
    response = request.execute()
    
    if 'items' in response and response['items']:
        item = response['items'][0]
        channel_details = {
            'channel_id': item.get('id'),
            'channel_name': item['snippet']['title'],
            'country': item['snippet'].get('country'),
            'total_videoCount': item['statistics']['videoCount'],
            'total_viewCount': item['statistics']['viewCount'],
            'subscriberCount': item['statistics']['subscriberCount'],
            'upload_id': item['contentDetails']['relatedPlaylists']['uploads'],
            'channel_published' : item['snippet']['publishedAt']
        }
        return channel_details
    else:
        return None

# Function to get video IDs
def get_video_ids(upload_id):
    youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=api_key)
    video_ids = []
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=upload_id,
        maxResults=50
    )

    response = request.execute()

    for item in response.get("items", []):
        video_ids.append(item['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')

    while next_page_token:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=upload_id,
            maxResults=50,
            pageToken=next_page_token)
        response = request.execute()

        for item in response.get("items", []):
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')

    return video_ids

# Function to get video stats
def videostats_details(video_ids):
    youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=api_key)
    videostats = []
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50]),
            maxResults=50
        )
        response_stats = request.execute()

        for item in response_stats.get('items', []):
            video_stat = {
                'Title': item['snippet']['title'],
                'video_id': item['id'],
                'comment_count': item['statistics']['commentCount'],
                'video_favoriteCount' : item['statistics']['favoriteCount'],
                'video_likecount' : item['statistics']['likeCount'],
                'video_viewcount': item['statistics']['viewCount'],
                'video_publishedat': item['snippet'].get ("publishedAt"),
            }
            videostats.append(video_stat)
    return videostats


# Function to get comments
def get_video_comments(video_ids):
    youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=api_key)
    video_comments = []
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=10
            )
            response_comments = request.execute()

            for item in response_comments.get('items', []):
                comment = {
                    'video_id': video_id,
                    'comment_text': item['snippet']['topLevelComment']['snippet']['textOriginal'],
                    'comment_author': item['snippet']['topLevelComment']['snippet']['authorDisplayName']                    
                }
                video_comments.append(comment)
        except googleapiclient.errors.HttpError as e:
            if e.resp.status == 403:
                st.warning(f"Comments are disabled for video {video_id}. Skipping...")
            else:
                st.error(f"An error occurred while fetching comments for video {video_id}: {e}")
    return video_comments


# Function to save data into MongoDB
def save_to_mongodb(channel_info, videostats_info, comments_info):
    try:
        if channel_info is None:
            st.error("Channel information is not available. Data cannot be saved to MongoDB.")
            return False

        # Connect to MongoDB
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/") # used local host
        mongo_db = mongo_client["om"]
        mongo_collection = mongo_db["youtubedata"]
        
        # Prepare data
        data = {
            'channel_info': channel_info,
            'videostats_info': videostats_info,
            'comments_info': comments_info
        }
        
        # Insert data into MongoDB
        mongo_collection.insert_one(data)
        st.success("Data saved successfully in MongoDB.")
        return True
    except pymongo.errors.PyMongoError as e:
        st.error(f"Error saving data to MongoDB: {e}")
        return False


# Function to create tables in MySQL if they don't exist
def create_mysql_tables():
    try:
        mysql_connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="**Password**",
            database="youtubedata"
        )
        cursor = mysql_connection.cursor()
        
        # Create channels table if not exists
        cursor.execute("CREATE TABLE IF NOT EXISTS channels (channel_id VARCHAR(255), channel_name VARCHAR(255), country VARCHAR(255), total_videoCount INT, total_viewCount INT, subscriberCount INT, channel_published DATETIME)")

        # Create videos table if not exists
        cursor.execute("CREATE TABLE IF NOT EXISTS videos (video_id VARCHAR(255), channel_id VARCHAR(255), title VARCHAR(255), comment_count INT, video_favoriteCount INT, video_likecount INT, video_viewcount INT, video_publishedat DATETIME)")

        # Create comments table if not exists
        cursor.execute("CREATE TABLE IF NOT EXISTS comments (comment_id INT AUTO_INCREMENT PRIMARY KEY, video_id VARCHAR(255), comment_text TEXT, comment_author VARCHAR(255))")

        mysql_connection.commit()
        cursor.close()
        mysql_connection.close()
        st.success("MySQL tables created successfully.")
    except mysql.connector.Error as e:
        st.error(f"Error creating MySQL tables: {e}")
        return False

import datetime# rementioned since was not working while executing 
# Function to migrate data from MongoDB to MySQL
def migrate_to_sql_by_channel_id(channel_id):
    try:
        # Connect to MongoDB
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        mongo_db = mongo_client["om"]
        mongo_collection = mongo_db["youtubedata"]

        # Fetch the latest data for the given channel ID from MongoDB
        data = mongo_collection.find_one({"channel_info.channel_id": channel_id})
        if data:
            channel_info = data['channel_info']
            videostats_info = data['videostats_info']
            comments_info = data['comments_info']
        else:
            st.error("Data not found in MongoDB for the provided channel ID.")
            return False
        
        # Connect to MySQL
        mysql_connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="**Password**",
            database="youtubedata"
        )
        cursor = mysql_connection.cursor()


        # Check if data already exists in MySQL for the provided channel ID
        cursor.execute("SELECT * FROM channels WHERE channel_id = %s", (channel_info['channel_id'],))
        existing_data = cursor.fetchone()
        if existing_data:
            st.info("Data already migrated to MySQL.")
            return True
        
        # Format channel_published properly if it exists
        channel_published = None
        if 'channel_published' in channel_info and channel_info['channel_published']:
            try:
                # Parse the datetime string with milliseconds
                channel_published = datetime.datetime.strptime(channel_info['channel_published'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                st.warning("Invalid channel_published format. Using None.")
        
        # Insert channel info into MySQL
        channel_data = (channel_info['channel_id'], channel_info['channel_name'], 
                        channel_info['country'], channel_info['total_videoCount'],
                        channel_info['total_viewCount'], channel_info['subscriberCount'],
                        channel_published)
        
        channel_query = "INSERT INTO channels (channel_id, channel_name, country, total_videoCount, total_viewCount, subscriberCount, channel_published) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(channel_query, channel_data)
        
        # Insert video stats into MySQL
        for video_stat in videostats_info:
            video_data = (video_stat['video_id'], channel_info['channel_id'], video_stat['Title'], 
                          video_stat['video_likecount'], video_stat['video_viewcount'])
            
            video_query = "INSERT INTO videos (video_id, channel_id, title, video_likecount, video_viewcount) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(video_query, video_data)

        # Insert comments into MySQL
        for comment in comments_info:
            comment_data = (comment['video_id'], comment['comment_text'], 
                            comment['comment_author'])
            comment_query = "INSERT INTO comments (video_id, comment_text, comment_author) VALUES (%s, %s, %s)"
            cursor.execute(comment_query, comment_data)

        mysql_connection.commit()
        cursor.close()
        mysql_connection.close()
        st.success("Data migrated successfully to MySQL.")
        return True
    except (pymongo.errors.PyMongoError, mysql.connector.Error) as e:
        st.error(f"Error migrating data: {e}")
        return False
    
# query execution
def execute_sql_query(query):
    try:
        mysql_connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="**Password**",
            database="youtubedata"
        )
        cursor = mysql_connection.cursor(dictionary=True)
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        mysql_connection.close()
        return data
    except mysql.connector.Error as e:
        st.error(f"Error executing SQL query: {e}")
        return []

def display_extracted_data(data):
    st.subheader("Extracted Data")
    # Display channel data
    st.subheader("Channel Information")
    channel_info = data.get("channel_info", {})
    df_channel = pd.DataFrame({
        "Channel ID": [channel_info.get("channel_id", "")],
        "Channel Name": [channel_info.get("channel_name", "")],
        "Country": [channel_info.get("country", "")],
        "Total Video Count": [channel_info.get("total_videoCount", "")],
        "Total View Count": [channel_info.get("total_viewCount", "")],
        "Subscriber Count": [channel_info.get("subscriberCount", "")], 
        "Channel Published At": [channel_info.get("publishedAt", "")]
    })
    st.write(df_channel)

    # Display video data
    st.subheader("Video Information")
    videostats_info = data.get("videostats_info", [])
    if videostats_info:
        df_videos = pd.DataFrame(videostats_info)
        st.write(df_videos)
    else:
        st.info("No video data available.")

    # Display comments data
    st.subheader("Comments Information")
    comments_info = data.get("comments_info", [])
    if comments_info:
        df_comments = pd.DataFrame(comments_info)
        st.write(df_comments)
    else:
        st.info("No comments data available.")

# Function to display migrated data
def display_migrated_data(data):
    if not isinstance(data, list) or len(data) == 0:
        st.error("No migrated data found.")
        return

    # Display channel data
    st.subheader("Migrated channel info")
    df_channel = pd.DataFrame(data, columns=["channel_id", "channel_name", "country", "total_videoCount", "total_viewCount", "subscriberCount", "channel_published"]) 
    st.write(df_channel)

   
# Main function
def main():
    st.title("YouTube Data Harvesting and Warehousing")

    # Input field for YouTube channel ID
    channel_id = st.text_input("Enter YouTube Channel ID:")
    
    # Button to extract data from YouTube API
    if st.button("Extract Data from YouTube API"):
        if channel_id:
            # Connect to MongoDB
            mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
            mongo_db = mongo_client["om"]
            mongo_collection = mongo_db["youtubedata"]

            # Check if data for the channel already exists in MongoDB
            existing_data = mongo_collection.find_one({"channel_info.channel_id": channel_id})
            if existing_data:
                st.info("Data already extracted for the provided channel ID.")
                display_extracted_data(existing_data)
            else:            
            # Extract channel information
                channel_info = channel_information(channel_id)
                if channel_info:
                    # Extract video IDs
                    upload_id = channel_info['upload_id']
                    video_ids = get_video_ids(upload_id)
                    
                    # Extract video stats
                    videostats_info = videostats_details(video_ids)
                    
                    # Extract comments
                    comments_info = get_video_comments(video_ids)
                    st.success("Data extracted successfully from YouTube API.")
                    save_to_mongodb(channel_info, videostats_info, comments_info)
                    display_extracted_data({"channel_info": channel_info, "videostats_info": videostats_info, "comments_info": comments_info})
                else:
                    st.error("Failed to extract channel information from YouTube API.")
        else:
            st.error("Channel ID is required for extraction.")

    # Display Migrate to MySQL section
    st.title("Migrate Data to MySQL")
    
    # Button to save and migrate data
    if st.button("Migrate to MySQL"):
        create_mysql_tables()
        if channel_id:
            existing_data = execute_sql_query("SELECT * FROM channels WHERE channel_id = '{}'".format(channel_id))
            if existing_data:
                st.info("Data already extracted for the provided channel ID.")
                display_migrated_data(existing_data)
            else:
                migration_data = migrate_to_sql_by_channel_id(channel_id)
                if migration_data:
                    st.success("Data migrated successfully to MySQL.")
                    display_migrated_data(migration_data)
                else:
                    st.error("No migrated data found for the provided channel ID.")
        else:
            st.error("Channel ID is required for migration.")

    st.title("YouTube Data Analysis")
    queries = {
    "Names of all videos and their corresponding channels": "SELECT v.title AS Video_Name, c.channel_name AS Channel_Name FROM videos v JOIN channels c ON v.channel_id = c.channel_id",
    "Channels with the most number of videos and their counts": "SELECT c.channel_name AS Channel_Name, COUNT(v.video_id) AS Video_Count FROM channels c JOIN videos v ON c.channel_id = v.channel_id GROUP BY c.channel_name ORDER BY Video_Count DESC LIMIT 1",
    "Top 10 most viewed videos and their respective channels": "SELECT v.title AS Video_Name, c.channel_name AS Channel_Name, v.video_viewcount AS Views FROM videos v JOIN channels c ON v.channel_id = c.channel_id ORDER BY v.video_viewcount DESC LIMIT 10",
    "Number of comments on each video and their corresponding video names": "SELECT v.title AS Video_Name, COUNT(c.comment_id) AS Comment_Count FROM videos v JOIN comments c ON v.video_id = c.video_id GROUP BY v.title",
    "Videos with the highest number of likes and their corresponding channel names": "SELECT v.title AS Video_Name, c.channel_name AS Channel_Name, MAX(v.video_likecount) AS Likes FROM videos v JOIN channels c ON v.channel_id = c.channel_id GROUP BY v.title, c.channel_name ORDER BY Likes DESC LIMIT 1",
    "Total number of views for each channel and their corresponding channel names": "SELECT c.channel_name AS Channel_Name, SUM(v.video_viewcount) AS Total_Views FROM channels c JOIN videos v ON c.channel_id = v.channel_id GROUP BY c.channel_name",
    "Names of channels that published videos in the year 2022": "SELECT DISTINCT c.channel_name AS Channel_Name FROM channels c JOIN videos v ON c.channel_id = v.channel_id WHERE YEAR(v.video_publishedat) = 2022",
    "Videos with the highest number of comments and their corresponding channel names": "SELECT v.title AS Video_Name, c.channel_name AS Channel_Name, COUNT(c.comment_id) AS Comment_Count FROM videos v JOIN comments c ON v.video_id = c.video_id GROUP BY v.title, c.channel_name ORDER BY Comment_Count DESC LIMIT 1"}

    # Select a query
    selected_query = st.selectbox("Select SQL Query", list(queries.keys()))

    # Execute selected query and display results
    if st.button("Execute Query"):
        query_result = execute_sql_query(queries[selected_query])
        if query_result:
            st.write(pd.DataFrame(query_result))
        else:
            st.error("No data found for the selected query.")
    
if __name__ == "__main__":
    main()
