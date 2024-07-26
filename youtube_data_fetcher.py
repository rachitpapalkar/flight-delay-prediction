from googleapiclient.discovery import build
import os

# Function to read API key from file
def read_api_key(file_path='api_key.txt'):
    with open(file_path, 'r') as file:
        api_key = file.read().strip()
    return api_key

# Initialize YouTube Data API client
api_key = read_api_key()
youtube = build('youtube', 'v3', developerKey=api_key)

# Function to get channel statistics
def get_channel_stats(channel_id):
    request = youtube.channels().list(
        part="statistics",
        id=channel_id
    )
    response = request.execute()
    stats = response['items'][0]['statistics']
    return {
        'viewCount': stats.get('viewCount', 0),
        'commentCount': stats.get('commentCount', 0),
        'subscriberCount': stats.get('subscriberCount', 0),
        'videoCount': stats.get('videoCount', 0)
    }

# Function to get latest comments from a channel
def get_latest_comments(channel_id, max_results=20):
    request = youtube.commentThreads().list(
        part="snippet",
        allThreadsRelatedToChannelId=channel_id,
        maxResults=max_results
    )
    response = request.execute()
    comments = []
    for item in response['items']:
        comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
        comments.append(comment)
    return comments

# Example usage
if __name__ == "__main__":
    # Replace with actual channel IDs
    # Nike: UCUFgkRb0ZHc4Rpq15VRCICA
    # Adidas: UCuLUOxd7ezJ8c6NSLBNRRfg
    channel_ids = ['UCUFgkRb0ZHc4Rpq15VRCICA', 'UCuLUOxd7ezJ8c6NSLBNRRfg']
    for channel_id in channel_ids:
        stats = get_channel_stats(channel_id)
        comments = get_latest_comments(channel_id)
        print(f"Channel ID: {channel_id}")
        print(f"Statistics: {stats}")
        print(f"Latest Comments: {comments}")

