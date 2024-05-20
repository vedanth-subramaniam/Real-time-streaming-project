import praw
from kafka import KafkaProducer
import json
import time
import threading

# Replace these values with your Reddit API credentials
CLIENT_ID = 'X2BRlhIMxFnT_Ogp083m0w'
CLIENT_SECRET = '6akm7fO1Y9xK2JXd51I1bCxWG3NmHQ'
USER_AGENT = 'real-time by aadi'

# Initialize the Reddit instance
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT
)

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


# Function to stream posts from a subreddit
def stream_posts(subreddit_name):
    subreddit = reddit.subreddit(subreddit_name)
    print(f"Streaming new submissions from r/{subreddit_name}...")
    for submission in subreddit.stream.submissions():
        print(f"Title: {submission.title}")
        print(f"Score: {submission.score}")
        print(f'SubReddit : {subreddit_name}')
        comments = fetch_top_comments(submission.id, limit = 10)
        
        if comments:
            data = {
                'Title' : submission.title + submission.selftext,
                'comments': [ {'Body': comment.body} for comment in comments]
            }

            producer.send('reddit-data', value=json.dumps(data))

            print(data)
        time.sleep(2)
        print("-" * 40)

# Function to fetch the top 10 comments from a specific post
def fetch_top_comments(post_id, limit):
    submission = reddit.submission(id=post_id)
    submission.comment_sort = 'top'  # Sort comments by top
    submission.comment_limit = limit
    submission.comments.replace_more(limit=0)  # Fetch only top-level comments
    top_comments = submission.comments.list()

    return top_comments if len(top_comments) > 5 else None

# Main function to start streaming and fetching comments
if __name__ == '__main__':
    thread1 = threading.Thread(target=stream_posts, args=('soccer',))
    thread1.start()

    # Create and start the second thread
    thread2 = threading.Thread(target=stream_posts, args=('PremierLeague',))
    thread2.start()

    # Create and start the third thread
    thread3 = threading.Thread(target=stream_posts, args=('football',))
    thread3.start()

    thread1.join()
    thread2.join()
    thread3.join()