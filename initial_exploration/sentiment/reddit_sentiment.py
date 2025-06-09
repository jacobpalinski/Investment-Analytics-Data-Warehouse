# Import modules
import os
import pandas as pd
import praw
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize PRAW with Reddit API credentials
reddit = praw.Reddit(
    client_id= os.getenv("REDDIT_CLIENT_ID"),
    client_secret= os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent= os.getenv("REDDIT_USER_AGENT"),
    username= os.getenv("REDDIT_USERNAME"),
    password= os.getenv("REDDIT_PASSWORD")
)

# Define the subreddit and the number of posts to fetch
subreddit_name = "stockmarket"
num_submissions= 1000

# Fetch the subreddit
subreddit = reddit.subreddit(subreddit_name)

# Fetch the most recent submissions from the subreddit
submissions = subreddit.new(limit=num_submissions)

# Store submissions data in a list
submissions_data = []

# Iterate through the top posts and collect data
for submission in submissions:
    submission_id = submission.id
    submission_title = submission.title
    submission_utc = submission.created_utc
    submission_body = submission.selftext

    submission_contents = reddit.submission(id=submission_id)
    submission_contents.comments.replace_more(limit=None)  # Load all comments
    
    # Fetch all comments for the submission
    all_comments = submission_contents.comments.list()

    # Iterate through each comment and collect data
    for comment in all_comments:
        comment_data = {
            "submission_id": submission_id,
            "submission_title": submission_title,
            "submission_utc": submission_utc,
            "submission_body": submission_body,
            "comment_id": comment.id,
            "comment_body": comment.body,
            "comment_utc": comment.created_utc,
        }
        submissions_data.append(comment_data)

# Convert submissions data list to DataFrame
df = pd.DataFrame(submissions_data)

# Save to JSON file
df.to_json("reddit_sentiment.json", orient="records", indent=4)


