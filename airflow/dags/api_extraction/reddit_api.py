# Import modules
import time
import logging
import praw

# Create setup for logging
logger = logging.getLogger(__name__)

class RedditApi:
    """
    Class for interacting with Reddit API
    
    Attributes:
        reddit_api_client (praw.Reddit): API client for Reddit connection

    Methods:
        extract_reddit_submissions(subreddit_name, submissions_limit):
            Extract data from reddit submissions 
    """
    def __init__(self, client_id: str, client_secret: str, user_agent: str, username: str, password: str):
        self.reddit_client = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent, username=username, password=password)
    
    def extract_reddit_submissions(self, subreddit_name: str, submissions_limit: int) -> dict:
        """
        Extract data from reddit submissions

        Args:
            subreddit_name: Name of subreddit to retrieve submissions from
            submissions_limit: Limit on number of submissions to retrieve from Reddit API Client
        
        Returns:
            dict: Dictionary with submissions data
        """
        # Fetch the subreddit
        try: 
            subreddit = self.reddit_client.subreddit(subreddit_name)

            # Fetch the most recent submissions from the subreddit
            submissions = subreddit.new(limit=submissions_limit)

            # Get the current epoch time
            current_time = time.time()
            one_day_seconds = 846400

            # Store the submissions data in list, only including submissions from the past day
            submissions_filtered = [
                submission for submission in submissions
                if current_time - submission.created_utc <= one_day_seconds
            ]

            return submissions_filtered
        except Exception as e:
            logger.exception(f"Reddit API error for {subreddit_name}: {e}")
            return {}
