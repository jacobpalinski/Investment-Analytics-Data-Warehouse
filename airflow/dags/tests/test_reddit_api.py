# Import modules
import pytest
from dags.api_extraction.reddit_api import RedditApi
from dags.tests.fixtures import mock_reddit_env

class TestRedditApi:
    """ Test suite for RedditApi class """
    def test_extract_reddit_submissions_success(self, mock_reddit_env):
        """ Test that only recent Reddit submissions are returned. """
        env = mock_reddit_env

        reddit_api = RedditApi("client_id", "client_secret", "user_agent", "username", "password")
        reddit_api.reddit_client = env["DummyRedditClient"]()

        result = reddit_api.extract_reddit_submissions("investing", 5)

        # Only 1 recent submission expected
        assert len(result) == 1
        assert result[0].created_utc == env["new_post"].created_utc

    def test_extract_reddit_submissions_exception(self, mock_reddit_env, caplog):
        """ Test that exceptions from Reddit API are handled gracefully. """
        env = mock_reddit_env

        reddit_api = RedditApi("client_id", "client_secret", "user_agent", "username", "password")
        reddit_api.reddit_client = env["DummyRedditClient"](raise_error=True)

        with caplog.at_level("ERROR"):
            result = reddit_api.extract_reddit_submissions("investing", 5)

        assert result == {}
        assert "Reddit API error for investing" in caplog.text

