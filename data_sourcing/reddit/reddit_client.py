import praw
from utils.tools import Tools
from utils.sql import Sql

class RedditClient():
    def __init__(self, settings) -> None:
        self.settings=settings
        self.Connect()
        
    def Connect(self):
        self.connection = praw.Reddit(
    client_id=self.settings.client_id,
    client_secret=self.settings.client_secret,
    password=self.settings.password,
    user_agent=self.settings.user_agent,
    username=self.settings.username,)

