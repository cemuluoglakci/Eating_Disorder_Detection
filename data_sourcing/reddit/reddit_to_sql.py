import pandas as pd
import pymysql

from utils.tools import Tools
from utils.sql import Sql
from reddit.reddit_client import RedditClient

class RedditDb():
    USERS_TABLE_NAME = "tblUsers"
    SUBMISSIONS_TABLE_NAME = "tblSubmissions"
    COMMENTS_TABLE_NAME = "tblComments"
    SUBREDDIT_TABLE_NAME = "tblSubreddits"
    RULES_TABLE_NAME = "tblRules"

    def __init__(self, settings) -> None:
        self.settings = settings
        return
    
    def CreateRedditClient(self):
        self.redditClient = RedditClient(self.settings)

    def CreateSqlConnection(self):
        self.sql = Sql(self.settings)

    def GetTableAsDf(self, tableName):
        self.CheckConnections()
        tableDefinition = self.GetTableDefinition(tableName)
        query = tableDefinition.select()
        return pd.read_sql(query, self.sql.connection)

    def SaveUsersToSql(self):
        self.CheckConnections()
        tblUsers = self.GetTableDefinition(self.USERS_TABLE_NAME)
        savedUserCount = 0
        for key, value in self.settings.userNamePairs.items():
            user = self.redditClient.connection.redditor(value)
            year = Tools.getYearValue(key)
            insert_statement = tblUsers.insert().values(
                Id=key, 
                Name=user.name, 
                Fullname=user.fullname, 
                Year=year)
            try:
                self.sql.execute(insert_statement)
                savedUserCount += 1
            except:
                print(f"user {key} already saved in the database")
                continue
        print(f"{savedUserCount} user instance(s) saved to database")

    def SaveUserCommentsToSql(self):
        tblComments = self.GetTableDefinition(self.COMMENTS_TABLE_NAME)
        print(tblComments)
        users_df = self.GetTableAsDf(self.USERS_TABLE_NAME)
        print(f"Retrieving comments of {len(users_df. index)} distinct users")
        
        savedCommentCount = 0
        skippedCommentCount = 0

        for Name in users_df.Name:
            user = self.redditClient.connection.redditor(Name)
            for comment in user.comments.new():
                insert_statement = tblComments.insert().values(Id=comment.id, 
                    Name=comment.name,
                    SubmissionTitle=comment.submission.title, 
                    LinkTitle=comment.link_title,
                    CreatedUtc=comment.created_utc,
                    Body=comment.body,
                    SubmissionId=comment.submission.id,
                    AuthorId=comment.author.id)
                    # add image link...

                try:
                    self.sql.execute(insert_statement)
                    savedCommentCount += 1
                except:
                    skippedCommentCount += 1
                    print(skippedCommentCount)
                    continue
        print(f"{savedCommentCount} comment instance(s) saved to database")
        print(f"{skippedCommentCount} comment instance(s) already saved to database and skipped")

    def SaveUserSubmissionsToSql(self):
        tblSubmissions = self.GetTableDefinition(self.SUBMISSIONS_TABLE_NAME)
        users_df = self.GetTableAsDf(self.USERS_TABLE_NAME)
        print(f"Retrieving submissions of {len(users_df. index)} distinct users")
        
        self.savedSubmissionCount = 0
        self.skippedSubmissionCount = 0

        for Name in users_df.Name:
            user = self.redditClient.connection.redditor(Name)
            for submission in user.submissions.new():
                self.__SaveSubmissionInstance(submission)

        print(f"{self.savedSubmissionCount} submission instance(s) saved to database")
        print(f"{self.skippedSubmissionCount} submission instance(s) already saved to database and skipped")

    def SaveCommentSubmissionsToSql(self):
        tblSubmissions = self.GetTableDefinition(self.SUBMISSIONS_TABLE_NAME)
        comments_df = self.GetTableAsDf(self.COMMENTS_TABLE_NAME)
        SubmissionIdList = comments_df.SubmissionId.unique()
        print(f"Retrieving {len(SubmissionIdList)} distinct submissions")
        
        self.savedSubmissionCount = 0
        self.skippedSubmissionCount = 0

        for Id in SubmissionIdList:
            submission = self.redditClient.connection.submission(Id)
            self.__SaveSubmissionInstance(submission)

        print(f"{self.savedSubmissionCount} submission instance(s) saved to database")
        print(f"{self.skippedSubmissionCount} submission instance(s) already saved to database and skipped")

    def __SaveSubmissionInstance(self, submission):
        insert_statement = self.tblSubmissions.insert().values(Id=submission.id, 
            Name=submission.name,
            Title=submission.title, 
            Selftext=submission.selftext,
            CreatedUtc=submission.created_utc,
            NumComments=submission.num_comments,
            Over18=submission.over_18,
            UpvoteRatio=submission.upvote_ratio,
            Media=str(submission.media),
            MediaEmbed=str(submission.media_embed),
            MediaOnly=submission.media_only,
            SubredditId=submission.subreddit_id,
            AuthorId=None if submission.author==None else submission.author.name)
        try:
            self.sql.execute(insert_statement)
            self.savedSubmissionCount += 1
        except:
            self.skippedSubmissionCount += 1
            
    def SaveSubredditsToSql(self):
        tblSubreddits = self.GetTableDefinition(self.SUBREDDIT_TABLE_NAME)
        submissions_df = self.GetTableAsDf(self.SUBMISSIONS_TABLE_NAME)
        subredditIdList = submissions_df.SubredditId.unique()
        print(f"Retrieving {len(subredditIdList)} distinct subreddits")

        savedSubredditCount = 0
        skippedSubredditCount = 0

        for Id in subredditIdList:
            subreddit = list(self.redditClient.connection.info([Id]))[0]
            insert_statement = self.tblSubreddits.insert().values(
                Id=subreddit.id, 
                Name=subreddit.name,
                DisplayName=subreddit.display_name,                
                Title=subreddit.title, 
                CreatedUtc=subreddit.created_utc,
                AccountsActive=subreddit.accounts_active,
                ActiveUserAccount=subreddit.active_user_count,
                Subscribers=subreddit.subscribers,
                Over18=subreddit.over18,
                PublicDescription=subreddit.public_description,
                SubredditType=subreddit.subreddit_type)
            try:
                self.sql.execute(insert_statement)
                savedSubredditCount += 1
            except:
                skippedSubredditCount += 1

        print(f"{savedSubredditCount} subreddit instance(s) saved to database")
        print(f"{skippedSubredditCount} subreddit instance(s) skipped")


    def CheckConnections(self):
        if not hasattr(self, "redditClient"):
            self.CreateRedditClient()
        if not hasattr(self, "sql"):
            self.CreateSqlConnection()

    def GetTableDefinition(self, tableName):
        if not hasattr(self, tableName):
            setattr(self, tableName, self.sql.setTableDefinition(tableName))
        return getattr(self, tableName)




