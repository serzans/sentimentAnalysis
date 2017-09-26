import pandas as pd
import numpy as np
import praw
import datetime 

#change to timestamp

def get_date(comment):
    time = comment.created
    return datetime.datetime.fromtimestamp(time)

# Create a Reddit instance
reddit = praw.Reddit(user_agent = "sentimentAnalysis", client_id = "zwiURjj8Ml4KmA",
		     client_secret = "I0bb_hg7CjgK6za3CKEznqBP8XQ")
#list of submissions and comments we're interested in
submissions = reddit.subreddit("politics").top(limit = None)
comments = []
iter = 0
while (len(comments)<30000)):
	submission = submissions[iter]
	submission.comments.replace_more(limit = 0)
	comments.extend(submission.comments.list())
	

filename = 'redditComments.h5'

# Open an HDF store
hdf = pd.HDFStore(filename) # Might need to run sudo pip3 install tables

columns=['comment','timestamp','points','user','category']

df = pd.DataFrame(columns=columns)

hdf.put('d1',df,format='table',data_columns=True)


# Put chunks of comments into the store
df_temp = pd.DataFrame(index=range(10000),columns=columns)
for x in range(1,30000):
	
	comment = comments[x]

	df_temp.iloc[x%10000]['comment'] = comment.body
	df_temp.iloc[x%10000]['timestamp'] = get_date(comment)
	df_temp.iloc[x%10000]['points'] = comment.score
	df_temp.iloc[x%10000]['user'] = comment.author
	df_temp.iloc[x%10000]['category'] = 'test'

	if x%10000==0:
		# Dump dataframe into HDF store
		hdf.append('d1',df_temp,format='table',data_columns=True)
		# Reinitialise the temporary dataframe
		df_temp = pd.DataFrame(index=range(10000),columns=columns)


# Close the store
hdf.close()


# Test if everything went fine

df = pd.read_hdf(filename,'d1',columns=columns)

df.head()
