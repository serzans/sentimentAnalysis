import pandas as pd
import numpy as np

filename = 'redditComments.h5'

# Open an HDF store
hdf = pd.HDFStore(filename) # Might need to run sudo pip3 install tables

columns=['comment','timestamp','points','user','category']

df = pd.DataFrame(columns=columns)

hdf.put('d1',df,format='table',data_columns=True)


# Put chunks of comments into the store
df_temp = pd.DataFrame(index=range(10000),columns=columns)
for x in range(1,30000):
	
	# Need API calls here

	df_temp.iloc[x%10000]['comment'] = 'test'
	df_temp.iloc[x%10000]['timestamp'] = 'test'
	df_temp.iloc[x%10000]['points'] = 'test'
	df_temp.iloc[x%10000]['user'] = 'test'
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
