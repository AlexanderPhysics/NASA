# Only master mode sees these imports
from __future__ import division, absolute_import
#from IPython.parallel import Client
from ipyparallel import Client

import time
import pandas as pd
import numpy as np
from collections import defaultdict
 
# connect to cluster running the mpi profile
# connection will fail without explicit mentioning of
# which profile to connect to
print "starting 3 node cluster..."

#rc = Client(profile='mpi')
rc = Client()
dview = rc[:]
#dview = rc.load_balanced_view() # default load-balanced view
 
# Remote functions are just like normal functions, but when they are called, 
# they execute on one or more engines, rather than locally
@dview.remote(block=True)
def getpid():
    import os
    return os.getpid()
 
print "engine IDs: ", getpid()
 
@dview.remote(block=True)
def gethostname():
    import os
    return os.uname()
 
print "Host Machine Info:  ", set(gethostname())
 
# external imports on engines
# if local=True is not set the serial calculation below will fail
with dview.sync_imports(local=True):
    import numpy as np
    import pandas as pd
 
# function for master node
def get_master_features(product_df):
	master_cols = []
	for col in product_df.columns[1:]:
	    for val in product_df[col].values:
	        master_cols.append(col + "_" + val)
	return np.unique(master_cols)

# data paths
product_path = "/Users/Alexander/WINE/filtered_wine_df.csv"
purchase_path = "/Users/Alexander/WINE/filtered_purchase_df.csv"

# load product data and clean 
product_df = pd.read_csv(product_path)
product_cols = product_df.columns[1:]
product_df = product_df[product_cols]

# load purchase data and clean
purchase_df = pd.read_csv(purchase_path)
purchase_cols = [u'CustomerHash', u'ProductKey',u'Units']
purchase_df = purchase_df[purchase_cols]

# get master set of wine features for master matrix 
group_cust = purchase_df.groupby(["CustomerHash", "ProductKey"])["Units"].sum()

# make dictionary (costumerHash, [(ProductKey, Units), ..., (ProductKey, Units)])
cost_productKeys = defaultdict(list)
for custHash_proKey, units in group_cust.iteritems():
    cost_productKeys[custHash_proKey[0]].append(  (custHash_proKey[1], units) )

# get feature list for master matrix
master_cols = get_master_features(product_df)

# move a copy of the product_df to every engine
dview["product_df"] = product_df

# move a copy of the master_UserFeatures to every engine
dview["master_UserFeatures"] = master_cols

# get sub sample of cost_productKeys
sub_sample = cost_productKeys.items()[:6]
# move sub sample to every engine
#divew["cost_productKeys"] = sub_sample

@dview.parallel() # can be called on sequences and in parallel
def get_count(cost_productKeys_):

	def get_pid():
	    import os
	    return os.getpid()

	#return  product_df.shape, len(master_UserFeatures), get_pid(), cost_productKeys_[0]
	return cost_productKeys_[0], get_pid()


results = get_count.map(sub_sample)

#results = dview.map_async(get_count, [1,2,3])

print results.get()

# starttime = time.time()
# parallel_result = dview.map_sync(lambda n: sympy.ntheory.isprime(n), xrange(500))
# paralleltime = time.time() - starttime

 
 
#print "Time Elapsed = {}".format(paralleltime)

print "Reminder: YOU NEED TO STOP THE CLUSTER WITH A TERMINAL COMMAND!"



