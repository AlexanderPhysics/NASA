from ipyparallel import Client
import os
from time import time 

start = time()

rc = Client()
dview = rc[:]

# get print of worker engine ids
dview.execute('import test_parallel_2')
dview.execute('engine_ids = test_parallel_2.get_pid()')
print "worker engine ids: ", dview["engine_ids"]

# image and resutls paths

image_path = "/Users/Alexander/NASA/NASA_Sample_Data/Images/"
db_path = "/Users/Alexander/NASA/Database/"
noaa_home = "/Users/Alexander/NASA/noaa_data/"
trans_image_path = "/Users/Alexander/NASA/trans_image_data_singleDay/"
feature_results_path = "/Users/Alexander/NASA/feature_extraction_results_singleDay/"

# load file names
file_name_list = []
for filename in os.listdir(image_path):
    if "ds_store" not in filename.lower():
        file_name_list.append(filename)

#load 3 sample images
test_index = range(0,3)     
print "getting 3 sample images..."
file_names = [ file_name_list[index] for index in test_index]


# import user defined scripts into worker engines
dview.execute('import Cylindrical_Map_Transformation')
dview.execute('import sunspot_feature_extraction')
dview.execute('import extract_features_script')
dview.execute('import Centroid_Labeling')
dview.execute('import extract_image_features')

# scatter file names among the worker engines 
dview.scatter('file_name', file_names)

# push a copy of the image and results paths to every worker engine
dview.push(dict(image_path = image_path,
               trans_image_path=trans_image_path,
               feature_results_path=feature_results_path,
               db_path = db_path,
               noaa_home=noaa_home))

print "extracting features from images..."
dview.execute('extract_features_script.extract_features_from_images' + 
	          '(image_path,trans_image_path,feature_results_path, file_name[0])').get()

print "loading noaa data..."
dview.execute('noaa_data = extract_features_script.load_noaa_data(noaa_home)')
#print "noaa_data len ", len(dview['noaa_data'][0])
print "creating feature data objects..."
dview.execute('feats = extract_image_features.get_features(feature_results_path)')

dview.execute('new_feat_objects = [extract_image_features.map_centroids_long_lat(feat) for feat in feats]')

#print "new_feat_objects len ", type(dview["new_feat_objects"])
#print "new_feat_objects len ", type(dview["new_feat_objects"][0][0])

print "getting image and noaa centroids..."
dview.execute('my_cents, noaa_cents = extract_features_script.get_noaa_and_image_centroids(new_feat_objects[0], noaa_data)')

#print "my_cent len = {0}, noaa_cents = {1}".format(len(dview['my_cents']),  len(dview['noaa_cents'])  )
#print "my_cent len = {0}, noaa_cents = {1}".format(len(dview['my_cents'][0]),  len(dview['noaa_cents'][0])  )
print "filtering, labeling, and saving features to file..."
dview.execute('extract_features_script.filter_label_saveToFile(new_feat_objects[0], db_path,  my_cents, noaa_cents, noaa_data)')




end = time()
print "Time Elapsed = {} mins ".format((end - start)/ 60)

