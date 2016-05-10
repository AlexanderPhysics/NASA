from ipyparallel import Client
import os
from time import time 
import Cylindrical_Map_Transformation
import sunspot_feature_extraction
import extract_features_script
import Centroid_Labeling
import extract_image_features
import extract_features_script_test

start = time()

#rc = Client(profile='mpi')
rc = Client()
dview = rc[:]

# get print of worker engine ids
dview.execute('import test_parallel_2')
dview.execute('engine_ids = test_parallel_2.get_pid()')
print "worker engine ids: ", dview["engine_ids"]
print "Number of worker engines deployed = {}".format(len(dview))

# image and resutls paths

# Paths for Pleiades
#drop everything to lower case !!!
# image_path = "/u/abarriga/nasa_sample_data/images/"
# db_path = "/u/abarriga/nasa_sample_data/database/"
# noaa_home = "/u/abarriga/nasa_sample_data/noaa_data/"
# trans_image_path = "/u/abarriga/nasa_sample_data/trans_image_data/"
# feature_results_path = "/u/abarriga/nasa_sample_data/feature_extraction_results/"

# LOCAL paths 
#image_path = "/Users/Alexander/NASA/NASA_Sample_Data/Images/"
image_path = "/Users/Alexander/NASA/NASA_Sample_Data/HMI_Alexander/"
db_path = "/Users/Alexander/NASA/Database/"
noaa_home = "/Users/Alexander/NASA/noaa_data/"
trans_image_path = "/Users/Alexander/NASA/trans_image_data_singleDay/"
feature_results_path = "/Users/Alexander/NASA/feature_extraction_results_singleDay/"

# load file names
file_name_list = []
for filename in os.listdir(image_path):
    if "ds_store" not in filename.lower():
        file_name_list.append(filename)

#load n sample images
# test_index = range(0,len(dview) )     
# print "getting {} sample images...".format(len(dview) )
# file_names = [ file_name_list[index] for index in test_index]

file_names = file_name_list[10:12]

	
# import user defined scripts into worker engines
dview.execute('import Cylindrical_Map_Transformation')
dview.execute('import sunspot_feature_extraction')
dview.execute('import extract_features_script')
dview.execute('import extract_features_script_test')
dview.execute('import Centroid_Labeling')
dview.execute('import extract_image_features')

# scatter file names among the worker engines 
dview.scatter('file_name', file_names)

# push a copy of the image and results paths to every worker engine
dview.push(dict(image_path=image_path,
               trans_image_path=trans_image_path,
               feature_results_path=feature_results_path,
               db_path=db_path,
               noaa_home=noaa_home))



# print "extracting features from images... Step 1 of 3"
# dview.execute('new_feat_objects = extract_features_script.extract_features_from_images' + 
# 	          '(image_path,trans_image_path,feature_results_path, file_name[0])').get()

# #print "new_feat_objects ", dview["new_feat_objects"]
# end = time()
# #print "Time Elapsed = {:.4} mins \n".format((end - start)/ 60)

# start = time()
# print "loading noaa data...Step 2 of 3 "
# dview.execute('noaa_data = extract_features_script.load_noaa_data(noaa_home)').get()

# #print "noaa_data len ", len(dview["noaa_data"])

# end = time()
# #print "Time Elapsed = {:.4} mins \n".format((end - start)/ 60)

# start = time()
# print "filtering, labeling, and saving features to file...Step 3 of 3"
# dview.execute('extract_features_script.filter_label_saveToFile(new_feat_objects, db_path,  noaa_data)').get()


# #print "magneto_vector ", dview['magneto_vector']
# #print "my_cents ", dview['my_cents']
# #print "noaa_cents ", dview['noaa_cents']
# #print "clean_feat_object ", dview['clean_feat_object']

# end = time()






print '\nTEST_0: Print out of paths and file name variables directly after they have been imported into dview\n'

print 'file_name ', dview["file_name"]
print 'image_path', dview["image_path"]
print 'trans_image_path', dview["trans_image_path"]
print 'feature_results_path', dview["feature_results_path"]
print 'db_path', dview["db_path"]
print 'noaa_home', dview["noaa_home"]

print "\nTEST_1: extract_features_from_images_test_1 (PRE-CONVOLUTION)\n"

dview.execute('image_test, image_path_test,trans_image_path_test,feature_results_path_test, file_name_test'+
			  ' = extract_features_script_test.extract_features_from_images_test_1' + 
	          										'(image_path,trans_image_path,feature_results_path, file_name[0])').get()

print "image_test ", dview['image_test']
print "image_path_test ", dview['image_path_test']
print "trans_image_path_test ", dview['trans_image_path_test']
print "feature_results_path_test ", dview['feature_results_path_test']
print "file_parallel_pipeline_pleiades.pyname_test ", dview['file_name_test']
print 'db_path', dview["db_path"]
print 'noaa_home', dview["noaa_home"]

end = time()
print "Time Elapsed = {:.4} mins ".format((end - start)/ 60)


print "\nTEST_2: extract_features_from_images_test_2 (POST-CONVOLUTION)\n"

dview.execute('image_test2, image_path_test2,trans_image_path_test2,feature_results_path_test2, file_name_test2, hdu_test2'+
			  ' = extract_features_script_test.extract_features_from_images_test_2' + 
	          										'(image_path,trans_image_path,feature_results_path, file_name[0])')

print "image_test2 ", dview['image_test2'] 
print "image_path_test2 ", dview['image_path_test2']
print "trans_image_path_test2 ", dview['trans_image_path_test2']
print "feature_results_path_test2", dview['feature_results_path_test2']
print "file_name_test2 ", dview['file_name_test2']
print 'db_path', dview["db_path"]
print 'noaa_home', dview["noaa_home"]
#print "hdu_test2 ", type(dview['hdu_test2'])  #<-- LocalHost ERROR:tornado.general:Uncaught exception, closing connection.
# Apparently, ipyparallel can't handel passing hdu objects between engines 

end = time()
print "Time Elapsed = {:.4} mins ".format((end - start)/ 60)



print "\nTEST_3: save_to_file (SAVE IMAGE MAPPED TO CYLINDRICAL SPACE)\n"
print "SKIPPING"
#dview.execute('extract_features_script_test.save_to_file(image_test2, trans_image_path_test2, file_name_test2)').get()

end = time()
print "Time Elapsed = {:.4} mins ".format((end - start)/ 60)


print "\nTEST_4: get_active_region_labels_test (LABEL ACTIVE REGIONS)\n"
dview.execute('labels = extract_features_script_test.get_active_region_labels_test(image_test2)').get()

print "labels ", dview['labels'] 

end = time()
print "Time Elapsed = {:.4} mins ".format((end - start)/ 60)


print "\nTEST_5: extract_features (EXTRACT SUNSPOT FEATURES)\n"
dview.execute('extract_results = sunspot_feature_extraction.extract_features(labels,image_test2, hdu_test2, file_name_test2, num_pixel_in_active_region=100)').get()

print "extract_results ", dview['extract_results'] 
print "extract_results TYPE", type(dview['extract_results']), type(dview['extract_results'][0])

print "\nSaving extracted features to pickle file from image\n"
print "SKIPPING"
#dview.execute('extract_features_script_test.save_to_file([file_name_test2, extract_results], feature_results_path, file_name[0])').get()

end = time()
print "Time Elapsed = {:.4} mins ".format((end - start)/ 60)


print "\nTEST_6: extract_features (LOAD NOAA DATA)\n"
dview.execute('noaa_data = extract_features_script.load_noaa_data(noaa_home)').get()

print "noaa_data ", len(dview['noaa_data']) 

end = time()
print "Time Elapsed = {:.4} mins ".format((end - start)/ 60)

print "\nTEST_7: get_features (LOAD EXTRACTED FEATURES FROM FILE)\n"
print "SKIPPING"
#dview.execute('feats = extract_image_features.get_features(feature_results_path)').get()

#print "feats ", dview['feats'] 

# NOTE ON TEST_7: EACH ENGINE IS LOADING BOTH FILES -- EACH FILE IS LOADED TWICE ! 
# NEED TO ENSURE THAT EACH ENGINE ONLY REFERENCE THE FILE PATH THAT IS UNQIUE TO THAT ENGINE ! 

end = time()
print "Time Elapsed = {:.4} mins ".format((end - start)/ 60)


print "\nTest_8: map_centroids_long_lat (MAP CARTESIAN COORD TO LONG/LAT COORD)\n"
dview.execute('new_feat_objects = extract_image_features.map_centroids_long_lat(extract_results)').get()

print "new_feat_objects ", dview['new_feat_objects'] 

print "Test 9: get_noaa_and_image_centroids "
dview.execute('my_cents, noaa_cents_labels = extract_features_script.get_noaa_and_image_centroids(new_feat_objects, noaa_data)').get()

print "my_cents ", dview["my_cents"]
print "\nnoaa_cents ", dview["noaa_cents_labels"]

print "Test_9.1: get_shortest_distance_pair"
dview.execute('shortest_dist_pairs = extract_image_features.get_shortest_distance_pair(my_cents, noaa_cents_labels)').get()
print "\nshortest_dist_pairs",  dview['shortest_dist_pairs'] 

print "Test_9.2: check_repeating_noaa_assignments"
dview.execute("clean_ar_assignments = extract_image_features.check_repeating_noaa_assignments(shortest_dist_pairs)").get()
print "\nclean_ar_assignments ", dview["clean_ar_assignments"]

print "Test_9.3: filter_extra_active_region_assignments"
dview.execute("clean_feat_object = Centroid_Labeling.filter_extra_active_region_assignments(new_feat_objects,clean_ar_assignments)").get()
print "\nclean_feat_object ", dview["clean_feat_object"]



print "Test 10: filter_label_saveToFile"
dview.execute('magneto_vector = extract_features_script.filter_label_saveToFile(clean_feat_object, db_path, noaa_data)').get()

print  "\nmagneto_vector", dview["magneto_vector"]


#----------------------------------------

print "Time Elapsed = {:.4} mins ".format((end - start)/ 60)

