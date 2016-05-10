from ipyparallel import Client
import os
from time import time 
import Cylindrical_Map_Transformation
import sunspot_feature_extraction
import extract_features_script
import Centroid_Labeling
import extract_image_features

start = time()
# To run on Pleiades use : Client(profile='mpi')
# to run locally use: Client()
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
#image_path = "/u/abarriga/nasa_sample_data/images/"
# image_path = "/u/abarriga/nasa_sample_data/HMI_Alexander/"
# db_path = "/u/abarriga/nasa_sample_data/database/"
# noaa_home = "/u/abarriga/nasa_sample_data/noaa_data/"
# trans_image_path = "/u/abarriga/nasa_sample_data/trans_image_data/"
# feature_results_path = "/u/abarriga/nasa_sample_data/feature_extraction_results/"

# LOCAL paths 
image_path = "/Users/Alexander/NASA/NASA_Sample_Data/Images/"
db_path = "/Users/Alexander/NASA/Database/"
noaa_home = "/Users/Alexander/NASA/noaa_data/"
trans_image_path = "/Users/Alexander/NASA/trans_image_data_singleDay/"
feature_results_path = "/Users/Alexander/NASA/feature_extraction_results_singleDay/"

# load file names
file_names = []
for filename in os.listdir(image_path):
    if "ds_store" not in filename.lower():
        file_names.append(filename)

load n sample images
test_index = range(0,len(dview) )     
print "getting {} sample images...".format(len(dview) )
file_names = [ file_names[index] for index in test_index]

	
# import user defined scripts into worker engines
dview.execute('import Cylindrical_Map_Transformation')
dview.execute('import sunspot_feature_extraction')
dview.execute('import extract_features_script')
dview.execute('import Centroid_Labeling')
dview.execute('import extract_image_features')
#dview.execute('import extract_features_script_test')

# scatter file names among the worker engines 
#dview.scatter('file_name', file_names)

# push a copy of the image and results paths to every worker engine
dview.push(dict(image_path = image_path,
               trans_image_path=trans_image_path,
               feature_results_path=feature_results_path,
               db_path = db_path,
               noaa_home=noaa_home))

## map_async
def process_image(file_name):
    new_feat_objects = extract_features_script.extract_features_from_images(image_path,trans_image_path,feature_results_path, file_name)
    noaa_data = extract_features_script.load_noaa_data(noaa_home)
    extract_features_script.filter_label_saveToFile(new_feat_objects, db_path,  noaa_data)

#print "importing process_image..."
#dview.execute("import process_image(file_name)")

print "running dview.map_async(process_image, file_names).get()..."
dview.map_async(process_image, file_names).get()

end = time()
print "Time Elapsed = {:.4} minutes".format((end - start)/60)

