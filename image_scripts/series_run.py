# Import Python Libraries 
import os
from astropy.io import fits
from math import pi, sin, cos, sqrt, atan2
from time import time
import numpy as np
import pandas as pd
from astropy.convolution import convolve_fft, convolve
import astropy.convolution
import matplotlib.pyplot as plt

# Import user defined modules 
from Cylindrical_Map_Transformation import get_header_params_MDI, map_disk_cylindric
import sunspot_feature_extraction
import extract_features_script
import Centroid_Labeling
import extract_image_features

# navagate to image location
#image_path = "/Users/Alexander/NASA/NASA_Sample_Data/Images/"
image_path = "/Users/Alexander/NASA/NASA_Sample_Data/HMI_Alexander/"
# load file names
file_names = []
for filename in os.listdir(image_path):
    if "ds_store" not in filename.lower():
        file_names.append(filename) 

file_name = "HMI.m2011.01.17_00_00_00.fits"

start = time()
new_feat_objects = extract_features_script.extract_features_from_images(image_path, file_name)
end = time()
print "Time Elapsed = {}".format(end - start)

print "new_feat_object ", new_feat_objects

print "\n load noaa data..."
noaa_home = "/Users/Alexander/NASA/noaa_data/"
noaa_data = extract_features_script.load_noaa_data(noaa_home)

print "running filter_label_saveToFile..."
db_path = "/Users/Alexander/NASA/Database/"
extract_features_script.filter_label_saveToFile(new_feat_objects, db_path, noaa_data)