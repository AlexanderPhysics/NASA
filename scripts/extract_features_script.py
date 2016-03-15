import astropy
from astropy.io import fits
from math import pi, sin, cos, sqrt, atan2
from time import time
import numpy as np
import pandas as pd
import os
import json
from collections import Counter,defaultdict
import astropy.convolution
from astropy.convolution import convolve_fft, convolve
from time import time
from scipy import ndimage as ndi
from skimage.morphology import watershed
from skimage.feature import peak_local_max
import pickle

import extract_image_features 
import Centroid_Labeling
from sunspot_feature_extraction import extract_features
from Cylindrical_Map_Transformation import get_header_params_MDI,get_header_params_HMI, map_disk_cylindric

# ----------- extract_features_from_images helper functions -------------

def preprocess_image(hdu_object):
    # upload data to dataframe in order to replace nans with zeros
    df = pd.DataFrame(hdu_object.data)
    df.fillna(value=0,
              inplace=True)
    clean_data = df.values
    
    # choice appropriate method base on file type
    if hdu_object.header["INSTRUME"] == "MDI":
        params_method = get_header_params_MDI
    else:
        params_method = get_header_params_HMI
        
    # extract parameters from header  
    xCen,yCen,s0,nx,pixsize,p0,b0, r0 = params_method(hdu_object.header)
    
    # transform data into cylindrical equal area map
    return map_disk_cylindric(xCen,yCen,s0,nx,pixsize,p0,b0, r0, clean_data)

def get_active_region_map(path, image_file, mdi_flux_filter, hmi_flux_filter, kernal_std):
    

    if "HMI" in image_file:
        flux_magnitude_filter = hmi_flux_filter
        kernal_std = kernal_std * 4
        hdu_index = 1
        hdu_index = 0
        flux_magnitude_filter = mdi_flux_filter
    else:
        hdu_index = 0
        flux_magnitude_filter = mdi_flux_filter

    # if hdu.header["INSTRUME"] == "MDI":
    #     flux_magnitude_filter = mdi_flux_filter # <-- use this value, confirmed by David
    #     hdu_index = 0
    # else: 
    #     flux_magnitude_filter = hmi_flux_filter # TODO --> NEED TO FIND TRUE VALUE FOR HMI DATA !!!
    #     kernal_std = kernal_std * 4
    #     hdu_index = 1

    gauss = astropy.convolution.Gaussian2DKernel(stddev=kernal_std)
    hdulist = fits.open(path + image_file)
    hdu = hdulist[hdu_index] # <-- NEED CASE CONDITIONAL FOR MDI/HMI
    clean_data = preprocess_image(hdu)
    data_abs = np.abs(clean_data)

    # smooth data with Fast Fourier Transform
    smoothing = convolve_fft(data_abs, gauss)
    smoothing[smoothing < flux_magnitude_filter] = 0.
    smoothing[smoothing >= flux_magnitude_filter] = 1 
    # filter original image with active region map 
    return np.where(smoothing !=0.0 , clean_data, smoothing), hdu

def get_active_region_labels(image):
        # Exact euclidean distance transform
        distance = ndi.distance_transform_edt(image)
        # Find peaks in an image, and return them as coordinates or a boolean array
        local_maxi = peak_local_max(distance, 
                                    indices=False, 
                                    footprint=np.ones((3, 3)),
                                    labels=image)
        # return labels of peaks
        markers = ndi.label(local_maxi)[0]
        # return active region labels 
        return watershed(-distance, markers, mask=image)

def save_to_file(data, path, filename):
    # Save data to pickle file 
    pd.DataFrame(data).to_pickle(path + "{}.pk".format(filename.rstrip(".fits").replace(".","_")))


# ------------ filter_label_saveToFile helper functions -------

# noaa data will be loaded into each engine before images are sent through the pipeline 
def load_noaa_data(noaa_home):
    return extract_image_features.get_noaa_sunspot_files(noaa_home)

def get_noaa_and_image_centroids(image_features_dict, noaa_data):
    
    # For Local Series Computing, uncomment function 
    #noaa_data = load_noaa_data()
    
    
    # Cluster/Parallel computing 
    # noaa_data will have already been pushed  into each engine
    noaa_cents_sameDay,\
    noaa_cents_prevDay =\
    Centroid_Labeling.get_currentDay_previusDay_noaa_activeRegions(image_features_dict, noaa_data)

    _, hour, minute = image_features_dict["image_time"].split(":")

    # check if image occured in 1st or 2nd half of the day 
    if int(hour) >= 12:
        noaa_cents = noaa_cents_sameDay
    else:
        noaa_cents = noaa_cents_prevDay

    my_cents = extract_image_features.get_image_active_region_centroids(image_features_dict, split_centroids = False)
    
    return my_cents, noaa_cents





# ------------ API Functions --------------------

def extract_features_from_images(image_path,trans_image_path,feature_results_path, file_name, mdi_flux_filter = 90, hmi_flux_filter = 85, kernal_std = 8,num_pixel_in_active_region = 100):
    '''
    Changing results path for processing a single day of images 
    INPUT: image_path, string
           file_name, string
           mdi_flux_filter (int) is passed into get_active_region_map 
           hmi_flux_filter (int) is passed into get_active_region_map 
           kernal_std (int) is passed into get_active_region_map
    '''
    feat_results = []
    
    print "get active region map..."
    image, hdu= get_active_region_map(image_path, file_name,  mdi_flux_filter , hmi_flux_filter, kernal_std)

    print "save image data to picke file..."
    save_to_file(image, trans_image_path, file_name)
    
    # Now we want to separate the  active regions in the image
    # Generate the markers as local maxima of the distance to the background
    print "identify active regions in image for file"
    labels = get_active_region_labels(image)

    # extract features from image
    print "extract features from image"
    extract_results = extract_features(labels,image, hdu, file_name, num_pixel_in_active_region)

    print "saving extracted features to pickle file from image"
    save_to_file([file_name, extract_results], feature_results_path, file_name)
    print "extracted features have been saved to file!"

def filter_label_saveToFile(image_features_dict, db_path, my_cents, noaa_cents, noaa_data):
    '''
    Filters image AR assingments to NOAA AR labels,
    Labels image AR with NOAA AR labels or with generate labels,
    Saves final image features to file

    Input: image_features_dict, dictionary
           keys are image active region numbers
           values are active region features i.e. centroids, flux, 
    Output: None
    '''
    
    #db_path = "/Users/Alexander/NASA/Database/"

    my_cents, noaa_cents = get_noaa_and_image_centroids(image_features_dict, noaa_data)
    # Assinge extracted AR to NOAA AR if dist between any 2 is less than 5 degrees
    # this will return one-to-many parings, if they exist 
    shortest_dist_pairs = extract_image_features.get_shortest_distance_pair(my_cents, noaa_cents)

    
    # returns one-to-one pairs with the shortest distance between them
    # {noaa_ar: [{Image_ar : dist}]
    clean_ar_assignments = extract_image_features.check_repeating_noaa_assignments(shortest_dist_pairs)

    clean_feat_object = Centroid_Labeling.filter_extra_active_region_assignments(image_features_dict, 
                                                                                 clean_ar_assignments)

    Centroid_Labeling.save_features_to_file(db_path, clean_feat_object)



