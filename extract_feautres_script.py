import astropy
from astropy.io import fits
from math import pi, sin, cos, sqrt, atan2
from time import time
import numpy as np
import pandas as pd
import os
from Cylindrical_Map_Transformation import get_header_params_MDI, map_disk_cylindric
from collections import Counter,defaultdict
import astropy.convolution
from astropy.convolution import convolve_fft, convolve
from time import time
from scipy import ndimage as ndi
from skimage.morphology import watershed
from skimage.feature import peak_local_max
import pickle
from sunspot_feature_extraction import extract_features
import json


def preprocess_image(hdu_object):
    # upload data to dataframe in order to replace nans with zeros
    df = pd.DataFrame(hdu_object.data)
    df.fillna(value=0,inplace=True)
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

def get_active_region_map(path, image_file):
    kernal_std = 10
    gauss = astropy.convolution.Gaussian2DKernel(stddev=kernal_std)
    hdulist = fits.open(path + image_file)
    hdu = hdulist[0] # <-- NEED CASE CONDITIONAL FOR MDI/HMI
    #image_date = hdu.header["DATE-OBS"].replace("/", " ")
    #image_time = hdu.header["TIME-OBS"].rstrip(".").partition(".")[0]
    clean_data = preprocess_image(hdu)
    data_abs = np.abs(clean_data)

    if hdu.header["INSTRUME"] == "MDI":
        k = 130 # <-- use this value, confirmed by David
    else: 
        k = 130 # TODO --> NEED TO FIND TRUE K VALUE FOR HMI DATA !!!

    # smooth data with Fast Fourier Transform
    smoothing = convolve_fft(data_abs, gauss)
    smoothing[smoothing < k] = 0.
    smoothing[smoothing >= k] = 1 
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

def extract_features_from_images(image_path, file_name):
    '''
    Changing results path for processing a single day of images 
    '''
    feat_results = []
    
    print "get active region map..."
    image, hdu= get_active_region_map(image_path, file_name)

    print "save image data to picke file..."
    #trans_image_path = "/Users/Alexander/NASA/trans_image_data_test/"
    trans_image_path = "/Users/Alexander/NASA/trans_image_data_singleDay/"
    save_to_file(image, trans_image_path, file_name)
    
    # Now we want to separate the  active regions in the image
    # Generate the markers as local maxima of the distance to the background
    print "identify active regions in image for file"
    labels = get_active_region_labels(image)

    # extract features from image
    print "extract features from image"
    extract_results = extract_features(labels,image, hdu, file_name)
    #feat_results.append((file_name, extract_results))

    print "saving extracted features to pickle file from image"
    #feature_results_path = "/Users/Alexander/NASA/feature_extraction_results_test/"
    feature_results_path = "/Users/Alexander/NASA/feature_extraction_results_singleDay/"

    #save_to_file((file_name, extract_results), feature_results_path, file_name)
    save_to_file([file_name, extract_results], feature_results_path, file_name)
        

if __name__ == "__main__":

    print "get images from file..."
    image_path = "/Users/Alexander/NASA/NASA_Sample_Data/Images/"
    filelist = []
    for filename in os.listdir(image_path):
        if "ds_store" not in filename.lower():
            filelist.append(filename)

    # process images that are sufficiently differnt from each other
    #test_index = np.arange(1, 400, 50) # used for diverse image sub-smaple
    test_index = range(0,15)     
    print "getting test_files for single day..."
    test_files = [ filelist[index] for index in test_index]

    print "run extract_feautres_from_images function..."
    for i, file_ in enumerate(test_files):
        start = time()
        extract_features_from_images(image_path, file_)
        end = time()
        print "TIME ELAPSED {0} mins, IMAGE DONE {1}".format((end - start)/60.0, i)
