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
from astropy.convolution import convolve_fft
from time import time
from scipy import ndimage as ndi
from skimage.morphology import watershed
from skimage.feature import peak_local_max
import pickle
import extract_image_features 
import Centroid_Labeling
from sunspot_feature_extraction import extract_features
from Cylindrical_Map_Transformation import get_header_params_MDI,get_header_params_HMI, map_disk_cylindric



# ------- Test Bundle 1 (START) ---------

def extract_features_from_images_test_1(image_path,trans_image_path,feature_results_path, file_name, mdi_flux_filter = 120, hmi_flux_filter = 120, kernal_std = 8,num_pixel_in_active_region = 100):

	image = get_active_region_map_1(image_path, file_name,  mdi_flux_filter , hmi_flux_filter, kernal_std)
	return image, image_path,trans_image_path, feature_results_path, file_name

def extract_features_from_images_test_2(image_path,trans_image_path,feature_results_path, file_name, mdi_flux_filter = 120, hmi_flux_filter = 120, kernal_std = 8,num_pixel_in_active_region = 100):

    image, hdu = get_active_region_map_2(image_path, file_name,  mdi_flux_filter , hmi_flux_filter, kernal_std)
    return image, image_path, trans_image_path, feature_results_path, file_name, hdu

def get_active_region_map_1(path, image_file, mdi_flux_filter, hmi_flux_filter, kernal_std):
    

    if "HMI" in image_file:
        flux_magnitude_filter = hmi_flux_filter
        kernal_std = kernal_std * 4
        hdu_index = 1
        hdu_index = 0
        flux_magnitude_filter = mdi_flux_filter
    else:
        hdu_index = 0
        flux_magnitude_filter = mdi_flux_filter

    gauss = astropy.convolution.Gaussian2DKernel(stddev=kernal_std)
    hdulist = fits.open(path + image_file)
    hdu = hdulist[hdu_index] # <-- NEED CASE CONDITIONAL FOR MDI/HMI
    clean_data = preprocess_image(hdu)
    data_abs = np.abs(clean_data)

    return data_abs


def get_active_region_map_2(path, image_file, mdi_flux_filter, hmi_flux_filter, kernal_std):
    

    if "HMI" in image_file:
        flux_magnitude_filter = hmi_flux_filter
        kernal_std = kernal_std * 4
        hdu_index = 1
        hdu_index = 0
        flux_magnitude_filter = mdi_flux_filter
    else:
        hdu_index = 0
        flux_magnitude_filter = mdi_flux_filter

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

def preprocess_image(hdu_object):
    # upload data to dataframe in order to replace nans with zeros
    df = pd.DataFrame(hdu_object.data)
    df.fillna(value=0,
              inplace=True)
    clean_data = df.values
    return clean_data
    
    # # choice appropriate method base on file type
    # if hdu_object.header["INSTRUME"] == "MDI":
    #     params_method = get_header_params_MDI
    # else:
    #     params_method = get_header_params_HMI
        
    # # extract parameters from header  
    # xCen,yCen,s0,nx,pixsize,p0,b0, r0 = params_method(hdu_object.header)
    
    # # transform data into cylindrical equal area map
    # return map_disk_cylindric(xCen,yCen,s0,nx,pixsize,p0,b0, r0, clean_data)



def save_to_file(data, path, filename):
    # Save data to pickle file 
    pd.DataFrame(data).to_pickle(path + "{}.pk".format(filename.rstrip(".fits").replace(".","_")))

# ------- Test Bundle 1 (END) ---------

# ------- Test Bundle 2 (START) ---------

def get_active_region_labels_test(image):
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


