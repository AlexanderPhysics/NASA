import numpy as np
import pandas as pd
import os
from collections import Counter,defaultdict
import pickle
from scipy.spatial.distance import  euclidean

'''
	This file contains helper functions (functions that whos name ends with an underscore 
	and are called by other functions and NOT the user directly)
	as well as functions called by the user.
'''


def get_image_file_name_path(path):
	filelist = []
	for i, filename in enumerate(os.listdir(path)):
		# exclude the error log file
		if "ds_store" not in filename.lower():
			filelist.append(filename)
	return filelist


def get_image_trans_filenames(path):
	# get image file names
	return [image for image in os.listdir(path)]


def get_trans_image(path,image_data):
	# get transformed image data -- Equal Area Cylindrical Maps\
	trans_images = []
	for i, image in enumerate(image_data):
		if "ds_store" not in image.lower():
			trans_images.append(pickle.load(open(path + image,'r')).values)
	return trans_images

def get_features(path):
	file_names = []
	feat_extract_list = []
	for result in os.listdir(path):
		if "ds_store" not in result.lower():
			file_names.append(result)
			feat_extract_list.append( pd.read_pickle(path + result).values)
	return feat_extract_list


def truncate_(num):
	before_dec, after_dec = str(num).split('.')
	return float('.'.join((before_dec, after_dec[0:2])))


def map_centroids_long_lat(features):
	'''INPUT: feat_dict for single image
	'''
	feat_dict = features[1][0]    
	feat_dict_copy = feat_dict.copy()
	# get active region centroids in order to convert to long and lat
	active_regions = dict()
	for feature, value in feat_dict.iteritems():
		if isinstance(feature, int) == True or feature == 'car_long'or feature == 'image_type':
			# collect features for each active region as key:value pairs
			active_regions[feature] = value
			
	# all active regions, from the same image, share the same carinton longitude 
	carrinton_long = active_regions['car_long'] 
	image_type = active_regions['image_type']
	
	return map_coordinates_(active_regions, feat_dict_copy, carrinton_long, image_type )


def get_long_lat_(Xcen, Ycen, clon1, image_type):
	from math import asin, pi
	convert_degrees_MDI = 180.0/1024
	convert_degrees_HMI = 180.0/4096
	
	if image_type == "MDI":
		Longitude = clon1 + ((Xcen - 512.0) * convert_degrees_MDI)
		Latitude = asin((Ycen - 512.0)/512)
	else:
		Longitude = clon1 + ((Xcen - 2047.5) * convert_degrees_HMI)
		Latitude = asin((Ycen - 2047.5)/2048)
		
	# truncate bleeding edges
	Longitude = (Longitude + 360) % 360
	# conversion from radius to degrees
	Latitude = Latitude * 180/pi
	
	return Longitude, Latitude



	# longitude error may be here 
def map_coordinates_(active_regions, feat_dict_copy, carrinton_long, image_type):
	for key, value in active_regions.iteritems():
		if key == 0:
			continue
		if isinstance(key, int) == True:
			pos_long, pos_lat = get_long_lat_(value['x_pos_ave'], 
											 value['y_pos_ave'],
											 carrinton_long,
											 image_type)

			neg_long, neg_lat = get_long_lat_(value['x_neg_ave'], 
											 value['y_neg_ave'],
											 carrinton_long,
											 image_type)

			feat_dict_copy[key]['long_pos'] = truncate_(pos_long)
			feat_dict_copy[key]['lat_pos'] = truncate_(pos_lat)
			feat_dict_copy[key]['long_neg'] = truncate_(neg_long)
			feat_dict_copy[key]['lat_neg'] = truncate_(neg_lat)
	return feat_dict_copy


# tokenize features for all years
def tokenize_NOAA_files_(noaa_path, files):
	'''INPUT: path to NOAA directory, text files 
	   OUTPUT: tokenized text files
	'''
	import regex as re
	tokenized_files = []
	regexp = "[0-9a-zA-Z]+"
	for year in files:
		year = pd.read_table(noaa_path + year, sep='\t').values
		tokenized_files.append([re.findall(regexp,row[0]) for row in year])
	return tokenized_files

def check_date_syntax_(row):
	'''INPUT: list with tokenized string
	   OUTPUT: list with correctly tokenized string
	'''
	if len(row[1]) == 1 and len(row[2]) == 1:
		mod = [row[0] + "0"+ row[1] + "0"+ row[2]]
		return mod + row[3:]
	elif len(row[1]) == 3:
		mod  = [row[0] + "0"+row[1]]
		return mod + row[2:]
	else:
		return row

def correct_date_syntax_(years):
	'''INPUT: nested list, each list corresponds to one year
	   OUTPUT: dictionary, 
			   keys: years YYYY
			   values: nested list, each list corresponds to one year
			   list of date string lengths for debugging 
	'''
	correct_years = dict()
	santiy_check = []
	for year in years:
		correct_year = []
		for row in year:
			correct_row = check_date_syntax_(row)
			correct_year.append(correct_row)
			santiy_check.append(len(correct_row[0]))
		correct_years[row[0][0:4]] = correct_year
	return correct_years, np.unique(santiy_check)


def get_noaa_sunspot_files(path):
	# load file names
	sunspot_files = [file_ for file_ in os.listdir(path)]
	# each row is a string, tokenize it
	tokenized_results = tokenize_NOAA_files_(path, sunspot_files)
	# normalize date syntax
	results, _ = correct_date_syntax_(tokenized_results)
	return results


def get_noaa_centroids(df, scan_year):
	'''
	gets x and y components of centroids from NOAA data
	INPUT: dataframe containing all NOAA documented active regions (and revelent features) 
	OUTPUT: active region centroids x and y components seperately and jointly 
	'''
	#nova_x = []
	#nova_y = []
	y_i = None
	nova_cents = []
	for lat, clon in zip(df[scan_year == df.date].latLong.values, df[scan_year == df.date].clon.values ):

		if lat[0] == "S":
			#nova_y.append( -1 * int(lat[1:3]))
			y_i = -1 * int(lat[1:3])
		else:
			#nova_y.append(int(lat[1:3]))
			y_i = int(lat[1:3])

		#nova_x.append( int(clon))
		nova_cents.append( [int(clon), y_i] )
		#nova_y.append(y_i)
		
	return nova_cents,  df[scan_year == df.date]


def get_image_active_region_centroids(features_dict, split_centroids = True):
	'''
	gets the longitude and latitude values of every active region in the image
	'''

	x_cents = []
	y_cents = []
	ar_cents = []
	for ar_num, ar_vals in features_dict.iteritems():
		if isinstance(ar_num, int) and ar_num > 0:
			#x_cents.append(ar_vals['long_pos'])
			#y_cents.append(ar_vals['lat_pos'])
			#x_cents.append(ar_vals['long_neg'])
			#y_cents.append(ar_vals['lat_neg'])
			# get active region centroid pairs for the labeling process
			x_cent = (ar_vals['long_pos'] + ar_vals['long_neg'])/2.
			y_cent = (ar_vals['lat_pos'] + ar_vals['lat_neg'])/2.
			x_cents.append(x_cent)
			y_cents.append(y_cent)
			ar_cents.append([x_cent,y_cent])

	if split_centroids == False:
		return ar_cents
	else:
		return x_cents, y_cents 


def get_neighbor_distances_(ar_cents,nova_cents):
	'''
	Function Called by get_shortest_distance_pair

	Calculate the distance between every possible pair of points: mydata points and nova data points 
	All active regions from images that are more than dist_radius (units degrees) away 
	from noaa active regions will be considered unrelated 
	will be 

	INPUT: ar_cents, active region centroids from image 
		   nova_cents, active region centroids from image 

	'''
	dist_radius = 5

	ar_dist = defaultdict(list)
	for i, ar_cent in enumerate(ar_cents):
		novaDict = dict()
		for j, nova_cent in enumerate(nova_cents):
			dist = euclidean(ar_cent, nova_cent)
			# if distance is less than 5 degrees, 
			# append distance and ar_cent to dict
			if dist <= dist_radius:
				ar_dist[i].append(dict({j:dist}))
	return ar_dist



def get_shortest_distance_pair(ar_cents, nova_cents):

	ar_dist = get_neighbor_distances_(ar_cents,nova_cents)

	for mydata_index, novaDist in ar_dist.iteritems():
		# check if active region has more than one NOAA match
		if len(novaDist) > 1:
			dist_i = None
			noaa_i = None

			ar_match_i = 0 
			for dist_dict in novaDist:
				for noaaInd, dist in dist_dict.iteritems():
					if ar_match_i == 0:
						noaa_i = noaaInd
						dist_i = dist
					else:
						if dist < dist_i:
							dist_i = dist
							noaa_i = noaaInd
					ar_match_i += 1
			ar_dist[mydata_index] = [{noaa_i: dist_i}]
	return ar_dist

def get__noaa_ar_assignments_(mydata):
	''''


	returns list with noaa active region numbers
	the output of this funtion is used to check for repeated noaa ar assignments to image ar

	INPUT: dictionary
				keys: image ar indices that have been paired with noaa ar
				values: list embeded dictionaries
					dictionary	
						keys: noaa active region indices
						values: dist between image ar and noaa ar
	OUTPUT: list of noaa active region numbers 
	'''

	noaa_ar = []
	for mydata_ind, noaaInd_minDist in mydata.iteritems():
		noaa_ar.append(noaaInd_minDist[0].keys()[0])
	
	return noaa_ar


def get_repeating_noaa_points_(noaa_ar):
	'''
	collect repeating noaa points in dictionary
	
	INPUT: list of noaa active region number appearances 
	OUTPTU: dictionary of noaa active region number and
			count of appearances key:value pairs 
	'''
	ar_count  = defaultdict(list)
	add_one = 1
	# append one everytime a noaa active region number appears
	for ar_num in noaa_ar:
		ar_count[ar_num].append(add_one)
	# sum the number of times a noaa active region number appears 
	for key in ar_count.keys():
		ar_count[key] = np.sum(ar_count[key])
		
	# only collect the repeating noaa active region numbers 
	repeating_noaa_ar = []
	for noaa_ar_num, count in ar_count.iteritems():
		if count > 1:
			repeating_noaa_ar.append(noaa_ar_num)
	 
	return repeating_noaa_ar


def get_mydata_index_for_poping_(repeating_noaa, mydata):
	'''
	   There are multiple mydata points that have been assigned to 
	   the same noaa active region number. Identify the matched pair 
	   that has the greatest distance between, return the correstponding 
	   mydata index. 
	'''
	loop_counter = 0 
	dist_i = None
	mydata_i = None
	
	for mydata_ind, noaaInd_minDist in mydata.iteritems():
		# check if noaa active region number  
		# is in list of repeating noaa numbers 
		noaa_ar_num = noaaInd_minDist[0].keys()[0]
		
		if noaa_ar_num == repeating_noaa:
			if loop_counter == 0:
				dist_i = noaaInd_minDist[0].values()
				mydata_i = mydata_ind
			else:
				if dist_i < noaaInd_minDist[0].values():
					dist_i = noaaInd_minDist[0].values()
					mydata_i = mydata_ind  
			loop_counter += 1
	return mydata_i
	

def get_noaa_myimage_pair_min_dist_(mydata):
    loop_counter = 0 
    dist_i = None
    mydata_i = None

    for mydata_ind, noaaInd_minDist in mydata.iteritems():
        noaa_ar_num = noaaInd_minDist[0].keys()[0]

        if loop_counter == 0:
            dist_i = noaaInd_minDist[0].values()[0]
            mydata_i = mydata_ind
        else:
            if dist_i > noaaInd_minDist[0].values():
                dist_i = noaaInd_minDist[0].values()[0]
                mydata_i = mydata_ind  

        loop_counter += 1
    
    keep_key = mydata_i
    mydata_copy = mydata.copy()
    
    for mydata_key in mydata.keys():
        if mydata_key != keep_key:
            mydata_copy.pop(mydata_key)
    return mydata_copy



def get_min_dist_for_repeated_noaa_assignment_(repearing_noaa, data ):
    noaa_dist = dict()
    image_noaa_dist = dict()
    dist_i = None
    match_counter = 0 

    for imageInd, noaaInd_dist in data.iteritems():
        for noaa_index, dist in noaaInd_dist[0].iteritems():
            if noaa_index == repearing_noaa:
                if match_counter == 0:
                    noaa_dist[noaa_index] = dist
                    dist_i = dist
                    image_noaa_dist[imageInd] = noaa_dist
                    match_counter += 1
                else:
                    if noaa_dist[noaa_index] < dist_i:
                        noaa_dist[noaa_index] = dist
                        image_noaa_dist[imageInd] = noaa_dist
                        dist_i = dist
                        match_counter += 1
    return image_noaa_dist


def get_one_to_one_assignments_(data_dict, repeating_noaa_ar):
    # insert one-to-one pairs in new dict
    unique_assignments  = dict()
    for imageInd, noaaInd_dist in data_dict.iteritems():
        for noaa_index in noaaInd_dist[0].keys():
            if noaa_index not in repeating_noaa_ar:
                unique_assignments[imageInd] = noaaInd_dist
    return unique_assignments


def check_repeating_noaa_assignments(data):
	'''
	INPUT: dictionary
				keys are index of active region from image data
				values is a list of single item dictionaries 
				dictionary
					key noaa active region index
					value distance between noaa AR and image AR 

	Check if a multiple mydata points were assiged to the same noaa active region number
	
	CASE 1: no repeating assignments
	CASE 2: k out of n (where k < n) mydata points were assigned to the same noaa active region number
	CASE 3: k = n, all mydata points were assigned to the same noaa active region number 
	CASE 4: more than 1 mydata points are assiged to repated noaa active region numbers    
	'''

	noaa_ar = get__noaa_ar_assignments_(data)
	# CASE 1
	# if True, then there does not exist repeating noaa assignments 
	# check: number of noaa ar equals number of unique noaa ar assinged 
	if len(noaa_ar) == len(np.unique(noaa_ar)):
		return data
    # check CASE 3

	# CASE 2 
	# if True, only one noaa ar number was assigned to mulitple mydata ar
	elif len(np.unique(noaa_ar)) == 2:
		# check CASE 2 
		# identify and retrieve noaa active region numbers
		repeating_noaa_ar = get_repeating_noaa_points_(noaa_ar)
		# loop through the noaa numbers and remove (pop) 
		# that don't have the min dist 
		for repeating_noaa in repeating_noaa_ar:
			pop_index = get_mydata_index_for_poping_(repeating_noaa, data)
			data.pop(pop_index)
		return data

	# Case 3
	# if True, all mydata points were assinged to a single noaa active region number
	elif len(np.unique(noaa_ar)) == 1:
		data = get_noaa_myimage_pair_min_dist_(data)
		return data

	# Case 4
	# if True, multiple noaa ar numbers have repeating assignments to mydata ar 
	else:
		repeating_noaa_ar = [noaa_index for noaa_index, count in Counter(noaa_ar).iteritems() if count > 1]
		data_results = get_one_to_one_assignments_(data, repeating_noaa_ar)

		for repeating_noaa in repeating_noaa_ar:
		    one_to_one = get_min_dist_for_repeated_noaa_assignment_(repeating_noaa, data)
		    data_results[one_to_one.keys()[0]] = one_to_one.values()

		return data_results





