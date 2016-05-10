from collections import defaultdict, Counter
import numpy as np
import pickle


# --------- helper functions ------------ 
def truncate(num):
    before_dec, after_dec = str(num).split('.')
    return float('.'.join((before_dec, after_dec[0:2])))


def get_header_parameters(hdu):
    image_date = hdu.header["DATE-OBS"].replace("/", " ")
    image_time = hdu.header["TIME-OBS"].rstrip(".").partition(".")[0]
    image_type = hdu.header["INSTRUME"]
    if image_type == "MDI":
        # MDIw
        car_long = hdu.header["OBS_L0"]
    else:
        # HMI 
        car_long = hdu.header["CRLN_OBS"]

    param_names = ["image_date", "image_time", "car_long", 'image_type']
    params = [image_date, image_time, car_long, image_type]
    
    return params, param_names


# ------ Extract Active Region Functions --------------


# 0
def identify_active_regions(labels, num_pixel_in_active_region): 
    '''INPUT: 2D array of labeled active regions
              num_pixel_in_active_region, int 
       OUTPUT: 2D array of labeled active regions 
    '''
    count_ar = defaultdict(list)
    for row in labels:
        for elem in row:
            count_ar[elem].append(elem)
            
    # Need a pixel threshold for filtering false positive regions\
    # let's try 100 pixels
    # index 0 is for the background 
    
    active_labels = []
    for key, val in count_ar.iteritems():
        # exclude false positive regions and exclude zeroed-out background
        if Counter(val)[key] >= num_pixel_in_active_region and np.sum(val) > 1 :
            active_labels.append(key)
    return active_labels


# 1
def get_flux_from_active_regions(image, labels, active_labels):
    '''INPUT: 2D array with each pixel labeled with the number of its corresponding active region
       OUTPUT: Dictionary, 
               keys indicate active region
               values are list flux values for each active region 
    '''
    active_regions_dict = defaultdict(list)
    for image_row, label_row in zip(image, labels):  
        for image_col, label_col in zip(image_row, label_row):   
            if label_col in active_labels:
                active_regions_dict[label_col].append(image_col)
                
    return active_regions_dict


# 2 
def get_net_flux_along_dim(image_array):
    net_B_pos = 0
    net_B_neg = 0 
    for elem in image_array:
        if np.sign(elem) == 1:
            net_B_pos += elem
        else:
            net_B_neg += elem    
    return net_B_pos, net_B_neg


# 3
def get_net_flux(active_regions):
    '''INPUT: Dictionary
              keys indicate active region 
              values are a list of flux values for each active region
       OUTPUT: Dictionary
              keys indicate active region
              values are a tuple of net flux values for each polarity of each active region
    '''
    flux_dict = dict()
    for ar, blist in active_regions.iteritems():
        flux_dict[ar] = get_net_flux_along_dim(blist)
    return flux_dict



# 4
# list of x * B(x,y)  values for each active region

# Assuming there exist more than one active regions
def get_flux_along_dim(valid_labels, image, labels):
    '''
       Calculates  x * B(x,y) and  y * B(x,y) values for each active region
       INPUT: 
             valid_labels, list of non-pseudo active regions
             image, array of image data
             labels, array of all labels for 
       OUTPUT: Dictionary
              keys indicate active region
              values are a tuple of net flux values for each polarity of each active region
    '''
    B_pos_dict = defaultdict(list)
    B_neg_dict = defaultdict(list)
#     B_pos_y_dict = defaultdict(list)
#     B_neg_y_dict = defaultdict(list)
    for row in xrange(0, len(image)):
        for col in xrange(1, len(image)):
            
            if labels[row][col] in valid_labels:
                
                if np.sign(image[row][col]) == 1:
                    B_pos_dict[labels[row][col]].append(col * image[row][col])
                else:
                    B_neg_dict[labels[row][col]].append(col * image[row][col])
                                                                    
    return B_pos_dict, B_neg_dict

# 5
def get_dim_sum(dim_dict):
    '''
       Calculates  x * B(x,y) and  y * B(x,y) values for each active region
       INPUT: 
             valid_labels, list of non-pseudo active regions
             image, array of image data
             labels, array of all labels for 
       OUTPUT: Dictionary
              keys indicate active region
              values are a tuple of net flux values for each polarity of each active region
    '''
    result_dict = dict()
    for active_region, b_list in dim_dict.iteritems():
        result_dict[active_region] = np.sum(b_list)
    return result_dict  

# 6
def get_features(net_flux_active_regions, B_pos_x_sum, B_neg_x_sum, B_pos_y_sum, B_neg_y_sum, hdu, file_name):
    '''
       Calculates  centroid of each polarity in each active region 
       INPUT: 
             valid_labels: list of active regions
             Flux Fields: dictinary
             hdu: fits file reader object for a single image
       OUTPUT: Dictionary
              keys indicate active region
              values are  centroids of each polarity in each active region 
    '''

    results_dict = dict()
    bugs = []
    bugs_found = False
    feat_names = ["x_pos_ave",
                  "x_neg_ave",
                  "y_pos_ave", 
                  "y_neg_ave",
                  "pos_net_flux",
                  "neg_net_flux"]

    active_region_num = 1

    # get centroids for each active region in the image
    for label, net_flux in net_flux_active_regions.iteritems():
      features_dict = dict()
      # (CASE 3 ) check if One or More Active Regions in Image have netflux = zero
      if net_flux[0] == 0.0 or net_flux[1] == 0.0:
          # assign a key:value pair of {0:(0,0)} indicating zero flux for this active region
          results_dict[0] = (0,0)
          error_type = "One or more flux for active region {} is zero.".format(active_region_num)
          bugs.append((file_name, error_type))
          bugs_found = True            
          
      else:
          # get centroids
          x_pos_ave = truncate(B_pos_x_sum[label]/ net_flux[0])
          x_neg_ave = truncate(B_neg_x_sum[label]/ net_flux[1])
          y_pos_ave = truncate(B_pos_y_sum[label]/ net_flux[0])
          y_neg_ave = truncate(B_neg_y_sum[label]/ net_flux[1])

          feats = [x_pos_ave, 
                   x_neg_ave, 
                   y_pos_ave, 
                   y_neg_ave,
                   truncate(net_flux[0]),
                   truncate(net_flux[1])]

          # pair each feature name with feature values   
          for feat, name in zip(feats, feat_names):
              features_dict[name] = feat 

          # pair each active region number with it's list of features 
          results_dict[active_region_num] = features_dict

      active_region_num += 1


    # if bugs_found == True:
    #   # write bugs to log file
    #   log_path = "/u/abarriga/nasa_sample_data/image_error_log/"
    #   file_ = file_name.rstrip(".fits").replace(".","_")
    #   with open(log_path + file_ + ".pk", 'w') as file_path:
    #     pickle.dump(net_flux,file_path)
    
    # get header parameters for the image
    params, param_names = get_header_parameters(hdu)
    for param, name, in zip(params, param_names):
        results_dict[name] = param

    return results_dict

# --------- Clean Up Functions (Post Active Region Extraction) ----------- 



def zero_active_regions_image(hdu):
    # assign a key:value pair of {0:(0,0)} indicating zero active regions
    results_dict = dict()
    results_dict[0] = (0,0)
    # get header parameters for the image
    params, param_names = get_header_parameters(hdu)
    for param, name, in zip(params, param_names):
        results_dict[name] = param
    return results_dict

def single_active_region_zero_netflux(hdu, file_name):
    # this is an entry in the log ----->  file save to picke file 
    # write bugs to log file
    # log_path = "/u/abarriga/nasa_sample_data/image_error_log/"
    # with open(log_path + file_name, 'w') as file_path:
    #pickle.dump(test,file_path)
    # assign a key:value pair of {0:(0,0)} indicating zero active regions
    results_dict = dict()
    results_dict[0] = (0,0)
    # get header parameters for the image
    params, param_names = get_header_parameters(hdu)
    for param, name, in zip(params, param_names):
      results_dict[name] = param
    return results_dict


# ---------- API Function -------------- 

# 7 extract_features is the only function that the user runs
def extract_features(labels,image, hdu, file_name, num_pixel_in_active_region ):

    # Get Features Has 4 Cases:
    # 1. Zero Active Regions in Image
    # One Active region
    #     2. Netflux = Zero
    # One or More Active Regions in Image
    #     3. Netflux = Zero for at least one active region 
    #     4. Netflux != Zero for all active regions
    # Case 3 and 4 are handled in the get_features function

    # exclude false positive active regions
    ar_labels = identify_active_regions(labels, num_pixel_in_active_region )

    # (CASE 1) check for zero active regions in image
    if len(ar_labels) == 0:
      zero_active_regions_image(hdu)

    # list of b_pos and b_neg values for each active region
    active_region_flux = get_flux_from_active_regions(image, labels, ar_labels)
    # list of x * B(x,y)  values for each active region
    B_pos_x_dict, B_neg_x_dict = get_flux_along_dim(ar_labels, image, labels)
    # list of y * B(x,y) values for each active region
    B_pos_y_dict, B_neg_y_dict = get_flux_along_dim(ar_labels, image.T, labels.T)
    # net pos and net flux for each active region
    net_flux_active_regions = get_net_flux(active_region_flux)

    # (CASE 2)check for one active region with at least one netflux = zero
    # check for single active region 
    if sum(net_flux_active_regions) ==1:
      # check if one of the netflux = 0 
      if net_flux_active_regions.values()[0][0] == 0 or net_flux_active_regions.values()[0][1] == 0:
        single_active_region_zero_netflux(hdu, file_name)


    #Calculates  x * B(x,y) and  y * B(x,y) values for each active region
    B_pos_x_sum =  get_dim_sum(B_pos_x_dict)
    B_neg_x_sum =  get_dim_sum(B_neg_x_dict)
    B_pos_y_sum =  get_dim_sum(B_pos_y_dict)
    B_neg_y_sum =  get_dim_sum(B_neg_y_dict)

    return get_features(net_flux_active_regions, B_pos_x_sum, B_neg_x_sum, B_pos_y_sum, B_neg_y_sum, hdu, file_name)
