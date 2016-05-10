import extract_image_features as extract
#import matplotlib.pyplot as plt
import pandas as pd
import numpy as np



def plot_sunspots_and_active_regions(df, scan_year, features, time_slice):
    '''
        use scan_year to shift through noaa observations
        use time_slice to scan through image data
    '''
    
    noaa_cents, _  = extract.get_noaa_centroids(df, scan_year)
    noaa_x, noaa_y = unpack_noaa_cents(noaa_cents)
    x_cents, y_cents= extract.get_image_active_region_centroids(features[time_slice])
    plt.figure(figsize=(10,10))
    noaa = plt.scatter(noaa_x, noaa_y, c='b', marker='o');
    me = plt.scatter(x_cents, y_cents , c='r',marker='+');
    plt.title("Sunspots & Active Regions    " + scan_year);
    plt.legend((me, noaa),
               ('mydata-sunspots', 'noaa-AR'),
               scatterpoints=1,
               loc='lower right',
               ncol=2,
               fontsize=15);



def unpack_noaa_cents(noaa_cents):
    '''
        Input: nested lists of noaa get_noaa_centroids
        Output: lists
                x_cents, y_cents
    '''
    x_cents = []
    y_cents = []
    for noaa_cent in noaa_cents:
        x_cents.append(noaa_cent[0])
        y_cents.append(noaa_cent[1])
    return x_cents, y_cents


def get_current_date(features_dict):
    return "".join(features_dict['image_date'].split(" "))


def plot_sunSpots_on_activeRegions(image_activeRegions_array, sunSpot_features_dict, scan_year_singleDay):
    
    plt.figure(figsize = (10,10));
    test_plot_diff  = np.ma.masked_where(image_activeRegions_array == 0.0,\
                                         image_activeRegions_array )

    for index in sunSpot_features_dict.keys():
        if isinstance(index, int) and index > 0:
            plt.scatter(sunSpot_features_dict[index]["x_pos_ave"],\
                        sunSpot_features_dict[index]["y_pos_ave"], c='k', marker ='o');
            plt.scatter(sunSpot_features_dict[index]["x_neg_ave"],\
                        sunSpot_features_dict[index]["y_neg_ave"], c='k', marker ='o');

    plt.imshow(test_plot_diff, interpolation='none', origin='lower');
    plt.title("Sunspots & Active Regions    " + scan_year_singleDay);
    # plt.xlim([775,850])
    # plt.ylim([500,600])
    plt.show();

# Search for previous date algorithm
#
# check day - if day >= 2 , then subtract 1 from day and pass on data to exraction function 
#             if day == 1, then check if 31st of previous month exist
#
#
# Bottom portion is a seperate function
#                              if true, pass on data to exraction function 
#                          elif check if 30st of previous month exist
#                              if true, pass on data to exraction function 
#                          elif check if 29st of previous month exist
#                              if true, pass on data to exraction function 
#                          elif check if 28st of previous month exist
#                              if true, pass on data to exraction function 
#                          else skip image because data doesn't exist
def get_previous_date(year, month, day):
    '''
    Get the date the for day that preceeds image data date
    
    INPUT: Image date, string YYYYMMDD
    OUTPUT: Previous date, string YYYYMMDD
    '''
    # if day >= 2 , then subtract 1 from day
    if int(day) >= 2:
        day = int(day)
        day -= 1
        day = str(day)
        # if resulting day is single digit, add leading '0'
        if len(day) == 1:
            day = "0" + day
    # if day == 1, then get 31st of previous month 
    else:
        # if month >= 2 , then subtract 1 from month
        if int(month) >= 2:
            month = str(int(month) - 1)
            # if month is single digit, add leading '0'
            if len(month) == 1:
                month = "0" + month
        # if month == 1, then set month to 12 and get previous year 
        else:
            month = str(12)
            year = str(int(year) - 1)
        day = "31"

    return year + month + day


def get_prevous_day_noaa_df(noaa_data, year, month, day):
    import pandas as pd
    
    # |date|  |ar_num| |lat/long| |clon| |area(Msolar-hemishpere)|
    df = pd.DataFrame(noaa_data[year])
    df.columns = ["date", 'activeRegionNum', "latLong", "clon", "area", 5,6,7,8]
    df = df[["date", 'activeRegionNum', "latLong", "clon", "area"]]
    
    # get previous date
    previous_day_scan = get_previous_date(year, month, day)
    # get size of data for previous day
    df_size = df[previous_day_scan == df.date].size
    # if df of previous day is empty
    if df_size== 0 :
        
        # update year
        year = previous_day_scan[:4]
        # update month
        month = previous_day_scan[4:6]
        # update day
        day = previous_day_scan[-2:]
        
        # if df is empty and day is greater than 28 (reference to Feburary)
        while df_size == 0 and day >= 28:
            # go back one more day
            #print "previous_day_scan ", previous_day_scan
            previous_day_scan = get_previous_date(year, month, day)
            # check if this day has data
            df_size = df[previous_day_scan == df.date].size
            
            if day == 27:
                return "SKIP IMAGE: day 27 reached"
            if df_size > 0:
                break
                
            # update year
            year = previous_day_scan[:4]
            # update month
            month = previous_day_scan[4:6]
            # update day
            day = previous_day_scan[-2:]
            #print "Year Month Day ", year, month, day

    # return noaa data
    return df[previous_day_scan == df.date], previous_day_scan


def get_current_day_noaa_df(new_feat_objects_singleDay, noaa_data):
    '''
    ToDo: address edge case in which current day is Jan 1st
          and previous day is Dec 31st
          noaa_df will have to the last 3 days of Dec
    '''
    import pandas as pd
    # scan_date YYYMMDD
    scan_date = get_current_date(new_feat_objects_singleDay)
    # get current year
    year = scan_date[:4]
    # get current month
    month = scan_date[4:6]
    # get current day
    day = scan_date[-2:]
    
    # |date|  |ar_num| |lat/long| |clon| |area(Msolar-hemishpere)|
    df = pd.DataFrame(noaa_data[year])
    df.columns = ["date", 'activeRegionNum', "latLong", "clon", "area", 5,6,7,8]
    df = df[["date", 'activeRegionNum', "latLong", "clon", "area"]]
    
    return df, year, month, day


def get_currentDay_previusDay_noaa_activeRegions(new_feat_objects, noaa_data, get_df = False):
    '''
    Output: nova_cents_labels_sameDay, tuple
            nova_cents_labels_prevDay, tuple
    '''
    import pandas as pd
    # get noaa file from same date as image data
    same_day_df, year, month, day = get_current_day_noaa_df(new_feat_objects, 
                                                            noaa_data)
    
    current_day = year + month + day
    # these are NOAA active region centroid 
    nova_cents_labels_sameDay, sameDay_df = extract.get_noaa_centroids(same_day_df, 
                                                                current_day)

    previous_day_df, previous_day = get_prevous_day_noaa_df(noaa_data, 
                                                            year, 
                                                            month, 
                                                            day)
    # these are NOAA active region centroid 
    nova_cents_labels_prevDay, prevDay_df = extract.get_noaa_centroids(previous_day_df, 
                                                                previous_day)

    if get_df == False :
        return  nova_cents_labels_sameDay, nova_cents_labels_prevDay
        
    else:
        return  nova_cents_sameDay, nova_cents_prevDay, same_day_df, previous_day_df


def filter_extra_active_region_assignments(new_feat_objects, clean_ar_assignments):
    new_feat_objects_copy = new_feat_objects.copy()
    
    all_int_image_keys = [key for key in new_feat_objects_copy if isinstance(key, int)]

    clean_ar_numbers = [ key for key in clean_ar_assignments.keys()]
    
    for image_ar_num in all_int_image_keys:
        if image_ar_num in clean_ar_numbers:
            noaa_ar_label = clean_ar_assignments[image_ar_num][0]["noaa_ar_label"]
            new_feat_objects_copy[image_ar_num]["noaa_ar_label"] = noaa_ar_label
        else:
            new_feat_objects_copy.pop(image_ar_num)
    
    if 0 in new_feat_objects_copy.keys():
        new_feat_objects_copy.pop(0)
        
    return new_feat_objects_copy



def save_features_to_file(path, single_image):
    import pandas as pd	    
    '''
        Input: 
            path: string,  path to final results file 
            single_image: dictionary, extracted features from n number of sunspots 

        Output: 
            n rows of data appended to resutls file, features.txt
    '''
    rows = []
    for feat_key, feat_val in single_image.iteritems():
        # filter out non-active regions and false positive active regions
        if isinstance(feat_key, int) and feat_key > 0:
            
            # get postive value features for active region
            row = [single_image['image_date'], 
                   single_image['image_time'], 
                   feat_val["noaa_ar_label"] + "P", 
                   feat_val['pos_net_flux'], 
                   feat_val['long_pos'], 
                   feat_val['lat_pos']]
            rows.append('\t'.join(map(str,row)))
            
            # get negative value features for active region
            row = [single_image['image_date'],
                   single_image['image_time'], 
                   feat_val["noaa_ar_label"] + "N", 
                   feat_val['neg_net_flux'], 
                   feat_val['long_neg'], 
                   feat_val['lat_neg']]
            
            rows.append('\t'.join(map(str,row)))

    # TODO - CHECK IF FILE EXIST, IF NOT CREATE FILE !
    # save active region features to text file
    #magneto_vector = pd.DataFrame(rows)
    #magneto_vector.to_csv(path + 'features.txt', 
    #                       index=False, 
    #                       header=False, 
    #                       mode='a')




    if single_image["image_type"].lower() == "mdi":
        pd.DataFrame(rows).to_csv(path + 'features_mdi.txt', 
                                  index=False, 
                                  header=False, 
                                  mode='a')
    else:
        pd.DataFrame(rows).to_csv(path + 'features_hmi.txt', 
                          index=False, 
                          header=False, 
                          mode='a')
