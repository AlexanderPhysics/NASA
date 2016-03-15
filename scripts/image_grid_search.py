
import extract_image_features as extract
def get_current_date(features_dict):
    for index in range(len(features_dict)):
        try:
            date = "".join(features_dict[index]['image_date'].split())
            break
        except KeyError: 
            pass
    return date

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
def get_previous_date_(year, month, day):
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


def get_prevous_day_noaa_df_(noaa_data, year, month, day):
    
    # |date|  |ar_num| |lat/long| |clon| |area(Msolar-hemishpere)|
    df = pd.DataFrame(noaa_data[year])
    df.columns = ["date", 'activeRegionNum', "latLong", "clon", "area", 5,6,7,8]
    df = df[["date", 'activeRegionNum', "latLong", "clon", "area"]]
    
    # get previous date
    previous_day_scan = get_previous_date_(year, month, day)
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
            previous_day_scan = get_previous_date_(year, month, day)
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

def get_current_day_noaa_df_(new_feat_objects_singleDay, noaa_data):
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

def get_currentDay_previusDay_noaa_activeRegions(new_feat_objects_singleDay, noaa_data, get_df = False):
    import pandas as pd
    # get noaa file from same date as image data
    same_day_df, year, month, day = get_current_day_noaa_df_(new_feat_objects_singleDay, 
                                                            noaa_data)
    
    current_day = year + month +day
    # these are NOAA active region centroid 
    nova_cents_sameDay, sameDay_df = extract.get_noaa_centroids(same_day_df, 
                                                                current_day)

    previous_day_df, previous_day = get_prevous_day_noaa_df(noaa_data, 
                                                            year, 
                                                            month, 
                                                            day)
    # these are NOAA active region centroid 
    nova_cents_prevDay, prevDay_df = extract.get_noaa_centroids(previous_day_df, 
                                                                previous_day)
    
    return  nova_cents_sameDay, nova_cents_prevDay, sameDay_df, prevDay_df

    if get_df == True:
        return  nova_cents_sameDay, nova_cents_prevDay, sameDay_df, prevDay_df
    else:
        return  nova_cents_sameDay, nova_cents_prevDay


def extract_metrics_only(image_path, metrics_path, file_name, mdi_flux_filter, hmi_flux_filter, kernal_std, num_pixel_in_active_region):
    '''
    This function is used to do a grid search in Parameter Space. 
    Because the same images need to be processed 10's or 100's of times, 
    it is not necessary (or wise) to save the image data to file. 
    The only data that should be saved is the metrics.

    For now, we are only processing MDI images.

    INPUT:  ...
    OUTPUT: ...
    '''

    image, hdu= extract_script.get_active_region_map(image_path, file_name,  mdi_flux_filter , hmi_flux_filter, kernal_std)
    labels = extract_script.get_active_region_labels(image)
    extract_results = extract_script.extract_features(labels,image, hdu, file_name, num_pixel_in_active_region)

    save_to_file([file_name,
                 (mdi_flux_filter, kernal_std, num_pixel_in_active_region),
                  extract_results], 
                  metrics_path, 
                  file_name)



if __name__ == "__main__":
    import numpy as np
    import pandas as pd
    from time import time,sleep
    import extract_image_features as extract
    import extract_features_script as extract_script
    import os
    # import the ipython parallel computing client 
    from IPython.parallel import Client
    client = Client()
    print "There are currently {} nodes in the cluster".format(len(client))

    lb_view = client.load_balanced_view()

    print "number of engines {}".format(lb_view.targets)

    print "get images from file..."
    # change path for Pleiades job
    image_path = "/Users/Alexander/NASA/NASA_Sample_Data/Images/"
    trans_image_path = "/Users/Alexander/NASA/trans_image_data_singleDay/"
    feature_results_path = "/Users/Alexander/NASA/feature_extraction_results_singleDay/"
    metrics_path = "/Users/Alexander/NASA/metrics_results/"

    file_name_list = []

    # MAY HAVE CHANGE THIS FOR PLEIADES
    for filename in os.listdir(image_path):
        if "ds_store" not in filename.lower():
            file_name_list.append(filename)

    # process images that are sufficiently differnt from each other
    #test_index = np.arange(1, 400, 50) # used for diverse image sub-smaple
    test_index = range(0,3)     
    print "getting test_files for single day..."
    test_files = [ file_name_list[index] for index in test_index]

#     print "run extract_feautres_from_images function..."
#     # QUESTION:
#     # Do I need to wrap the following code around python parallel
#     # Or does Pleiades know to automatically distribute jobs to multiple cores ????????
#     mdi_flux_filter_list = [130,140,150]
#     kernal_std_list = [8,10,12]
#     num_pixel_in_active_region_list = [100,150,200]
#     for mdi_flux_filter in mdi_flux_filter_list:
#         for kernal_std in kernal_std_list:
#             for num_pixel_in_active_region in num_pixel_in_active_region_list:
#                 for i, file_ in enumerate(test_files):
#                     start = time()
#                     # include parameter values for tunning
#                     extract_metrics_only(image_path, metrics_path, file_name, mdi_flux_filter, hmi_flux_filter, kernal_std, num_pixel_in_active_region)
#                     end = time()
#                     print "TIME ELAPSED {0} mins, IMAGE DONE {1}".format((end - start)/60.0, i)


    from IPython.parallel import Client
    client = Client()
    print "There are currently {} nodes in the cluster".format(len(client))


    print "run extract_feautres_from_images function..."
    # QUESTION:
    # Do I need to wrap the following code around python parallel
    # Or does Pleiades know to automatically distribute jobs to multiple cores ????????
    mdi_flux_filter_list = [130,140,150]
    kernal_std_list = [8,10,12]
    num_pixel_in_active_region_list = [100,150,200]
    hmi_flux_filter = 130

    task_results = []

    for mdi_flux_filter in mdi_flux_filter_list:
        for kernal_std in kernal_std_list:
            for num_pixel_in_active_region in num_pixel_in_active_region_list:
                for i, file_name in enumerate(test_files):
                    
                    start = time()
                    # include parameter values for tunning
                    task = lb_view.apply(extract_metrics_only,
                                                image_path, metrics_path, 
                                                file_name, 
                                                mdi_flux_filter, 
                                                hmi_flux_filter, 
                                                kernal_std,
                                                num_pixel_in_active_region)
                    end = time()
                    print "TIME ELAPSED {0} mins, IMAGE DONE {1}".format((end - start), i)
                    
                    task_results.append(task.get())


