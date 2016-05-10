
Jupyter Notebooks
-----------------

All .ipynb scripts are IPython Notebooks (a.k.a Jupyter Notebooks) 
In order to open these files, one needs to download and install jupyter notebook. 
The scientific computing package Anaconda provides a nearly complete environment 
that includes jupuyter notebook and a list of python packages, like numpy and pandas. 

This link will walk you through the installation process for jupyter notebooks (and Anaconda). 

Jupyter Notebook Documentation: https://jupyter-notebook-beginner-guide.readthedocs.org/en/latest/

Some Python libraries are not included in the Anaconda environment. 
To install Skl a missing library simply type: pip install name_of_package 
into the command line ( i.e. pip install astropy )

It is industry practice to import all libraries and user defined modules at the
top of all notebooks and scripts. 

The notebooks are useful for debuging, viewing images, and changing code to see it's 
effects in real time -- faster than iterating through the command line. They are similar
to Emacs in that notebooks provide an IDE, but more flexible because they allow for plotting
and they make exploratory data analysis a lot easier. They are also good for technical presentations. 

I recommend you start reviewing the code on jupyter notebooks first, then view the .py scripts. 
Personally, I always work thorugh the notebooks first, develop proto-type code, then covert it
to .py scripts that can be ran on the terminal. 

The FIRST FILE YOU SHOULD OPEN is Display_Notebook.ipynb
This file provides an introduction the pipeline in its entirety

Lastely, wheneve`r you come across a python issue, google "StackOverFlow" followed by a concise 
description/question. 80% of python issues have solutions on StackOverFlow. 

Also, all python libraries have free online documentation. Recall, python is an open source language. 

For instance:

Numpy: http://docs.scipy.org/doc/numpy-1.10.0/user/index.html#user
Pandas: http://pandas.pydata.org/pandas-docs/version/0.15.2/tutorials.html
ipyparallel: https://ipyparallel.readthedocs.org/en/latest/
sklearn: http://scikit-image.org/ 
         http://scikit-image.org/docs/dev/api/skimage.feature.html#skimage
astropy: http://docs.astropy.org/en/stable/index.html
python built in functions: https://docs.python.org/2/library/functions.html\

NOTE: I am using python version 2.7, if you use a different version (i.e. python 3)
my code will have to be refractered. Python 3 is not yet widely adopted in industry. 


All .py scripts involved in this pipeline:

Run Core Scripts
------------
ipyparallel_test.pbs
parallel_pipeline_pleiades.py
series_pipeline.py

Core Scripts
------------
main_script.py
Cylindrical_Map_Transformation.py
sunspot_features_extraction.py
extract_image_features.py
Centroid_Labeling.py

Supplimentary Scripts
---------------------
debug_script.py
extract_features_script_test.py


The "Run Core Scripts" scripts call functions from the core scripts, provide paths to images and result files,
so that the functions from the core scripts can do the calculations. 

The "Core Scripts" contain all the functions that are used in the pipeline to process each image. 
More on these below. 

The "Supplimentary Scripts" are mainly debugging scripts. 
	debug_script.py
		This script provides a print out of the image data at key points along the pipeline. 

		This code is in a parallelized form. 


Script Discriptions
--------------------

NOTE: Most functions contain comments about their inputs, outputs, and their purpose. 

ipyparallel_test.pbs
	Executeable file that submits job to Pleaides 

parallel_pipeline_pleiades.py 
	The parallel implementation of this pipeline. 
	This file can be ran on Pleiades or locally. 
	This scripts call 3 functions from main_script.py

series_pipeline.py
	The series implementation of this pipeline. 
	This scripts call 3 functions from main_script.py

main_script.py
	Contain the 3 main functions that are called to process each image.
	These 3 functions are dependent on all scripts that follow below. 
	The 3 functions are:
		    extract_features_from_images
    		load_noaa_data
    		filter_label_saveToFile

Cylindrical_Map_Transformation.py
	Converts the images into equal area cylindrical space 


sunspot_features_extraction.py
	Contains the functions that extract the centroids and flux from the sunspots. 
	The extracted feautres are ultimately stored in a feature dictionary data object.


extract_image_features.py
	Contians functions that extract information from the feature dictionary, 
	and assigne NOAA active region labels to sunspots from MDI and HMI images. 


Centroid_Labeling.py
	Contains functions that prase through the NOAA dates and pair up images and noaa data 
	based on the time of day that the images were taken so that they can be match with 
	the nearest occuring noaa observation, and save final results to file. 


Jupyter Notebooks
------------------

Feature_Extraction.ipynb
	Exploratory/ proto-type code of 1st half of the pipeline

Feature_extraction.ipynb
	Exploratory/ proto-typ code of 2nd half of the pipeline
