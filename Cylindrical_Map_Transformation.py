import numpy as np
import astropy
from astropy.io import fits
from math import pi, sin, cos, sqrt, atan2
import matplotlib.pyplot as plt
from matplotlib.pyplot import plot, scatter, show
from time import time
import numpy as np
import pandas as pd
import os
from collections import Counter,defaultdict
import astropy.convolution
from astropy.convolution import convolve_fft, convolve

def get_header_params_MDI(header):
    '''Gets parameters from MDI image file that are required to 
       perform an Equal Area Map Cylindrical Projection 
       INPUT:  Image header
       OUTPUT: 
               x-center, Distance along x-axis to center of solar disk from first pixel center in array pixels
               y-center, Distance along y-axis to center of solar disk from first pixel center in array pixels
               s0,       Semi-diameter of solar image
               nx,       Number of pixels in x and y dimension (assuming square image)
               pixsize,  Pixel size in arc seconds per pixel
               p0,       Polar angle offset between sun and image vertical axis
               b0,       Longatudinal offset between sun equator and image horizontal axis
               r0,       Radius of the solar disk in pixels
    '''
    xCen = header["X0"]
    yCen = header["Y0"]
    s0 = header["OBS_R0"]
    nx = header["NAXIS1"]
    pixsize = header["FD_SCALE"]
    p0 = header["P_ANGLE"]
    b0 =header["OBS_B0"]
    r0 = s0/pixsize
    yCen = xCen + 0.5
    yCen = yCen + 0.5
    p0 = p0 - 0.21
    return xCen,yCen,s0,nx,pixsize,p0,b0, r0


def get_header_params_HMI(header):
    '''Gets parameters from HMI image file that are required to 
       perform an Equal Area Map Cylindrical Projection 
       INPUT:  Image header
       OUTPUT: 
               x-center, Distance along x-axis to center of solar disk from first pixel center in array pixels
               y-center, Distance along y-axis to center of solar disk from first pixel center in array pixels
               s0,       Semi-diameter of solar image
               nx,       Number of pixels in x and y dimension (assuming square image)
               pixsize,  Pixel size in arc seconds per pixel
               p0,       Polar angle offset between sun and image vertical axis
               b0,       Longatudinal offset between sun equator and image horizontal axis
               r0,       Radius of the solar disk in pixels
    '''
    xCen = header["CRPIX1"]
    yCen = header["CRPIX2"]
    s0 = header["RSUN_OBS"]
    nx = header["NAXIS1"]
    pixsize = header["CDELT1"]
    p0 = header["CROTA2"]
    b0 =header["CRLT_OBS"]
    r0 = s0/pixsize
    
    # Shift origin from center of pixel to lower left of pixel
    yCen = xCen + 0.5
    yCen = yCen + 0.5
    # Correct for the misalignment of the CCD with the satellite
    p0 = -p0
    return xCen,yCen,s0,nx,pixsize,p0,b0, r0

def map_disk_cylindric(xCen,yCen,s0,nx,pixsize,p0,b0, r0, v, dim = 4):
    '''Performs an Equal Area Map Cylindrical Projection 
       INPUT: 
               x-center, Distance along x-axis to center of solar disk from first pixel center in array pixels
               y-center, Distance along y-axis to center of solar disk from first pixel center in array pixels
               s0,       Semi-diameter of solar image
               nx,       Number of pixels in x and y dimension (assuming square image)
               pixsize,  Pixel size in arc seconds per pixel
               p0,       Polar angle offset between sun and image vertical axis
               b0,       Longatudinal offset between sun equator and image horizontal axis
               r0,       Radius of the solar disk in pixels
       OUTPUT: transformed data as an array
    '''
    
   
    vmap = np.empty([nx,nx]) # create array for transformed data
    wx = np.empty([dim,dim])
    wy = np.empty([dim,dim])
    data = np.empty([dim,dim])
    
    pihalf=pi/2.             # pi/2
    rsq=r0*r0                # radius of solar disk squared 
    rsqtest= (r0-2.)*(r0-2.) # exclude the two pixels at the limb
    sr=s0*pi/(180.*3600.)    # convert semi-diameter units from arcseconds to radians
    sins0=sin(sr)            # sin component of semi-diameter
    coss0=cos(sr)            # cos component of semi-diameter
    radsol=r0*coss0          # solar radius 

    # Trignometric factors for position angle of rotation axis (wrt vertical axis of image)
    cosp0=cos(p0*pi/180.)
    sinp0=sin(p0*pi/180.)

    # Trignometric factors for tilt angle of rotation axis (latitude at disk center), wrt horizontal axis of image
    cosb0=cos(b0*pi/180.)
    sinb0=sin(b0*pi/180.)

    # Maximum z position for image pixels to be visible
    zpmin=radsol*sins0

    # Heliographic coordinate step sizes
    dcosTheta=2./nx
    dphi=pi/nx

    pihalf=pi/2.             # pi/2
    rsq=r0*r0                # radius of solar disk squared 
    rsqtest= (r0-2.)*(r0-2.) # exclude the two pixels at the limb
    sr=s0*pi/(180.*3600.)    # convert semi-diameter units from arcseconds to radians
    sins0=sin(sr)            # sin component of semi-diameter
    coss0=cos(sr)            # cos component of semi-diameter
    radsol=r0*coss0          # solar radius 

    # Trignometric factors for position angle of rotation axis (wrt vertical axis of image)
    cosp0=cos(p0*pi/180.)
    sinp0=sin(p0*pi/180.)

    # Trignometric factors for tilt angle of rotation axis (latitude at disk center), wrt horizontal axis of image
    cosb0=cos(b0*pi/180.)
    sinb0=sin(b0*pi/180.)

    # Maximum z position for image pixels to be visible
    zpmin=radsol*sins0

    # Heliographic coordinate step sizes
    dcosTheta=2./nx
    dphi=pi/nx
    
    
    # Latitude at pixel center

    # South pole is at bottom edge of bottom row of pixels
    # North pole is at top edge of top row of pixels

    # TODO: Check the i,j indexing and make sure that you understand 
    #       why they are included angle calculations !
    for j in xrange(1, nx + 1): # rows, range [1,1024]
        cosTheta = -1.0 + (j-0.5) * dcosTheta
        sinb = cosTheta
        cosb = sqrt(1. - sinb*sinb)

        for i in xrange(1, nx + 1): # cols, range [1,1024]

            # Longitude at pixel center
            # Central meridian goes through the leading edge of pixel column at i=nx/2

            phi=(i-0.5)*dphi-pihalf
            cosphi=cos(phi)
            sinphi=sin(phi)

            # Heliographic cartesian coordinates
            xs=radsol*sinphi*cosb
            ys=radsol*sinb
            zs=radsol*cosphi*cosb

            # Rotated Heliographic cartesian coordinates with B-angle
            xb=xs
            yb=cosb0*ys - sinb0*zs
            zb=cosb0*zs + sinb0*ys

            # Rotated Heliographic cartesian coordinates with CCW P-angle
            xp=cosp0*xb - sinp0*yb
            yp=cosp0*yb + sinp0*xb
            zp=zb

            # Test for visibility (skip if point is behind limb)
            if (zp > zpmin):
                # Image coordinate transformation for perspective effects
                rhop=sqrt(xp*xp + yp*yp)
                thetap=0.0

                if (rhop >  0.):
                    thetap=atan2(yp,xp)

                rhoi=rhop*(1.0 + zp/(radsol/sins0 - zp))
                x=rhoi*cos(thetap)
                y=rhoi*sin(thetap)

                xi=xCen + x
                yi=yCen + y

                radsq=x*x + y*y

                # Exclude pixels near the limb
                if (radsq < rsqtest):
                    ix=int(xi)
                    iy=int(yi)
                    dx=xi-ix
                    dy=yi-iy


                    # Bi-cubic interpolation from adjacent points.
                    #data = v[ix-2:ix+2, iy-2:iy+2]
                    #      v[rows, cols]
                    data = v[iy-2:iy+2, ix-2:ix+2]

                    # Weights for cubic interpolation
                    dx2=dx*dx
                    dx3=dx*dx2
                    dy2=dy*dy
                    dy3=dy*dy2

                    for ind in xrange(0,4):
                        wx[ind, 0:4] =  [-0.5*dx + dx2 - 0.5*dx3, \
                                         1.0 - 2.5*dx2 + 1.5*dx3,\
                                         0.5*dx + 2.*dx2 - 1.5*dx3,\
                                                -0.5*dx2 + 0.5*dx3]

                        wy[0:4, ind] = [-0.5*dy + dy2 - 0.5*dy3,\
                                        1.0 - 2.5*dy2 + 1.5*dy3,\
                                        0.5*dy + 2.*dy2 - 1.5*dy3,\
                                               -0.5*dy2 + 0.5*dy3]

                    weight = wx*wy # Broadcasting a row with a col 
                    #vmap[i,j]= np.sum(weight*data)
                    vmap[j-1,i-1]= np.sum(weight*data)
    return vmap