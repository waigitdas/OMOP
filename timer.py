##################################################################
####  Graphs in Genealogy Code  ##################################
####  David A Stumpf, MD, PhD; Woodsock, IL, USA #################
####  copyright 2020 #############################################
##################################################################
import ctypes, os 
import time

Neo4jTime=0
Neo4jQueryCt=0

def StartTimer():
    Neo4jTime=0

def getTicks():
    return time.clock_gettime(time.CLOCK_MONOTONIC_RAW)
    return time.time()   

    #clock_gettime()
    #tics = ctypes.c_int64()
    #freq = ctypes.c_int64()

    ##get ticks on the internal ~2MHz QPC clock
    #ctypes.windll.Kernel32.QueryPerformanceCounter(ctypes.byref(tics)) 
    ##get the actual freq. of the internal ~2MHz QPC clock
    #ctypes.windll.Kernel32.QueryPerformanceFrequency(ctypes.byref(freq))  

    #t_us = tics.value*1e6/freq.value
    #return t_us
