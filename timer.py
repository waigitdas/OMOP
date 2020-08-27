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

 
