#! /usr/bin/env python

# Needed for python functionality
import threading
import time
import socket
import datetime
import os
import sys
import re
import subprocess
import logging
import psutil
from psutil._common import bytes2human
import smtplib
import argparse

# Globals
DEBUG = False
k32_tmp_bytes = 322122547200        # 300GiB; 300*1024*1024*1024
k32_dest_bytes = 108879151104       # Anecdotally biggest k=32 plot is 108879151104 

class winPC(object):
    def __init__(self, logger, parallelPlots=10, RAM_MB=30000):
        # Hardcode CPU_core and RAM_MB for your system!
        # TODO: have python figure out the CPU_core and RAM_MB details
        self.chia_path = "C:\\Users\\ssdrive\\AppData\\Local\\chia-blockchain\\app-1.1.5\\resources\\app.asar.unpacked\\daemon\\"
        self.parallelPlots = parallelPlots
        self.logger = logger
        self.RAM_MB = RAM_MB

        # Clean printout of all devices
        templ = "%-17s %8s %8s %8s %5s%% %9s  %s"
        print(templ % ("Device", "Total", "Used", "Free", "Use ", "Type", "Mount"))
        for memory in psutil.disk_partitions(all=False):
            usage = psutil.disk_usage(memory.mountpoint)
            print(templ % (
                memory.device,
                bytes2human(usage.total),
                bytes2human(usage.used),
                bytes2human(usage.free),
                int(usage.percent),
                memory.fstype,
                memory.mountpoint))
        print ""    # Formatting

        # Search for Temp and Destination Paths. Requirements:
        #     Temp has a folder named 'plot'
        #     Dest has a folder named 'farm'
        self.tmp_memory = []
        self.dest_memory = []
        for memory in psutil.disk_partitions(all=False):
            usage = psutil.disk_usage(memory.mountpoint)
            
            # Check for Temp disks
            if(os.path.isdir(memory.device + "plot")):
                # Do not use plotting SSD if the memory is less than 300GB (k32_tmp_bytes)
                if usage.free < k32_tmp_bytes:
                    self.logger.warning(str(memory.device) + " does not have 300GB of space for plotting! It has " + str(usage.free) + " bytes of free space.")
                else:
                    self.tmp_memory.append(memoryClass(memory, isPlotter=True, logger=logger))
                    # Give a warning if tmp used space is too high (1GiB)
                    if usage.used > 1*1024*1024*1024:
                        self.logger.warning(str(memory.device) + " is not empty! It has " + str(usage.used) + " bytes used.")

            # Check for Temp disks
            if(os.path.isdir(memory.device + "farm")):
                # if not re.search('E', memory.device, re.I):     # skip this
                self.dest_memory.append(memoryClass(memory, isPlotter=False, logger=logger))

        # Exit if self.tmp_memory is empty
        if not self.tmp_memory:
            raise Exception("No tmp plotting SSDs found!")

        # Exit if self.dest_memory is empty
        if not self.dest_memory:
            raise Exception("No plot destinations found!")

        # Print Summary of tmp and dest locations
        print ""    # Formatting
        self.logger.info("Summary:")
        tmp_print = "tmp locations: "
        for x in range(0, len(self.tmp_memory)):
            if x != 0:
                tmp_print += ", "
            tmp_print += str(self.tmp_memory[x].memory.device)
        self.logger.info(tmp_print)
        dest_print = "dest locations: "
        for x in range(0, len(self.dest_memory)):
            if x != 0:
                dest_print += ", "
            dest_print += str(self.dest_memory[x].memory.device)
        self.logger.info(dest_print)
        
        # Calculate best plotting values
        self.calculate_best_plotting()
        
    def calculate_best_plotting(self):
        # values to set
        self.total_processes = self.parallelPlots
        self.mem_per_process = 0
        self.stringCmds = []
    
        # TODO: Clean up plot directory; Give a warning here
        
        # TODO: Optimize for disk space with k=32 and k=33 plotting
    
        # 1) Find the best amount of processes to run in parallel based on tmp space
        total_tmp_mem = 0
        for memory in self.tmp_memory:
            total_tmp_mem += memory.usage.free
        self.logger.debug("total_tmp_mem = " + str(total_tmp_mem))
        # if DEBUG:
            # k32_tmp_bytes = 2000*1024*1024*1024
        process_max_size = k32_tmp_bytes * self.parallelPlots       # 300GB tmp space per plot in parallel
        while process_max_size > total_tmp_mem:
            self.total_processes -= 1
            process_max_size = process_max_size - k32_tmp_bytes
            self.logger.debug("total_processes reduced to " + str(self.total_processes) + "; new process_max_size = " + str(process_max_size))
            if process_max_size < 0:
                raise ValueError("Not enough temp space to plot!")

        # 2) Find total plots to fill up destination memory
        self.total_plots = 0
        for memory in self.dest_memory:
            self.total_plots += memory.totalplots
        self.logger.info("Need " + str(self.total_plots) + " plots to fill up destination disk space")
        
        # 3) Check if total processes is greater than destination plots
        if self.total_processes > self.total_plots:
            self.total_processes = self.total_plots
            self.logger.debug("total_processes reduced to " + str(self.total_processes) + " based on total destination plots")
        self.logger.info("Total Concurrent Plot Processes: " + str(self.total_processes))
        
        # TODO: check for concurrent chia plotting processes
        
        # 4) Find the best amount of memory to utilize
        self.mem_per_process = int(self.RAM_MB/ self.total_processes)
        if(self.mem_per_process > 4000):        # 3400 is max size noted on chia.net - https://www.chia.net/2021/02/22/plotting-basics.html, but have seen examples of more. Max at 4000?
            self.mem_per_process = 4000
        self.logger.info("mem_per_process = " + str(self.mem_per_process))
        
        # Initialize destination plot counts
        destplotcount = []
        for x in range(0, len(self.dest_memory)):
            destplotcount.append(self.dest_memory[x].totalplots)
            
        # Initialize tmp list
        tmpPlotList = []
        while(len(tmpPlotList) < self.total_plots):
            tmpPlotCnt = [1]*len(self.tmp_memory)
            processCnt = 0
            # Balance per process group
            while(processCnt < self.total_processes):
                for x in range(0, len(self.tmp_memory)):
                    if not(tmpPlotCnt[x] > self.tmp_memory[x].totalplots):
                        tmpPlotList.append(x)
                        tmpPlotCnt[x] += 1
                        processCnt += 1
        
        # Calculate plot command strings
        totalpltcount = 0
        while totalpltcount < self.total_plots:
            for x in range(0, len(self.dest_memory)):
                if(destplotcount[x] > 0):
                    cmd = self.chia_path + "chia.exe plots create -k 32 -b " + str(self.mem_per_process) + " -u 128 -r 2 -t " + str(self.tmp_memory[tmpPlotList[totalpltcount]].memory.device + "plot") + " -d " + str(self.dest_memory[x].memory.device + "farm") + " -n 1"
                    if DEBUG:
                        cmd = "echo '" + cmd + "'"
                    self.stringCmds.append(cmd)
                    destplotcount[x] -= 1
                    totalpltcount += 1
        
        for x in range(0, len(self.stringCmds)):
            self.logger.debug(str(x+1) + " " + self.stringCmds[x])
        
class memoryClass(object):
    # TODO: make a superclass of psutil_memclass... duh
    def __init__(self, psutil_memclass, isPlotter, logger):
        self.memory = psutil_memclass
        self.usage = psutil.disk_usage(self.memory.mountpoint)
        self.isPlotter = isPlotter
        self.logger = logger
        if self.isPlotter:
            self.isDestination = False
        else:
            self.isDestination = True
        self.calculateTotalPlots()
        
    def calculateTotalPlots(self):
        if self.isPlotter:
            # Need 300GB for plotters
            self.totalplots = int(float(self.usage.free) / k32_tmp_bytes)
        else:
            # Need 108.8GiB for destination plot
            self.totalplots = int(float(self.usage.free) / k32_dest_bytes)

        self.logger.debug(str(self.memory.device) + " has " + str(self.usage.free) + " space for " + str(self.totalplots) + " total plots")
            
    def recalculateUsage(self):
        self.usage = psutil.disk_usage(self.memory.mountpoint)
        self.calculateTotalPlots()
        
        
class winWorkloadThread(threading.Thread):
    # TODO: add plot information to this class
    def __init__(self, string_command, workerID, logger):
        threading.Thread.__init__(self)
        self.what_to_run_string = string_command
        self.workerID = workerID
        self.logger = logger

    def run(self):
        new_string = "start /wait C:\\Windows\\system32\\cmd.exe /c " + self.what_to_run_string + ""
        self.logger.info("'" + new_string + "'")
        os.system(new_string)

        
# Works on Micron network only!
def SendEmail(receiver, subject, message, senderName="youremail"):
    sender = "relay.micron.com"

    header = "To:" + str(receiver) + "\nFrom:" + senderName + "\nSubject:" + subject + "\n"
    msg = header + '\n' + message + "\n"

    smtObj = smtplib.SMTP(sender, 25)
    smtObj.sendmail(senderName, receiver, msg)
    smtObj.quit()
        
###########################################################
# Main                                                    #
###########################################################
def main(argv):
    global DEBUG

    # Command line arguments
    parser = argparse.ArgumentParser(description='ERROR: Invalid inputs')
    parser.add_argument('--mNetworkEmail',default=False,type=bool,metavar="mNetworkEmail",help="Use this flag if you want email updates on plots")
    parser.add_argument('--staggerMin',default=10,type=float,metavar="staggerMin",help="Stagger time between plots in minutes")
    parser.add_argument('--sleepMin',default=10,type=float,metavar="sleepMin",help="Sleep time when checking plots")
    parser.add_argument('--parallelPlots',default=10,type=int,metavar="parallelPlots",help="How many total plots run in parallel")
    parser.add_argument('--RAM_MB',default=30000,type=int,metavar="RAM_MB",help="Total RAM space in MB")
    parser.add_argument('--email',default="youremail@gmail.com",type=str,metavar="email",help="Stagger time between plots in minutes")
    parser.add_argument('--DEBUG',default=False,type=bool,metavar="DEBUG",help="Use to test in DEBUG mode. DOES NOT generate plots")
    
    # TODO: Pass in public farmer/pool keys for David
    args = parser.parse_args()

    # Get arguments
    emailUpdateFlag = args.mNetworkEmail
    staggerTimeMin = args.staggerMin
    sleepTimeMin = args.sleepMin
    parallelPlots = args.parallelPlots
    RAM_MB = args.RAM_MB
    emailAddress = args.email
    DEBUG = args.DEBUG
    
    # Use logger for timestamps
    logger = logging.getLogger(__name__)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    isDst = time.daylight and time.localtime().tm_isdst > 0
    tzOffset = '-%02d' % ((time.altzone if isDst else time.timezone) / 3600)
    formatter = logging.Formatter('%(asctime)s' + tzOffset + ' %(levelname) -8s %(module)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    if DEBUG:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Determine if Windows / Linux
    if sys.platform.startswith('win'):
        thisPC = winPC(logger, parallelPlots, RAM_MB)
    else:
        emailUpdateFlag = False
        raise Exception("Linux plotBoy not supported yet!")
    
    # TODO: Insert pause for user input here
    # raw_input works with python2.7 only!!!
    # TODO: Check for python 2.7 and python3
    goplot = str(raw_input("Continue with plotting (y/n)? "))
    if not((goplot is 'y') or (goplot is 'Y')):
        logger.info("Goodbye!")
        return
    
    workloads = []
    WorkerID = 1
    PlotCompleted = 1
    for x in range(0, thisPC.total_processes):
        workloads.append(winWorkloadThread(thisPC.stringCmds[WorkerID-1], WorkerID, logger))
        WorkerID += 1
        
    # Kick off plotting processes
    plotID = 1
    for workload in workloads:
        logger.info("Plot " + str(plotID) + " started")
        workload.start()
        plotID += 1
        time.sleep(staggerTimeMin * 60)       # Stagger time in minutes
        
    # Monitor Workloads
    while len(workloads) > 0:
    # while WorkerID < thisPC.total_plots:
        # Check sleepTimeMin amount of minutes
        time.sleep(sleepTimeMin*60)
        for x in range(0, len(workloads)):
            if not workloads[x].is_alive():
                # Email that plot is done if email flag is set
                if emailUpdateFlag:
                    emailMsg = "Plot " + str(PlotCompleted) + " is done on " + str(socket.gethostname()) + "!"
                    SendEmail(receiver=emailAddress, subject=emailMsg, message=emailMsg, senderName=emailAddress)
                logger.info("Plot " + str(workloads[x].workerID) + " has finished")
                workloads.remove(workloads[x])
                # TODO: recalculate plots
                if WorkerID <= thisPC.total_plots:
                    thisWorkload = winWorkloadThread(thisPC.stringCmds[WorkerID-1], WorkerID, logger)
                    logger.info("Plot " + str(plotID) + " started")
                    thisWorkload.start()
                    workloads.append(thisWorkload)
                    WorkerID += 1
                    plotID += 1
                PlotCompleted += 1
                break
                
    if emailUpdateFlag:
        emailMsg = "All plotting completed on " + str(socket.gethostname()) + "!!"
        SendEmail(receiver=emailAddress, subject=emailMsg, message=emailMsg, senderName=emailAddress)
                
if __name__ == "__main__":
    main(sys.argv[1:])