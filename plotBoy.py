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


class winPC(object):
    def __init__(self):
        # Hardcode CPU_core and RAM_MB for your system!
        # TODO: have python figure out the CPU_core and RAM_MB details
        self.chia_path = "C:\\Users\\ssdrive\\AppData\\Local\\chia-blockchain\\app-1.1.5\\resources\\app.asar.unpacked\\daemon\\"
        self.CPU_cores = 10
        self.RAM_MB = 38000
        
        # Search for Temp and Destination Paths. Requirements:
        #     Temp has a folder named 'plot'
        #     Dest has a folder named 'farm'
        self.tmp_memory = []
        self.dest_memory = []
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
                
            # Check for Temp disks
            if(os.path.isdir(memory.device + "plot")):
                # TODO: Need mitigation plan if device's memory is less than 300GB
                self.tmp_memory.append(memoryClass(memory, isPlotter=True))
            
            # Check for Temp disks
            if(os.path.isdir(memory.device + "farm")):
                self.dest_memory.append(memoryClass(memory, isPlotter=False))
        
        # Print tmp and dest locations
        print "\nSummary:"
        tmp_print = "tmp locations: "
        for x in range(0, len(self.tmp_memory)):
            if x != 0:
                tmp_print += ", "
            tmp_print += str(self.tmp_memory[x].memory.device)
        print(tmp_print)
        dest_print = "dest locations: "
        for x in range(0, len(self.dest_memory)):
            if x != 0:
                dest_print += ", "
            dest_print += str(self.dest_memory[x].memory.device)
        print(dest_print)
        
        # Calculate best plotting values
        self.calculate_best_plotting()
        
    def calculate_best_plotting(self):
        # values to set
        self.total_processes = self.CPU_cores
        self.mem_per_process = 0
        self.stringCmds = []
    
        # TODO: Clean up plot directory
    
        # 1) Find the best amount of processes to run in parallel
        total_tmp_mem = 0
        for memory in self.tmp_memory:
            total_tmp_mem += memory.usage.free
        # print "total_tmp_mem = " + str(total_tmp_mem)
        process_max_size = 300*1024*2024 * self.CPU_cores       # 300GB tmp space per plot in parallel
        while process_max_size > total_tmp_mem:
            self.total_processes = self.total_processes - 1
            process_max_size = process_max_size - 300*1024*1024
            if process_max_size < 0:
                raise ValueError("Not enough temp space to plot!")
        print "Total Concurrent Plot Processes: " + str(self.total_processes)
        
        # 2) Find the best amount of memory to utilize
        self.mem_per_process = int(self.RAM_MB/ self.total_processes)
        if(self.mem_per_process > 4000):        # 3400 is max size noted on chia.net - https://www.chia.net/2021/02/22/plotting-basics.html, but have seen examples of more. Max at 4000?
            self.mem_per_process = 4000
        print "mem_per_process = " + str(self.mem_per_process)
        
        # 3) Find total plots to fill up destination memory
        self.total_plots = 0
        for memory in self.dest_memory:
            self.total_plots += memory.totalplots
        # self.total_plots = int(float(total_dest_mem) / 116823110452)       # Each plot is 108.8GB, or 114085068 bytes
        # print "total_dest_mem = " + str(total_dest_mem) + ", or " + str(float(total_dest_mem) / (1024*1024*1024*1024*1024)) + "TB"
        # print "Each plot is 108.8GB, or " + str(108.8*1024*1024*1024) + " bytes"
        print "Need " + str(self.total_plots) + " plots to fill up destination disk space"
        
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
                    self.stringCmds.append(self.chia_path + "chia.exe plots create -k 32 -b " + str(self.mem_per_process) + " -u 128 -r 2 -t " + str(self.tmp_memory[tmpPlotList[totalpltcount]].memory.device + "plot") + " -d " + str(self.dest_memory[x].memory.device + "farm") + " -n 1")
                    destplotcount[x] -= 1
                    totalpltcount += 1
        
        # print ""
        # for x in range(0, len(self.stringCmds)):
            # print str(x+1) + " " + self.stringCmds[x]
        
class memoryClass(object):
    def __init__(self, psutil_memclass, isPlotter):
        self.memory = psutil_memclass
        self.usage = psutil.disk_usage(self.memory.mountpoint)
        self.isPlotter = isPlotter
        if self.isPlotter:
            self.isDestination = False
        else:
            self.isDestination = True
        self.calculateTotalPlots()
        
    def calculateTotalPlots(self):
        if self.isPlotter:
            # Need 300GB for plotters
            self.totalplots = int(float(self.usage.free) / 322122547200)
        else:
            # Need 108.8GB for destination plot
            self.totalplots = int(float(self.usage.free) / 116823110452)
        # print str(self.memory.device) + " has " + str(self.usage.free) + " space for " + str(self.totalplots) + " total plots"
            
    def recalculateUsage(self):
        self.usage = psutil.disk_usage(memory.mountpoint)
        self.calculateTotalPlots()
        
        
class winWorkloadThread(threading.Thread):
    def __init__(self, string_command, workerID):
        threading.Thread.__init__(self)
        self.what_to_run_string = string_command

    def run(self):
        new_string = "start /wait C:\\Windows\\system32\\cmd.exe /c " + self.what_to_run_string + ""
        print new_string + "\n"
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
    # TODO: Use logger to capture time

    # Command line arguments
    parser = argparse.ArgumentParser(description='ERROR: Invalid inputs')
    parser.add_argument('--mNetworkEmail',default=False,type=bool,metavar="mNetworkEmail",help="Use this flag if you want email updates on plots")
    parser.add_argument('--staggerMin',default=10,type=int,metavar="staggerMin",help="Stagger time between plots in minutes")
    parser.add_argument('--email',default="youremail@gmail.com",type=str,metavar="email",help="Stagger time between plots in minutes")
    args = parser.parse_args()

    # Get arguments
    emailUpdateFlag = args.mNetworkEmail
    staggerTimeMin = args.staggerMin
    emailAddress = args.email

    # Determine if Windows / Linux
    if sys.platform.startswith('win'):
        thisPC = winPC()
    else:
        emailUpdateFlag = False
        raise Exception("Linux plotBoy not supported yet!")
    
    workloads = []
    WorkerID = 1
    PlotCompleted = 1
    for x in range(0, thisPC.total_processes):
        workloads.append(winWorkloadThread(thisPC.stringCmds[WorkerID-1], "WorkerID " + str(WorkerID)))
        WorkerID += 1
        
    # Kick off plotting processes
    plotID = 1
    for workload in workloads:
        print "Plot " + str(plotID) + " started"
        workload.start()
        plotID += 1
        # TODO: Stagger by plotting stage
        time.sleep(staggerTimeMin * 60)       # Stagger time in minutes
        
    # Monitor Workloads
    while WorkerID < thisPC.total_plots:
        # Check every 10 min
        time.sleep(10*60)
        # time.sleep(60)
        for x in range(0, len(workloads)):
            if not workloads[x].is_alive():
                # Email that plot is done if email flag is set
                if emailUpdateFlag:
                    emailMsg = "Plot " + str(PlotCompleted) + " is done on " + str(socket.gethostname()) + "!"
                    SendEmail(receiver=emailAddress, subject=emailMsg, message=emailMsg, senderName=emailAddress)
                # print "workloads[" + str(x) + "] has finished"
                workloads.remove(workloads[x])
                thisWorkload = winWorkloadThread(thisPC.stringCmds[WorkerID-1], "WorkerID " + str(WorkerID))
                print "Plot " + str(plotID) + " started"
                thisWorkload.start()
                workloads.append(thisWorkload)
                WorkerID += 1
                plotID += 1
                PlotCompleted += 1
                break
            # else:
                # print "workloads[" + str(x) + "] still running"
        
    
    # # Don't Need
    # for workload in workloads:
        # workload.join()
    
        
if __name__ == "__main__":
    main(sys.argv[1:])