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
        self.CPU_cores = 8
        self.RAM_MB = 30000
        
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
                self.tmp_memory.append(memory)
                # print str(memory.device) + " found as a temp disk!"
            # else:
                # print str(memory.device) + " not a temp disk!"
            
            # Check for Temp disks
            if(os.path.isdir(memory.device + "farm")):
                self.dest_memory.append(memory)
                # print str(memory.device) + " found as a farm disk!"
            # else:
                # print str(memory.device) + " not a farm disk!"
        
        # Print tmp and dest locations
        print "\nSummary:"
        tmp_print = "tmp locations: "
        for x in range(0, len(self.tmp_memory)):
            if x != 0:
                tmp_print += ", "
            tmp_print += str(self.tmp_memory[x].device)
        print(tmp_print)
        dest_print = "dest locations: "
        for x in range(0, len(self.dest_memory)):
            if x != 0:
                dest_print += ", "
            dest_print += str(self.dest_memory[x].device)
        print(dest_print)
        
        # Calculate best plotting values
        self.calculate_best_plotting()
        
    def calculate_best_plotting(self):
        # values to set
        self.total_processes = self.CPU_cores
    
        # TODO: Clean up plot directory
    
        # 1) Find the best amount of processes to run in parallel
        total_tmp_mem = 0
        for memory in self.tmp_memory:
            usage = psutil.disk_usage(memory.mountpoint)
            total_tmp_mem += usage.free
        # print "total_tmp_mem = " + str(total_tmp_mem)
        process_max_size = 300*1024*2024 * self.CPU_cores       # 300GB tmp space per plot in parallel
        while process_max_size > total_tmp_mem:
            self.total_processes = self.total_processes - 1
            process_max_size = process_max_size - 300*1024*2024
            if process_max_size < 0:
                raise ValueError("Not enough temp space to plot!")
        
                
        # 2) Find the best amount of memory to utilize
        self.mem_per_process = int(self.RAM_MB/ self.total_processes)
        if(self.mem_per_process > 3400):        # 3400 is max size noted on chia.net - https://www.chia.net/2021/02/22/plotting-basics.html
            self.mem_per_process = 3400
        print "mem_per_process = " + str(self.mem_per_process)
        
        # 3) Find total plots to fill up destination memory
        total_dest_mem = 0
        for memory in self.dest_memory:
            usage = psutil.disk_usage(memory.mountpoint)
            total_dest_mem += usage.free
        self.total_plots = int(float(total_dest_mem) / 116823110452)       # Each plot is 108.8GB, or 114085068 bytes
        # print "total_dest_mem = " + str(total_dest_mem) + ", or " + str(float(total_dest_mem) / (1024*1024*1024*1024*1024)) + "TB"
        # print "Each plot is 108.8GB, or " + str(108.8*1024*1024*1024) + " bytes"
        print "Need " + str(self.total_plots) + " plots to fill up destination disk space"
        
        # Final: Set run strings
        # TODO: Need to add in multiple tmp_paths
        self.stringCmd = self.chia_path + "chia.exe plots create -k 32 -b " + str(self.mem_per_process) + " -u 128 -r 2 -t " + str(self.tmp_memory[0].device + "plot") + " -d " + str(self.dest_memory[0].device + "farm") + " -n 1"
        
class winWorkloadThread(threading.Thread):
    def __init__(self, string_command, workerID):
        threading.Thread.__init__(self)
        self.what_to_run_string = string_command

    def run(self):
        new_string = "start /wait C:\\Windows\\system32\\cmd.exe /c " + self.what_to_run_string + ""
        print new_string
        os.system(new_string)

        
# Works on Micron network only!
def SendEmail(receiver, subject, message, senderName="iwheeler"):
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
    # Command line arguments
    parser = argparse.ArgumentParser(description='ERROR: Invalid inputs')
    parser.add_argument('--mNetworkEmail',default=False,type=bool,metavar="mNetworkEmail",help="Use this flag if you want email updates on plots")
    parser.add_argument('--staggerMin',default=10,type=int,metavar="staggerMin",help="Stagger time between plots in minutes")
    args = parser.parse_args()

    # Get arguments
    emailUpdateFlag = args.mNetworkEmail
    staggerTimeMin = args.staggerMin

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
        workloads.append(winWorkloadThread(thisPC.stringCmd, "WorkerID " + str(WorkerID)))
        print "Plot " + str(WorkerID) + " started"
        WorkerID += 1
        
    # Kick off plotting processes
    for workload in workloads:
        workload.start()
        # TODO: Stagger by plotting stage
        time.sleep(staggerTimeMin * 60)       # Stagger time in minutes
        
    # Monitor Workloads
    
    while WorkerID < thisPC.total_plots:
        # Check every 10 min
        time.sleep(10*60)
        for x in range(0, len(workloads)):
            if not workloads[x].is_alive():
                # Email that plot is done if email flag is set
                if emailUpdateFlag:
                    emailMsg = "Plot " + str(PlotCompleted) + " is done on " + str(socket.gethostname()) + "!"
                    SendEmail(receiver="iwheeler@micron.com", subject=emailMsg, message=emailMsg, senderName="iwheeler@micron.com")
                # print "workloads[" + str(x) + "] has finished"
                workloads.remove(workloads[x])
                thisWorkload = winWorkloadThread(thisPC.stringCmd, "WorkerID " + str(WorkerID))
                thisWorkload.start()
                workloads.append(thisWorkload)
                print "Plot " + str(WorkerID) + " started"
                WorkerID += 1
                PlotCompleted +=1
                break
            # else:
                # print "workloads[" + str(x) + "] still running"
        
    
    # # Don't Need
    # for workload in workloads:
        # workload.join()
    
        
if __name__ == "__main__":
    main(sys.argv[1:])