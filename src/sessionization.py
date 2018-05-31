import os
import sys
from datetime import datetime

from itertools import repeat
import gc

# verbose feedback
DEBUG = False

# get main directory path
PATH = os.path.dirname(os.path.realpath(__file__))[:-3]

# yyyy-mm-dd and hh:mm:ss
date_format = "%Y-%m-%d %H:%M:%S"

def timestamp_delta(olddate, oldtime, newdate, newtime):
    old_time_stamp = datetime.strptime(olddate + " " + oldtime, '%Y-%m-%d %H:%M:%S')
    new_time_stamp = datetime.strptime(newdate + " " + newtime, '%Y-%m-%d %H:%M:%S')
    return int((new_time_stamp - old_time_stamp).total_seconds())

# file holds single value from 1 to 86,400
def get_inactivity_period():
    os.chdir(PATH + "input/") 
    try:
        with open("inactivity_period.txt", "r") as f:
            if DEBUG: print("Opened inactivity period file")
            x = f.readline().rstrip()
            if DEBUG: print("  Inactivity limit is " + x)
            try:
                x = int(x)
            except:
                sys.exit("  File value can't be taken in as an integer!")
            return x
    except IOError:
        sys.exit("Couldn't open ineactivity_period file.")

TIMER = int(get_inactivity_period()) + 1
if DEBUG: print("Timer limit is " + str(TIMER) + " seconds.")

class Session:
    
    def __init__(self, ip, date, time, iterator, count):
        if DEBUG: print("  Session for ip " + ip + " is being created.")
        self.ip = ip
        self.duration = 1
        self.firstReqDate = date
        self.firstReqTime = time
        self.lastReqDate = date
        self.lastReqTime = time
        self.requests = 1
        self.live = True
        self.dying = False
        self.count = count
        SESSIONS[iterator].append(self)

        if DEBUG: print("  Session for " + self.ip + " is born.")

    def request(self, ip, date, time, iterator):
        if not self.live:
            if DEBUG: print("  Stale session resurrected.")
        self.requests += 1
        self.lastReqDate = date
        self.lastReqTime = time
        present = False
        for sec in SESSIONS:
            for s in sec:
                if s.ip == self.ip:
                    if sec == SESSIONS[iterator]:
                        present = True
                    else:
                        sec.remove(s)
        if not present:
            SESSIONS[iterator].append(self)
        
        if DEBUG: print("  Session for " + self.ip + " got a new request.")
        
# list of live Sessions - maybe a set would scale up better?
# position in this list is indicative of last request
SESSIONS = [[] for sec in repeat(None, int(TIMER))]

def get_session_with_ip(ip):
    for sec in SESSIONS:
        for s in sec:
            if s.ip == ip:
                return s
    return None

# on your marks!
count = 0
iterator = -1
date = "-1"
time = "-1"
first_line = True
second_line = True
first_round = True
time_change = False
delta = 0
count = 0

if DEBUG:
    x = 'a+'
else:
    x = 'w+'

# open output file
os.chdir(PATH + "output/") 
try:
    o = open("sessionization.txt", x)
    if DEBUG: print("Opened output file,")

except:
    sys.exit("  Couldn't open output file.")

# now to start down the road before us
os.chdir(PATH + "input/") 
csv_file_name = "log.csv"

# open log file
try:
    with open(csv_file_name) as fp:
        print("EDGAR log analysis startin at " + str(datetime.now()))

        if (DEBUG): 
            print("Opened log file. Here are the lines as I process them:")
            o.write("\n")
            
        for line in fp:
            
            if (DEBUG): print(line)
            
            # data listed in chronological order
            lineBuffer = line.split(",")
            
            # grab header positions from first line
            if first_line:
                if DEBUG:
                    print("  Processing header line:")
                
                i=0
                for header in lineBuffer:
                    if DEBUG: print("  Looking at header: " + header)
                    if header == "ip": 
                        ipPos = i # ip making request
                        if DEBUG: print("  IP position in header is "+ str(ipPos))
                    if header == "date": 
                        datePos = i # date of the request (yyyy-mm-dd)
                        if DEBUG: print("  Date position in header is "+ str(datePos))
                    if header == "time": 
                        timePos = i # time of the request (hh:mm:ss)
                        if DEBUG: print("  Time position in header is "+ str(timePos))
                    i += 1
                first_line = False
                continue
            
            # data
            ip = lineBuffer[ipPos]
            if (time != lineBuffer[timePos]) or (date != lineBuffer[datePos]):
                time_change = True
                if DEBUG: print("  Iterator changed from " + str(iterator) + "...")
                if second_line:
                    iterator += 1;
                else:
                    delta = timestamp_delta(date, time, lineBuffer[datePos], lineBuffer[timePos])
                    iterator += delta%TIMER
                if iterator >= TIMER:
                    iterator = 0
                    if first_round: first_round = False
                if DEBUG: print("                    ... to " + str(iterator))
            date = lineBuffer[datePos]
            time = lineBuffer[timePos]
            
            # after second line, monitor for time changes
            if (time_change):
                # old session purge
                for s in SESSIONS[iterator]:
                    SESSIONS[iterator].remove(s)
                    staleness = timestamp_delta(s.lastReqDate, s.lastReqTime, date, time)
                    if DEBUG: print("  Session for ip " + s.ip + " hasn't made a request in " + str(staleness) + " seconds.")
                    if staleness >= TIMER:
            #            s.live = False                                
                        s.duration = 1 + timestamp_delta(s.firstReqDate, s.firstReqTime, s.lastReqDate, s.lastReqTime)
                        o.write(s.ip + "," + s.firstReqDate + " " + s.firstReqTime + "," +  s.lastReqDate + " " + s.lastReqTime + "," + str(s.duration) + "," + str(s.requests) + "\n")
                        if DEBUG: print("    End of session for ip " + s.ip + ". It made " + str(s.requests) + " request(s) and lasted " + str(s.duration) + " second(s).")
                    s = None
                #cunknown if this helps
                gc.collect()

                time_change = False

            # case in which lotsa time passes between requests
            if delta>TIMER:
                for sec in SESSIONS:
                    for s in sec:
                        s.live = False
                        s.duration = 1 + timestamp_delta(s.firstReqDate, s.firstReqTime, s.lastReqDate, s.lastReqTime)
                        if DEBUG: print("  Output for ip " + s.ip + ". It made " + str(s.requests) + " request(s) and lasted " + str(s.duration) + " second(s).")
                        o.write(s.ip + "," + s.firstReqDate + " " + s.firstReqTime + "," +  s.lastReqDate + " " + s.lastReqTime + "," + str(s.duration) + "," + str(s.requests) + "\n")
                        s = None
                gc.collect()
            # reset trigger
            delta = 0
            #check if session exists
            existing = get_session_with_ip(ip)
            
            # WARNING: assumes that each line makes a request
            #if none, new session
            if existing == None:
                if DEBUG: print("  New session for ip " + ip)
                s = Session(ip, date, time, iterator, count)
            
            #otherwise, update session
            else:
                if DEBUG: print("  Existing session for ip " + ip + " gets new request at " + str( iterator))
                existing.request(ip, date, time, iterator)

            # for sec in SESSIONS:
            #     for s in sec:
            #         if not s.live:
            #             s.duration = 1 + timestamp_delta(s.firstReqDate, s.firstReqTime, s.lastReqDate, s.lastReqTime)
            #             o.write(s.ip + "," + s.firstReqDate + " " + s.firstReqTime + "," +  s.lastReqDate + " " + s.lastReqTime + "," + str(s.duration) + "," + str(s.requests) + "\n")
            #             if DEBUG: print("  Output for ip " + s.ip + " made " + str(s.requests) + " request(s) and lasted " + str(s.duration) + " second(s).")
            #             s = None
            #     #unknown if this helps
            #     gc.collect()

            count += 1
            if (count % 100000 == 0): print("Processed " + str(count) + " requests.")
        
    # at end of input file:
    if DEBUG: print("End of input process:")

    sorted_sessions = []

    for sec in SESSIONS:
        for s in sec:
            s.duration = 1 + timestamp_delta(s.firstReqDate, s.firstReqTime, s.lastReqDate, s.lastReqTime)
            s.start_time = (datetime.strptime(s.firstReqDate + " " + s.firstReqTime, '%Y-%m-%d %H:%M:%S') - datetime(2003,1,1)).total_seconds()
            sorted_sessions.append(s)
            if DEBUG: print("  Sorting session " + s.ip + " with time " + str(s.start_time) + " and count " + str(s.count))

    sorted_sessions = sorted(sorted_sessions, key = lambda s: (s.start_time, s.count))

    for s in sorted_sessions:
        if DEBUG: print("  Output for ip " + s.ip + ". It made " + str(s.requests) + " request(s) and lasted " + str(s.duration) + " second(s).")
        o.write(s.ip + "," + s.firstReqDate + " " + s.firstReqTime + "," +  s.lastReqDate + " " + s.lastReqTime + "," + str(s.duration) + "," + str(s.requests) + "\n")
    
    print("EDGAR log analysis done at " + str(datetime.now()) + ", after processing " + str(count) + " requests. Please check output.")

except IOError:
    sys.exit("Couldn't open log file.")
