import os
import sys
from itertools import repeat


# verbose UI
DEBUG = True

PATH = "/home/ubuntu/workspace/"

# yyyy-mm-dd and hh:mm:ss
date_format = "%Y-%m-%d %H:%M:%S"

# file holds single value from 1 to 86,400
def get_inactivity_period():
    os.chdir(PATH + "input/") # don't know if this is needed or works...
    try:
        with open("inactivity_period.txt", "r") as f:
            if DEBUG: print("Opened inactivity period file")
            x = f.readline().rstrip()
            if DEBUG: print("  Inactivity limit is " + x)
            try:
                x = int(x)
            except:
                sys.exit("  File value can't be taken in ass an integer!")
            return x
            # it would be better practice to close the file, but this is more legible
    except IOError:
        sys.exit("Couldn't open ineactivity_period file.")

TIMER = int(get_inactivity_period())
if DEBUG:
    print("Timer limit is " + str(TIMER) + " seconds.")

class Session:
    
    def __init__(self, ip, date, time):
        if DEBUG:
            print("  Session for ip " + ip + " is being created.")
        self.ip = ip
        self.duration = 0
        self.firstReqDate = date
        self.firstReqTime = time
        self.lastReqDate = date
        self.lastReqTime = time
        self.requests = 1
        self.live = True
        self.dying = False
        if DEBUG:
            print("  " + self.ip + " is born.")

    def request(self, ip, date, time, iterator):
        self.requests += 1
        self.lastReqDate = date
        self.lastReqTime = time
        add_to_SESSIONS(self, iterator)
        
# list of live Sessions - maybe use set to scale up better?
# position in this list is indicative of last time
SESSIONS = [[] for sec in repeat(None, int(TIMER))]

def add_to_SESSIONS(s, iterator):
    SESSIONS[iterator].append(s)