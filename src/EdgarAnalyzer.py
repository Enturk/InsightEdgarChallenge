# SEC EDGAR analysis tool by Nazim Karaca

import os
from datetime import datetime
import re

bool debug = True

# file holds single value from 1 to 86,400
def get_inactivity_period:
    os.chdir("../input/") # don't know if this is needed or works...
    with open(inactivity_period.txt) as f:
        if (debug) : print("Opened inactivity period file")
        return [int(x) for x in f]
    # it would be better practice to close the file, but this is more legible

# TODO data listed in chronological order


# TODO IP address uniquely identifies a single user

# TODO session starts when the IP address first requests a document from the EDGAR
def session_maker



# TODO process the EDGAR weblogs line by line



# when user session has ended (by inactivity_period or at end of log.csv)
# TODO write a line to an output file, sessionization.txt, with:
# IP address, duration of the session and number of documents accessed.