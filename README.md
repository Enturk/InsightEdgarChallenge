# Insight Edgar Challenge
My work on the [Edgar Challenge](https://github.com/InsightDataScience/edgar-analytics) by Insight Data Science.

## License
Licensed under the GNU Affero General Public License 3.0, which is described in full in the LICENSE file.

Without modifying that license in any way, it has been described by others as granting full permission to reuse, as long as the same license is used, and without any liability on my part. Use this code at your own peril, and join the free world.

## Dataset
The dataset used was a random 2017 EDGAR zipped folder from [the SEC website](https://www.sec.gov/dera/data/edgar-log-file-data-set.html).

StubbyCode:
Get timer
session class
    add request
List of lists containing sessions
    each iteration purges one list of sessions
main
    open output
    open input
    for input line
        firstline: find header positions
        from third line on, if time has changed
            for session in expired list of sessions
                if last request timestamp past timer, write data to output
                remove from list
        if ip is in list of Sessions
            update requestInt and last request timestamp
        else: new session at current iteration
    
    at end of input file:
        iterate over all sessions
            writing the data for each to output