## TMX web automation process

Automate the starting and stopping of the TMX cluster including running the ETF notebooks and downloading the file. Following are the steps: 

    remove old files
    login to grapevine
    star cluster
    launch notebook
    run notebook
    download result
    rename file
    upload to s3
    shutdown cluster

---
### Getting Started
Please follow below instructions to run the process 



#### Prerequisites

This process requires google-chrome and chromedriver installed in following paths.

* Google Chrome 90.0.4430.93 in /usr/bin/google-chrome
* ChromeDriver 90.0.4430.24 (4c6d850f087da467d926e8eddb76550aed655991-refs/branch-heads/4430@{#429}) in /usr/local/bin/chromedriver

Install libraries
```
run 'pip install -r requirements.txt'
```

Create environment variables
* BUCKET_NAME
* WORKLOAD_NAME
* BUCKET_PATH

---
### Job Schedule

At 9 am on the day after every Canadian business days. For example:
* normal week with no holidays, run Tues - Sat at 9am since business days are from Mon - Fri
* week where Monday is a holiday, run Wed - Sat at 9am since business days are from Tues - Fri

#### Run process

```
run 'python TMX_Start.py'
```

