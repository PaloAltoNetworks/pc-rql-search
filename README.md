# Prisma Cloud RQL search to CSV 

This script is supported under a "best effort" policy. Please see SUPPORT.md for details.

Version: *3.1*

### Summary
This script will run RQL search and take the results and format them into CSV (based on current CSV output on Investigate page)  

### Requirements and Dependencies

1. Python 3.7 or newer

2. OpenSSL 1.0.2 or newer

(if using on Mac OS, additional items may be nessessary.)

3. Pip

```sudo easy_install pip```

4. Install dependencies

```pip install -r requirements.txt```


### Configuration

1. Navigate to config/README.md. Use the example format to create a file named ```configs.yml```. Full path should be ```config/configs.yml```

2. Fill out your Prisma Cloud access key/secret, stack info, and RQL to be run.
   *To determine stack, look at your browser when access console (appX.prismacloud.io, where X is the stack number.  
   Change this to apiX.prismacloud.io and populate it in the configs.yml.  
    Or go here for more information:* https://api.docs.prismacloud.io/

### Run

For generic config searches
```
python config.py
```
For config from network searches
```
python network.py
```
