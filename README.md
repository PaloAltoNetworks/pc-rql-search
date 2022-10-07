# Prisma Cloud RQL search to CSV 

Version: *1.1*

### Summary
This script will run RQL search and take the results and format them into CSV (based on current CSV output on Investigate page)  

### Requirements and Dependencies

1. Python 3.7 or newer

2. OpenSSL 1.0.2 or newer

(if using on Mac OS, additional items may be nessessary.)

3. Pip

```sudo easy_install pip```

4. Requests (Python library)

```sudo pip install requests```

5. YAML (Python library)

```sudo pip install pyyaml```


### Configuration

1. Navigate to config/README.md. Use the example format to create a file named ```configs.yml```. Full path should be ```config/configs.yml```

2. Fill out your Prisma Cloud access key/secret, stack info, and RQL to be run. (Ignore filename - this will be used in future.)  
   *To determine stack, look at your browser when access console (appX.prismacloud.io, where X is the stack number.  
   Change this to apiX.prismacloud.io and populate it in the configs.yml.  
    Or go here for more information:* https://api.docs.prismacloud.io/

### Run

```
python main.py
```
