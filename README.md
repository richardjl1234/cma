##  Standalone Music Audit Program

This application is a standalone music audit program. 



### User Guide: 

1.  Make sure python enviroment is installed. 
1.  Install the necessary packages by executing the following command: 
```
pip install -r requirements.txt
```
1.  Prepare the client statements document, and list of the artists name.
1.  Setup the database username and password in local environment. When the application is running, it will get the following envionment vaiable value. 
```
POSTGRE_USER=
POSTGRE_PWD=
POSTGRE_HOST=
```
3.  Update the settings.py to setup the paremeters which are needed by the application. The following variables can be customized: 
```
# The LOG_LEVEL control the logs to be printed during the executing
LOG_LEVEL=logging.INFO

# The databases to be processes (qqmusicv2 has not enabled yet)
# use the databases name instead of the platform name
PLATFORMS = ('netease_max', 'kugou')

# specify folder where the log files to be stored
LOG_PATH = Path('log')   


OUTPUT_PATH = Path('output') # specify the folder where the output files to be stored

# the artist name which to be processed
ARTIST_NAMES = [  "Thomas Bergersen",  "Nick Phoenix","Two Steps From Hell",]

# This is useful when we need to resume the process from the previous aborted process 
START_ARTIST_INDEX = 0 # specfiy the start artist index. This index is INCLUDED
START_DATA_FEED_IDX =0 # this index will be INCLUDED 

END_ARTIST_INDEX = 99999 # specfiy when the process will be stopped
END_DATA_FEED_IDX = 99999 # this index will INCLUDED in the processing 

# include the refine_similarity_level_2 records, level 2 are the rows with exact matching on song name, but does not match on singer name and alblum name
INCLUDE_SIMILARITY_LEVEL2 = False
```

4.  The application is a long run program, it might terminate unexpectedly. The application supports restart operation. Just run the following command, and the program will resumed from where it was stopped: 
```
python main.py restart
```

### Technical information

1. The application will take snapshot for each artist and each datafeed. When the application is terminated unexpectedly, it will read the snapshot file and resume the process for the last checkpoint
1. Since each query usually will cost more then 3~5 minutes, the retry mechanics is setup in the rds_access.py module. The query will try to rerun the query statement for a few times. When it fails, it will wait for 30 seconds and retry until the total retry limits is reached.
1. The iterations in the main process are designed into 2 levels:  artist and data_feeds. Each data_feeds contains the information artist_name, platform name, song name. Once a data feed is processed, it will process for the data for a song name in a specific platform only. 
