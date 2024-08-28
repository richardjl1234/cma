##  Standalone Music Audit Program

This application is a standalone music audit program. 



### User Guide: 

1. Unzip the zip file 
1. Go to the folder where the audit programs are unzipped. 
1.  Make sure python enviroment is installed. 
1.  Install the necessary packages by executing the following command: 
```
pip install -r requirements.txt
```
1.  Create the folder `input_data`, `output`, `output/debug`, `output/excel`, `output/pickle`, `outout/archive` and `log` folder. Run the following command so that those folders will be created. 
```
sh startup.sh
```
1.  Prepare the client statements document, copy the input file into the the folder `input_data` 
1.  Setup the database username and password in local environment. When the application is running, it will get the following envionment vaiable value. 
```
POSTGRE_USER=
POSTGRE_PWD=
POSTGRE_HOST=
```
4.  Update the settings.py to setup the paremeters which are needed by the application. The following variables can be customized. Most importantly, update the `INPUT_FILE`, `START_SONG_INDEX` and `END_SONG_INDEX`
```
LOG_LEVEL=logging.DEBUG

# TODO, the platform qqmusicv2 need to added, but now the performance is not good, so it is not ready to be added
PLATFORMS = ('netease_max', 'kugou')

LOG_PATH = Path('log')   # specify folder where the log files to be stored
OUTPUT_PATH = Path('output') # specify the folder where the output files to be stored

# the input file should be in the input_data folder, the format can be xlsx, csv or pkl
INPUT_PATH = "input_data"
INPUT_FILE = "cc_soave.xlsx"  

# This is useful when we need to resume the process from the previous aborted process 
START_SONG_INDEX = 0 # specfiy the start song index. This index is INCLUDED
END_SONG_INDEX = 30 # specfiy when the process will be stopped

```
5.  The application is a long run program, it might terminate unexpectedly. The application supports restart operation. Just run the following command, and the program will resumed from where it was stopped: 
```
python main.py restart
```
6. Once a song is process, the merge result will be output to the `output/matched_result_final.xlsx` file and `output/unmatched_result.csv`

### Technical information

1. The application will remember for  which song has been process successfully. When the application is terminated unexpectedly, it will read the restart_song_index file and resume the process from where it was termicated.  
1. Since each query usually will cost more than around 30 seconds, the retry mechanics is setup in the rds_access.py module. The query will try to rerun the query statement for a few times. When it fails, it will wait for 30 seconds and retry until the total retry limits is reached.
1. The log will be printed on the console as well as output to the `log/song_audit-yyyymmdd.log` file.