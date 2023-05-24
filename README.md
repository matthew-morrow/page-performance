# Page Performance Command Line Script

## Overview/Summary

## Installing Required Packages
```
pip install -r requirements.txt
```

## Running the script

The current command line arguments require the starting date of both the previous and current timeframe. Optional arguments exist for user customization

Basic sample command line with user in folder with script
```
./page_performance_calculator.py -p 20230309 -c 20230330
```

Expanded arguments
```
-h, --help            show this help message and exit
-p previousdate, --previous_start_date previousdate
                        String representation for the start date of the previous timeframe as (yyyymmdd) format
-c currentdate, --current_start_date currentdate
                        String representation for the start date of the current timeframe as (yyyymmdd) format
-tf [timeframe], --time_frame [timeframe]
                        Optionally specify the window of time for each dataset. Default is two weeks (14 days inclusive of start date)
-i [inputfile], --input_file [inputfile]
                        Override default file found on Box with a user specified dataset
-o [outputfile], --output_file [outputfile]
                        Override default output path of current directory to user specified destination
-a [activeurlfile], --active_urls_file [activeurlfile]
                        Override default file found on Box with a user specified active URLs dataset
```
## TODO

- [x] Comment code
- [ ] Upload Sample Files to Box
- [ ] Implement ECLKC API for Link Checking
- [ ] Etc...
