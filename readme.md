The configuration file should be a simple JSON blob that specifes the desired actions in the format:
                    {
                        "TRANSFORM":{
                            "h": "list of column numbers or ranges to hash",
                            "i": "list of column numbers or ranges to keep",
                            "r": "list of column numbers or ranges to remove"
                        },
                        "SAMPLE":{
                            "rand": " fractional amount of random rows to be returned (ie. .3, .75)",
                            "row": "list of row numbers or ranges""
                        }
                        "PERMISSIONS?"
                        "DB_NAME?"

                    }
    where:
        "h" correlates to applying a hash to the columns specified (eg. [1, 5-7])
        "i" correlates to applying the identity function to the columns specified 
        "r" correlates to removing the columns specified

        and sample can return either the list of specified rows OR a random set of rows, not both.

The name of db table should be specified via the command line argument '--name'.
The csv to parse should be specified via the command line argument '--csv'.
The config file file should be specified via the command line argument '--config'.  If no config is specified, the csv will be be saved, as is, to the database. 
A sample execution might look like: `python parser.py --csv 'sample.csv' --name 'sample' --config 'config.json'`