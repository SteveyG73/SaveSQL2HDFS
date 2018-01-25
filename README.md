# SaveSQL2HDFS

## Description
Read a SQL Server table and save it to HDFS

## Usage
```
usage: SaveSQL2HDFS.py
       [-h] [-w WEBHDFS] [-o OUTDIR] -s DBSERVER -d DBNAME -u SCHEMA -t TABLE
       [-c]


  -h, --help            show this help message and exit
  -w WEBHDFS, --webhdfs WEBHDFS
                        The root URL for WEBHDFS
                        Defaults to http://lonvm2178.markelintl.markelgroup.com:50070/webhdfs/v1
  -o OUTDIR, --outdir OUTDIR
                        Directory to write output to in HDFS
                        Defaults to /user/<currentuser>/
  -s DBSERVER, --dbserver DBSERVER
                        Database server format <server>:<port>
                        Mandatory parameter
  -d DBNAME, --dbname DBNAME
                        Database name
                        Mandatory parameter
  -u SCHEMA, --schema SCHEMA
                        Schema
                        Mandatory parameter
  -t TABLE, --table TABLE
                        Table name
                        Mandatory parameter
  -c, --noconvertdates  Don\'t convert dates to timestamps
                        Defaults to False if not specified
```

## Notes

### Kerberos
The Kerberos user passed to WEBHDFS is the current Windows user (for a Windows Server), or whatever is in the ``klist`` for a Linux server. It's up to the user to make sure a Kerberos ticket is available. This should *just work* on Windows.

### Logging
The program logs to the console and to a log file which can be found in the ``logs/`` directory underneath the directory the program has been deployed to.

## Dependencies

Package | URL
--- | ---
dataset | https://dataset.readthedocs.io/en/latest/
requests | http://docs.python-requests.org/en/master/
requests_kerberos | https://github.com/requests/requests-kerberos
