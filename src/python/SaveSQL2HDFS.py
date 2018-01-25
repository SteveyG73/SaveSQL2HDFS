import dataset
import csv
from io import StringIO
import sys,os,getopt,logging,datetime,argparse, getpass
import requests
from requests_kerberos import HTTPKerberosAuth, OPTIONAL

LOG = None

def set_logging(log_dir=None):
    consolelevel = logging.DEBUG
    logger = logging.getLogger(__name__)
    logger.setLevel(consolelevel)
# create formatter and add it to the handlers
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(consolelevel)
    ch.setFormatter(formatter)
# add the handlers to logger
    logger.addHandler(ch)
     
# create file handler which logs error messages

    if log_dir==None:
        working_dir = os.path.join(os.path.dirname(__file__), 'logs')
    else:
        working_dir = log_dir


    os.makedirs(working_dir, exist_ok=True)
    log_file = os.path.basename(__file__).split('.')[0]+'.log'

    working_file = os.path.join(working_dir, os.path.basename(__file__).split('.')[0]+'.log' )

        
    filelevel = logging.INFO
    fh = logging.FileHandler(working_file)
    fh.setLevel(filelevel)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.info('Logging to: '+working_file)
    
 
#test logging
#    logger.debug("debug message")
#    logger.info("info message")
#    logger.warn("warn message")
#    logger.error("error message")
#    logger.critical("critical message")
    return(logger)

def write_to_hdfs(string_data, webhdfs_root, hdfs_path, file_name):
    path_list = hdfs_path.split('/')
    path_list.append(file_name)
    hdfs_file= '/'.join(path_list)
    LOG.info('Writing data to '+hdfs_file)
    LOG.info('Creating kerberos context')
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    request_url = webhdfs_root+hdfs_file+'?op=CREATE&overwrite=true'
    LOG.info('Requesting HTTP PUT from URL '+ request_url)
    r=requests.put(request_url, auth=kerberos_auth, allow_redirects=False)

    LOG.info('Request responded with status code '+str(r.status_code))
    LOG.info('Redirect location given as '+r.headers['Location'])

    location = r.headers['Location']
    r=requests.put(location, data=string_data, auth=kerberos_auth)

    LOG.info('Request responded with status code '+str(r.status_code))
    LOG.info('Sucessfully created file at location '+r.headers['Location'])

def get_user_folder():
    username = getpass.getuser()
    if ('\\' in username):
        username = username.split('\\')[1]
    return '/user/'+username

def date_parse(in_col, convert_dates):

    if (type(in_col) is datetime.datetime):
        if(convert_dates):

            '''
            Unix timestamps have some limitations. The minimum date is 01/01/1970 and the maximum is around 19/01/2038,
            so this little routine overrides any invalid values with these values.
            The upshot is that the output of the table is different to the input, which is why I've included the option
            to just upload the data as a string.
            '''
            if in_col < datetime.datetime(1970,1,1):
                out_col = 0
            elif in_col > datetime.datetime(2038,1,19):
                out_col = datetime.datetime(2038,1,19).timestamp()
            else:
                out_col = in_col.timestamp()
        else:
            out_col = in_col.strftime('%Y-%m-%d %H:%M:%S')
    else:
        out_col = in_col
        
    return out_col

def main(argv):
    global LOG

    parser = argparse.ArgumentParser(prog=__file__, description='Read a SQL Server table and save it to HDFS')
    parser.add_argument('-w','--webhdfs', dest='webhdfs', help='The root URL for WEBHDFS', default = 'http://<namenode>:50070/webhdfs/v1')
    parser.add_argument('-o','--outdir', dest='outdir', help='Directory to write output to in HDFS', default=get_user_folder())
    parser.add_argument('-s','--dbserver', dest='dbserver', help='Database server format <server>:<port>', required=True)
    parser.add_argument('-d','--dbname', dest='dbname', help='Database name', required=True)
    parser.add_argument('-u','--schema', dest='schema', help='Schema', required=True)
    parser.add_argument('-t','--table', dest='table', help='Table name', required=True)
    parser.add_argument('-c','--noconvertdates', dest='convertdates', help='Don\'t convert dates to timestamps', action='store_true', default=False,)

    if len(argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    args = parser.parse_args(argv[1:])



    LOG = set_logging();

    for arg in vars(args):
        LOG.info('Argument '+str(arg)+' = '+str(getattr(args,arg)))
                          
    LOG.info('Connecting to database at '+args.dbserver+','+args.dbname+' and sourcing tables from schema '+args.schema)

    db = dataset.connect('mssql+pyodbc://'
                         +args.dbserver+'/'
                         +args.dbname
                         +'?driver=SQL+Server+Native+Client+11.0',
                         schema=args.schema
                         )

    LOG.info('Connection successful')
    LOG.info('Loading data from table '+args.table)

    table = db.load_table(args.table)
    LOG.info('Table '+args.table+' loaded successfully')
    LOG.info('Table '+args.table+' has '+str(len(table.columns))+' columns and '+str(len(table))+' rows')

    csvfile = StringIO()

    headers = table.columns

    LOG.info('Creating delimited table in memory...')
    writer= csv.DictWriter(csvfile, fieldnames = headers, delimiter='|')
    writer.writeheader()
    for source in table:
        writer_row = {}
        for k,v in source.items():
            writer_row[k]=date_parse(v,  not args.convertdates)

        writer.writerow(writer_row)

    write_to_hdfs(csvfile.getvalue(),args.webhdfs,args.outdir, args.table+'.csv')

    csvfile.close()



if __name__ == '__main__':
    main(sys.argv)



