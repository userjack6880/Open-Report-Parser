# dmarcts-report-parser
A Perl based tool to parse DMARC reports, based on John Levine's [rddmarc](http://www.taugh.com/rddmarc/), but extended by the following features:
* Allow to read messages from an IMAP server and not only from the local filesystem.
* Store much more XML values into the database (for example the missing SPF and DKIM results from the policy_evaluated section) and also the entire XML for later reference.
* Needed database tables and columns are created automatically, user only needs to provide a database. The database schema is compatible to the one used by rddmarc, but extends it by additional fields. Users can switch from rddmarc to dmarcts-report-parser.pl without having to do any changes to the database by themself.


## Installation and Configuration

To install dependencies on Debian:
```
apt-get install libmail-imapclient-perl libmime-tools-perl libxml-simple-perl \
libclass-dbi-mysql-perl libio-socket-inet6-perl libio-socket-ip-perl libperlio-gzip-perl
```

To get your copy of the dmarcts-report-parser, you can either clone the repository:
```
git clone https://github.com/techsneeze/dmarcts-report-parser.git
```
or download a zip file containg all files from [here](https://github.com/techsneeze/dmarcts-report-parser/archive/master.zip). Once the files have been downloaded, you will need to copy/rename `dmarcts-report-parser.conf.sample` to `dmarcts-report-parser.conf`. Next, edit the configuration options:

```
####################################################################
### configuration ##################################################
####################################################################

# If IMAP access is not used, config options starting with $imap
# do not need to be set and are ignored.

$debug = 0;
$delete_reports = 0;

$dbname = 'dmarc';
$dbuser = 'dmarc';
$dbpass = 'xxx';
$dbhost = ''; # Set the hostname if we can't connect to the local socket.

$imapserver = 'mail.example.com:143';
$imapuser = 'dmarcreports';
$imappass = 'xxx';
$imapssl = '0'; # If set to 1, remember to change server port to 993.
$imaptls = '1'; # Enabled as the default and best-practice.
$imapreadfolder = 'Inbox';

# If $imapmovefolder is set, processed IMAP messages
# will be moved (overruled by the --delete option!)
$imapmovefolder = 'Inbox.processed';
```
The script is looking for `dmarcts-report-parser.conf` in the current working directory.

## Usage

```
./dmarcts-report-parser.pl [OPTIONS] [PATH]
```
If `PATH` is not provided, reports are read from an IMAP server, otherwise they are read from PATH from local filesystem. PATH can be a filename of a single mime message file or multiple mime message files - wildcard expression are allowed.

**Remember**: To run, this script needs custom configurations: a database server and credentials and (if used) an IMAP server and credentials. These values must be in the config file as described above.

The following options are always allowed:
```
#        -d : Print debug info.
#        -r : Replace existing reports rather than failing.
#  --delete : Delete processed message files (the XML is stored in the
#             database for later reference).
```

If a `PATH` is given, the following option is also allowed:
```
         -x : Files specified by PATH are XML report files, rather than
              mime messages containing the XML report files.
```

More info can currently be found at : [TechSneeze.com](http://www.techsneeze.com/how-parse-dmarc-reports-imap/)

