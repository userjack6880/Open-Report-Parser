# imap-dmarcts
A Perl based tool to parse DMARC reports, based on John Levine's [rddmarc](http://www.taugh.com/rddmarc/), but extended by the following features:
* Allow to read messages from an IMAP server and not only from the local filesystem.
* Store much more XML values into the database (for example the missing SPF and DKIM results from the policy_evaluated section) and also the entire XML for later reference.
* Needed database tables and columns are created automatically, user only needs to provide a database. The database schema is compatible to the one used by rddmarc, but extends it by additional fields. Users can switch from rddmarc to imap-dmarcts without having to do any changes to the database by themself.


## Installation and Configuration

To install dependencies on Debian:
```
apt-get install libmail-imapclient-perl libmime-tools-perl libxml-simple-perl \
libclass-dbi-mysql-perl libio-socket-inet6-perl libio-socket-ip-perl libperlio-gzip-perl
```
Once the script has been downloaded, you'll want to edit these basic configuration options at the top of the script.  Most of them are self-explanatory:
```
####################################################################
### configuration ##################################################
####################################################################

# If IMAP access is not used, config options starting with $imap
# do not need to be set and are ignored.

$debug = 0;
$keep_reports_at_original_location = 0;

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

# If $imapmovefolder is empty (''), processed IMAP messages
# will be deleted, if -k option is not given.
$imapmovefolder = 'Inbox.processed';
```
The alternative is to provide these lines in an `imap-dmarcts.conf` file in the current working directory. If that file is found, configuration options are taken from there.

## Usage

```
./imap-dmarcts.pl [OPTIONS] [PATH]
```
If `PATH` is not provided, reports are read from an IMAP server, otherwise they are read from PATH from local filesystem. PATH can be a filename of a single mime message file or multiple mime message files - wildcard expression are allowed.

**Remember**: To run, this script needs custom configurations: a database server and credentials and (if used) an IMAP server and credentials. These values can be set inside the script or by providing them via `imap-dmarcts.conf` in the current working directory.

The default behaviour of the script is to DELETE processed message files, since the XML is stored in the database and the original messages are no longer needed. For IMAP access, this can be modified by setting `$imapmovefolder` (message is moved rather than being deleted). In general this can be modified by setting `$keep_reports_at_original_location` or by
providing the -k option.

The following options are always allowed:
```
 -d : Print debug info.
 -r : Replace existing reports rather than failing.
 -k : Do not delete processed message files and keep them at their
      original location.
```

If a `PATH` is given, the following option is also allowed:
```
 -x : Files specified by PATH are XML report files, rather than
      mime messages containing the XML report files.
      Processed XML files are not deleted.
```

More info can currently be found at : [TechSneeze.com](http://www.techsneeze.com/how-parse-dmarc-reports-imap/)