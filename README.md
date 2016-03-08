# imap-dmarcts
A Perl based tool to Parse DMARC reports from IMAP and insert into a database.

To install dependencies on Debian:
```
apt-get install libmail-imapclient-perl libmime-tools-perl libxml-simple-perl \
libclass-dbi-mysql-perl libio-socket-inet6-perl libio-socket-ip-perl libperlio-gzip-perl
```
Once the script has been downloaded, you'll want to edit these basic components.  Most others are self-explanatory.
```
our $imapserver = 'mail.example.com:143';
our $imapuser = 'dmarcreports';
our $imappass = 'xxx';
our $mvfolder = 'processed';
our $readfolder = 'Inbox';
our $dbname = 'dmarc';
our $dbuser = 'dmarc';
our $dbpass = 'xxx';
```
The alternative is to use the `imap-dmarcts.conf` file.

More info can currently be found at : [TechSneeze.com](http://www.techsneeze.com/how-parse-dmarc-reports-imap/)
