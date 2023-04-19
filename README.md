# Open Report Parser

A Perl based tool to parse DMARC reports, based on John Levine's [rddmarc](http://www.taugh.com/rddmarc/), but extended by the following features:
- Allow to read messages from an IMAP server and not only from the local filesystem.
- Store much more XML values into the database (for example the missing SPF and DKIM results from the policy_evaluated section) and also the entire XML for later reference.
- Needed database tables and columns are created automatically, user only needs to provide a database. The database schema is compatible to the one used by rddmarc, but extends it by additional fields. Users can switch from rddmarc to dmarcts-report-parser without having to do any changes to the database by themselves.
- Support for MySQL and PostgreSQL

Open Report Parser is a fork of [techsneeze's dmarcts-report-parser](https://github.com/techsneeze/dmarcts-report-parser), and was forked to more closely match the needs of [Open DMARC Analyzer](https://github.com/userjack6880/Open-DMARC-Analyzer).

Open Report Parser Version 0 Alpha 1 (0-α1) is an [Anomaly \<Codebase\>](https://systemanomaly.com/codebase) project by John Bradley (john@systemanomaly.com).

# Minimum Requirements

- Perl 5
- MySQl 15.1 or equivalent
- PostgreSQL 13.9

# Dependencies

## on Debian

```
apt-get install libfile-mimeinfo-perl libmail-imapclient-perl libmime-tools-perl libxml-simple-perl \
libio-socket-inet6-perl libio-socket-ip-perl libperlio-gzip-perl \
libmail-mbox-messageparser-perl unzip
```

- For MySQL: `libdbd-mysql-perl`
- For PostgreSQL: `libdbd-pg-perl`


## on Fedora (Fedora 23)

```
sudo dnf install perl-File-MimeInfo perl-Mail-IMAPClient perl-MIME-tools perl-XML-Simple perl-DBI \
perl-Socket6 perl-PerlIO-gzip unzip
```

- For MySQL: `perl-DBD-MySQL`
- For PostgreSQL: `perl-DBD-Pg`

## on CentOS (CentOS 7)

```
yum install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
yum install perl-File-MimeInfo perl-Mail-IMAPClient perl-MIME-tools perl-XML-Simple perl-DBI \
perl-Socket6 perl-PerlIO-gzip unzip perl-Mail-Mbox-MessageParser
```

- For MySQL: `perl-DBD-MySQL`
- For PostgreSQL: `perl-DBD-Pg`

## on FreeBSD (FreeBSD 11.4)

```
sudo pkg install p5-File-MimeInfo p5-Mail-IMAPClient p5-MIME-tools p5-XML-Simple p5-DBI p5-Socket6 p5-PerlIO-gzip p5-Mail-Mbox-MessageParser unzip
```

- For MySQL: `p5-DBD-MySQL`
- For PostgreSQL: `p5-DBD-Pg`

## on macOS (macOS 10.13)

```
brew install mysql shared-mime-info
update-mime-database /usr/local/share/mime
perl -MCPAN -e 'install Mail::IMAPClient'
perl -MCPAN -e 'install Mail::Mbox::MessageParser'
perl -MCPAN -e 'install File::MimeInfo'
```

- For MySQL: `perl -MCPAN -e 'install DBD::mysql'`
- For PostgreSQL: `perl -MCPAN -e 'install DBD::pg'`

# Setting up Open Report Parser

Optaining Open Report Parser through `git` is probably the easiest way, in addition to doing occasional pulls to get up-to-date versions.

```
git clone https://github.com/userjack6880/Open-Report-Parser.git
```

Optionally, a [zip file of the latest release](https://github.com/userjack6880/Open-Report-Parser/releases) can be downloaded.

Rename `repoart-parser.conf.pub` to `report-parser.conf` and edit the configuration for your environment (see the next section on **Configuration Options** for details). Finally, some condierations need to be taken in account due to limitations in stock configurations of MySQL/MariaSQL on some distros, it may be necessary to add the following to your configuration (i.e. in /etc/mysql/mariadb.conf.d/50-server.cnf):

```
innodb_large_prefix	= on
innodb_file_format	= barracuda
innodb_file_per_table	= true
```

# Configuration Options

**Debug Options**

```perl
$debug = 0;
$delete_reports = 0;
```

**Database Options**

```perl
#$dbtype = 'mysql';                         # Supported types - mysql, postgres - defaults to mysql if unset
$dbname = 'dmarc';
$dbuser = 'dmarc';
$dbpass = 'password';
$dbhost = 'dbhost';                         # Set the hostname if we can't connect to the local socket.
$dbport = '3306';
```

**IMAP Options**

```perl
$imapserver       = 'imap.server';
$imapuser         = 'username';
$imappass         = 'password';
$imapport         = '143';
$imapssl          = '0';                    # If set to 1, remember to change server port to 993 and disable imaptls.
$imaptls          = '0';                    # Enabled as the default and best-practice.
$tlsverify        = '0';                    # Enable verify server cert as the default and best-practice.
$imapignoreerror  = '0';                    # set it to 1 if you see an "ERROR: message_string() 
                                            # expected 119613 bytes but received 81873 you may 
                                            # need the IgnoreSizeErrors option" because of malfunction
                                            # imap server as MS Exchange 2007, ...
$imapreadfolder   = 'dmarc';

# If $imapmovefolder is set, processed IMAP messages will be moved (overruled by
# the --delete option!)
$imapmovefolder = 'dmarc/processed';
```

These settings are ignored when using the -m flag.

**XML Storage Options**

```perl
# maximum size of XML files to store in database, long files can cause transaction aborts
$maxsize_xml = 50000;

# store XML as base64 encopded gzip in database (save space, harder usable)
$compress_xml = 0;
```

**Processing Failure Action***

```perl
# if there was an error during file processing (message does not contain XML or ZIP parts, 
# or a database error) the parser reports an error and does not delete the file, even if 
# delete_reports is set (or --delete is given). Deletion can be enforced by delete_failed, 
# however not for database errors.
$delete_failed = 0;
```

The script is looking for `report-parser.conf` in the current working directory. If not found it will look by the calling path. If neither is found than it will abort.

Note: Be sure to use the proper hierarchy separator for your server in all folder specs, and if your IMAP server flattens the hierarchy (i.e. Cyrus IMAP with "altnamespace: yes") then leave "Inbox" off of the beginning of such specs.

# Usage

```
./dmarcts-report-parser.pl [OPTIONS] [PATH]
```

PATH can be the filename of a single file or a list of files - wildcard expression are allowed.

**Remember**: This script needs a configurations file called <report-parser.conf> in the current working directory, which defines a database server with credentials and (if used) an IMAP server with credentials.

One of the following source options must be provided:

```
      -i : Read reports from messages on IMAP server as defined in the config file.
      -m : Read reports from mbox file(s) provided in PATH.
      -e : Read reports from MIME email file(s) provided in PATH.
      -x : Read reports from xml file(s) provided in PATH.
      -z : Read reports from zip file(s) provided in PATH.
```

The following options are always allowed:

```
      -d : Print debug info.
      -r : Replace existing reports rather than failing.
--delete : Delete processed message files (the XML is stored in the database for later reference).
  --info | Print out number of XML files or emails processed.
```

# Latest Changes

## 0-α1
- Fork renamed
- Incorporate changes made to original repository after fork ([commit 51ba1de](https://github.com/userjack6880/Open-Report-Parser/commit/51ba1de8521559647ebe4b8a1db291c26b572de4))

# Tested System Configurations
| OS          | Perl      | SQL             |
| ----------- | --------- | --------------- |
| Debian 11.6 | Perl 5.32 | MariaDB 10.5.18 |

# Release Cycle and Versioning

This project regular release cycle is not yet determined. Versioning is under the Anomaly Versioning Scheme (2022), as outlined in `VERSIONING` under `docs`.

# Support

Support will be provided as outlined in the following schedule. For more details, see `SUPPORT`.

| Version                             | Support Level    | Released         | End of Support   | End of Life      |
| ----------------------------------- | ---------------- | ---------------- | ---------------- | ---------------- |
| Version 1 Alpha 1                   | Full Support     | TBD              | TBD              | TBD              |

# Contributing

Public contributions are encouraged. Please review `CONTRIBUTING` under `docs` for contributing procedures. Additionally, please take a look at our `CODE_OF_CONDUCT`. By participating in this project you agree to abide by the Code of Conduct.

# Contributors

Primary Contributors
- John Bieling - Initial Work
- TechZneeze.com - Expansion of Initial Work
- John Bradley - This Fork

Thanks to [all who contributed](https://github.com/userjack6880/Open-Report-Parser/graphs/contributors) ([and here](https://github.com/techsneeze/dmarcts-report-parser/graphs/contributors)) and [have given feedback](https://github.com/userjack6880/Open-Report-Parser/issues?q=is%3Aissue).

# Licenses and Copyrights

Copyright © 2023 John Bradley (userjack6880), Copyright © 2016 TechSneeze.com, Copyright © 2012 John Bieling. Open Report Parser is released under GNU GPLv3. See `LICENSE`.