#!/usr/bin/perl

# -----------------------------------------------------------------------------
#
# Open Report Parser - Open Source DMARC report parser
# Copyright (C) 2023 John Bradley (userjack6880)
# Copyright (C) 2016 TechSneeze.com
# Copyright (C) 2012 John Bieling
#
# report-parser.pl
#   main script
#
# Available at: https://github.com/userjack6880/Open-Report-Parser
#
# -----------------------------------------------------------------------------
#
#  This file is part of Open Report Parser.
#
#  Open Report Parser is free software: you can redistribute it and/or modify it under
#  the terms of the GNU General Public License as published by the Free Software 
#  Foundation, either version 3 of the License, or (at your option) any later 
#  version.
#
#  This program is distributed in the hope that it will be useful, but WITHOUT ANY 
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#  PARTICULAR PURPOSE.  See the GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along with 
#  this program.  If not, see <https://www.gnu.org/licenses/>.
#
# -----------------------------------------------------------------------------
#
# The subroutines storeXMLInDatabase() and getXMLFromMessage() are based on
# John R. Levine's rddmarc (http://www.taugh.com/rddmarc/). The following
# special conditions apply to those subroutines:
#
# Copyright 2012, Taughannock Networks. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# Redistributions in binary form must reproduce the above copyright notice, this
# list of conditions and the following disclaimer in the documentation and/or
# other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# -----------------------------------------------------------------------------

# Always be safe
use strict;
use warnings;

# Use these modules
use Getopt::Long;
use IO::Compress::Gzip qw(gzip $GzipError);
#use Data::Dumper;
use Mail::IMAPClient;
use Mail::Mbox::MessageParser;
use MIME::Base64 qw(encode_base64);
use MIME::Words qw(decode_mimewords);
use MIME::Parser;
use MIME::Parser::Filer;
use XML::Simple;
use JSON;
use DBI;
use Socket;
use Socket6;
use PerlIO::gzip;
use File::Basename ();
use File::MimeInfo;
use IO::Socket::SSL;
#use IO::Socket::SSL 'debug3';

# -----------------------------------------------------------------------------
# usage
# -----------------------------------------------------------------------------

sub show_usage {
  print "\n";
  print " Usage: \n";
  print "    ./report-parser.pl [OPTIONS] [PATH] \n";
  print "\n";
  print " This script needs a configuration file called <report-parser.conf> in the\n";
  print " current working directory, which defines a database server with credentials \n";
  print " and (if used) an IMAP server with credentials. \n";
  print "\n";
  print " Additionaly, one of the following source options must be provided: \n";
  print "        -i : Read reports from messages on IMAP server as defined in the \n";
  print "             config file. \n";
  print "        -m : Read reports from mbox file(s) provided in PATH. \n";
  print "        -e : Read reports from MIME email file(s) provided in PATH. \n";
  print "        -x : Read reports from xml file(s) provided in PATH. \n";
  print "        -j : Read reports from json files(s) provided in PATH. \n";
  print "        -z : Read reports from zip file(s) provided in PATH. \n";
  print "\n";
  print " The following optional options are allowed: \n";
  print "        -d : Print debug info. \n";
  print "        -r : Replace existing reports rather than skipping them. \n";
  print "  --delete : Delete processed message files (the XML is stored in the \n";
  print "             database for later reference). \n";
  print "    --info : Print out number of XML files or emails processed. \n";
  print "     --tls : Force TLS-Only Mode. \n";
  print "\n";
}

# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------

# Define all possible configuration options.
our ($debug, $delete_reports, $delete_failed, $reports_replace, $dmarc_only,
     $maxsize_xml, $compress_xml, $maxsize_json, $compress_json,
     $dbtype, $dbname, $dbuser, $dbpass, $dbhost, $dbport, $db_tx_support,
     $imapserver, $imapport, $imapuser, $imappass, $imapignoreerror, $imapssl, $imaptls, 
     $imapdmarcfolder, $imapdmarcproc, $imapdmarcerr, 
     $imaptlsfolder, $imaptlsproc, $imaptlserr,
     $imapopt, $tlsverify, $processInfo);

# defaults
$maxsize_xml     = 50000;
$maxsize_json    = 50000;
$dbtype          = 'mysql';
$dbhost          = 'localhost';
$db_tx_support   = 1;
$dmarc_only      = 1;
$reports_replace = 0;

# used in messages
my $scriptname = 'Open Report Parser';
my $version    = 'Version 0 Alpha 3';

# allowed values for the DB columns, also used to build the enum() in the
# CREATE TABLE statements in checkDatabase(), in order defined here
use constant ALLOWED_DISPOSITION => qw(
  none
  quarantine
  reject
  unknown
);
use constant ALLOWED_DKIM_ALIGN => qw(
  fail
  pass
  unknown
);
use constant ALLOWED_SPF_ALIGN => qw(
  fail
  pass
  unknown
);
use constant ALLOWED_DKIMRESULT => qw(
  none
  pass
  fail
  neutral
  policy
  temperror
  permerror
  unknown
);
use constant ALLOWED_SPFRESULT => qw(
  none
  neutral
  pass
  fail
  softfail
  temperror
  permerror
  unknown
);

# Load script configuration options from local config file. The file is expected
# to be in the current working directory.
my $conf_file = 'report-parser.conf';

# Get command line options.
my %options = ();
use constant { TS_IMAP => 0, 
               TS_MESSAGE_FILE => 1, 
               TS_XML_FILE => 2, 
               TS_MBOX_FILE => 3, 
               TS_ZIP_FILE => 4, 
               TS_JSON_FILE => 5 };
GetOptions( \%options, 'd', 'r', 'x', 'j', 'm', 'e', 'i', 'z', 'delete', 'info', 'c' => \$conf_file );

# locate conf file or die
if ( -e $conf_file ) {
  #$conf_file = "./$conf_file";
} 
elsif ( -e  (File::Basename::dirname($0) . "/$conf_file" ) ) {
  $conf_file = ( File::Basename::dirname($0) . "/$conf_file" );
}
else {
  show_usage();
  die "$scriptname: Could not read config file '$conf_file' from current working directory or path (" . File::Basename::dirname($0) . ')'
}

# load conf file with error handling
if ( substr($conf_file, 0, 1) ne '/'  and substr($conf_file, 0, 1) ne '.') {
  $conf_file = "./$conf_file";
}
my $conf_return = do $conf_file;
die "$scriptname: couldn't parse $conf_file: $@" if $@;
die "$scriptname: couldn't do $conf_file: $!"    unless defined $conf_return;

# check config
if (!defined $imapdmarcfolder ) {
  die "$scriptname: \$imapdmarcfolder not defined. Check config file";
}
if (!defined $imapignoreerror ) {
  $imapignoreerror = 0;   # maintain compatibility to old version
}

# Evaluate command line options
my $source_options = 0;
our $reports_source;

if (exists $options{m}) {
  $source_options++;
  $reports_source = TS_MBOX_FILE;
  $dmarc_only = 1;
}

if (exists $options{x}) {
  $source_options++;
  $reports_source = TS_XML_FILE;
  $dmarc_only = 1;
}

if (exists $options{j}) {
  $source_options++;
  $reports_source = TS_JSON_FILE;
  $dmarc_only = -1;
}

if (exists $options{e}) {
  $source_options++;
  $reports_source = TS_MESSAGE_FILE;
  $dmarc_only = 1;
}

if (exists $options{i}) {
  $source_options++;
  $reports_source = TS_IMAP;
}

if (exists $options{z}) {
  $source_options++;
  $reports_source = TS_ZIP_FILE;
  $dmarc_only = 1;
}

if (exists $options{c}) {
  $source_options++;
}

if ($source_options > 1) {
  show_usage();
  die "$scriptname: Only one source option can be used (-i, -x, -m, -e or -z).\n";
} 
elsif ($source_options == 0) {
  show_usage();
  die "$scriptname: Please provide a source option (-i, -x, -m, -e or -z).\n";
}

if ($ARGV[0]) {
  if ($reports_source == TS_IMAP) {
    show_usage();
    die "$scriptname: The IMAP source option (-i) may not be used together with a PATH.\n";
  }
} 
else {
  if ($reports_source != TS_IMAP && $source_options == 1) {
    show_usage();
    die "$scriptname: The provided source option requires a PATH.\n";
  }
}

# Override config options by command line options.
if (exists $options{r})      {$reports_replace = 1;}
if (exists $options{d})      {$debug = 1;}
if (exists $options{delete}) {$delete_reports = 1;}
if (exists $options{info})   {$processInfo = 1;}
if (exists $options{tls})    {$dmarc_only = -1;}

# Cludgy, but it lets us preserve filename for dbx_postgres.pl
my $dbitype = 'mysql';
$dbitype = 'Pg' if $dbtype eq 'postgres';

# Print info
printInfo($scriptname."\n  $version");

# Print out config if debug
if ($debug) {
  print "$scriptname DEBUG ENABLED\n".
        "-- Script Options --\n\n".
        "Report Source:   $reports_source\n".
        "(0: IMAP, 1: Message, 2: XML, 3: MBOX, 4: ZIP, 5: JSON)\n".
        "Show Processed:  $processInfo\n".
        "Delete Reports:  $delete_reports\n".
        "Delete Failed:   $delete_failed\n".
        "Replace Reports: $reports_replace\n".
        "DMARC Only:      $dmarc_only\n".
        "(0: DMARC\\TLS, 1: DMARC Only, -1: TLS Only)\n\n".
        "-- Database Options --\n\n".
        "DB Type:         $dbtype\n".
        "DB Name:         $dbname\n".
        "DB User:         $dbuser\n".
        "DB Host/Port:    $dbhost:$dbport\n".
        "DB TX Support:   $db_tx_support\n\n".
        "Max XML Size:    $maxsize_xml\n".
        "Max JSON Size:   $maxsize_json\n".
        "Compress XML:    $compress_xml\n".
        "Compress JSON:   $compress_json\n\n".
        "-- IMAP Options --\n\n".
        "IMAP Server:     $imapserver\n".
        "IMAP Port:       $imapport\n".
        "TLS:             $imaptls\n".
        "SSL:             $imapssl\n".
        "TLS Verify:      $tlsverify\n".
        "IMAP User:       $imapuser\n".
        "IMAP Ignore Err: $imapignoreerror\n".
        "DMARC Folders: \n".
        "   Reports:      $imapdmarcfolder\n"; 
  print "   Processed:    $imapdmarcproc\n" if defined($imapdmarcproc);
  print "   Error:        $imapdmarcerr\n" if defined($imapdmarcerr);
  print "TLS Folders: \n".
        "   Reports:      $imaptlsfolder\n";
  print "   Processed:    $imaptlsproc\n" if defined($imaptlsproc);
  print "   Error:        $imaptlserr\n" if defined($imaptlserr);
  print "----\n\n";
}

# Setup connection to database server.
our %dbx;
my $dbx_file = File::Basename::dirname($0) . "/dbx_$dbtype.pl";
my $dbx_return = do $dbx_file;
die "$scriptname: couldn't load DB definition for type $dbtype: $@" if $@;
die "$scriptname: couldn't load DB definition for type $dbtype: $!" unless defined $dbx_return;

my $dbh = DBI->connect("DBI:$dbitype:database=$dbname;host=$dbhost;port=$dbport",
                            $dbuser,
                            $dbpass)
or die "$scriptname: Cannot connect to database\n";

if ($db_tx_support) {
  $dbh->{AutoCommit} = 0;
}

checkDatabase($dbh);

# Process messages based on $reports_source.
if ($reports_source == TS_IMAP) {
  my $socketargs = '';
  my $processedReport = 0;

  # Disable verify mode for TLS support.
  if ($imaptls == 1) {
    if ( $tlsverify == 0 ) {
      printDebug("use tls without verify servercert.");
      $imapopt = [ SSL_verify_mode => SSL_VERIFY_NONE ];
    } 
    else {
      printDebug("use tls with verify servercert.");
      $imapopt = [ SSL_verify_mode => SSL_VERIFY_PEER ];
    }
  # The whole point of setting this socket arg is so that we don't get the nasty warning
  } 
  else {
    printDebug("using ssl without verify servercert.");
    $socketargs = [ SSL_verify_mode => SSL_VERIFY_NONE ];
  }
  
  printDebug("connection to $imapserver with Ssl => $imapssl, User => $imapuser, Ignoresizeerrors => $imapignoreerror");

  # Setup connection to IMAP server.
  my $imap = Mail::IMAPClient->new(
    Server     => $imapserver,
    Port       => $imapport,
    Ssl        => $imapssl,
    Starttls   => $imapopt,
    Debug      => $debug,
    Socketargs => $socketargs
  )
  # module uses eval, so we use $@ instead of $!
  or die "$scriptname: IMAP Failure: $@";

  # This connection is finished this way because of the tradgedy of exchange...
  $imap->User($imapuser);
  $imap->Password($imappass);
  $imap->connect();

  # Ignore Size Errors if we're using Exchange
  $imap->Ignoresizeerrors($imapignoreerror);

  # Set $imap to UID mode, which will force imap functions to use/return
  # UIDs, instead of message sequence numbers. UIDs are not allowed to
  # change during a session and are not allowed to be used twice. Looping
  # over message sequence numbers and deleting a msg in between could have
  # unwanted side effects.
  $imap->Uid(1);

  # How many msgs are we going to process?
  printInfo("Processing ". $imap->message_count($imapdmarcfolder)." messages in folder <$imapdmarcfolder>.") if $dmarc_only >= 0;
  printInfo("Processing ". $imap->message_count($imaptlsfolder)." messages in folder <$imaptlsfolder>.") if $dmarc_only <= 0;

  # Only select and search $imapdmarcfolder, if we actually
  # have something to do.

  printDebug("report processing");

  if ($imap->message_count($imapdmarcfolder) && $dmarc_only >= 0) {
    $processedReport = processIMAP($imap, $processedReport, $imapdmarcfolder, $imapdmarcproc, $imapdmarcerr, 0);
  }
  if ($imap->message_count($imaptlsfolder) && $dmarc_only <= 0) {
    $processedReport = processIMAP($imap, $processedReport, $imaptlsfolder, $imaptlsproc, $imaptlserr, 1);
  }

  # We're all done with IMAP here.
  $imap->logout();
  printInfo("Processed $processedReport emails.");

} 
else { # TS_MESSAGE_FILE or TS_XML_FILE or TS_MBOX_FILE

  my $counts = 0;
  foreach my $a (@ARGV) {
    # Linux bash supports wildcard expansion BEFORE the script is
    # called, so here we only see a list of files. Other OS behave
    # different, so we should not depend on that feature: Use glob
    # on each argument to manually expand the argument, if possible.
    my @file_list = glob($a);

    foreach my $f (@file_list) {
      my $filecontent;

      if ($reports_source == TS_MBOX_FILE) {
        my $parser = Mail::Mbox::MessageParser->new({"file_name" => $f, "debug" => $debug, "enable_cache" => 0});
        my $num = 0;

        do {
          $num++;
          $filecontent = $parser->read_next_email();
          if (defined($filecontent)) {
            if ($dmarc_only == 1) {
              if (processXML(TS_MESSAGE_FILE, $filecontent, "message #$num of mbox file <$f>") & 2) {
                # processXML return a value with delete bit enabled
                warn "$scriptname: Removing message #$num from mbox file <$f> is not yet supported.\n";
              }
            }
            else {
              if (processJSON(TS_MESSAGE_FILE, $filecontent, "message #$num of mbox file <$f>") & 2) {
                warn "$scriptname: Removing message #$num from mbox file <$f> is not yet supportd.\n";
              }
            }
            $counts++;
          }
        } while(defined($filecontent));

      } 
      elsif ($reports_source == TS_ZIP_FILE) {
        # filecontent is zip file
        if ($dmarc_only == 1) {
          $filecontent = getXMLFromFile($f);
          if (processXML(TS_ZIP_FILE, $filecontent, "xml file <$f>") & 2) {
            # processXML return a value with delete bit enabled
            unlink($f);
          }
        }
        else {
          $filecontent = getJSONFromFile($f);
          if (processJSON(TS_ZIP_FILE, $filecontent, "xml file <$f>") & 2) {
            unlink($f);
          }
        }
        $counts++;
      } 
      elsif (open(FILE, "<", $f)) {

        $filecontent = join("", <FILE>);
        close FILE;

        if ($reports_source == TS_MESSAGE_FILE) {
          # filecontent is a mime message with zip or xml part
          if ($dmarc_only == 1) {
            if (processXML(TS_MESSAGE_FILE, $filecontent, "message file <$f>") & 2) {
              # processXML return a value with delete bit enabled
              unlink($f);
            }
          }
          else {
            if (processJSON(TS_MESSAGE_FILE, $filecontent, "message file <$f>") & 2) {
              unlink($f);
            }
          }
          $counts++;
        } 
        elsif ($reports_source == TS_XML_FILE) {
          # filecontent is xml file
          if (processXML(TS_XML_FILE, $filecontent, "xml file <$f>") & 2) {
            # processXML return a value with delete bit enabled
            unlink($f);
          }
          $counts++;
        } 
        elsif ($reports_source == TS_JSON_FILE) {
          # filecontent is json file
          if (processJSON(TS_JSON_FILE, $filecontent, "json file <$f>") & 2) {
            unlink($f);
          }
          $counts++;
        } 
        else {
          warn "$scriptname: Unknown reports_source <$reports_source> for file <$f>. Skipped.\n";
        }

      } 
      else {
        warn "$scriptname: Could not open file <$f>: $!. Skipped.\n";
        # Could not retrieve filecontent, the skipped message
        # will be processed every time the script is run even if
        # delete_reports and delete_failed is given. The user
        # has to look at the actual file.
      }
    }
  }
  printInfo("$scriptname: Processed $counts messages(s).");
}

# -----------------------------------------------------------------------------
# subroutines
# -----------------------------------------------------------------------------

sub printDebug {
  my $message = shift;
  print "\n\n--- DEBUG ---\n".
        "  $message".
        "\n-------------\n" if $debug;
}

# -----------------------------------------------------------------------------

sub printInfo {
  my $message = shift;
  print "\n\n--- DEBUG ---\n" if $debug;
  print "  $message\n" if $debug || $processInfo;
  print "-------------\n" if $debug;
}

# -----------------------------------------------------------------------------

sub processIMAP {
  my $imap = shift;
  my $processedReport = shift;
  my $imapfolder = shift;
  my $imapproc = shift;
  my $imaperr = shift;
  my $type = shift;

  printDebug("processing <$imapfolder>.");

  # Select the mailbox to get messages from.
  $imap->select($imapfolder)
    or die "$scriptname: IMAP Select Error: $!";

  # Store each message as an array element.
  my @msgs = $imap->search('ALL')
    or die "$scriptname: Couldn't get all messages\n";

  # Loop through IMAP messages.
  foreach my $msg (@msgs) {

    my $processResult;
    if ($type == 0) {
      $processResult = processXML(TS_MESSAGE_FILE, $imap->message_string($msg), "IMAP message with UID #".$msg);
    }
    else {
      $processResult = processJSON(TS_MESSAGE_FILE, $imap->message_string($msg), "IMAP message with UID #".$msg);
    }
    $processedReport++;
    if ($processResult & 4) {
      # processXML/JSON returned a value with database error bit enabled, do nothing at all!
      if ($imaperr) {
        # if we can, move to error folder
        moveToImapFolder($imap, $msg, $imaperr);
      } 
      else {
        # do nothing at all
        next;
      }
    } 
    elsif ($processResult & 2) {
      # processXML/JSON return a value with delete bit enabled.
      $imap->delete_message($msg)
      or warn "$scriptname: Could not delete IMAP message. [$@]\n";
    } 
    elsif ($imapproc) {
      if ($processResult & 1 || !$imaperr) {
        # processXML/JSON processed the XML OK, or it failed and there is no error imap folder
        moveToImapFolder($imap, $msg, $imapproc);
      } 
      elsif ($imaperr) {
        # processXML/JSON failed and error folder set
        moveToImapFolder($imap, $msg, $imaperr);
      }
    } 
    elsif ($imaperr && !($processResult & 1)) {
      # processXML/JSON failed, error imap folder set, but imapdmarcproc unset. An unlikely setup, but still...
      moveToImapFolder($imap, $msg, $imaperr);
    }
  }

  # Expunge and close the folder.
  $imap->expunge($imapfolder);
  $imap->close($imapfolder);
  return $processedReport;
}

# -----------------------------------------------------------------------------

sub moveToImapFolder {
  my $imap = $_[0];
  my $msg = $_[1];
  my $imapfolder = $_[2];

  print "Moving (copy and delete) IMAP message file to IMAP folder: $imapfolder\n" if $debug;

  # Try to create $imapfolder, if it does not exist.
  if (!$imap->exists($imapfolder)) {
    $imap->create($imapfolder)
    or warn "$scriptname: Could not create IMAP folder: $imapfolder.\n";
  }

  # Try to move the message to $imapfolder.
  my $newid = $imap->copy($imapfolder, [ $msg ]);
  if (!$newid) {
    warn "$scriptname: Error on moving (copy and delete) processed IMAP message: Could not COPY message to IMAP folder: <$imapfolder>!\n";
    warn "$scriptname: Messsage will not be moved/deleted. [$@]\n";
  } 
  else {
    $imap->delete_message($msg)
    or do {
      warn "$scriptname: Error on moving (copy and delete) processed IMAP message: Could not DELETE message\n";
      warn "$scriptname: after copying it to <$imapfolder>. [$@]\n";
    }
  }
}

# -----------------------------------------------------------------------------

sub processXML {
  my ($type, $filecontent, $f) = (@_);

  if ($debug) {
    print "\n";
    print "----------------------------------------------------------------\n";
    print "Processing $f \n";
    print "----------------------------------------------------------------\n";
    print "Type: $type\n";
    print "FileContent: $filecontent\n";
    print "MSG: $f\n";
    print "----------------------------------------------------------------\n";
  }

  my $xml; #TS_XML_FILE or TS_MESSAGE_FILE
  if ($type == TS_MESSAGE_FILE) {$xml = getXMLFromMessage($filecontent);}
  elsif ($type == TS_ZIP_FILE) {$xml = $filecontent;}
  else {$xml = getXMLFromXMLString($filecontent);}

  # If !$xml, the file/mail is probably not a DMARC report.
  # So do not storeXMLInDatabase.
  if ($xml && storeXMLInDatabase($xml) <= 0) {
    # If storeXMLInDatabase returns false, there was some sort
    # of database storage failure and we MUST NOT delete the
    # file, because it has not been pushed into the database.
    # The user must investigate this issue.
    warn "$scriptname: Skipping $f due to database errors.\n";
    return 5; #xml ok(1), but database error(4), thus no delete (!2)
  }

  # Delete processed message, if the --delete option
  # is given. Failed reports are only deleted, if delete_failed is given.
  if ($delete_reports && ($xml || $delete_failed)) {
    if ($xml) {
      printDebug("Removing after report has been processed.");
      return 3; #xml ok (1), delete file (2)
    } 
    else {
      # A mail which does not look like a DMARC report
      # has been processed and should now be deleted.
      # Print its content so it gets send as cron
      # message, so the user can still investigate.
      warn "$scriptname: The $f does not seem to contain a valid DMARC report. Skipped and Removed. Content:\n";
      warn $filecontent."\n";
      return 2; #xml not ok (!1), delete file (2)
    }
  }

  if ($xml) {
    return 1;
  } 
  else {
    warn "$scriptname: The $f does not seem to contain a valid DMARC report. Skipped.\n";
    return 0;
  }
}

# -----------------------------------------------------------------------------

# Walk through a mime message and return a reference to the XML data containing
# the fields of the first ZIPed XML file embedded into the message. The XML
# itself is not checked to be a valid DMARC report.
sub getXMLFromMessage {
  printDebug("getting XML from message");
  my ($message) = (@_);
  
  # fixup type in trustwave SEG mails
        $message =~ s/ContentType:/Content-Type:/;

  my $parser = new MIME::Parser;
  $parser->output_dir("/tmp");
  $parser->filer->ignore_filename(1);
  my $ent = $parser->parse_data($message);

  my $body = $ent->bodyhandle;
  my $mtype = $ent->mime_type;
  my $subj = decode_mimewords($ent->get('subject'));
  chomp($subj); # Subject always contains a \n.

  printDebug("Subject: $subj\n".
           "  MimeType: $mtype");

  my $location;
  my $isgzip = 0;

  if(lc $mtype eq "application/zip") {
    printDebug("This is a ZIP file");

    $location = $body->path;

  } 
  elsif (lc $mtype eq "application/gzip" or lc $mtype eq "application/x-gzip") {
    printDebug("This is a GZIP file");

    $location = $body->path;
    $isgzip = 1;

  } 
  elsif (lc $mtype =~ "multipart/") {
    # At the moment, nease.net messages are multi-part, so we need
    # to breakdown the attachments and find the zip.
    printDebug("This is a multipart attachment");
    #print Dumper($ent->parts);

    my $num_parts = $ent->parts;
    for (my $i=0; $i < $num_parts; $i++) {
      my $part = $ent->parts($i);

      # Find a zip file to work on...
      if(lc $part->mime_type eq "application/gzip" or lc $part->mime_type eq "application/x-gzip") {
        $location = $ent->parts($i)->{ME_Bodyhandle}->{MB_Path};
        $isgzip = 1;
        printDebug("$location");
        last; # of parts
      } 
      elsif(lc $part->mime_type eq "application/x-zip-compressed"
        or $part->mime_type eq "application/zip") {

        $location = $ent->parts($i)->{ME_Bodyhandle}->{MB_Path};
        printDebug("$location");
      } 
      elsif(lc $part->mime_type eq "application/octet-stream") {
        $location = $ent->parts($i)->{ME_Bodyhandle}->{MB_Path};
        $isgzip = 1 if $location =~ /\.gz$/;
        printDebug("$location");
      } 
      else {
        # Skip the attachment otherwise.
        printDebug("Skipped an unknown attachment (".lc $part->mime_type.")");
        next; # of parts
      }
    }
  } 
  else {
    ## Clean up dangling mime parts in /tmp of messages without ZIP.
    my $num_parts = $ent->parts;
    for (my $i=0; $i < $num_parts; $i++) {
      if ($debug) {
        if ($ent->parts($i)->{ME_Bodyhandle} && $ent->parts($i)->{ME_Bodyhandle}->{MB_Path}) {
          printDebug($ent->parts($i)->{ME_Bodyhandle}->{MB_Path});
        } 
        else {
          printDebug("undef");
        }
      }
      if($ent->parts($i)->{ME_Bodyhandle}) {$ent->parts($i)->{ME_Bodyhandle}->purge;}
    }
  }


  # If a ZIP has been found, extract XML and parse it.
  my $xml;
  printDebug("body is in " . $location) if defined($location);
  $xml = getXMLFromZip($location,$isgzip);
  if($body) {$body->purge;}
  if($ent) {$ent->purge;}
  return $xml;
}

# -----------------------------------------------------------------------------

sub getXMLFromFile {  my $filename = $_[0];
  printDebug("getting XML from file");
  my $mtype = mimetype($filename);

  if ($debug) {
    print "Filename: $filename, MimeType: $mtype\n";
  }

  my $isgzip = 0;

  if(lc $mtype eq "application/zip") {
    if ($debug) {
      print "This is a ZIP file \n";
    }
  } 
  elsif (lc $mtype eq "application/gzip" or lc $mtype eq "application/x-gzip") {
    if ($debug) {
      print "This is a GZIP file \n";
    }

    $isgzip = 1;
  } 
  else {
    if ($debug) {
      print "This is not an archive file \n";
    }
  }

  # If a ZIP has been found, extract XML and parse it.
  my $xml = getXMLFromZip($filename,$isgzip);
  return $xml;
}

# -----------------------------------------------------------------------------

sub getXMLFromZip {
  printDebug("getting XML from ZIP");
  my $filename = shift;
  my $isgzip = shift;

  my $xml;
  if (defined($filename)) {
    # Open the zip file and process the XML contained inside.
    my $unzip = "";
    if ($isgzip) {
      open(XML, "<:gzip", $filename)
      or $unzip = "ungzip";
    } 
    else {
      open(XML, "-|", "unzip", "-p", $filename)
      or $unzip = "unzip"; # Will never happen.

      # Sadly unzip -p never fails, but we can check if the
      # filehandle points to an empty file and pretend it did
      # not open/failed.
      if (eof XML) {
        $unzip = "unzip";
      }
    }

    # Read XML if possible (if open)
    if ($unzip eq "") {
      $xml = getXMLFromXMLString(join("", <XML>));
      if (!$xml) {
        warn "$scriptname: The XML found in ZIP file (<$filename>) does not seem to be valid XML! \n";
      }
      close XML;
    } 
    else {
      warn "$scriptname: Failed to $unzip ZIP file (<$filename>)! \n";
      close XML;
    }
  } 
  else {
    warn "$scriptname: Could not find an <$filename>! \n";
  }

  return $xml;
}

# -----------------------------------------------------------------------------

sub getXMLFromXMLString {
  printDebug("getting XML from string");
  my $raw_xml = $_[0];

  eval {
    my $xs = XML::Simple->new();
    my $ref = $xs->XMLin($raw_xml, SuppressEmpty => '');
    $ref->{'raw_xml'} = $raw_xml;

    return $ref;
  } 
  or do {
    return undef;
  }
}

# -----------------------------------------------------------------------------

# Extract fields from the XML report data hash and store them into the database.
# return 1 when ok, 0, for serious error and -1 for minor errors
sub storeXMLInDatabase {
  printDebug("storing XML in database");
  my $xml = $_[0]; # $xml is a reference to the xml data

  my $from  = $xml->{'report_metadata'}->{'date_range'}->{'begin'};
  my $to    = $xml->{'report_metadata'}->{'date_range'}->{'end'};
  my $org   = $xml->{'report_metadata'}->{'org_name'};
  my $id    = $xml->{'report_metadata'}->{'report_id'};
  my $email = $xml->{'report_metadata'}->{'email'};
  my $extra = $xml->{'report_metadata'}->{'extra_contact_info'};

  my $domain       = undef;
  my $policy_adkim = undef;
  my $policy_aspf  = undef;
  my $policy_p     = undef;
  my $policy_sp    = undef;
  my $policy_pct     = undef;
 
  if (ref $xml->{'policy_published'} eq "HASH") {
    $domain       =  $xml->{'policy_published'}->{'domain'};
    $policy_adkim = $xml->{'policy_published'}->{'adkim'};
    $policy_aspf  = $xml->{'policy_published'}->{'aspf'};
    $policy_p     = $xml->{'policy_published'}->{'p'};
    $policy_sp    = $xml->{'policy_published'}->{'sp'};
    $policy_pct   = $xml->{'policy_published'}->{'pct'};
  } 
  else {
    $domain       =  $xml->{'policy_published'}[0]->{'domain'};
    $policy_adkim = $xml->{'policy_published'}[0]->{'adkim'};
    $policy_aspf  = $xml->{'policy_published'}[0]->{'aspf'};
    $policy_p     = $xml->{'policy_published'}[0]->{'p'};
    $policy_sp    = $xml->{'policy_published'}[0]->{'sp'};
    $policy_pct   = $xml->{'policy_published'}[0]->{'pct'};
  }

  my $record = $xml->{'record'};
  if ( ! defined($record) ) {
    warn "$scriptname: $org: $id: No records in report. Skipped.\n";
    return 0;
  }

  # see if already stored
  my $sth = $dbh->prepare(qq{SELECT org, serial FROM report WHERE reportid=?});
  $sth->execute($id);
  while ( my ($xorg,$sid) = $sth->fetchrow_array() )
  {
    if ($reports_replace) {
      # $sid is the serial of a report with reportid=$id
      # Remove this $sid from rptrecord and report table, but
      # try to continue on failure rather than skipping.
      print "$scriptname: $org: $id: Replacing data.\n";
      $dbh->do(qq{DELETE from rptrecord WHERE serial=?}, undef, $sid);
      if ($dbh->errstr) {
        warn "$scriptname: $org: $id: Cannot remove report data from database. Try to continue.\n";
      }
      $dbh->do(qq{DELETE from report WHERE serial=?}, undef, $sid);
      if ($dbh->errstr) {
        warn "$scriptname: $org: $id: Cannot remove report from database. Try to continue.\n";
      }
    } 
    else {
      print "$scriptname: $org: $id: Already have report, skipped\n";
      # Do not store in DB, but return true, so the message can
      # be moved out of the way, if configured to do so.
      return 1;
    }
  }

  my $sql = qq{INSERT INTO report (mindate,maxdate,domain,org,reportid,email,extra_contact_info,policy_adkim, policy_aspf, policy_p, policy_sp, policy_pct, raw_xml)
                           VALUES ($dbx{epoch_to_timestamp_fn}(?),$dbx{epoch_to_timestamp_fn}(?),?,?,?,?,?,?,?,?,?,?,?)};
  my $storexml = $xml->{'raw_xml'};
  if ($compress_xml) {
    my $gzipdata;
    if(!gzip(\$storexml => \$gzipdata)) {
      warn "$scriptname: $org: $id: Cannot add gzip XML to database ($GzipError). Skipped.\n";
      rollback($dbh);
      return 0;
      $storexml = "";
    } 
    else {
      $storexml = encode_base64($gzipdata, "");
    }
  }
  if (length($storexml) > $maxsize_xml) {
    warn "$scriptname: $org: $id: Skipping storage of large XML (".length($storexml)." bytes) as defined in config file.\n";
    $storexml = "";
  }
  $dbh->do($sql, undef, $from, $to, $domain, $org, $id, $email, $extra, $policy_adkim, $policy_aspf, $policy_p, $policy_sp, $policy_pct, $storexml);
  if ($dbh->errstr) {
    warn "$scriptname: $org: $id: Cannot add report to database. Skipped.\n";
    return 0;
  }

  my $serial = $dbh->last_insert_id(undef, undef, 'report', undef);
  printDebug("serial $serial");

  sub dorow($$$$) {
    my ($serial,$recp,$org,$id) = @_;
    my %r = %$recp;

    my $ip = $r{'row'}->{'source_ip'};
    if ( $ip eq '' ) {
      warn "$scriptname: $org: $id: source_ip is empty. Skipped.\n";
      rollback($dbh);
      return 0;
    }
    my $count = $r{'row'}->{'count'};
    my $disp = $r{'row'}->{'policy_evaluated'}->{'disposition'};
    if ( ! grep { $_ eq $disp } ALLOWED_DISPOSITION ) {
      $disp = 'unknown';
    };
     # some reports don't have dkim/spf, "unknown" is default for these
    my $dkim_align = $r{'row'}->{'policy_evaluated'}->{'dkim'};
    if ( ! grep { $_ eq $dkim_align } ALLOWED_DKIM_ALIGN ) {
      $dkim_align = 'unknown';
    };
    my $spf_align = $r{'row'}->{'policy_evaluated'}->{'spf'};
    if ( ! grep { $_ eq $spf_align } ALLOWED_SPF_ALIGN ) {
      $spf_align = 'unknown';
    };

    my $identifier_hfrom = $r{'identifiers'}->{'header_from'};

    my ($dkim, $dkimresult, $spf, $spfresult, $reason);
    if(ref $r{'auth_results'} ne "HASH"){
      warn "$scriptname: $org: $id: Report has no auth_results data. Skipped.\n";
      return 0;
    }
    my $rp = $r{'auth_results'}->{'dkim'};
    if(ref $rp eq "HASH") {
      $dkim = $rp->{'domain'};
      $dkim = undef if ref $dkim eq "HASH";
      $dkimresult = $rp->{'result'};
    } 
    else { # array, i.e. multiple dkim results (usually from multiple domains)
      # glom sigs together
      $dkim = join '/',map { my $d = $_->{'domain'}; ref $d eq "HASH"?"": $d } @$rp;
      # report results
      my $rp_len = scalar(@$rp);
      for ( my $i=0; $i < $rp_len; $i++ ) {
        if ( $rp->[$i]->{'result'} eq "pass" ) {
          # If any one dkim result is a "pass", this should yield an overall "pass" and immediately exit the for loop, ignoring any remaing results
          # See
          # RFC 6376, DomainKeys Identified Mail (DKIM) Signatures
          # 	Section 4.2: https://tools.ietf.org/html/rfc6376#section-4.2 and
          # 	Section 6.1: https://tools.ietf.org/html/rfc6376#section-6.1
          # And the GitHub issues at
          #	https://github.com/techsneeze/dmarcts-report-viewer/issues/47
          #	https://github.com/techsneeze/dmarcts-report-parser/pull/78
          $dkimresult = "pass";
          last;
        } 
        else {
          for ( my $j=$i+1; $j < $rp_len; $j++ ) {
            if ( $rp->[$i]->{'result'} eq $rp->[$j]->{'result'} ) {
            # Compare each dkim result to the next one to see if all of the dkim results are the same.
            # If all of the dkim results are the same, that will be the overall result.
            # If any of them are different, and don't contain a "pass" result, then $dkimresult will be empty
              $dkimresult = $rp->[0]->{'result'};
            } else {
              $dkimresult = 'unknown';
            }
          }
        }
      }
    }
    if ( ! defined($dkimresult) || ! grep { $_ eq $dkimresult } ALLOWED_DKIMRESULT ) {
      $dkimresult = 'unknown';
    };

    $rp = $r{'auth_results'}->{'spf'};
    if(ref $rp eq "HASH") {
      $spf = $rp->{'domain'};
      $spf = undef if ref $spf eq "HASH";
      $spfresult = $rp->{'result'};
    } 
    else { # array, i.e. multiple dkim results (usually from multiple domains)
      # glom sigs together
      $spf = join '/',map { my $d = $_->{'domain'}; ref $d eq "HASH"?"": $d } @$rp;
      # report results
      my $rp_len = scalar(@$rp);
      for ( my $i=0; $i < $rp_len; $i++ ) {
        if ( $rp->[$i]->{'result'} eq "pass" ) {
          # If any one spf result is a "pass", this should yield an overall "pass" and immediately exit the for loop, ignoring any remaing results
          $spfresult = "pass";
          last;
        } 
        else {
          for ( my $j=$i+1; $j < $rp_len; $j++ ) {
            if ( $rp->[$i]->{'result'} eq $rp->[$j]->{'result'} ) {
            # Compare each spf result to the next one to see if all of the spf results are the same.
            # If all of the spf results are the same, that will be the overall result.
            # If any of them are different, and don't contain a "pass" result, then $spfresult will be empty
              $spfresult = $rp->[0]->{'result'};
            } 
            else {
              $spfresult = 'unknown';
            }
          }
        }
      }
    }
    if ( ! defined($spfresult) || ! grep { $_ eq $spfresult } ALLOWED_SPFRESULT ) {
      $spfresult = 'unknown';
    };

    $rp = $r{'row'}->{'policy_evaluated'}->{'reason'};
    if(ref $rp eq "HASH") {
      $reason = $rp->{'type'};
    } 
    else {
      $reason = join '/',map { $_->{'type'} } @$rp;
    }
    #print "ip=$ip, count=$count, disp=$disp, r=$reason,";
    #print "dkim=$dkim/$dkimresult, spf=$spf/$spfresult\n";

    # What type of IP address?
    my ($nip, $iptype, $ipval);
    printDebug("ip=$ip");

    if($nip = inet_pton(AF_INET, $ip)) {
      $ipval = unpack "N", $nip;
      $iptype = "ip";
    } 
    elsif($nip = inet_pton(AF_INET6, $ip)) {
      $ipval = $dbx{to_hex_string}($nip);
      $iptype = "ip6";
    } 
    else {
      warn "$scriptname: $org: $id: ??? mystery ip $ip\n";
      rollback($dbh);
      return 0;
    }

    $dbh->do(qq{INSERT INTO rptrecord (serial,$iptype,rcount,disposition,spf_align,dkim_align,reason,dkimdomain,dkimresult,spfdomain,spfresult,identifier_hfrom)
                               VALUES (?,$ipval,?,?,?,?,?,?,?,?,?,?)},undef,$serial,$count,$disp,$spf_align,$dkim_align,$reason,$dkim,$dkimresult,$spf,$spfresult,$identifier_hfrom);
    if ($dbh->errstr) {
      warn "$scriptname: $org: $id: Cannot add report data to database. Skipped.\n";
      rollback($dbh);
      return 0;
    }
    return 1;
  }

  my $res = 1;
  if(ref $record eq "HASH") {
    printDebug("single record");
    $res = -1 if !dorow($serial,$record,$org,$id);
  } 
  elsif(ref $record eq "ARRAY") {
    printDebug("multi record");
    foreach my $row (@$record) {
      $res = -1 if !dorow($serial,$row,$org,$id);
    }
  } 
  else {
    warn "$scriptname: $org: $id: mystery type " . ref($record) . "\n";
  }

  if ($res <= 0) {
    printDebug("Result $res XML: $xml->{raw_xml}");
  }

  if ($res <= 0) {
    if ($db_tx_support) {
      warn "$scriptname: $org: $id: Cannot add records to rptrecord. Rolling back DB transaction.\n";
      rollback($dbh);
    } 
    else {
      warn "$scriptname: $org: $id: errors while adding to rptrecord, serial $serial records likely obsolete.\n";
    }
  } 
  else {
    if ($db_tx_support) {
      $dbh->commit;
      if ($dbh->errstr) {
        warn "$scriptname: $org: $id: Cannot commit transaction.\n";
      }
    }
  }
  return $res;
}

# -----------------------------------------------------------------------------

sub processJSON {
  my ($type, $filecontent, $f) = (@_);

  if ($debug) {
    print "\n";
    print "----------------------------------------------------------------\n";
    print "Processing $f \n";
    print "----------------------------------------------------------------\n";
    print "Type: $type\n";
    print "FileContent: $filecontent\n";
    print "MSG: $f\n";
    print "----------------------------------------------------------------\n";
  }

  my $json; #TS_JSON_FILE or TS_MESSAGE_FILE
  if ($type == TS_MESSAGE_FILE) { $json = getJSONFromMessage($filecontent); }
  elsif ($type == TS_ZIP_FILE)  { $json = getJSONFromFile($filecontent); }
  else                          { $json = getJSONFromString($filecontent); }

  # If !$json, the file/mail is probably not a TLS report.
  # So do not storeJSONInDatabase.
  if ($json && storeJSONInDatabase($json) <= 0) {
    # If storeJSONInDatabase returns false, there was some sort
    # of database storage failure and we MUST NOT delete the
    # file, because it has not been pushed into the database.
    # The user must investigate this issue.
    warn "$scriptname: Skipping $f due to database errors.\n";
    return 5; #json ok(1), but database error(4), thus no delete (!2)
  }

  # Delete processed message, if the --delete option
  # is given. Failed reports are only deleted, if delete_failed is given.
  if ($delete_reports && ($json || $delete_failed)) {
    if ($json) {
      print "Removing after report has been processed.\n" if $debug;
      return 3; #json ok (1), delete file (2)
    } 
    else {
      # A mail which does not look like a TLS report
      # has been processed and should now be deleted.
      # Print its content so it gets send as cron
      # message, so the user can still investigate.
      warn "$scriptname: The $f does not seem to contain a valid TLS report. Skipped and Removed. Content:\n";
      warn $filecontent."\n";
      return 2; #json not ok (!1), delete file (2)
    }
  }

  if ($json) {
    return 1;
  } 
  else {
    warn "$scriptname: The $f does not seem to contain a valid TLS report. Skipped.\n";
    return 0;
  }
}

# -----------------------------------------------------------------------------

sub getJSONFromMessage {
  my ($message) = (@_);
  
  # fixup type in trustwave SEG mails
        $message =~ s/ContentType:/Content-Type:/;

  my $parser = new MIME::Parser;
  $parser->output_dir("/tmp");
  $parser->filer->ignore_filename(1);
  my $ent = $parser->parse_data($message);

  my $body = $ent->bodyhandle;
  my $mtype = $ent->mime_type;
  my $subj = decode_mimewords($ent->get('subject'));
  chomp($subj); # Subject always contains a \n.

  printDebug("Subject: $subj\n".
           "  MimeType: $mtype");

  my $location;
  my $isgzip = 0;

  if (lc $mtype eq "application/tlsrpt+gzip" or lc $mtype eq "application/tlsrpt+x-gzip") {
    printDebug("This is a GZIP file");

    $location = $body->path;
    $isgzip = 1;

  } 
  elsif (lc $mtype =~ "multipart/") {
    # At the moment, nease.net messages are multi-part, so we need
    # to breakdown the attachments and find the zip.
    printDebug("This is a multipart attachment");
    #print Dumper($ent->parts);

    my $num_parts = $ent->parts;
    for (my $i=0; $i < $num_parts; $i++) {
      my $part = $ent->parts($i);

      # Find a zip file to work on...
      if(lc $part->mime_type eq "application/tlsrpt+gzip" or lc $part->mime_type eq "application/tlsrpt+x-gzip") {
        $location = $ent->parts($i)->{ME_Bodyhandle}->{MB_Path};
        $isgzip = 1;
        printDebug("$location");
        last; # of parts
      } 
      elsif(lc $part->mime_type eq "application/octet-stream") {
        $location = $ent->parts($i)->{ME_Bodyhandle}->{MB_Path};
        $isgzip = 1 if $location =~ /\.gz$/;
        printDebug("$location");
      } 
      else {
        # Skip the attachment otherwise.
        printDebug("Skipped an unknown attachment (".lc $part->mime_type.")");
        next; # of parts
      }
    }
  } 
  else {
    ## Clean up dangling mime parts in /tmp of messages without ZIP.
    my $num_parts = $ent->parts;
    for (my $i=0; $i < $num_parts; $i++) {
      if ($debug) {
        if ($ent->parts($i)->{ME_Bodyhandle} && $ent->parts($i)->{ME_Bodyhandle}->{MB_Path}) {
          printDebug($ent->parts($i)->{ME_Bodyhandle}->{MB_Path});
        } 
        else {
          printDebug("undef");
        }
      }
      if($ent->parts($i)->{ME_Bodyhandle}) {$ent->parts($i)->{ME_Bodyhandle}->purge;}
    }
  }


  # If a ZIP has been found, extract XML and parse it.
  my $json;
  if (defined($location)) {
    printDebug("body is in " . $location);
  }
  $json = getJSONFromZip($location,$isgzip);
  if($body) {$body->purge;}
  if($ent) {$ent->purge;}
  return $json;
}

# -----------------------------------------------------------------------------

sub getJSONFromFile {
  printDebug("getting JSON from File");
  my $filename = $_[0];
  my $mtype = mimetype($filename);

  printDebug("Filename: $filename, MimeType: $mtype");

  my $isgzip = 0;

  if (lc $mtype eq "application/tlsrpt+gzip" or lc $mtype eq "application/tlsrpt+x-gzip") {
    printDebug("This is a GZIP file");

    $isgzip = 1;
  } 
  else {
    printDebug("This is not an archive file");
  }

  # If a ZIP has been found, extract XML and parse it.
  my $json = getJSONFromZip($filename,$isgzip);
  return $json;
}

# -----------------------------------------------------------------------------

sub getJSONFromZip {
  printDebug("getting JSON from ZIP");
  my $filename = shift;
  my $isgzip = shift;

  my $json;
  if (defined($filename)) {
    # Open the zip file and process the XML contained inside.
    my $unzip = "";
    if ($isgzip) {
      open(JSON, "<:gzip", $filename)
      or $unzip = "ungzip";
    } 
    else {
      open(JSON, "-|", "unzip", "-p", $filename)
      or $unzip = "unzip"; # Will never happen.

      # Sadly unzip -p never fails, but we can check if the
      # filehandle points to an empty file and pretend it did
      # not open/failed.
      if (eof JSON) {
        $unzip = "unzip";
      }
    }

    # Read JSON if possible (if open)
    if ($unzip eq "") {
      $json = getJSONFromString(join("", <JSON>));
      if (!$json) {
        warn "$scriptname: The JSON found in ZIP file (<$filename>) does not seem to be valid JSON! \n";
      }
      close JSON;
    } 
    else {
      warn "$scriptname: Failed to $unzip ZIP file (<$filename>)! \n";
      close JSON;
    }
  } 
  else {
    warn "$scriptname: Could not find an <$filename>! \n";
  }

  return $json;
}

# -----------------------------------------------------------------------------

sub getJSONFromString {
  printDebug("getting JSON from string");
  my $raw_json = $_[0];

  eval {
    my $json = decode_json($raw_json);
    return $json;
  } 
  or do {
    return undef;
  }
}

# -----------------------------------------------------------------------------

sub storeJSONInDatabase {
  printDebug("storing JSON into database");
  my $json = shift;

  my $org       = $json->{'organization-name'};
  my $from      = $json->{'date-range'}{'start-datetime'};
  my $to        = $json->{'date-range'}{'end-datetime'};
  my $email     = $json->{'contact-info'};
  my $reportid  = $json->{'report-id'};
  # I don't think mulptile policies per report are sent right now
  my $mode      = $json->{'policies'}[0]{'policy'}{'policy-string'}[1] // ''; # this can be blank for some reason...
  my $domain    = $json->{'policies'}[0]{'policy'}{'policy-domain'};
  my $success   = $json->{'policies'}[0]{'summary'}{'total-successful-session-count'};
  my $failure   = $json->{'policies'}[0]{'summary'}{'total-failure-session-count'} // 0; # we need to make sure this has a value

  my $record    = $json->{'policies'}[0]{'failure-details'};

  # remove "mode:"
  $mode =~ s/mode: //;

  # date timestamp is close, but needs to be cleaned up
  $from =~ s/T/ /;
  $from =~ s/Z//;
  $to   =~ s/T/ /;
  $to   =~ s/Z//;

  printDebug("org:           $org\n".
           "  date:          $from - $to\n".
           "  email:         $email\n".
           "  report id:     $reportid\n".
           "  sts mode:      $mode\n".
           "  policy domain: $domain\n".
           "  success count: $success\n".
           "  failure count: $failure");

  # see if already stored
  my $sth = $dbh->prepare(qq{SELECT org, serial FROM tls WHERE reportid=?});
  $sth->execute($reportid);
  while ( my ($xorg,$sid) = $sth->fetchrow_array() )
  {
    if ($reports_replace) {
      # $sid is the serial of a report with reportid=$id
      # Remove this $sid from rptrecord and report table, but
      # try to continue on failure rather than skipping.
      printDebug("$scriptname: $org: $reportid: Replacing data.");
      $dbh->do(qq{DELETE from tlsrecord WHERE serial=?}, undef, $sid);
      if ($dbh->errstr) {
        warn "$scriptname: $org: $reportid: Cannot remove report data from database. Try to continue.\n";
      }
      $dbh->do(qq{DELETE from tls WHERE serial=?}, undef, $sid);
      if ($dbh->errstr) {
        warn "$scriptname: $org: $reportid: Cannot remove report from database. Try to continue.\n";
      }
    } 
    else {
      printDebug("$scriptname: $org: $reportid: Already have report, skipped");
      # Do not store in DB, but return true, so the message can
      # be moved out of the way, if configured to do so.
      return 1;
    }
  }

  my $sql = qq{INSERT INTO tls (mindate,maxdate,domain,org,reportid,email,policy_mode,summary_success,summary_failure,raw_json) 
                        VALUES (?,?,?,?,?,?,?,?,?,?)};
  my $storejson = encode_json $json;
  if ($compress_json) {
    my $gzipdata;
    if(!gzip(\$storejson => \$gzipdata)) {
      warn "$scriptname: $org: $reportid: Cannot add gzip JSON to database ($GzipError). Skipped.\n";
      rollback($dbh);
      return 0;
      $storejson = "";
    } 
    else {
      $storejson = encode_base64($gzipdata, "");
    }
  }
  
  if (length($storejson) > $maxsize_json) {
    warn "$scriptname: $org: $reportid: Skipping storage of large JSON (".length($storejson)." bytes) as defined in config file.\n";
    $storejson = "";
  }
  
  $dbh->do($sql, undef, $from, $to, $domain, $org, $reportid, $email, $mode, $success, $failure, $storejson);
  if ($dbh->errstr) {
    warn "$scriptname: $org: $reportid: Cannot add report to database. Skipped.\n";
    return 0;
  }

  my $serial = $dbh->last_insert_id(undef, undef, 'tls', undef);
  printDebug("serial $serial");

  sub dorowtls($$$$) {
    my ($serial,$rec,$org,$id) = @_;
    my %r = %$rec;

    my $send_ip = $r{'sending-mta-ip'};
    my $recv_ip = $r{'receiving-ip'};
    my $type    = $r{'result-type'};
    my $recv_mx = $r{'receiving-mx-hostname'};
    my $count   = $r{'failed-session-count'};

    if ($debug) {
      # because apparently these can be undefined...
      print "\n\n--- DEBUG ---\n";
      print "  result-type:           $type\n"      if defined($type);
      print "  sending-mta-ip:        $send_ip\n"   if defined($send_ip);
      print "  receiving-ip:          $recv_ip\n"   if defined($recv_ip);
      print "  receiving-mx-hostname: $recv_mx\n"   if defined($recv_mx);
      print "  failed-session-count:  $count\n"     if defined($count);
      print "-------------\n\n";
    }

    my ($send_ip_type, $recv_ip_type) = "ip";

    # this subroutine reduces these lines from existing multiple times for both sender and reciever
    sub iptype($) {
      my $ip = shift;
      my ($iptype, $ipval, $nip);

      if ($nip = inet_pton(AF_INET, $ip)) {
        $ipval = unpack "N", $nip;
        $iptype = "ip";
      }
      elsif($nip = inet_pton(AF_INET6, $ip)) {
        $ipval = $dbx{to_hex_string}{$nip};
        $iptype = "ip6";
      }
      else {
        warn "$scriptname: ??? mystery ip $ip\n";
        return 0,0;
      }
      return $ipval,$iptype;
    }

    ($send_ip,$send_ip_type) = iptype($send_ip) if defined($send_ip);
    ($recv_ip,$recv_ip_type) = iptype($recv_ip) if defined($recv_ip);

    printDebug("sending ip type:   $send_ip_type\n".
             "  recieving ip type: $recv_ip_type");

    my $sql = qq{INSERT INTO tlsrecord (serial,send_$send_ip_type,recv_$recv_ip_type,recv_mx,type,count) 
                                VALUES (?,?,?,?,?,?)};
    $dbh->do($sql,undef,$serial,$send_ip,$recv_ip,$recv_mx,$type,$count);
    if ($dbh->errstr) {
      warn "$scriptname: $org: $id: Cannot add failure report data to database. Skipped.\n";
      rollback($dbh);
      return 0;
    }
    return 1;
  }

  my $res = 1;

  # if there are no records, don't bother
  if($failure == 0) {
    # do nothing
  }
  elsif(ref $record eq "HASH") {
    printDebug("single record");
    $res = -1 if !dorowtls($serial,$record,$org,$reportid);
  } 
  elsif(ref $record eq "ARRAY") {
    printDebug("multi record");
    foreach my $row (@$record) {
      $res = -1 if !dorowtls($serial,$row,$org,$reportid);
    }
  } 
  else {
    warn "$scriptname: $org: $reportid: mystery type " . ref($record) . "\n";
  }

  if ($res <= 0) {
    printDebug("Result $res JSON: ". encode_json $json);
  }

  if ($res <= 0) {
    if ($db_tx_support) {
      warn "$scriptname: $org: $reportid: Cannot add records to tlsrecord. Rolling back DB transaction.\n";
      rollback($dbh);
    } 
    else {
      warn "$scriptname: $org: $reportid: errors while adding to tlsrecord, serial $serial records likely obsolete.\n";
    }
  } 
  else {
    if ($db_tx_support) {
      $dbh->commit;
      if ($dbh->errstr) {
        warn "$scriptname: $org: $reportid: Cannot commit transaction.\n";
      }
    }
  }
  return $res;
}

# -----------------------------------------------------------------------------

# Tries to roll back the transaction (if enabled).
# If an error happens, warn the user, but continue execution
sub rollback {
  my $dbh = $_[0];

  if ($db_tx_support) {
    $dbh->rollback;
    if ($dbh->errstr) {
      warn "$scriptname: Cannot rollback transaction.\n";
    }
  }
}

# -----------------------------------------------------------------------------

# Check, if the database contains needed tables and columns. The idea is, that
# the user only has to create the database/database_user. All needed tables and
# columns are created automatically. Furthermore, if new columns are introduced,
# the user does not need to make any changes to the database himself.
sub checkDatabase {
  my $dbh = $_[0];

  my $tables = $dbx{tables};

  # Create missing tables and missing columns.
  for my $table ( keys %{$tables} ) {

    if (!db_tbl_exists($dbh, $table)) {

      # Table does not exist, build CREATE TABLE cmd from tables hash.
      printInfo("$scriptname: Adding missing table <" . $table . "> to the database.");
      my $sql_create_table = "CREATE TABLE " . $table . " (\n";
      for (my $i=0; $i <= $#{$tables->{$table}{"column_definitions"}}; $i+=3) {
        my $col_name = $tables->{$table}{"column_definitions"}[$i];
        my $col_type = $tables->{$table}{"column_definitions"}[$i+1];
        my $col_opts = $tables->{$table}{"column_definitions"}[$i+2];
        # add comma if second or later entry
        if ($i != 0) {
          $sql_create_table .= ",\n";
        }
        $sql_create_table .= "$col_name $col_type $col_opts";
      }
      # Add additional_definitions, if defined.
      if ($tables->{$table}{"additional_definitions"} ne "") {
        $sql_create_table .= ",\n" . $tables->{$table}{"additional_definitions"};
      }
      # Add options.
      $sql_create_table .= ") " . $tables->{$table}{"table_options"} . ";";
      # Create table.
      printDebug("$sql_create_table");
      $dbh->do($sql_create_table);

      # Create indexes.
      foreach my $sql_idx (@{$tables->{$table}{indexes}}) {
        printDebug("$sql_idx\n");
        $dbh->do($sql_idx);
      }
    } 
    else {

      #Table exists, get  current columns in this table from DB.
      my %db_col_exists = db_column_info($dbh, $table);

      # Check if all needed columns are present, if not add them at the desired position.
      my $insert_pos;
      for (my $i=0; $i <= $#{$tables->{$table}{"column_definitions"}}; $i+=3) {
        my $col_name = $tables->{$table}{"column_definitions"}[$i];
        my $col_type = $tables->{$table}{"column_definitions"}[$i+1];
        my $col_opts = $tables->{$table}{"column_definitions"}[$i+2];

        if (!$db_col_exists{$col_name}) {
          # add column
          my $sql_add_column = $dbx{add_column}($table, $col_name, $col_type, $col_opts, $insert_pos);
          printDebug("$sql_add_column\n");
          $dbh->do($sql_add_column);
        } 
        elsif ($db_col_exists{$col_name} !~ /^\Q$col_type\E/) {
          # modify column
          my $sql_modify_column = $dbx{modify_column}($table, $col_name, $col_type, $col_opts);
          printDebug("$sql_modify_column\n");
          $dbh->do($sql_modify_column);
        }
        $insert_pos = $col_name;
      }
    }
  }

  $dbh->commit;
}

# -----------------------------------------------------------------------------

# Checks if the table exists in the database
sub db_tbl_exists {
  my ($dbh, $table) = @_;

  my @res = $dbh->tables(undef, undef, $table, undef);
  return scalar @res > 0;
}

# -----------------------------------------------------------------------------

# Gets columns and their data types in a given table
sub db_column_info {
  my ($dbh, $table) = @_;

  my $db_info = $dbh->column_info(undef, undef, $table, undef)->fetchall_hashref('COLUMN_NAME');
  my %columns;
  foreach my $column (keys(%$db_info)) {
    $columns{$column} = $db_info->{$column}{$dbx{column_info_type_col}};
  }
  return %columns;
}