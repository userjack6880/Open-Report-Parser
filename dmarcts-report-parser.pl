#!/usr/bin/perl

################################################################################
# dmarcts-report-parser - A Perl based tool to parse DMARC reports from an IMAP
# mailbox or from the filesystem, and insert the information into a database.
# ( Formerly known as imap-dmarcts )
#
# Copyright (C) 2016 TechSneeze.com and John Bieling
#
# Available at:
# https://github.com/techsneeze/dmarcts-report-parser
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of  MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################

################################################################################
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
################################################################################

################################################################################
# Usage:
#    ./dmarcts-report-parser.pl [OPTIONS] [PATH]
#
# If PATH is not provided, reports are read from an IMAP server, otherwise they
# are read from PATH from local filesystem. PATH can be a filename of a single
# mime message file or multiple mime message files - wildcard expression are
# allowed.
#
# To run, this script needs custom configurations: a database server and
# credentials and (if used) an IMAP server and credentials. These values can be
# set inside the script or by providing them via <dmarcts-report-parser.conf> in
# the current working directory.
#
# The following options are always allowed:
#        -d : Print debug info.
#        -r : Replace existing reports rather than failing.
#  --delete : Delete processed message files (the XML is stored in the
#             database for later reference).
#
# If a PATH is given, the following option is also allowed:
#        -x : Files specified by PATH are XML report files, rather than
#             mime messages containing the XML report files.
################################################################################



# Always be safe
use strict;
use warnings;

# Use these modules
use Getopt::Long;
use Data::Dumper;
use Mail::IMAPClient;
use MIME::Words qw(decode_mimewords);
use MIME::Parser;
use MIME::Parser::Filer;
use XML::Simple;
use DBI;
use Socket;
use Socket6;
use PerlIO::gzip;

# Define all possible configuration options.
our ($debug, $delete_reports, $dbname, $dbuser, $dbpass, $dbhost,
	$imapserver, $imapuser, $imappass, $imapssl, $imaptls,
	$imapmovefolder, $imapreadfolder);



################################################################################
### main #######################################################################
################################################################################

# Load script configuration options from local config file. The file is expected
# to be in the current working directory.
do "dmarcts-report-parser.conf";
if ( ! -e "dmarcts-report-parser.conf" )
	{die "Could not read config file 'dmarcts-report-parser.conf' from current working directory."};

# Get command line options.
my %options = ();
GetOptions( \%options, 'd', 'r', 'x', 'delete' );

# Set default behaviour.
use constant { TS_IMAP => 0, TS_MESSAGE_FILE => 1, TS_XML_FILE => 2 };
our $reports_source = TS_IMAP;
our $reports_replace = 0;

# Check for further command line arguments (interpreted as PATH)
if ($ARGV[0]) {
	$reports_source = TS_MESSAGE_FILE;
}

# Evaluate command line options
if (exists $options{r}) {$reports_replace = 1;}
if (exists $options{x}) {
	if ($reports_source == TS_IMAP) {
		print "The -x OPTION requires a PATH.\n";
		exit;
	} else {
		$reports_source = TS_XML_FILE;
	}
}
# Override config options by command line options.
if (exists $options{d}) {$debug = 1;}
if (exists $options{delete}) {$delete_reports = 1;}


# Setup connection to database server.
my $dbh = DBI->connect("DBI:mysql:database=$dbname;host=$dbhost",
	$dbuser, $dbpass)
or die "Cannot connect to database\n";
checkDatabase($dbh);


# Process messages based on $reports_source.
if ($reports_source == TS_IMAP) {

	# Setup connection to IMAP server.
	my $imap = Mail::IMAPClient->new( Server => $imapserver,
		Ssl => $imapssl,
		Starttls => $imaptls,
		User => $imapuser,
		Password => $imappass)
	# module uses eval, so we use $@ instead of $!
	or die "IMAP Failure: $@";

	# Set $imap to UID mode, which will force imap functions to use/return
	# UIDs, instead of message sequence numbers. UIDs are not allowed to
	# change during a session and are not allowed to be used twice. Looping
	# over message sequence numbers and deleting a msg in between could have
	# unwanted side effects.
	$imap->Uid(1);

	if ($debug == 1) {
		# How many msgs are we going to process?
		print "There are ". $imap->message_count($imapreadfolder).
			" messages in the $imapreadfolder folder.\n";
	}

	# Only select and search $imapreadfolder, if we actually
	# have something to do.
	if ($imap->message_count($imapreadfolder)) {
		# Select the mailbox to get messages from.
		$imap->select($imapreadfolder)
			or die "IMAP Select Error: $!";

		# Store each message as an array element.
		my @msgs = $imap->search('ALL')
			or die "Couldn't get all messages\n";

		# Loop through IMAP messages.
		foreach my $msg (@msgs) {
			if ($debug == 1) {
				print "--------------------------------\n";
				print "The Current Message UID is: ";
				print $msg. "\n";
				print "--------------------------------\n";
			}

			my $xml = getXMLFromMessage($imap->message_string($msg),"IMAP message with UID #".$msg);
			# If !$xml, the mail is probably not a DMARC report, so
			# do not storeXMLInDatabase.
			if ($xml) {
				# If storeXMLInDatabase returns false, there was some sort
				# of database storage failure and we MUST stop the file
				# procession, because it is not pushed into the database.
				# The user must investigate this issue.
				if (!storeXMLInDatabase($xml)) {
					next;
				}
			}


			# Delete processed message files, if the --delete option
			# is given. Otherwise move msgs if $imapmovefolder is set.
			if ($delete_reports) {
				if ($debug == 1) {
					print "Deleting processed IMAP message file.\n";
				}
				if (!$xml) {
					# A mail which does not look like a DMARC report
					# has been processed and should now be deleted.
					# Print its content so it gets send as cron
					# message, so the user can still investigate.
					print $imap->message_string($msg)."\n"
				}
				$imap->delete_message($msg)
				or print "Could not delete IMAP message. [$@]\n";
			} elsif ($imapmovefolder) {
				if ($debug == 1) {
					print "Moving (copy and delete) processed IMAP message file to IMAP folder: $imapmovefolder\n";
				}

				# Try to create $imapmovefolder, if it does not exist.
				if (!$imap->exists($imapmovefolder)) {
					$imap->create($imapmovefolder)
					or print "Could not create IMAP folder: $imapmovefolder.\n";
				}

				# Try to move the message to $imapmovefolder.
				my $newid = $imap->copy($imapmovefolder, [ $msg ]);
				if (!$newid) {
					print "Error on moving (copy and delete) processed IMAP message: Could not COPY message to IMAP folder: <$imapmovefolder>!\n";
					print "Messsage will not be moved/deleted. [$@]\n";
				} else {
					$imap->delete_message($msg)
					or do {
						print "Error on moving (copy and delete) processed IMAP message: Could not DELETE message\n";
						print "after copying it to <$imapmovefolder>. [$@]\n";
					}
				}
			}
		}

		# Expunge and close the folder.
		$imap->expunge($imapreadfolder);
		$imap->close($imapreadfolder);
	}

	# We're all done with IMAP here.
	$imap->logout();

} else { # TS_MESSAGE_FILE or TS_XML_FILE

	foreach my $a (@ARGV) {
		# Linux bash supports wildcard expansion BEFORE the script is
		# called, so here we only see a list of files. Other OS behave
		# different, so we should not depend on that feature: Use glob
		# on each argument to manually expand the argument, if possible.
		my @file_list = glob($a);

		if ($debug == 1) {
			# How many msgs are we going to process?
			print "There are ". @file_list. " messages to be processed.\n";
		}

		foreach my $f (@file_list) {

			if ($debug == 1) {
				print "--------------------------------\n";
				print "The Current Message is: ";
				print $f. "\n";
				print "--------------------------------\n";
			}

			my $xml;
			my $filecontent;
			if (open FILE, $f)
			{
				$filecontent = join("", <FILE>);
				close FILE;
				if ($reports_source == TS_MESSAGE_FILE) {
					# Get XML data from mime message.
					$xml = getXMLFromMessage($filecontent,$f);
				} else {
					# Get XML data from XML file directly.
					$xml = getXMLFromXMLString($filecontent);
					if (!$xml) {
						print "File <$f> does not seem to be a valid XML file. Skipped.\n";
					}
				}
			} else {
				print "Could not open file <$f>: $!. Skipped.\n";
				# Could not retrieve filecontent, so it is not 
				# possible to --delete file and send filecontent
				# as cron message. The user has to look at the
				# actual file. The skipped message must be send
				# on each cron.
				next;
			}

			# If !$xml, the file/mail is probably not a DMARC report.
			# So do not storeXMLInDatabase.
			if ($xml) {
				# If storeXMLInDatabase returns false, there was some sort
				# of database storage failure and we MUST stop the file
				# procession, because it is not pushed into the database.
				# The user must investigate this issue.
				if (!storeXMLInDatabase($xml)) {
					next;
				}
			}

			# Delete processed message files, if the --delete option
			# is given.
			if ($delete_reports) {
				if (!$xml) {
					# A mail which does not look like a DMARC report
					# has been processed and should now be deleted.
					# Print its content so it gets send as cron
					# message, so the user can still investigate.
					print $filecontent."\n"
				}
				unlink($f);
			}
		}
	}
}



################################################################################
### subroutines ################################################################
################################################################################

# Walk through a mime message and return a reference to the XML data containing
# the fields of the first ZIPed XML file embedded into the message. The XML
# itself is not checked to be a valid DMARC report.
sub getXMLFromMessage {
	my $message = $_[0];
	my $messagefile = $_[1];
	
	my $parser = new MIME::Parser;
	$parser->output_dir("/tmp");
	$parser->filer->ignore_filename(1);
	my $ent = $parser->parse_data($message);

	my $body = $ent->bodyhandle;
	my $mtype = $ent->mime_type;
	my $subj = decode_mimewords($ent->get('subject'));

	if ($debug == 1) {
		print "Subject: $subj"; # Subject always contains a \n.
		print "MimeType: $mtype\n";
	}

	my $location;
	my $isgzip = 0;

	if(lc $mtype eq "application/zip") {
		if ($debug == 1) {
			print "This is a ZIP file \n";
		}

		$location = $body->path;

	} elsif (lc $mtype eq "application/gzip") {
		if ($debug == 1) {
			print "This is a GZIP file \n";
		}

		$location = $body->path;
		$isgzip = 1;

	} elsif (lc $mtype eq "multipart/mixed") {
		# At the moment, nease.net messages are multi-part, so we need
		# to breakdown the attachments and find the zip.
		if ($debug == 1) {
			print "This is a multipart attachment \n";
		}
		#print Dumper($ent->parts);

		my $num_parts = $ent->parts;
		for (my $i=0; $i < $num_parts; $i++) {
			my $part = $ent->parts($i);

			# Find a zip file to work on...
			if(lc $part->mime_type eq "application/gzip") {
				$location = $ent->parts($i)->{ME_Bodyhandle}->{MB_Path};
				$isgzip = 1;
				if ($debug == 1) {
					print $location;
					print "\n";
				}
				last; # of parts
			} elsif(lc $part->mime_type eq "application/x-zip-compressed"
				or $part->mime_type eq "application/zip"
				or lc $part->mime_type eq "application/octet-stream") {
			
				$location = $ent->parts($i)->{ME_Bodyhandle}->{MB_Path};

				if ($debug == 1) {
					print $location;
					print "\n";
				}
			} else {
				# Skip the attachment otherwise.
				if($debug == 1) {
					print "Skipped an unknown attachment \n";
				}
				next; # of parts
			}
		}
	} else {
		## Clean up dangling mime parts in /tmp of messages without ZIP.
		my $num_parts = $ent->parts;
		for (my $i=0; $i < $num_parts; $i++) {
			if($debug == 1) {	
				print $ent->parts($i)->{ME_Bodyhandle}->{MB_Path};
				print "\n";
			}
			$ent->parts($i)->{ME_Bodyhandle}->purge;

		}
	}


	# If a ZIP has been found, extract XML and parse it.
	my $xml;
	if(defined($location)) {
		if ($debug == 1) {
			print "body is in " . $location . "\n";
		}

		# Open the zip file and process the XML contained inside.
		my $unzip = "";
		if($isgzip) {
			open(XML, "<:gzip", $location)
			or $unzip = "ungzip";
		} else {
			open(XML,"unzip -p " . $location . " |")
			or $unzip = "unzip"; # Will never happen.

			# Sadly unzip -p never failes, but we can check if the
			# filehandle points to an empty file and pretend it did
			# not open/failed.
			if (eof XML) {
				$unzip = "unzip";
				close XML;
			}
		}

		# Read XML if possible (if open)
		if ($unzip eq "") {
			$xml = getXMLFromXMLString(join("", <XML>));
			if (!$xml) {
				print "The XML found in ZIP file (temp. location: <$location>) extracted from <$messagefile> does not seem to be valid XML. Skipped.\n";
			}
			close XML;
		} else {
			print "Failed to $unzip ZIP file (temp. location: <$location>) extracted from <$messagefile>. Skipped.\n";
		}
	} else {
		print "Could not find an embedded ZIP in <$messagefile>. Skipped.\n";
	}

	if($body) {$body->purge;}
	if($ent) {$ent->purge;}
	return $xml;
}


################################################################################

sub getXMLFromXMLString {
	my $raw_xml = $_[0];

	eval {
		my $xs = XML::Simple->new();
		my $ref = $xs->XMLin($raw_xml, SuppressEmpty => '');
		$ref->{'raw_xml'} = $raw_xml;

		return $ref;
	} or do {
		return undef;
	}
}


################################################################################

# Extract fields from the XML report data hash and store them into the database.
sub storeXMLInDatabase {
	my $xml = $_[0]; # $xml is a reference to the xml data

	my $from = $xml->{'report_metadata'}->{'date_range'}->{'begin'};
	my $to = $xml->{'report_metadata'}->{'date_range'}->{'end'};
	my $org = $xml->{'report_metadata'}->{'org_name'};
	my $id = $xml->{'report_metadata'}->{'report_id'};
	my $email = $xml->{'report_metadata'}->{'email'};
	my $extra = $xml->{'report_metadata'}->{'extra_contact_info'};
	my $domain =  $xml->{'policy_published'}->{'domain'};
	my $policy_adkim = $xml->{'policy_published'}->{'adkim'};
	my $policy_aspf = $xml->{'policy_published'}->{'aspf'};
	my $policy_p = $xml->{'policy_published'}->{'p'};
	my $policy_sp = $xml->{'policy_published'}->{'sp'};
	my $policy_pct = $xml->{'policy_published'}->{'pct'};

	# see if already stored
	my $sth = $dbh->prepare(qq{SELECT org, serial FROM report WHERE reportid=?});
	$sth->execute($id);
	while ( my ($xorg,$sid) = $sth->fetchrow_array() )
	{
		if ($reports_replace) {
			# $sid is the serial of a report with reportid=$id
			# Remove this $sid from rptrecord and report table, but
			# try to continue on failure rather than skipping.
			print "Replacing $xorg $id.\n";
			$dbh->do(qq{DELETE from rptrecord WHERE serial=?}, undef, $sid);
			if ($dbh->errstr) {
				print "Cannot remove report data from database (". $dbh->errstr ."). Try to continue.\n";
			}
			$dbh->do(qq{DELETE from report WHERE serial=?}, undef, $sid);
			if ($dbh->errstr) {
				print "Cannot remove report from database (". $dbh->errstr ."). Try to continue.\n";
			}
		} else {
			print "Already have $xorg $id, skipped\n";
			# Do not store in DB, but return true, so the message can
			# be moved out of the way, if configured to do so.
			return 1;
		}
	}

	my $sql = qq{INSERT INTO report(serial,mindate,maxdate,domain,org,reportid,email,extra_contact_info,policy_adkim, policy_aspf, policy_p, policy_sp, policy_pct, raw_xml)
			VALUES(NULL,FROM_UNIXTIME(?),FROM_UNIXTIME(?),?,?,?,?,?,?,?,?,?,?,?)};
	$dbh->do($sql, undef, $from, $to, $domain, $org, $id, $email, $extra, $policy_adkim, $policy_aspf, $policy_p, $policy_sp, $policy_pct,$xml->{'raw_xml'});
	if ($dbh->errstr) {
		print "Cannot add report to database (". $dbh->errstr ."). Skipped.\n";
		return 0;
	}

	my $serial = $dbh->{'mysql_insertid'} ||  $dbh->{'insertid'};
	if($debug == 1){
		print " serial $serial ";
	}
	my $record = $xml->{'record'};
	sub dorow($$) {
		my ($serial,$recp) = @_;
		my %r = %$recp;

		my $ip = $r{'row'}->{'source_ip'};
		#print "ip $ip\n";
		my $count = $r{'row'}->{'count'};
		my $disp = $r{'row'}->{'policy_evaluated'}->{'disposition'};
		my $dkim_align = $r{'row'}->{'policy_evaluated'}->{'dkim'};
		my $spf_align = $r{'row'}->{'policy_evaluated'}->{'spf'};
		
		my $identifier_hfrom = $r{'identifiers'}->{'header_from'};
		
		my ($dkim, $dkimresult, $spf, $spfresult, $reason);
		my $rp = $r{'auth_results'}->{'dkim'};
		if(ref $rp eq "HASH") {
			$dkim = $rp->{'domain'};
			$dkim = undef if ref $dkim eq "HASH";
			$dkimresult = $rp->{'result'};
		} else { # array
			# glom sigs together, report first result
			$dkim = join '/',map { my $d = $_->{'domain'}; ref $d eq "HASH"?"": $d } @$rp;
			$dkimresult = $rp->[0]->{'result'};
		}
		$rp = $r{'auth_results'}->{'spf'};
		if(ref $rp eq "HASH") {
			$spf = $rp->{'domain'};
			$spfresult = $rp->{'result'};
		} else { # array
			# glom domains together, report first result
			$spf = join '/',map { my $d = $_->{'domain'}; ref $d eq "HASH"? "": $d } @$rp;
			$spfresult = $rp->[0]->{'result'};
		}

		$rp = $r{'row'}->{'policy_evaluated'}->{'reason'};
		if(ref $rp eq "HASH") {
			$reason = $rp->{'type'};
		} else {
			$reason = join '/',map { $_->{'type'} } @$rp;
		}
		#print "ip=$ip, count=$count, disp=$disp, r=$reason,";
		#print "dkim=$dkim/$dkimresult, spf=$spf/$spfresult\n";

		# What type of IP address?
		my ($nip, $iptype, $ipval);
		if($debug == 1) {
			print "ip=$ip\n";
		}
		if($nip = inet_pton(AF_INET, $ip)) {
			$ipval = unpack "N", $nip;
			$iptype = "ip";
		} elsif($nip = inet_pton(AF_INET6, $ip)) {
			$ipval = "X'" . unpack("H*",$nip) . "'";
			$iptype = "ip6";
		} else {
			print "??? mystery ip $ip\n";
			next; # of dorow
		}

		$dbh->do(qq{INSERT INTO rptrecord(serial,$iptype,rcount,disposition,spf_align,dkim_align,reason,dkimdomain,dkimresult,spfdomain,spfresult,identifier_hfrom)
			VALUES(?,$ipval,?,?,?,?,?,?,?,?,?,?)},undef,$serial,$count,$disp,$spf_align,$dkim_align,$reason,$dkim,$dkimresult,$spf,$spfresult,$identifier_hfrom);
		if ($dbh->errstr) {
			print "Cannot add report data to database (". $dbh->errstr ."). Skipped.\n";
			return 0;
		}
	}

	if(ref $record eq "HASH") {
		if($debug == 1){
			print "single record\n";
		}
		dorow($serial,$record);
	} elsif(ref $record eq "ARRAY") {
		if($debug == 1){
			print "multi record\n";
		}
		foreach my $row (@$record) {
			dorow($serial,$row);
		}
	} else {
		print "mystery type " . ref($record) . "\n";
	}

	return 1;
}


################################################################################

# Check, if the database contains needed tables and columns. The idea is, that
# the user only has to create the database/database_user. All needed tables and
# columns are created automatically. Furthermore, if new columns are introduced,
# the user does not need to make any changes to the database himself.
sub checkDatabase {
	my $dbh = $_[0];

	my %tables = (
		"report" => {
			column_definitions 		=> [
				"serial"		, "int(10) unsigned NOT NULL AUTO_INCREMENT",
				"mindate"		, "timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
				"maxdate"		, "timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'",
				"domain"		, "varchar(255) NOT NULL",
				"org"			, "varchar(255) NOT NULL",
				"reportid"		, "varchar(255) NOT NULL",
				"email"			, "varchar(255) NULL",
				"extra_contact_info"	, "varchar(255) NULL",
				"policy_adkim"		, "varchar(20) NULL",
				"policy_aspf"		, "varchar(20) NULL",
				"policy_p"		, "varchar(20) NULL",
				"policy_sp"		, "varchar(20) NULL",
				"policy_pct"		, "tinyint unsigned",
				"raw_xml"		, "MEDIUMTEXT",
				],
			additional_definitions 		=> "PRIMARY KEY (serial), UNIQUE KEY domain (domain,reportid)",
			table_options			=> "",
			},
		"rptrecord" =>{
			column_definitions 		=> [
				"serial"		, "int(10) unsigned NOT NULL",
				"ip"			, "int(10) unsigned",
				"ip6"			, "binary(16)",
				"rcount"		, "int(10) unsigned NOT NULL",
				"disposition"		, "enum('none','quarantine','reject')",
				"reason"		, "varchar(255)",
				"dkimdomain"		, "varchar(255)",
				"dkimresult"		, "enum('none','pass','fail','neutral','policy','temperror','permerror')",
				"spfdomain"		, "varchar(255)",
				"spfresult"		, "enum('none','neutral','pass','fail','softfail','temperror','permerror')",
				"spf_align"		, "enum('fail', 'pass') NOT NULL",
				"dkim_align"		, "enum('fail', 'pass') NOT NULL",
				"identifier_hfrom"	, "varchar(255)",
				],
			additional_definitions 		=> "KEY serial (serial,ip), KEY serial6 (serial,ip6)",
			table_options			=> "ENGINE=MyISAM",
			},
	);

	# Get current tables in this DB.
	my %db_tbl_exists = ();
	for ( @{ $dbh->selectall_arrayref( "SHOW TABLES;") } ) {
		$db_tbl_exists{$_->[0]} = 1;
	}

	# Create missing tables and missing columns.
	for my $table ( keys %tables ) {

		if (!$db_tbl_exists{$table}) {

			# Table does not exist, build CREATE TABLE cmd from tables hash.
			print "Adding missing table <" . $table . "> to the database.\n";
			my $sql_create_table = "CREATE TABLE " . $table . " (\n";
			for (my $i=0; $i <= $#{$tables{$table}{"column_definitions"}}; $i+=2) {
				my $col_name = $tables{$table}{"column_definitions"}[$i];
				my $col_def = $tables{$table}{"column_definitions"}[$i+1];
				# add comma if second or later entry
				if ($i != 0) {
					$sql_create_table .= ",\n";
				}
				$sql_create_table .= $col_name . " " .$col_def;
			}
			# Add additional_definitions, if defined.
			if ($tables{$table}{"additional_definitions"} ne "") {
				$sql_create_table .= ",\n" . $tables{$table}{"additional_definitions"};
			}
			# Add options.
			$sql_create_table .= ") " . $tables{$table}{"table_options"} . ";";
			# Create table.
			##print $sql_create_table;
			$dbh->do($sql_create_table);
		} else {

			#Table exists, get  current columns in this table from DB.
			my %db_col_exists = ();
			for ( @{ $dbh->selectall_arrayref( "SHOW COLUMNS FROM $table;") } ) {
				$db_col_exists{$_->[0]} = 1;
			};

			# Check if all needed columns are present, if not add them at the desired position.
			my $insert_pos = "FIRST";
			for (my $i=0; $i <= $#{$tables{$table}{"column_definitions"}}; $i+=2) {
				my $col_name = $tables{$table}{"column_definitions"}[$i];
				my $col_def = $tables{$table}{"column_definitions"}[$i+1];
				if (!$db_col_exists{$col_name}) {
					# add column
					my $sql_add_column = "ALTER TABLE $table ADD $col_name $col_def $insert_pos;";
					##print $sql_add_column;
					$dbh->do($sql_add_column);
				}
				$insert_pos = "AFTER $col_name";
			}
		}
	}
}
