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

%dbx = (
  epoch_to_timestamp_fn => 'FROM_UNIXTIME',

  to_hex_string => sub {
    my ($bin) = @_;
    return "X'" . unpack("H*", $bin) . "'";
  },

  column_info_type_col => 'mysql_type_name',

  tables => {
    "report" => {
      column_definitions      => [
        "serial"              , "int"           , "unsigned NOT NULL AUTO_INCREMENT",
        "mindate"             , "timestamp"     , "NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "maxdate"             , "timestamp"     , "NULL",
        "domain"              , "varchar(255)"  , "NOT NULL",
        "org"                 , "varchar(255)"  , "NOT NULL",
        "reportid"            , "varchar(255)"  , "NOT NULL",
        "email"               , "varchar(255)"  , "NULL",
        "extra_contact_info"  , "varchar(255)"  , "NULL",
        "policy_adkim"        , "varchar(20)"   , "NULL",
        "policy_aspf"         , "varchar(20)"   , "NULL",
        "policy_p"            , "varchar(20)"   , "NULL",
        "policy_sp"           , "varchar(20)"   , "NULL",
        "policy_pct"          , "tinyint"       , "unsigned",
        "raw_xml"             , "mediumtext"    , "",
        ],
      additional_definitions  => "PRIMARY KEY (serial), UNIQUE KEY domain (domain, reportid)",
      table_options           => "ROW_FORMAT=COMPRESSED",
      indexes                 => [],
      },
    "rptrecord" => {
      column_definitions      => [
        "id"                  , "int"           , "unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY",
        "serial"              , "int"           , "unsigned NOT NULL",
        "ip"                  , "int"           , "unsigned",
        "ip6"                 , "binary(16)"    , "",
        "rcount"              , "int"           , "unsigned NOT NULL",
        "disposition"         , "enum('" . join("','", ALLOWED_DISPOSITION) . "')" , "",
        "reason"              , "varchar(255)"  , "",
        "dkimdomain"          , "varchar(255)"  , "",
        "dkimresult"          , "enum('" . join("','", ALLOWED_DKIMRESULT) . "')"  , "",
        "spfdomain"           , "varchar(255)"  , "",
        "spfresult"           , "enum('" . join("','", ALLOWED_SPFRESULT) . "')"   , "",
        "spf_align"           , "enum('" . join("','", ALLOWED_SPF_ALIGN) . "')"   , "NOT NULL",
        "dkim_align"          , "enum('" . join("','", ALLOWED_DKIM_ALIGN) . "')"  , "NOT NULL",
        "identifier_hfrom"    , "varchar(255)"  , ""
        ],
      additional_definitions  => "KEY serial (serial, ip), KEY serial6 (serial, ip6)",
      table_options           => "",
      indexes                 => [],
      },
    "tls" => {
      column_definitions      => [
        "serial"              , "int"           , "unsigned NOT NULL AUTO_INCREMENT",
        "mindate"             , "timestamp"     , "NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "maxdate"             , "timestamp"     , "NULL",
        "domain"              , "varchar(255)"  , "NOT NULL",
        "org"                 , "varchar(255)"  , "NOT NULL",
        "reportid"            , "varchar(255)"  , "NOT NULL",
        "email"               , "varchar(255)"  , "NULL",
        "policy_mode"         , "varchar(20)"   , "NULL",
        "summary_success"     , "int"           , "NULL",
        "summary_failure"     , "int"           , "NULL",
        "raw_json"            , "mediumtext"    , "",
        ],
      additional_definitions  => "PRIMARY KEY (serial), UNIQUE KEY domain(domain, reportid)",
      table_options           => "ROW_FORMAT=COMPRESSED",
      indexes                 => [],
      },
    "tlsrecord" => {
      column_definitions      => [
        "id"                  , "int"           , "unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY",
        "serial"              , "int"           , "unsigned NOT NULL",
        "send_ip"             , "int"           , "unsigned",
        "send_ip6"            , "binary(16)"    , "",
        "recv_ip"             , "int"           , "unsigned",
        "recv_ip6"            , "binary(16)"    , "",
        "recv_mx"             , "varchar(255)"  , "",
        "type"                , "varchar(255)"  , "",
        "count"               , "int"           , "unsigned NOT NULL",
        ],
      additional_definitions  => "KEY serial (serial, send_ip), KEY serial6 (serial, send_ip6)",
      table_options           => "",
      indexes                 => [],
      }
    },

  add_column => sub {
    my ($table, $col_name, $col_type, $col_opts, $after_col) = @_;

    my $insert_pos = "FIRST";
    if ($after_col) {
      $insert_pos = "AFTER $after_col";
    }
    return "ALTER TABLE $table ADD $col_name $col_type $col_opts $insert_pos;"
  },

  modify_column => sub {
    my ($table, $col_name, $col_type, $col_opts) = @_;
    return "ALTER TABLE $table MODIFY COLUMN $col_name $col_type $col_opts;"
  },
);

1;
