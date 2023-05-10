# -----------------------------------------------------------------------------
#
# Open Report Parser - Open Source DMARC report parser
# Copyright (C) 2023 John Bradley (userjack6880)
# Copyright (C) 2016 TechSneeze.com
# Copyright (C) 2012 John Bieling
#
# dbx_postgres.pl
#   postgres db module
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

%dbx = (
  epoch_to_timestamp_fn => 'TO_TIMESTAMP',

  to_hex_string => sub {
    my ($bin) = @_;
    return "'\\x" . unpack("H*", $bin) . "'";
  },

  column_info_type_col => 'pg_type',

  tables => {
    "report" => {
      column_definitions      => [
        "serial"              , "bigint"                      , "GENERATED ALWAYS AS IDENTITY",
        "mindate"             , "timestamp without time zone" , "NOT NULL",
        "maxdate"             , "timestamp without time zone" , "NULL",
        "domain"              , "character varying(255)"      , "NOT NULL",
        "org"                 , "character varying(255)"      , "NOT NULL",
        "reportid"            , "character varying(255)"      , "NOT NULL",
        "email"               , "character varying(255)"      , "NULL",
        "extra_contact_info"  , "character varying(255)"      , "NULL",
        "policy_adkim"        , "character varying(20)"       , "NULL",
        "policy_aspf"         , "character varying(20)"       , "NULL",
        "policy_p"            , "character varying(20)"       , "NULL",
        "policy_sp"           , "character varying(20)"       , "NULL",
        "policy_pct"          , "smallint"                    , "",
        "raw_xml"             , "text"                        , "",
        ],
      additional_definitions  => "PRIMARY KEY (serial)",
      table_options           => "",
      indexes                 => [
        "CREATE UNIQUE INDEX report_uidx_domain ON report (domain, reportid);"
        ],
      },
    "rptrecord" => {
      column_definitions      => [
        "id"                  , "bigint"                  , "GENERATED ALWAYS AS IDENTITY",
        "serial"              , "bigint"                  , "NOT NULL",
        "ip"                  , "bigint"                  , "",
        "ip6"                 , "bytea"                   , "",
        "rcount"              , "integer"                 , "NOT NULL",
        "disposition"         , "character varying(20)"   , "",
        "reason"              , "character varying(255)"  , "",
        "dkimdomain"          , "character varying(255)"  , "",
        "dkimresult"          , "character varying(20)"   , "",
        "spfdomain"           , "character varying(255)"  , "",
        "spfresult"           , "character varying(20)"   , "",
        "spf_align"           , "character varying(20)"   , "NOT NULL",
        "dkim_align"          , "character varying(20)"   , "NOT NULL",
        "identifier_hfrom"    , "character varying(255)"  , ""
        ],
      additional_definitions  => "PRIMARY KEY (id)",
      table_options           => "",
      indexes                 => [
        "CREATE INDEX rptrecord_idx_serial ON rptrecord (serial, ip);",
        "CREATE INDEX rptrecord_idx_serial6 ON rptrecord (serial, ip6);",
        ],
      },
    "tls" => {
      column_definitions      => [
        "serial"              , "bigint"                      , "GENERATED ALWAYS AS IDENTITY",
        "mindate"             , "timestamp without time zone" , "NOT NULL",
        "maxdate"             , "timestamp without time zone" , "NULL",
        "domain"              , "character varying(255)"      , "NOT NULL",
        "org"                 , "character varying(255)"      , "NOT NULL",
        "reportid"            , "character varying(255)"      , "NOT NULL",
        "email"               , "character varying(255)"      , "NULL",
        "policy_mode"         , "character varying(20)"       , "NULL",
        "summary_success"     , "integer"                     , "NULL",
        "summary_failure"     , "integer"                     , "NULL",
        "raw_json"            , "text"                        , "",
        ],
      additional_definitions  => "PRIMARY KEY (serial)",
      table_options           => "",
      indexes                 => [
        "CREATE UNIQUE INDEX tls_uidx_domain ON tls (domain, reportid);"
        ],
      },
    "tlsrecord" => {
      column_definitions      => [
        "id"                  , "bigint"                  , "GENERATED ALWAYS AS IDENTITY",
        "serial"              , "bigint"                  , "NOT NULL",
        "send_ip"             , "bigint"                  , "",
        "send_ip6"            , "bytea"                   , "",
        "recv_ip"             , "bigint"                  , "",
        "recv_ip6"            , "bytea"                   , "",
        "recv_mx"             , "character varying(255)"  , "",
        "type"                , "character varying(255)"  , "",
        "count"               , "integer"                 , "NOT NULL",
        ],
      additional_definitions  => "PRIMARY KEY (id)",
      table_options           => "",
      indexes                 => [
        "CREATE INDEX tlsrecord_idx_serial ON tlsrecord (serial, send_ip);",
        "CREATE INDEX tlsrecord_idx_serial6 ON tlsrecord (serial, send_ip6);",
        ],
      }
    },

  add_column => sub {
    my ($table, $col_name, $col_type, $col_opts, $after_col) = @_;

    # Postgres only allows adding columns at the end, so $after_col is ignored
    return "ALTER TABLE $table ADD COLUMN $col_name $col_type $col_opts;"
  },

  modify_column => sub {
    my ($table, $col_name, $col_type, $col_opts) = @_;
    return "ALTER TABLE $table ALTER COLUMN $col_name TYPE $col_type;"
  },
);

1;