# -----------------------------------------------------------------------------
#
# Open Report Parser - Open Source report parser
# Copyright (C) 2023 John Bradley (userjack6880)
# Copyright (C) 2016 TechSneeze.com
# Copyright (C) 2012 John Bieling
#
# oauth.pl
#   oauth2 module
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

package OAuth;
use LWP::UserAgent;
use LWP::Protocol::https;
use JSON;
use Encode;
use Time::Piece;

our @EXPORT = qw( get_oauth );

sub get_oauth {
  my ($oauthuri, $oauthclientid, $dbh, $db_tx_support, $clear_token) = (@_);

  # clear token if requested
  if ($clear_token) {
    my $sql = qq{DELETE FROM oauth};
    $dbh->do($sql, undef);
    if ($dbh->errstr) {
      warn "$scriptname: $org: $id: Cannot invalidate OAuth tokens.\n";
      exit;
    }
    else {
      if ($db_tx_support) {
        $dbh->commit;
        if ($dbh->errstr) {
          warn "$scriptname: $org: $id: Cannot commit transaction.\n";
        }
      }
    }
  }

  # check if valid oauth token exists
  my $sth = $dbh->prepare(qq{SELECT access_token, refresh_token, UNIX_TIMESTAMP(expire) AS expire, valid FROM oauth WHERE valid=1});
  $sth->execute();
  my $result = $sth->fetchrow_hashref();

  # first check if there's a result and it's valid
  if ($result && $result->{valid} == 1) {
    # check to see if it's expired
    if ($result->{expire} > time()) {
      return $result->{access_token};
    }
    # if token is expired, refresh
    else {
      print "expired token, refreshing\n";
      # setup the useragent
      my $ua = LWP::UserAgent->new(
        protocols_allowed => ['https'],
      );

      # send the device authorization request
      my $request = HTTP::Request->new(
        'POST',
        $oauthuri."token",
        [
          'Content-Type'   => 'application/x-www-form-urlencoded'
        ]
      );
      my $refresh_token = $result->{refresh_token};
      $request->content(encode("UTF-8","client_id=$oauthclientid&refresh_token=$refresh_token&grant_type=refresh_token"));

      my $response = $ua->request($request);
      my $respJSON = $response->decoded_content();
      my $respData = decode_json($respJSON);

      # if there's an error, print it out
      if (exists($respData->{error})) {
        print "Oauth2 Error: ".$respData->{error}."\n".
              $respData->{error_description}."\n";
        exit;
      }

      # mark all other oauth tokens invalid
      my $sql = qq{UPDATE oauth SET valid = 0 WHERE valid = 1};
      $dbh->do($sql, undef);
      if ($dbh->errstr) {
        warn "$scriptname: $org: $id: Cannot invalidate OAuth tokens.\n";
        exit;
      }
      else {
        if ($db_tx_support) {
          $dbh->commit;
          if ($dbh->errstr) {
            warn "$scriptname: $org: $id: Cannot commit transaction.\n";
          }
        }
      }

      # throw the info back into the DB
      my $access_token  = $respData->{access_token};
      $refresh_token    = $respData->{refresh_token};
      my $expires_in    = $respData->{expires_in};

      my $sql = qq{INSERT INTO oauth (access_token, refresh_token, expire, valid)
                  VALUES (?,?,FROM_UNIXTIME(?),1)};
      $token_expire = time()+$expires_in;
      $dbh->do($sql, undef, $access_token, $refresh_token, $token_expire);
      if ($dbh->errstr) {
        warn "$scriptname: $org: $id: Cannot add OAuth to database.\n";
        exit;
      }
      else {
        if ($db_tx_support) {
          $dbh->commit;
          if ($dbh->errstr) {
            warn "$scriptname: $org: $id: Cannot commit transaction.\n";
          }
        }
      }

      # now we can return the current access token
      return $access_token;
    }
  }

  # if there is no refresh or valid token does not exist, get a new token
  else {
    print "no token found, requesting\n";
    # setup the useragent
    my $ua = LWP::UserAgent->new(
      protocols_allowed => ['https'],
    );

    my $scope = "openid%20email";
    
    # if it's m365, we need to request offline_access too
    if ($oauthuri =~ m/microsoft/) {
      $scope .= "%20offline_access%20https%3A%2F%2Foutlook.office.com%2F.default";
    }

    # send the device authorization request
    my $request = HTTP::Request->new(
      'POST',
      $oauthuri."devicecode",
      [
        'Content-Type'   => 'application/x-www-form-urlencoded'
      ]
    );
    $request->content(encode("UTF-8","client_id=$oauthclientid&scope=$scope"));

    my $response = $ua->request($request);
    my $respJSON = $response->decoded_content();
    my $respData = decode_json($respJSON);

    # if there's an error, print it out
    if (exists($respData->{error})) {
      print "Oauth2 Error: ".$respData->{error}."\n".
            $respData->{error_description}."\n".
            $respData->{error_uri}."\n";
      exit;
    }

    # now we present a URI to the user and wait for auth
    my $device_code       = $respData->{device_code};
    my $user_code         = $respData->{user_code};
    my $verification_uri  = $respData->{verification_uri};
    my $expires_in        = $respData->{expires_in};
    my $interval          = $respData->{interval};
    my $message           = $respData->{message};

    print "$message\n\n".
          "URL: $verification_uri\n".
          "Expires in $expires_in seconds.\n".
          "User Code: $user_code\n";
    
    # count elapsed, we want to force a timeout regardless if we get a response
    my $elapsed = 0;
    while ($elapsed < $expires_in) {
      $request->uri($oauthuri."token");
      $request->content(encode("UTF-8","grant_type=urn:ietf:params:oauth:grant-type:device_code&client_id=$oauthclientid&device_code=$device_code"));
    
      $response = $ua->request($request);
      $respJSON = $response->decoded_content();
      $respData = decode_json($respJSON);

      if(exists($respData->{error})) {
        if ($respData->{error} eq 'authorization_pending') {
          sleep $interval;
          $elapsed += $interval-2; # this should give us a bit of headroom;
          next;
        }
        elsif ($respData->{error} eq 'authorization_declined') {
          print "Authorization declined. Exiting.\n";
          exit;
        }
        elsif ($respData->{error} eq 'bad_verification_code') {
          print "Bad verification code. Device code sent: $device_code. Exiting.\n";
          exit;
        }
        elsif ($respData->{error} eq 'expired_token') {
          print "Authorization took too long. Token expired. Exiting.\n";
          exit;
        }
        else {
          print "Other Error: ".$respData->{error}." - Exiting.\n".
                "Description: ".$respData->{error_description}."\n".
                "Error URI:   ".$respData->{error_uri}."\n";
          exit;
        }
      }

      # if it's a token... we can break out of the while loop
      if(exists($respData->{token_type})) {
        last;
      }
    }
    # if we get out of the loop and it didn't error, then we should have a token now
    # and we can save it to the database
    my $access_token   = $respData->{access_token};
    my $refresh_token  = $respData->{refresh_token};
    my $expires_in     = $respData->{expires_in};

    my $sql = qq{INSERT INTO oauth (access_token, refresh_token, expire, valid)
                VALUES (?,?,FROM_UNIXTIME(?),1)};
    my $token_expire = time()+$expires_in;
    $dbh->do($sql, undef, $access_token, $refresh_token, $token_expire);
    if ($dbh->errstr) {
      warn "$scriptname: $org: $id: Cannot add OAuth to database.\n";
      exit;
    }
    else {
      if ($db_tx_support) {
        $dbh->commit;
        if ($dbh->errstr) {
          warn "$scriptname: $org: $id: Cannot commit transaction.\n";
        }
      }
    }

    return $access_token;
  }
}

1;