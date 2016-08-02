#!/usr/bin/perl -w
#
#  Copyright (C) 2004 - 2015
#  %
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# Please consider the following "Modified history" a legacy revision management
# which has been taken over by git
####################################################################
# Author   :   Igor Pendan
# Date     :   2004
# Purposes :
# Modified history :
#      Name        Date           Comment
#      -------     -----------    ---------------------------------
#      Nga         8/31/04        Pass arguments
####################################################################

use DBI;
use strict;
use FileHandle;
use Log::Log4perl qw(:easy);

Log::Log4perl->easy_init($DEBUG);
my $logger = Log::Log4perl->get_logger('lrb_validator.indexes');

# Process arguments
my @arguments = @ARGV;
my $dbname = shift(@arguments);
my $dbhost = shift(@arguments);
my $dbuser = shift(@arguments);
my $dbpassword = shift(@arguments);
my $logfile = shift(@arguments);
my $logvar = shift(@arguments);

my $dbquery;
my $sth;
my $dbh  = DBI->connect(
            "DBI:Pg:dbname=$dbname;host=$dbhost", "$dbuser", "$dbpassword",
            {PrintError => 1}
          ) || $logger->logdie("Could not connect to database:  $DBI::errstr");

## Indexes on tollalerts
$logger->info ("Adding indexes on tollalerts and accidentalerts.");

    $dbquery="CREATE INDEX tollalertstime ON tollalerts (time);";
    $sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
    $sth->execute;
    $dbquery="CREATE INDEX tollalertscarid ON tollalerts (carid);";
    $sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
    $sth->execute;
    $dbquery="CREATE INDEX tollalertstoll ON tollalerts (toll);";
    $sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
    $sth->execute;

    $dbh->do("CREATE INDEX tollIdx1 ON tollAlerts(time, carid);");
    $dbh->do("CREATE INDEX accIdx1 ON accidentAlerts(time, carid, seg);");

$logger->info("Indexing complete.");
