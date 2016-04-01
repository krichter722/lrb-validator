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
# Author    :   Nga Tran
# Date      :       Aug 24, 2004
# Purposes  :
#   . Create alerts temporary tables
# Modified history :
#      Name        Date           Comment
#      -------     -----------    ---------------------------------
####################################################################


use strict;
use DBI qw(:sql_types);
use FileHandle;
use Log::Log4perl qw(:easy);

Log::Log4perl->easy_init($DEBUG);
my $logger = Log::Log4perl->get_logger('lrb_validator.createalerttmptables');

#BEGIN {
#    open (STDERR, ">>execution.log");
#}

# Process arguments
my @arguments = @ARGV;
my $dbname = shift(@arguments);
my $dbhost = shift(@arguments);
my $dbuser = shift(@arguments);
my $dbpassword = shift(@arguments);
my $logFile = shift(@arguments);
my $logVar = shift(@arguments);

$logger->info("createAlertTmpTables in progress ...");

# Connect to test Postgres database
my $dbh  = DBI->connect(
            "DBI:Pg:dbname=$dbname;host=$dbhost", "$dbuser", "$dbpassword",
            {PrintError => 1}
          ) || $logger->logdie("Could not connect to database:  $DBI::errstr");

eval
{
   my $startTime = time;

   createTollAccAlertsTmpTable($dbh);

   my $runningTime = time - $startTime;
   $logger->info("Total createAlertTmpTables running time: $runningTime seconds");
};
print $@;

$dbh->disconnect;

exit(0);

#----------------------------------------------------------------


sub createTollAccAlertsTmpTable
{
   my ($dbh) = @_;

   $dbh->do("DROP TABLE tollAccAlertsTmp;");

#   $dbh->do("CREATE TABLE TollAccAlertsTmp(
#             time   INTEGER,
#             carid  INTEGER,
#                 xway   INTEGER,
#             dir    INTEGER,
#             seg    INTEGER,
#             lav    INTEGER,
#             toll   INTEGER,
#             accidentSeg INTEGER);");

   $dbh->do("CREATE TABLE TollAccAlertsTmp(
              time   INTEGER,
              carid  INTEGER,
              dir    INTEGER,
              seg    INTEGER,
              lav    INTEGER,
              toll   INTEGER,
              accidentSeg INTEGER);");
}
