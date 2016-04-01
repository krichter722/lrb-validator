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
# Author	:	Nga Tran
# Date  	:	Aug, 2004
# Purposes	:
#	. Produce toll & accident alerts
# Modified history :
#      Name        Date           Comment
#      -------     -----------    ---------------------------------
#      Nga         8/24/04        Generate for only 1 expresss way
####################################################################

use strict;
use DBI qw(:sql_types);
use FileHandle;
use Log::Log4perl qw(:easy);

Log::Log4perl->easy_init($DEBUG);
my $logger = Log::Log4perl->get_logger('lrb_validator.import');

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

$logger->info("createAlerts in progress ...\n");

# Constants
my $EXIT_LANE = 4;

# Connect to Postgres database
my $dbh  = DBI->connect(
            "DBI:Pg:dbname=$dbname;host=$dbhost", "$dbuser", "$dbpassword",
            {PrintError => 1}
          ) || $logger->logdie("Could not connect to database:  $DBI::errstr");

eval
{

   my $startTime = time;

   truncateTables($dbh);
   createAlerts($dbh);
   updateLavs($dbh);
   updateTolls($dbh);
   updateAccidentSegments($dbh);

   my $runningTime = time - $startTime;
   $logger->info("Total createAlerts running time: $runningTime seconds\n\n");
};
print $@;

$dbh->disconnect;

exit(0);

#-------------------------------------------------------------------------------

sub  truncateTables
{
   my ($dbh) = @_;

   # Delete all data in ouput table
   my $sql = "TRUNCATE TABLE tollAccAlerts;";
   my $statment = $dbh->prepare($sql);
   $statment->execute;
   $dbh->commit;

   $logger->info("     truncate done\n");
}

#-------------------------------------------------------------------------------

sub  createAlerts
{
   my ($dbh) = @_;

   my $startTime = time;

#   my $sql = "INSERT INTO tollAccAlerts(time, carid, xway, dir, seg)
#           SELECT  min(time),
#                   carid,
#                   xway,
#                   dir,
#                   seg
#           FROM    input
#           WHERE   type = 0 AND
#                   lane <> $EXIT_LANE
#           GROUP BY xway, dir, seg, carid;";

   my $sql = "INSERT INTO tollAccAlerts(time, carid, dir, seg)
           SELECT  min(time),
                   carid,
                   dir,
                   seg
           FROM    input
           WHERE   type = 0 AND
                   lane <> $EXIT_LANE
           GROUP BY dir, seg, carid;";


   my $statment = $dbh->prepare($sql);
   $statment->execute;
   $dbh->commit;

   my $runningTime = time - $startTime;
   $logger->info("     createAlerts running time: $runningTime\n");
}

#-------------------------------------------------------------------------------

sub  updateLavs
{
   my ($dbh) = @_;

   my $startTime = time;

#   my $sql = "UPDATE tollAccAlerts
#           SET    lav = (SELECT statistics.lav
#                         FROM   statistics
#                         WHERE  statistics.xway = tollAccAlerts.xway AND
#                                statistics.dir = tollAccAlerts.dir AND
#                                statistics.seg = tollAccAlerts.seg AND
#                                statistics.minute = trunc(tollAccAlerts.time/60) + 1
#                         );";

   my $sql = "UPDATE tollAccAlerts
           SET    lav = (SELECT statistics.lav
                         FROM   statistics
                         WHERE  statistics.dir = tollAccAlerts.dir AND
                                statistics.seg = tollAccAlerts.seg AND
                                statistics.minute = trunc(tollAccAlerts.time/60) + 1
                         );";

   my $statment = $dbh->prepare($sql);
   $statment->execute;
   $dbh->commit;

   my $runningTime = time - $startTime;
   $logger->info("     updateLavs running time: $runningTime\n");

}

#-------------------------------------------------------------------------------

sub  updateTolls
{
   my ($dbh) = @_;

   my $startTime = time;

#   my $sql = "UPDATE tollAccAlerts
#           SET    toll = (SELECT statistics.toll
#                         FROM   statistics
#                         WHERE  statistics.xway = tollAccAlerts.xway AND
#                                statistics.dir = tollAccAlerts.dir AND
#                                statistics.seg = tollAccAlerts.seg AND
#                                statistics.minute = trunc(tollAccAlerts.time/60) + 1
#                         );";

   my $sql = "UPDATE tollAccAlerts
           SET    toll = (SELECT statistics.toll
                         FROM   statistics
                         WHERE  statistics.dir = tollAccAlerts.dir AND
                                statistics.seg = tollAccAlerts.seg AND
                                statistics.minute = trunc(tollAccAlerts.time/60) + 1
                         );";

   my $statment = $dbh->prepare($sql);
   $statment->execute;
   $dbh->commit;

   my $runningTime = time - $startTime;
   $logger->info("     updateTolls running time: $runningTime\n");
}

#-------------------------------------------------------------------------------

sub  updateAccidentSegments
{
   my ($dbh) = @_;

   my $startTime = time;

#   my $sql = "UPDATE tollAccAlerts
#           SET    accidentSeg = (SELECT statistics.accidentSeg
#                         FROM   statistics
#                         WHERE  statistics.xway = tollAccAlerts.xway AND
#                                statistics.dir = tollAccAlerts.dir AND
#                                statistics.seg = tollAccAlerts.seg AND
#                                statistics.minute = trunc(tollAccAlerts.time/60) + 1
#                         );";

   my $sql = "UPDATE tollAccAlerts
           SET    accidentSeg = (SELECT statistics.accidentSeg
                         FROM   statistics
                         WHERE  statistics.dir = tollAccAlerts.dir AND
                                statistics.seg = tollAccAlerts.seg AND
                                statistics.minute = trunc(tollAccAlerts.time/60) + 1
                         );";


   my $statment = $dbh->prepare($sql);
   $statment->execute;
   $dbh->commit;

   my $runningTime = time - $startTime;
   $logger->info("     updateAccidentSegments running time: $runningTime\n");
}
