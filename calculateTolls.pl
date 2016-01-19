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
# Date  	:	August 4, 2004
# Purposes	:
#	. Produce toll for every (xway, seg, dir, minute)
# Modified history :
#      Name        Date           Comment
#      -------     -----------    ---------------------------------
#      Nga         8/24/04        Generate for only 1 expresss way
####################################################################


use strict;
use DBI qw(:sql_types);
use FileHandle;

#BEGIN {
#    open (STDERR, ">>execution.log");
#}

# Process arguments
my @arguments = @ARGV;
my $dbName = shift(@arguments);
my $userName = shift(@arguments);
my $password = shift(@arguments);
my $logFile = shift(@arguments);
my $logVar = shift(@arguments);

writeToLog($logFile, $logVar, "calculateTolls IN PROGRESS\n");

# Constants
my $MAX_DOWNSTREAM_SEGMENT = 4;
my $MAX_NUM_VEHICLE = 50; # 50 vehicles/segment
my $MAX_LAV = 40; # 40mph

# Connect to Postgres database
my $dbh = DBI->connect(
            "DBI:PgPP:$dbName", "$userName", "$password",
            {PrintError => 1, AutoCommit => 0}
          ) || die "Could not connect to database:  $DBI::errstr";

eval {
   my $startTime = time;

   highLavsLowVehicles($dbh);
   accidents($dbh);
   highToll($dbh);
   accidentSegments($dbh);
   notAccidents($dbh);

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "Total calculateTolls running time:  $runningTime\n\n");
};
print $@;   # Print out errors

$dbh->disconnect;

exit(0);


#------------------------------------------------------------------------
# Toll for high average speed and low number of vehicles
#------------------------------------------------------------------------

sub highLavsLowVehicles
{
   my ($dbh) = @_;

   my $startTime = time;

   my $sql =  "UPDATE statistics
               SET    toll = 0
               WHERE  lav >= $MAX_LAV OR
                      numVehicles <= $MAX_NUM_VEHICLE;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     highLavsLowVehicles running time:  $runningTime\n");
}


#------------------------------------------------------------------------
# Toll for high congestion
#------------------------------------------------------------------------

sub accidents
{
   my ($dbh) = @_;

   my $startTime = time;

#   my $sql =  "UPDATE statistics
#               SET    toll = 0,
#                      accident = 1
#               WHERE EXISTS(
#                         SELECT acc.seg
#                         FROM   accident AS acc
#                         WHERE  acc.xway = statistics.xway AND
#                                acc.dir = statistics.dir AND
#                                acc.firstMinute + 1 <= statistics.minute AND
#                                acc.lastMinute + 1 >= statistics.minute AND
#                                ( ( (acc.dir = 0) AND
#                                    (acc.seg >= statistics.seg) AND
#                                    (acc.seg <= statistics.seg + $MAX_DOWNSTREAM_SEGMENT)
#                                   ) OR
#                                   ( (acc.dir <> 0) AND
#                                     (acc.seg <= statistics.seg) AND
#                                     (acc.seg >= statistics.seg - $MAX_DOWNSTREAM_SEGMENT)
#                                   )
#                                 )
#                      );";

   my $sql =  "UPDATE statistics
               SET    toll = 0,
                      accident = 1
               WHERE EXISTS(
                         SELECT acc.seg
                         FROM   accident AS acc
                         WHERE  acc.dir = statistics.dir AND
                                acc.firstMinute + 1 <= statistics.minute AND
                                acc.lastMinute + 1 >= statistics.minute AND
                                ( ( (acc.dir = 0) AND
                                    (acc.seg >= statistics.seg) AND
                                    (acc.seg <= statistics.seg + $MAX_DOWNSTREAM_SEGMENT)
                                   ) OR
                                   ( (acc.dir <> 0) AND
                                     (acc.seg <= statistics.seg) AND
                                     (acc.seg >= statistics.seg - $MAX_DOWNSTREAM_SEGMENT)
                                   )
                                 )
                      );";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     accidents running time:  $runningTime\n");
}

#------------------------------------------------------------------------
# Toll for others
#------------------------------------------------------------------------

sub highToll
{
   my ($dbh) = @_;

   my $startTime = time;

   my $sql =  "UPDATE statistics
               SET    toll = (2 * (numVehicles - 50) * (numVehicles - 50))
               WHERE  toll IS NULL;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     highToll running time: $runningTime\n");
}

#------------------------------------------------------------------------
# accident segment
#------------------------------------------------------------------------

sub accidentSegments
{
   my ($dbh) = @_;

   my $startTime = time;

#   my $sql =  "UPDATE statistics
#               SET    accidentSeg = (
#                         SELECT acc.seg
#                         FROM   accident AS acc
#                         WHERE  acc.xway = statistics.xway AND
#                                acc.dir = statistics.dir AND
#                                acc.firstMinute + 1 <= statistics.minute AND
#                                acc.lastMinute + 1 >= statistics.minute AND
#                                ( ( (acc.dir = 0) AND
#                                    (acc.seg >= statistics.seg) AND
#                                    (acc.seg <= statistics.seg + $MAX_DOWNSTREAM_SEGMENT)
#                                   ) OR
#                                   ( (acc.dir <> 0) AND
#                                     (acc.seg <= statistics.seg) AND
#                                     (acc.seg >= statistics.seg - $MAX_DOWNSTREAM_SEGMENT)
#                                   )
#                                 )
#                      )
#                 WHERE statistics.accident = 1;";

   my $sql =  "UPDATE statistics
               SET    accidentSeg = (
                         SELECT acc.seg
                         FROM   accident AS acc
                         WHERE  acc.dir = statistics.dir AND
                                acc.firstMinute + 1 <= statistics.minute AND
                                acc.lastMinute + 1 >= statistics.minute AND
                                ( ( (acc.dir = 0) AND
                                    (acc.seg >= statistics.seg) AND
                                    (acc.seg <= statistics.seg + $MAX_DOWNSTREAM_SEGMENT)
                                   ) OR
                                   ( (acc.dir <> 0) AND
                                     (acc.seg <= statistics.seg) AND
                                     (acc.seg >= statistics.seg - $MAX_DOWNSTREAM_SEGMENT)
                                   )
                                 )
                      )
                 WHERE statistics.accident = 1;";



   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     accidentSegments running time:  $runningTime\n");
}


#------------------------------------------------------------------------
# Not accidents
#------------------------------------------------------------------------

sub notAccidents
{
   my ($dbh) = @_;

   my $startTime = time;

   my $sql =  "UPDATE statistics
               SET    accident = 0,
                      accidentSeg = -1
               WHERE  accident IS NULL;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     notAccidents running time:  $runningTime\n");
}

#--------------------------------------------------------------------------------

sub logTime {
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time);
	return ( ($mon+1)."-".$mday."-".($year+1900)." ".$hour.":".$min.":".$sec );
}


sub writeToLog {
	my ( $logfile, $logvar, $logmessage ) = @_;
	if ($logvar eq "yes") {
		open( LOGFILE1, ">>$logfile")  || die("Could not open file: $!");
		LOGFILE1->autoflush(1);
		print LOGFILE1 ( logTime()."> $logmessage"."\n");
		close (LOGFILE1);
	}
}
