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
# Date   	:	Aug, 2004
# Purposes	:
#	. Extract numbver of vehicles from input table
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

writeToLog($logFile, $logVar, "insertStatistics in progess ...\n");

# Constants
my $SIMULATION_TIME = 180; # 180 minutes

# Connect to Postgres database
my $dbh = DBI->connect(
            "DBI:PgPP:$dbName", "$userName", "$password",
            {PrintError => 1, AutoCommit => 0}
          ) || die "Could not connect to database:  $DBI::errstr";

eval {
   my $startTime = time;

   insertData($dbh);
   createIndexes($dbh);

   my $runningTime =  time - $startTime;
  writeToLog($logFile, $logVar, "Total insertStatistics running time:  $runningTime\n\n");
};
print $@;   # Print out errors
$dbh->disconnect;
exit(0);

#------------------------------------------------------------------------
# Insert data into statistics table
#------------------------------------------------------------------------

sub insertData
{
   my ($dbh) = @_;

   my $startTime = time;

   # Delete all data in the table
   $dbh->do("TRUNCATE TABLE statistics;");
   $dbh->commit;

#   my $sql =  "INSERT INTO statistics(xway, dir, seg, minute, numvehicles)
#              SELECT   xway,
#                       dir,
#                       seg,
#                       trunc(time/60) + 1,
#                      0
#              FROM     input
#              WHERE    type = 0
#              GROUP BY xway, dir, seg, trunc(time/60);";

   my $sql =  "INSERT INTO statistics(dir, seg, minute, numvehicles)
              SELECT   dir,
                       seg,
                       trunc(time/60) + 1,
                       0
              FROM     input
              WHERE    type = 0
              GROUP BY dir, seg, trunc(time/60);";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     insertData running time:  $runningTime\n");
}

#------------------------------------------------------------------------
# Create index for statistics table
#------------------------------------------------------------------------

sub createIndexes
{
   my ($dbh) = @_;

   my $startTime = time;

#   $dbh->do("CREATE UNIQUE INDEX statisticsIdx1
#             ON statistics (xway, dir, seg, minute);");

   $dbh->do("CREATE UNIQUE INDEX statisticsIdx1
             ON statistics (dir, seg, minute);");

   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     createIndexes running time:  $runningTime\n");
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

