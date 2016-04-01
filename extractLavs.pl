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
# Date  	:	AUg, 2004
# Purposes	:
#	. Extract LAV from input table
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

writeToLog($logFile, $logVar, "extractLavs in progess ...\n");

# Constants
my $SIMULATOR_DURATION = 180; # 180 minutes, 3 hours

# Connect to Postgres database
my $dbh  = DBI->connect(
            "DBI:Pg:dbname=$dbname;host=$dbhost", "$dbuser", "$dbpassword",
            {PrintError => 1}
          ) || $logger->logdie("Could not connect to database:  $DBI::errstr");


eval {
   my $startTime = time;

   createTmpTables($dbh);
   preLav($dbh);
   indexPreLav($dbh);
   lav($dbh);
   noLav($dbh);
   dropTmpTables($dbh);

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "Total lavExtract running time:  $runningTime seconds\n\n");
};
print $@;   # Print out errors

$dbh->disconnect;

exit(0);


#------------------------------------------------------------------------
# Create all temporary tables
#------------------------------------------------------------------------

sub createTmpTables
{
   my ($dbh) = @_;

#   $dbh->do("
#          CREATE TABLE preLav(
#               xway integer,
#               dir  integer,
#               seg  integer,
#               minute integer,
#               lav  float);");

   $dbh->do("
          CREATE TABLE preLav(
               dir  integer,
               seg  integer,
               minute integer,
               lav  float);");

    $dbh->commit;
}

#------------------------------------------------------------------------
# Drop all temporary tables
#------------------------------------------------------------------------

sub dropTmpTables
{
   my ($dbh) = @_;
   $dbh->do("DROP TABLE preLav;");
   $dbh->commit;
}

#------------------------------------------------------------------------
# Calculate lav for (xway, dir, seg)  at every minute
#
# 53191	rows inserted into preLav table
#------------------------------------------------------------------------
sub preLav
{
   my ($dbh) = @_;

   my $startTime = time;

#   my $sql =  "INSERT INTO prelav
#               SELECT xway, dir, seg,  minute, avg(speed)
#               FROM   (SELECT   xway AS xway,
#                                dir AS dir,
#                                seg AS seg,
#                               carid as carid,
#                                trunc(time/60) + 1 AS minute,
#                                avg(speed) AS speed
#                        FROM      input
#                        WHERE     type = 0
#                        GROUP BY  xway, dir, seg, carid, minute) AS filter
#               GROUP BY xway, dir, seg,  minute;";

   my $sql =  "INSERT INTO prelav
               SELECT dir, seg,  minute, avg(speed)
               FROM   (SELECT   dir AS dir,
                                seg AS seg,
                                carid as carid,
                                trunc(time/60) + 1 AS minute,
                                avg(speed) AS speed
                        FROM      input
                        WHERE     type = 0
                        GROUP BY  dir, seg, carid, minute) AS filter
               GROUP BY dir, seg,  minute;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     preLav running time:  $runningTime\n");
}


#------------------------------------------------------------------------
# Create index for preLav Table
#------------------------------------------------------------------------

sub indexPreLav
{
   my ($dbh) = @_;

   my $startTime = time;

#   $dbh->do("CREATE UNIQUE INDEX preLavIdx1
#             ON preLav (xway, dir, seg, minute);");

   $dbh->do("CREATE UNIQUE INDEX preLavIdx1
             ON preLav (dir, seg, minute);");
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     indexPreLav running time:  $runningTime\n");
}

#------------------------------------------------------------------------
# Calculate lav for (xway, dir, seg, minute)
# It is the average speed of cars in (xway, dir, seg) in 5 previous minutes
#------------------------------------------------------------------------
sub lav
{
   my ($dbh) = @_;

   my $startTime = time;

#   my $sql =  "UPDATE statistics
#               SET    lav = (SELECT trunc(avg(prelav.lav))
#                             FROM   prelav
#                             WHERE  statistics.xway = prelav.xway AND
#                                    statistics.dir = prelav.dir AND
#                                    statistics.seg = prelav.seg AND
#                                    prelav.minute <= statistics.minute - 1 AND
#                                    prelav.minute  >= statistics.minute - 5);";

   my $sql =  "UPDATE statistics
               SET    lav = (SELECT trunc(avg(prelav.lav))
                             FROM   prelav
                             WHERE  statistics.dir = prelav.dir AND
                                    statistics.seg = prelav.seg AND
                                    prelav.minute <= statistics.minute - 1 AND
                                    prelav.minute  >= statistics.minute - 5);";


   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     lav running time:  $runningTime\n");
}

#------------------------------------------------------------------------
# No LAV
#------------------------------------------------------------------------

sub noLav
{
   my ($dbh) = @_;

   my $startTime = time;

   my $sql =  "UPDATE statistics
               SET    lav = -1
               WHERE  lav IS NULL;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     noLav running time: $runningTime\n");
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
