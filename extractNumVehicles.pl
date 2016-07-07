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
#	. Extract numbver of vehicles from input table
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

$logger->info("extractNumVehicles in progress ...");

# Connect to Postgres database
my $dbh  = DBI->connect(
            "DBI:Pg:dbname=$dbname;host=$dbhost", "$dbuser", "$dbpassword",
            {PrintError => 1}
          ) || $logger->logdie("Could not connect to database:  $DBI::errstr");


eval {
   my $startTime = time;

   createTmpTables($dbh);
   preVehicles($dbh);
   indexPreVehicles($dbh);
   vehicles($dbh);
   noVehicles($dbh);
   dropTmpTables($dbh);


   my $runningTime =  time - $startTime;
   $logger->info("Total extractNumVehicles running time:  $runningTime");
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
#          CREATE TABLE preVehicle(
#               xway integer,
#               dir  integer,
#               seg  integer,
#               minute integer,
#               numVehicles  integer);");

   $dbh->do("
          CREATE TABLE preVehicle(
               dir  integer,
               seg  integer,
               minute integer,
               numVehicles  integer);");

    $dbh->commit;
}

#------------------------------------------------------------------------
# Drop all temporary tables
#------------------------------------------------------------------------

sub dropTmpTables
{
   my ($dbh) = @_;
   $dbh->do("DROP TABLE  preVehicle;");
   $dbh->commit;
}

#------------------------------------------------------------------------
# Calculate numVehicles for (xway, dir, seg) fro previous minute
# This function is for performance purpose
#------------------------------------------------------------------------
sub preVehicles
{
   my ($dbh) = @_;

   my $startTime = time;

  # Extract number of vehicles in an
  # (express way, direction, segment, minute)

#   my $sql =  "INSERT INTO  preVehicle(xway, dir, seg, minute, numvehicles)
#              SELECT   xway,
#                       dir,
#                       seg,
#                       trunc(time/60) + 2,
#                       count(distinct carid)
#              FROM     input
#              WHERE    type = 0
#              GROUP BY xway, dir, seg, trunc(time/60);";

   my $sql =  "INSERT INTO  preVehicle(dir, seg, minute, numvehicles)
              SELECT   dir,
                       seg,
                       trunc(time/60) + 2,
                       count(distinct carid)
              FROM     input
              WHERE    type = 0
              GROUP BY dir, seg, trunc(time/60);";


   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info("     preVehicles running time:  $runningTime");
}


#------------------------------------------------------------------------
# Create index for preVehicle Table
#------------------------------------------------------------------------

sub indexPreVehicles
{
   my ($dbh) = @_;

   my $startTime = time;

#   $dbh->do("CREATE UNIQUE INDEX preVehicleIdx1
#             ON preVehicle (xway, dir, seg, minute);");

   $dbh->do("CREATE UNIQUE INDEX preVehicleIdx1
             ON preVehicle (dir, seg, minute);");

   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info("     indexPreVehicles running time:  $runningTime");
}

#------------------------------------------------------------------------
# Update numVehicles for statistics table
#------------------------------------------------------------------------
sub vehicles
{
   my ($dbh) = @_;

   my $startTime = time;

#   my $sql =  "UPDATE statistics
#               SET    numVehicles = (SELECT numVehicles
#                             FROM   preVehicle
#                             WHERE  statistics.xway = preVehicle.xway AND
#                                    statistics.dir = preVehicle.dir AND
#                                    statistics.seg = preVehicle.seg AND
#                                    statistics.minute = preVehicle.minute);";

   my $sql =  "UPDATE statistics
               SET    numVehicles = (SELECT numVehicles
                             FROM   preVehicle
                             WHERE  statistics.dir = preVehicle.dir AND
                                    statistics.seg = preVehicle.seg AND
                                    statistics.minute = preVehicle.minute);";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info("     vehicles running time:  $runningTime");
}


#------------------------------------------------------------------------
# Update numVehicles for statistics table
#------------------------------------------------------------------------
sub noVehicles
{
   my ($dbh) = @_;

   my $startTime = time;

   my $sql =  "UPDATE statistics
               SET    numVehicles = 0
               WHERE  numVehicles IS NULL;";


   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info("     no vehicles running time:  $runningTime");
}
