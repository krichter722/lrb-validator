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

$logger->info("extractLavs in progess ...");

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

   intermediate($dbh);

   dropTmpTables($dbh);

   my $runningTime =  time - $startTime;
   $logger->info("Total lavExtract running time:  $runningTime seconds");
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
   $logger->info("     preLav running time:  $runningTime");
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
   $logger->info("     indexPreLav running time:  $runningTime");
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
   $logger->info("     lav running time:  $runningTime");
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
   $logger->info("     noLav running time: $runningTime");
}


#-------------------------------------------------------------------------------
# Intermediate query results
#-------------------------------------------------------------------------------

sub intermediate
{
   avgVehicleSpeed($dbh);
   avgVehicleSpeedVal($dbh);
   avgSpeed($dbh);
   lav0($dbh);
   carCount($dbh);
}

# subquery for average vehicle speed

# table for LRB output
sub avgVehicleSpeed
{
    my ($dbh) = @_;
    my $startTime = time;
    my $sql = "create table avgVehicleSpeed ( carid integer, time integer, xway
integer, seg integer, dir integer, avgspeed float );
copy avgVehicleSpeed from
'/home/mjsax/workspace_aeolus/aeolus/queries/lrb/data/avg-vehicle-speed.txt'
DELIMITER ',' CSV;"
   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info("     avgVehicleSpeed running time: $runningTime");
}


# table for validator

sub avgVehicleSpeedVal
{
    my ($dbh) = @_;
    my $startTime = time;
    my $sql = "create table avgVehicleSpeedVal( dir integer, seg integer, carid
integer, minute integer, speed float);
insert into avgVehicleSpeedVal select dir, seg, carid, trunc(time/60) +
1, avg(speed) from input where type = 0 group by dir, seg, carid,
trunc(time/60) + 1;"
   my $statement = $dbh->prepare($sql);
   $statement->execute;
   # add "insert into notInValiator" and "insert into "notInOriginal"
   # what is missing
   my $sql = "select carid, minute, seg, dir, speed from avgVehicleSpeedVal except
select carid, time, seg, dir, avgspeed from avgVehicleSpeed;"
   my $statement = $dbh->prepare($sql);
   $statement->execute;
   # what is wrong
   my $sql = "select carid, time, seg, dir, avgspeed from avgVehicleSpeed except
select carid, minute, seg, dir, speed from avgVehicleSpeedVal;"
   my $statement = $dhb->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info("     avgVehicleSpeedVal running time: $runningTime");
}


# subquery for average speed

sub avgSpeed
{
    # table for LRB output

    my ($dbh) = @_;
    my $startTime = time;
    my $sql = "create table avgSpeed ( time integer, xway integer, seg integer, dir
    integer, avgspeed float );
    copy avgSpeed from
    '/home/mjsax/workspace_aeolus/aeolus/queries/lrb/data/avg-speed.txt'
    DELIMITER ',' CSV;"
   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   # table for validator
   # -> we can use table preLav (see extractLavs.pl)

   # add "insert into notInValiator" and "insert into "notInOriginal"
   # what is missing
   my $sql = "select minute, seg, dir, round(cast(lav as numeric), 10) from prelav
except select time, seg, dir, round(cast(avgspeed as numeric), 10) from
avgSpeed;"
   my $statement = $dbh->prepare($sql);
   $statement->execute;

   # what is wrong
   my $sql = "select time, seg, dir, round(cast(avgspeed as numeric), 10) from
avgSpeed except select minute, seg, dir, round(cast(lav as numeric), 10)
from prelav;"
   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info("     avgSpeed running time: $runningTime");
}


# subquery for average speed

sub lav0
{
    # table for LRB output
    my ($dbh) = @_;
    my $startTime = time;
    my $sql = "create table lav ( time integer, xway integer, seg integer, dir integer,
lav float );
copy lav from
'/home/mjsax/workspace_aeolus/aeolus/queries/lrb/data/lav.txt' DELIMITER
',' CSV;"
   my $statement = $dbh->prepare($sql);
   $statement->execute;

   # table for validator
   #-> we can use table statistics

   # add "insert into notInValiator" and "insert into "notInOriginal"
   # what is missing
   my $sql = "select minute, seg, dir, lav from statistics where lav != -1 except
select time, seg, dir, lav from lav;"
   # what is wrong (do not include this -- validator does not store all
   # lavs -> only the lavs that are required to compute a toll)
   my $statement = $dbh->prepare($sql);
   $statement->execute;

   my $sql = "select time, seg, dir, lav from lav except select minute, seg, dir, lav
from statistics where lav != -1;"
   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info("     avgSpeed running time: $runningTime");
}


sub carCount
{
   # subquery for car count

   # table for LRB output
    my ($dbh) = @_;
    my $startTime = time;
   my $sql = "create table cars ( time integer, xway integer, seg integer, dir
integer, count integer );
copy cars from
'/home/mjsax/workspace_aeolus/aeolus/queries/lrb/data/cars.txt'
DELIMITER ',' CSV;"
   my $statement = $dbh->prepare($sql);
   $statement->execute;

   # table for validator
   # -> we can use table statistics


   # add "insert into notInValiator" and "insert into "notInOriginal"
   # what is missing
   my $sql = "select minute, seg, dir, numvehicles from statistics where numvehicles >
0 except select time+1, seg, dir, count from cars;"
   my $statement = $dbh->prepare($sql);
   $statement->execute;

   # what is wrong (do not include this -- validator does not store all
   # counts -> only the counts that are required to compute a toll)
   my $sql = "select time+1, seg, dir, count from cars except select minute, seg,
dir, numvehicles from statistics where numvehicles > 0;"
   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info("     avgSpeed running time: $runningTime");
}
