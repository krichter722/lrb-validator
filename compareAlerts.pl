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
#	. Comparing 2 pairs of outputs (original one <> new one)
#     For every pair:
#  	   . Get all tuples from the original accident alerts that do
#		not  exist in the new one
#	   . Get all tuples from the  new accident alerts  that do not
#		exist in the original
#
#	   . Get all tuples from the original toll alerts that do not
#		exist in the new one
#	   . Get all tuples from the originalnew toll alerts that do
#		not exist in the original
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
my $logger = Log::Log4perl->get_logger('lrb_validator.comparealters');

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
my $tollaccalertstablename = shift(@arguments);

$logger->info( "compareALerts in progress ...");

# Connect to test Postgres database
my $dbh  = DBI->connect(
            "DBI:Pg:dbname=$dbname;host=$dbhost", "$dbuser", "$dbpassword",
            {PrintError => 1}
          ) || $logger->logdie("Could not connect to database:  $DBI::errstr");

eval
{
   my $startTime = time;

   createIndex($dbh);
   accAlertNotInValidator($dbh);
   accAlertNotInOriginal($dbh);
   tollAlertNotInValidator($dbh);
   tollAlertNotInOriginal($dbh);
   results($dbh);

   my $runningTime = time - $startTime;
   $logger->info( "Total alertComparing running time: $runningTime seconds");
};
print $@;

$dbh->disconnect;

exit(0);

#------------------------------------------------------------------------
# Create index for tollaccalerts table
#------------------------------------------------------------------------
sub createIndex
{
   my ($dbh) = @_;

   my $startTime = time;

   $dbh->do("CREATE INDEX tollAccIdx1 ON $tollaccalertstablename(time, carid, accidentSeg);");
   $dbh->do("CREATE INDEX tollAccIdx2 ON $tollaccalertstablename(time, carid);");

   my $runningTime =  time - $startTime;
   $logger->info( "     createIndex running time:  $runningTime");
}


#------------------------------------------------------------------------
# Get all tuples from the original accident alerts that do not  exist in
# the new one
#------------------------------------------------------------------------
sub accAlertNotInValidator
{
   my ($dbh) = @_;

   my $startTime = time;

   $dbh->do("TRUNCATE TABLE accAlertNotInValidator");

   my $sql =  "INSERT INTO  accAlertNotInValidator
               SELECT time, carid, seg
               FROM   accidentAlerts
               EXCEPT
               SELECT acc1.time, acc1.carid, acc1.seg
               FROM   accidentAlerts AS acc1,
                      $tollaccalertstablename AS acc2
               WHERE  acc1.time = acc2.time AND
                      acc1.carid = acc2.carid AND
                      acc1.seg = acc2.accidentSeg;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info("     accAlertNotInValidator running time:  $runningTime");
}


#------------------------------------------------------------------------
# Get all tuples from the  new accident alerts  that do not exist in the
# original
#------------------------------------------------------------------------
sub accAlertNotInOriginal
{
   my ($dbh) = @_;

   my $startTime = time;

   $dbh->do("TRUNCATE TABLE accAlertNotInOriginal");

   my $sql =  "INSERT INTO  accAlertNotInOriginal
               SELECT time, carid, accidentSeg
               FROM   $tollaccalertstablename
               WHERE  accidentSeg <> -1
               EXCEPT
               SELECT acc1.time, acc1.carid, acc1.accidentSeg
               FROM   $tollaccalertstablename AS acc1,
                      accidentAlerts AS acc2
               WHERE  acc1.time = acc2.time AND
                      acc1.carid = acc2.carid AND
                      acc1.accidentSeg = acc2.seg;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info( "     accAlertNotInOriginal running time:  $runningTime");
}


#------------------------------------------------------------------------
# Check if all (time, carid) report in the same segment
#------------------------------------------------------------------------
sub accAlertDifferentSegment
{
   my ($dbh) = @_;

   my $startTime = time;

   $dbh->do("TRUNCATE TABLE accAlertDifferentSegment");

   my $sql =  "INSERT INTO accAlertDifferentSegment
               SELECT acc1.time, acc1.carid, acc1.seg, acc2.accidentSeg
               FROM   accidentAlerts AS acc1,
                      $tollaccalertstablename AS acc2
               WHERE  acc1.time = acc2.time AND
                      acc1.carid = acc2.carid AND
                      acc1.seg <> acc2.accidentSeg;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info( "     accAlertDifferentSegment running time:  $runningTime");
}

#------------------------------------------------------------------------
# Get all (time, carid) that have different LAV
#------------------------------------------------------------------------
sub tollAlertDifferentLav
{
   my ($dbh) = @_;

   my $startTime = time;

   $dbh->do("TRUNCATE TABLE tollAlertDifferentLav");

   my $sql =  "INSERT INTO tollAlertDifferentLav
               SELECT toll1.time,
                      toll1.carid,
                      toll1.lav,
                      toll2.lav
               FROM   tollAlerts toll1,
                      $tollaccalertstablename toll2
               WHERE  toll1.carid = toll2.carid AND
                      toll1.time = toll2.time AND
                      toll1.lav <> toll2.lav;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info("     tollAlertDifferentLav running time:  $runningTime");
}


#------------------------------------------------------------------------
# Get all (time, carid) that have different toll
#------------------------------------------------------------------------
sub tollAlertDifferentToll
{
   my ($dbh) = @_;

   my $startTime = time;

   $dbh->do("TRUNCATE TABLE tollAlertDifferentToll");

   my $sql =  "INSERT INTO tollAlertDifferentToll
               SELECT toll1.time,
                      toll1.carid,
                      toll1.toll,
                      toll2.toll
               FROM   tollAlerts toll1,
                      $tollaccalertstablename toll2
               WHERE  toll1.carid = toll2.carid AND
                      toll1.time = toll2.time AND
                      toll1.toll <> toll2.toll;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info( "     tollAlertDifferentToll running time:  $runningTime");
}

#------------------------------------------------------------------------
# Get all tuples from the original toll alerts that do not exist in the new one
#------------------------------------------------------------------------
sub tollAlertNotInValidator
{
   my ($dbh) = @_;

   my $startTime = time;

   $dbh->do("TRUNCATE TABLE tollAlertNotInValidator");

   my $sql =  "INSERT INTO  tollAlertNotInValidator
               SELECT time, carid
               FROM   tollAlerts
               EXCEPT
               SELECT toll1.time, toll1.carid
               FROM   tollAlerts AS toll1,
                      $tollaccalertstablename AS toll2
               WHERE  toll1.time = toll2.time AND
                      toll1.carid = toll2.carid AND
                      toll1.lav = toll2.lav AND
                      toll1.toll = toll2.toll;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info( "     tollAlertNotInValidator running time:  $runningTime");
}


#------------------------------------------------------------------------
# Get all tuples from the originalnew toll alerts that do
#		not exist in the original
#------------------------------------------------------------------------
sub tollAlertNotInOriginal
{
   my ($dbh) = @_;

   my $startTime = time;

   $dbh->do("TRUNCATE TABLE tollAlertNotInOriginal");

   my $sql =  "INSERT INTO  tollAlertNotInOriginal
               SELECT time, carid
               FROM   $tollaccalertstablename
               EXCEPT
               SELECT toll1.time, toll1.carid
               FROM   $tollaccalertstablename AS toll1,
                      tollAlerts AS toll2
               WHERE  toll1.time = toll2.time AND
                      toll1.carid = toll2.carid AND
                      toll1.lav = toll2.lav AND
                      toll1.toll = toll2.toll;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   $logger->info("     tollAlertNotInOriginal running time:  $runningTime");
}
#------------------------------------------------------------------------
# Check the compared results
#------------------------------------------------------------------------
sub results
{
   my ($dbh) = @_;

   my $startTime = time;
   my $isDifferent = 0;

   my $sql =  "SELECT count (*) from accAlertNotInValidator;";
   my $statement = $dbh->prepare($sql);
   $statement->execute;
   my @row = $statement->fetchrow_array;
   $statement->finish;
   if ($row[0] > 0) {
      $isDifferent = 1;
      $logger->info("     *** There are more accident alerts in the original. Wrong answers stored in accAlertsNotInOriginal table");
   }

   $sql =  "SELECT count (*) from accAlertNotInOriginal;";
   $statement = $dbh->prepare($sql);
   $statement->execute;
   @row = $statement->fetchrow_array;
   $statement->finish;
   if ($row[0] > 0) {
      $isDifferent = 1;
      $logger->info( "     * There are more accident alerts in the validator. Wrong answers stored in accAlertsNotInValidator table");
   }

   if ( $isDifferent) {
       $logger->info("     *** Accident alerts validation failed");
   } else {
       $logger->info("    *** Accident alerts validation completed sucessfully");
   }

   $isDifferent = 0;


#   $sql =  "SELECT count (*) from accAlertDifferentSegment;";
#   $statement = $dbh->prepare($sql);
#   $statement->execute;
#   @row = $statement->fetchrow_array;
#   $statement->finish;
#   if ($row[0] > 0) {
#      $isDifferent = 1;
#      $isDifferent = 1;
#      print "     * Accident segments reported are different\n";
#   }

#   $sql =  "SELECT count (*) from tollAlertDifferentLav;";
#   $statement = $dbh->prepare($sql);
#   $statement->execute;
#   @row = $statement->fetchrow_array;
#   $statement->finish;
#   if ($row[0] > 0) {
#      $isDifferent = 1;
#      print "     * Some Lavs are different\n";
#   }

#   $sql =  "SELECT count (*) from tollAlertDifferentToll;";
#   $statement = $dbh->prepare($sql);
#   $statement->execute;
#   @row = $statement->fetchrow_array;
#   $statement->finish;
#   if ($row[0] > 0) {
#      $isDifferent = 1;
#      $isDifferent = 1;
#      print "     * Some tolls are different\n";
#   }

   $sql =  "SELECT count (*) from tollAlertNotInValidator;";
   $statement = $dbh->prepare($sql);
   $statement->execute;
   @row = $statement->fetchrow_array;
   $statement->finish;
   if ($row[0] > 0) {
      $isDifferent = 1;
      $logger->info("     * There are more toll alerts in the original. Wrong answers stored in tollAlertsNotInValidator table");

   }

   $sql =  "SELECT count (*) from tollAlertNotInOriginal;";
   $statement = $dbh->prepare($sql);
   $statement->execute;
   @row = $statement->fetchrow_array;
   $statement->finish;
   if ($row[0] > 0) {
      $isDifferent = 1;
      $logger->info("     * There are more toll alerts in the validator. . Wrong answers stored in accAlertsNotInOriginal table");
   }

   if ( $isDifferent) {
       $logger->info("   *** Toll alerts validation failed");
   } else {
       $logger->info("   *** Toll alerts validation completed sucessfully");
   }


   my $runningTime =  time - $startTime;
   $logger->info("     checking running time:  $runningTime");
}
