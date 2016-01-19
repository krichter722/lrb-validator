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

writeToLog($logFile, $logVar,  "compareALerts in progress ...\n");

# Connect to test Postgres database
my $dbh = DBI->connect(
            "DBI:PgPP:$dbName", "$userName", "$password",
            {PrintError => 0, AutoCommit => 1}
          ) || die "Could not connect to database:  $DBI::errstr";

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
   writeToLog($logFile, $logVar,  "Total alertComparing running time: $runningTime seconds\n\n");
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

   $dbh->do("CREATE INDEX tollAccIdx1 ON tollAccAlerts(time, carid, accidentSeg);");
   $dbh->do("CREATE INDEX tollAccIdx2 ON tollAccAlerts(time, carid);");

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar,  "     createIndex running time:  $runningTime\n");
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
                      tollAccAlerts AS acc2
               WHERE  acc1.time = acc2.time AND
                      acc1.carid = acc2.carid AND
                      acc1.seg = acc2.accidentSeg;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     accAlertNotInValidator running time:  $runningTime\n");
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
               FROM   tollAccAlerts
               WHERE  accidentSeg <> -1
               EXCEPT
               SELECT acc1.time, acc1.carid, acc1.accidentSeg
               FROM   tollAccAlerts AS acc1,
                      accidentAlerts AS acc2
               WHERE  acc1.time = acc2.time AND
                      acc1.carid = acc2.carid AND
                      acc1.accidentSeg = acc2.seg;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar,  "     accAlertNotInOriginal running time:  $runningTime\n");
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
                      tollAccAlerts AS acc2
               WHERE  acc1.time = acc2.time AND
                      acc1.carid = acc2.carid AND
                      acc1.seg <> acc2.accidentSeg;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar,  "     accAlertDifferentSegment running time:  $runningTime\n");
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
                      tollAccAlerts toll2
               WHERE  toll1.carid = toll2.carid AND
                      toll1.time = toll2.time AND
                      toll1.lav <> toll2.lav;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     tollAlertDifferentLav running time:  $runningTime\n");
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
                      tollAccAlerts toll2
               WHERE  toll1.carid = toll2.carid AND
                      toll1.time = toll2.time AND
                      toll1.toll <> toll2.toll;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar,  "     tollAlertDifferentToll running time:  $runningTime\n");
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
                      tollAccAlerts AS toll2
               WHERE  toll1.time = toll2.time AND
                      toll1.carid = toll2.carid AND
                      toll1.lav = toll2.lav AND
                      toll1.toll = toll2.toll;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar,  "     tollAlertNotInValidator running time:  $runningTime\n");
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
               FROM   tollAccAlerts
               EXCEPT
               SELECT toll1.time, toll1.carid
               FROM   tollAccAlerts AS toll1,
                      tollAlerts AS toll2
               WHERE  toll1.time = toll2.time AND
                      toll1.carid = toll2.carid AND
                      toll1.lav = toll2.lav AND
                      toll1.toll = toll2.toll;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     tollAlertNotInOriginal running time:  $runningTime\n");
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
      print "     * There are more accident alerts in the original\n";
      writeToLog($logFile, $logVar, "     *** There are more accident alerts in the original. Wrong answers stored in accAlertsNotInOriginal table\n");
   }

   $sql =  "SELECT count (*) from accAlertNotInOriginal;";
   $statement = $dbh->prepare($sql);
   $statement->execute;
   @row = $statement->fetchrow_array;
   $statement->finish;
   if ($row[0] > 0) {
      $isDifferent = 1;
      print "     * There are more accident alerts in the validator\n";
      writeToLog($logFile, $logVar,  "     * There are more accident alerts in the validator. Wrong answers stored in accAlertsNotInValidator table\n");
   }

   if ( $isDifferent) {
       print "   *** Accident alerts validation failed\n";
       writeToLog($logFile, $logVar, "     *** Accident alerts validation failed\n");
   } else {
       print "   *** Accident alerts validation completed sucessfully\n";
       writeToLog($logFile, $logVar, "    *** Accident alerts validation completed sucessfully\n");
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
      print "     * There are more toll alerts in the original\n";
      writeToLog($logFile, $logVar,"     * There are more toll alerts in the original. Wrong answers stored in tollAlertsNotInValidator table\n");

   }

   $sql =  "SELECT count (*) from tollAlertNotInOriginal;";
   $statement = $dbh->prepare($sql);
   $statement->execute;
   @row = $statement->fetchrow_array;
   $statement->finish;
   if ($row[0] > 0) {
      $isDifferent = 1;
      print "     * There are more toll alerts in the validator\n";
      writeToLog($logFile, $logVar, "     * There are more toll alerts in the validator. . Wrong answers stored in accAlertsNotInOriginal table\n");
   }

   if ( $isDifferent) {
       print "   *** Toll alerts validation failed\n";
       writeToLog($logFile, $logVar, "   *** Toll alerts validation failed\n");
   } else {
       print "   *** Toll alerts validation completed sucessfully\n";
       writeToLog($logFile, $logVar, "   *** Toll alerts validation completed sucessfully\n");
   }


   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     checking running time:  $runningTime\n");
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
