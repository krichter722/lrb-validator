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
# Date  	:	Aug 26, 2004
# Purposes	:
#	. Split input
# Modified history :
#      Name        Date           Comment
#      -------     -----------    ---------------------------------
####################################################################


use strict;
use DBI qw(:sql_types);
use FileHandle;
use Log::Log4perl qw(:easy);

Log::Log4perl->easy_init($DEBUG);
my $logger = Log::Log4perl->get_logger('lrb_validator.xwayloop');

#BEGIN {
#    open (STDERR, ">>execution.log");
#}

# Process arguments
my @arguments = @ARGV;
my $dbname = shift(@arguments);
my $dbhost = shift(@arguments);
my $dbuser = shift(@arguments);
my $dbpassword = shift(@arguments);
my $logFile =  shift(@arguments);
my $logVar = shift(@arguments);

$logger->info("xwayLoop.pl in progress ...\n");


# Connect to Postgres database
my $dbh  = DBI->connect(
            "DBI:Pg:dbname=$dbname;host=$dbhost", "$dbuser", "$dbpassword",
            {PrintError => 1}
          ) || $logger->logdie("Could not connect to database:  $DBI::errstr");

eval {

   my $startTime = time;
   my $maxXway = 0;

   # get maximun expressway
   $maxXway = getMaxXway($dbh);

   print "Max xway: $maxXway\n";

   if ($maxXway > 0){
       # more than 1 xways, rename input table
       renameInputTable($dbh);
       print "renameInputTable done\n";

       # Create temporary table for toll and accident alerts
       system("perl createAlertTmpTables.pl $dbname $dbuser $dbpassword, $logFile $logVar");
       print " createAlertTmpTables.pl done\n";
   }

   for (my $i = 0; $i <= $maxXway; $i++)
   {
       if ($maxXway > 0 ){
         # Drop input and create new one
	 dropInput($dbh);
         createInput($dbh);
         print " recreateInput done\n";

         # extract data of xway $i from inputTmp to input
         extractInput($dbh, $i);
         print "extractInput done\n";
       }

      # Create indexes for input
       createInputIndexes($dbh);
       print "create input indexes done\n";


       # generate toll and accident alerts for xway $i
       system("perl generateAlerts.pl $dbname $dbuser $dbpassword $logFile $logVar");
       print "generate Alerts done\n";


       if ($maxXway > 0 ){
          # more than 1 xways, insert the alerts into Tmp table
          system ("perl addAlerts.pl $dbname $dbuser $dbpassword $logFile $logVar");
          print " addAlerts.pl done\n";

      }
   }


   if ($maxXway > 0){
      dropInput($dbh);
      # rename inputTmp table to input
       renameInputTmpTable($dbh);
       print "renameInputTmpTable done\n";

      # rename tollAccAlertsTmp to tollAccAlerts
      system("perl renameAlertTmpTables.pl $dbname $dbuser $dbpassword $logFile $logVar");
       print "renameAlertTmpTables.pl done\n";
   }

   my $runningTime = time - $startTime;
   $logger->info("Total xwayLoop running time: $runningTime seconds\n\n");

};
print$@;

$dbh->disconnect;

exit(0);


#-------------------------------------------------------------------------------
# Get maximun number of expressways
#-------------------------------------------------------------------------------

sub  getMaxXway
{
     my($dbh) = @_;

      my $startTime = time;
     my $maxXway = 0;

      my $sql = "SELECT max(xway) FROM input;";

      my $statement = $dbh->prepare($sql);
      $statement->execute;

      if (my @row = $statement->fetchrow_array) {
          $maxXway = $row[0];
      }

      my $runningTime = time - $startTime;
      $logger->info("     getNumXway running time: $runningTime\n");

     return $maxXway;
}

#-------------------------------------------------------------------------------
# Rename input table to inputTmp
#-------------------------------------------------------------------------------

sub  renameInputTable
{
      my($dbh) = @_;

      my $startTime = time;

      my $sql = "ALTER TABLE input RENAME TO inputTmp;";
      $dbh->do($sql);
      $dbh->commit;

      my $runningTime = time - $startTime;
      $logger->info("     renameInputTable running time: $runningTime\n");
}

#-------------------------------------------------------------------------------
# Rename inputTmp table to input
#-------------------------------------------------------------------------------

sub  renameInputTmpTable
{
      my($dbh) = @_;

      my $startTime = time;

      my $sql = "ALTER TABLE inputTmp RENAME TO input;";
      $dbh->do($sql);
      $dbh->commit;

      my $runningTime = time - $startTime;
      $logger->info("renameInputTmpTable running time: $runningTime");
}

#-------------------------------------------------------------------------------
# Drop input table
#-------------------------------------------------------------------------------

sub  dropInput
{
   my($dbh) = @_;

   my $startTime = time;

   $dbh->do("DROP TABLE input;");
    $dbh->commit;

  my $runningTime = time - $startTime;
  $logger->info("dropInput running time: $runningTime");

}

#-------------------------------------------------------------------------------
# Crete input table
#-------------------------------------------------------------------------------

sub  createInput
{
   my($dbh) = @_;

   my $startTime = time;

   $dbh->do("CREATE TABLE input (
                    type   integer,
                    time   integer,
                    carid  integer,
                    speed  integer,
                    xway   integer,
                    lane   integer,
                    dir    integer,
                    seg    integer,
                    pos    integer,
                    qid    integer,
                    m_init  integer,
                    m_end   integer,
                    dow    integer,
                    tod    integer,
                    day    integer);");
    $dbh->commit;

  my $runningTime = time - $startTime;
  logger->info("recreateInput running time: $runningTime");
}

#-------------------------------------------------------------------------------
# Extract data for xway i
#-------------------------------------------------------------------------------

sub  extractInput
{
     my($dbh, $xway) = @_;

      my $startTime = time;

      my $sql = "INSERT INTO input
                 SELECT * FROM inputTmp
                 WHERE xway = $xway;";

      my $statment = $dbh->prepare($sql);
      $statment->execute;
      $dbh->commit;

      my $runningTime = time - $startTime;
      $logger->info("extractInput running time: $runningTime");
}
#-------------------------------------------------------------------------------
# Create Indexes
#-------------------------------------------------------------------------------

sub  createInputIndexes
{
   my($dbh, $xway) = @_;

   my $startTime = time;

   $dbh->do("CREATE INDEX idx1
             ON input (type, lane);");
   $dbh->do("CREATE INDEX idx2
             ON input (type);");
   $dbh->do("CREATE INDEX idx3
             ON input (type, speed);");

#   $dbh->do("CREATE INDEX idx4
#             ON input (xway, dir, seg);");
   $dbh->do("CREATE INDEX idx4
             ON input ( dir, seg);");

#   $dbh->do("CREATE INDEX idx5
#             ON input (xway, dir, seg, time);");
#   $dbh->do("CREATE INDEX idx5
#             ON input (dir, seg, time);");

#   $dbh->do("CREATE INDEX idx6
#             ON input (xway, dir, seg, carid);");
   $dbh->do("CREATE INDEX idx6
             ON input (dir, seg, carid);");

#   $dbh->do("CREATE INDEX idx7
#             ON input (xway, dir, seg, carid, time);");
#   $dbh->do("CREATE INDEX idx7
#             ON input (dir, seg, carid, time);");

#   $dbh->do("CREATE INDEX idx8
#             ON input (xway, dir, seg, type);");
   $dbh->do("CREATE INDEX idx8
             ON input (dir, seg, type);");

#   $dbh->do("CREATE INDEX idx9
#             ON input (xway, dir, seg, type, time);");
#   $dbh->do("CREATE INDEX idx9
#             ON input (dir, seg, type, time);");

    $dbh->commit;

    my $runningTime = time - $startTime;
    $logger->info("createInputIndexes running time: $runningTime");
}
