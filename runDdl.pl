#!/usr/bin/perl -w

####################################################################
# Author	:	Nga Tran
# Date   	:	Aug, 2004
# Purposes	:
#	. Drop/Create tables and Indexes
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

writeToLog($logFile, $logVar, "runDdl in progress ...\n");

# Connect to test Postgres database
my $dbh = DBI->connect(
            "DBI:PgPP:$dbName", "$userName", "$password",
            {PrintError => 0, AutoCommit => 1}
          ) || die "Could not connect to database:  $DBI::errstr";

eval
{
   my $startTime = time;

   createAccidentTable($dbh);
   createStatisticsTable($dbh);
   createTollAccAlertsTable($dbh);
   createComparedTables($dbh);

   my $runningTime = time - $startTime;
   writeToLog($logFile, $logVar,  "Total runDdl running time: $runningTime seconds\n\n");
};
print $@;

$dbh->disconnect;

exit(0);

#----------------------------------------------------------------

sub createAccidentTable
{
   my ($dbh) = @_; 

   $dbh->do("DROP TABLE accident;");

#   $dbh->do("CREATE TABLE accident(
#               carid1 integer,
#               carid2 integer,
#               firstMinute integer,
#               lastMinute integer,
#               xway integer,               
#               dir integer,
#               seg integer,
#               pos integer,
#               PRIMARY KEY (xway, dir, pos, firstMinute) 
#              );");

   $dbh->do("CREATE TABLE accident(
               carid1 integer,
               carid2 integer,
               firstMinute integer,
               lastMinute integer,
               dir integer,
               seg integer,
               pos integer,
               PRIMARY KEY ( dir, pos, firstMinute) 
              );");
}

#----------------------------------------------------------------

sub createStatisticsTable
{
   my ($dbh) = @_; 

   $dbh->do("DROP TABLE statistics;");

#   $dbh->do("CREATE TABLE statistics(
#	          xway integer,
#	          dir  integer,
#	          seg  integer,
#	          minute integer,
#	          numvehicles  integer,
#	          lav   integer,
#                 toll  integer,
#                 accident integer,
#                 accidentSeg integer);");

   $dbh->do("CREATE TABLE statistics(
	          dir  integer,
	          seg  integer,
	          minute integer,
	          numvehicles  integer,
	          lav   integer,
                  toll  integer,
                  accident integer,
                  accidentSeg integer);");

}

#----------------------------------------------------------------

sub createTollAccAlertsTable
{
   my ($dbh) = @_; 

   $dbh->do("DROP TABLE tollAccAlerts;");

#   $dbh->do("CREATE TABLE TollAccAlerts(   
#  	          time   INTEGER,
#  	          carid  INTEGER,
#   	         xway   INTEGER,
#  	          dir    INTEGER,
#  	          seg    INTEGER,
#  	          lav    INTEGER,
#  	          toll   INTEGER,
#  	          accidentSeg INTEGER);");

   $dbh->do("CREATE TABLE TollAccAlerts(   
  	          time   INTEGER,
  	          carid  INTEGER,
  	          dir    INTEGER,
  	          seg    INTEGER,
  	          lav    INTEGER,
  	          toll   INTEGER,
  	          accidentSeg INTEGER);");

}

#----------------------------------------------------------------

sub createComparedTables
{
   my ($dbh) = @_; 

   $dbh->do("DROP TABLE accAlertNotInValidator;");
   $dbh->do("DROP TABLE accAlertNotInOriginal;");
   $dbh->do("DROP TABLE tollAlertNotInValidator;");
   $dbh->do("DROP TABLE tollAlertNotInOriginal;");

   $dbh->do("CREATE TABLE accAlertNotInValidator(
               time  INTEGER,
               carid INTEGER,
               seg   INTEGER);");

   $dbh->do("CREATE TABLE accAlertNotInOriginal(
               time  INTEGER,
               carid INTEGER,
               seg   INTEGER);");

   $dbh->do("CREATE TABLE tollAlertNotInValidator(
               time  INTEGER,
               carid INTEGER);");

   $dbh->do("CREATE TABLE tollAlertNotInOriginal(
               time  INTEGER,
               carid INTEGER);");
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
