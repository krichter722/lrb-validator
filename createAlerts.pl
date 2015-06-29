#!/usr/bin/perl -w

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

writeToLog($logFile, $logVar, "createAlerts in progress ...\n");

# Constants
my $EXIT_LANE = 4;

# Connect to Postgres database
my $dbh = DBI->connect(
            "DBI:PgPP:$dbName", "$userName", "$password",
            {PrintError => 1, AutoCommit => 0}
          ) || die "Could not connect to database:  $DBI::errstr";

eval
{

   my $startTime = time;

   truncateTables($dbh);
   createAlerts($dbh);
   updateLavs($dbh);
   updateTolls($dbh);
   updateAccidentSegments($dbh);

   my $runningTime = time - $startTime;
   writeToLog($logFile, $logVar, "Total createAlerts running time: $runningTime seconds\n\n");     
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

   writeToLog($logFile, $logVar, "     truncate done\n");
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
   writeToLog($logFile, $logVar, "     createAlerts running time: $runningTime\n");
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
   writeToLog($logFile, $logVar, "     updateLavs running time: $runningTime\n");

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
   writeToLog($logFile, $logVar, "     updateTolls running time: $runningTime\n");
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
   writeToLog($logFile, $logVar, "     updateAccidentSegments running time: $runningTime\n");
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
