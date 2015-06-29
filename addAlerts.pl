#!/usr/bin/perl -w

####################################################################
# Author	:	Nga Tran
# Date  	:	Aug 24, 2004
# Purposes	:
#	. Produce toll & accident alerts
# Modified history :
#      Name        Date           Comment
#      -------     -----------    ---------------------------------
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

writeToLog($logFile, $logVar, "addAlerts in progress ...\n");

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

   insertAlerts($dbh);

   my $runningTime = time - $startTime;
   writeToLog($logFile, $logVar, "Total addAlerts running time: $runningTime seconds\n\n");     
};
print $@;

$dbh->disconnect;

exit(0);

#-------------------------------------------------------------------------------

sub  insertAlerts
{
   my ($dbh) = @_;

   my $startTime = time;

   my $sql = "INSERT INTO tollAccAlertsTmp
              SELECT * from tollAccAlerts;";

   my $statment = $dbh->prepare($sql);
   $statment->execute;  
   $dbh->commit;

   my $runningTime = time - $startTime;
   writeToLog($logFile, $logVar, "     insertAlerts running time: $runningTime\n");
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
