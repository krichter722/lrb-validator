#!/usr/bin/perl -w

####################################################################
# Author	:	Nga Tran
# Date          :	Aug, 2004
# Purposes	:
#	. Generate toll alerts and accident alerts
# Modified history :
#      Name        Date           Comment
#      -------     -----------    ---------------------------------
#      Nga         8/24/04        Generate for only 1 expresss way
####################################################################


use strict;
use DBI qw(:sql_types);
use FileHandle;

# Arguments
my @arguments = @ARGV;
my $dbName = shift(@arguments);
my $userName = shift(@arguments);
my $password = shift(@arguments);
my $logFile =  shift(@arguments);
my $logVar =  shift(@arguments);

my $startTime = time;

#BEGIN {
#    open (STDERR, ">>execution.log");
#}

writeToLog($logFile, $logVar, "generateAlerts in progess ...\n\n");

system ("perl runDdl.pl $dbName $userName $password $logFile $logVar");
system ("perl extractAccidents.pl $dbName $userName $password $logFile $logVar");
system ("perl insertStatistics.pl  $dbName $userName $password $logFile $logVar");
system ("perl extractNumVehicles.pl $dbName $userName $password $logFile $logVar");
system ("perl extractLavs.pl  $dbName $userName $password $logFile $logVar");
system ("perl calculateTolls.pl  $dbName $userName $password $logFile $logVar");
system ("perl createAlerts.pl  $dbName $userName $password $logFile $logVar");

my $runningTime = time - $startTime;
writeToLog($logFile, $logVar, "Total generateAlerts running time: $runningTime seconds\n\n");

exit(0);

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
