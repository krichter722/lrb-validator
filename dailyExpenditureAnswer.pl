#!/usr/bin/perl -w

####################################################################
# Author	:	Igor Pendan
# Date         	:       2004
# Purposes	:
# Modified history :
#      Name        Date           Comment
#      -------     -----------    ---------------------------------
#      Nga         8/31/04        Pass arguments
####################################################################

use DBI;
use strict;
use FileHandle;

# Process arguments
my @arguments = @ARGV;
my $dbname = shift(@arguments);
my $dbuser = shift(@arguments);
my $dbpassword = shift(@arguments);
my $logfile = shift(@arguments);
my $logvar = shift(@arguments);

my $dbquery;
my $sth;    
my $dbh = DBI->connect("DBI:PgPP:$dbname", $dbuser, $dbpassword)
                or die "Couldn't connect to database: ". DBI->errstr;

writeToLog ( $logfile, $logvar, "Calculating dailyExpenditureanswer.");
#Summing query on relevant account balances.
	$dbquery="SELECT completehistory.carid AS carid, completehistory.day AS day, completehistory.toll AS bal, dailyExpenditurerequests.qid INTO dailyExpenditureanswer FROM dailyExpenditurerequests INNER JOIN completehistory ON (dailyExpenditurerequests.day = completehistory.day) AND (dailyExpenditurerequests.carid = completehistory.carid) AND (dailyExpenditurerequests.xway = completehistory.xway);";
	$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
	$sth->execute;
writeToLog ( $logfile, $logvar, "Answer stored in dailyExpenditureanswer table. ");


#### SUBS
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
