#!/usr/bin/perl -w

####################################################################
# Author	:	Igor Pendan
# Date  	:	2004
# Purposes	:
# Modified history :
#      Name        Date           Comment
#      -------     -----------    ---------------------------------
#      Nga         8/27/04        Pass arguments
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
my $dbh = DBI->connect("DBI:PgPP:$dbname", $dbuser, $dbpassword, {PrintError => 0})
                or die "Couldn't connect to database: ". DBI->errstr;

## Split into a table of dailyExpenditureRequests
$dbh->do("DROP TABLE dailyExpenditureRequests;");
writeToLog ( $logfile, $logvar, "Extracting type 3 requests from input.");
	$dbquery="SELECT type, time, carid, xway, qid, day INTO dailyExpenditureRequests FROM input WHERE type=3;";
	$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
	$sth->execute;
writeToLog ( $logfile, $logvar, "Type 3 extraction complete.");

## Split into a table of  accountBalanceRequest
$dbh->do("DROP TABLE accountBalanceRequests;");
writeToLog ( $logfile, $logvar, "Extracting type 2 requests from input.");
	$dbquery="SELECT type, time, carid as carid, xway, qid, day INTO accountBalanceRequests FROM input WHERE type=2;";
	$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
	$sth->execute;
writeToLog ( $logfile, $logvar, "Type 2 extraction complete.");


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
