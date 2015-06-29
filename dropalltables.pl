#!/usr/bin/perl -w

####################################################################
# Author	:	Igor Pendan
# Date	:	2004
# Purposes	:
#	. Produce toll for every (xway, seg, dir, minute)
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
my $dbh = DBI->connect("DBI:PgPP:$dbname", $dbuser, $dbpassword, {PrintError => 0})
                or die "Couldn't connect to database: ". DBI->errstr;

$dbquery="DROP TABLE completehistory;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE input;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE outputAccountBalance;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE outputDailyExpenditure;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE tollalerts;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE accidentalerts;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE accountBalanceAncient;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE  accountBalanceAnswer;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE accountBalanceLast;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE accountBalanceMiddle;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE accountBalanceNow;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE accountBalanceQueryAtenterance;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE accountBalanceRequests;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE accountBalanceTime0;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE accountBalanceTime1;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE accountBalanceTime10;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE accountBalanceTime2;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE accountBalanceTime60;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE accountBalanceTimeeq;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE accountBalanceWrongAnswers;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE dailyExpenditureAnswer;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DROP TABLE dailyExpenditureRequests;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

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
