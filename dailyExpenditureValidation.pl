#!/usr/bin/perl -w

####################################################################
# Author	:	Igor Pendan
# Date	:	2004
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

## Compare counts to make sure they are the same.
## If counts aren't the same, the delete can still delete all from the wrong answer table--despite wrong answer.
writeToLog ( $logfile, $logvar, "Comparing output and answer table sizes for type 3.");
	$dbquery="SELECT Count(*) AS CountOfqueryid FROM dailyExpenditureanswer;";
	$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
	$sth->execute;
my @answerCount = $sth->fetchrow_array;
	$dbquery="SELECT Count(*) AS CountOfqueryid FROM outputdailyExpenditure;";
	$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
	$sth->execute;
my @outputCount = $sth->fetchrow_array;


if ($answerCount[0]!=$outputCount[0] and $answerCount[0] ne $outputCount[0] ) {
	writeToLog ( $logfile, $logvar, "Daily Expenditure validation failed! Your output has: $outputCount[0] tuples. The answer has: $answerCount[0] tuples.");
	exit(0);
}else {
	writeToLog ( $logfile, $logvar, "Daily Expenditure count comparison ok. Total tuples in answer: $answerCount[0]. ");
}

## Compare answers query
	$dbquery="select * into dailyExpenditurewronganswers from outputdailyExpenditure;";
	$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
	$sth->execute;
	
	$dbquery="DELETE FROM dailyExpenditurewronganswers WHERE dailyExpenditurewronganswers.qid=outputdailyExpenditure.qid and dailyExpenditurewronganswers.bal=outputdailyExpenditure.bal;";
	$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
	$sth->execute;

#	$dbquery="SELECT * FROM dailyExpenditurewronganswers LIMIT 50;";
#	$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
#	$sth->execute;

	$dbquery="SELECT count (*) FROM dailyExpenditurewronganswers;";
	$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
	$sth->execute;


my @dailyExpenditurecomparison = $sth->fetchrow_array;

if ( $dailyExpenditurecomparison[0] != 0){

        print "   *** Daily Expenditure validation failed! Wrong answers stored in dailyExpenditurewronganswers table.\n";
	writeToLog ( $logfile, $logvar, "Daily Expenditure validation failed! Wrong answers stored in dailyExpenditurewronganswers table.");
	exit (0);
} else {
	print "   *** Daily Expenditure Validition Completed Successfully!\n";
	writeToLog ( $logfile, $logvar, "Daily Expenditure Validition Completed Successfully!");
	$dbquery="DROP TABLE dailyExpenditurewronganswers;";
	$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
	$sth->execute;
}

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
