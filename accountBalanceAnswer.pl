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
my $dbh = DBI->connect("DBI:PgPP:$dbname", "$dbuser", "$dbpassword")
                or die "Couldn't connect to database: ". DBI->errstr;

writeToLog ( $logfile, $logvar, "Calculating type 2 answer.");
writeToLog ( $logfile, $logvar, "Generating table accountBalancetimeEq with times for carids with tollAccAlerts at same time as type 2 request. (resulttime=querytime)");
$dbquery="SELECT t2.carid, t2.qid,  t2.time as querytime, t2.time as resulttime INTO accountBalancetimeEq FROM  accountBalancerequests as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.time = tollAccAlerts.time GROUP BY t2.carid, t2.time, t2.qid ORDER BY t2.carid, t2.time;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Using table accountBalancetimeEq to generate balances for carids with tollAccAlerts at same time as type 2 requests.");
$dbquery="SELECT t2.carid, t2.querytime, t2.resulttime, t2.qid, sum(tollAccAlerts.toll) AS toll INTO accountBalancenow FROM  accountBalancetimeEq as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.resulttime > tollAccAlerts.time GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid ORDER BY t2.carid, t2.querytime;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Finding max times of a tollalert for all carids having querytime>=resulttime1.");
$dbquery="SELECT t2.carid, t2.qid,  t2.time as querytime, max (tollAccAlerts.time) as resulttime INTO accountBalancetime0 FROM  accountBalancerequests as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.time >=tollAccAlerts.time and t2.time-30<tollAccAlerts.time GROUP BY t2.carid, t2.time, t2.qid ORDER BY t2.carid, t2.time;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Calculating maximum times where querytime>resulttime1 (second possible answer time).");
$dbquery="SELECT t2.carid, t2.qid,  t2.querytime, max(tollAccAlerts.time) AS resulttime INTO accountBalancetime1 FROM  accountBalancetime0 as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.resulttime > tollAccAlerts.time GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid ORDER BY t2.carid, t2.resulttime;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Compensating for cars having t-30 as their last tollalert.");
$dbquery="SELECT t2.carid, t2.querytime as querytime, t2.resulttime as resulttime, t2.qid, sum(tollAccAlerts.toll) AS toll INTO accountBalancemiddle FROM  accountBalancetime1 as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.resulttime > tollAccAlerts.time GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Calculating possible resulttime2 thats different from querytime for all carids having resulttime2<Resulttime1<querytime.");
$dbquery="SELECT t2.carid, t2.qid,  t2.time as querytime, min (tollAccAlerts.time) as resulttime INTO accountBalancetime10 FROM  accountBalancerequests as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.time >=tollAccAlerts.time and t2.time-60<tollAccAlerts.time GROUP BY t2.carid, t2.time, t2.qid ORDER BY t2.carid, t2.time;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Calculating balances for resulttime2.");
$dbquery="SELECT t2.carid, t2.querytime, max(tollAccAlerts.time) AS resulttime, t2.qid, integer '0' as toll INTO accountBalancetime2 FROM  accountBalancetime10 as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.resulttime >= tollAccAlerts.time GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid ORDER BY t2.carid, t2.resulttime;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Compensating for cars having t-60 as their last tollalert.");
$dbquery="SELECT t2.carid, t2.querytime as querytime, t2.resulttime as resulttime, t2.qid, sum(tollAccAlerts.toll) AS toll INTO accountBalancelast FROM  accountBalancetime2 as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.resulttime > tollAccAlerts.time GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Calculating max times for cars not having any tollAccAlerts in the 60 second window prior to a type 2 request.");
$dbquery="SELECT t2.carid, t2.time as querytime, max(tollAccAlerts.time) as resulttime,  t2.qid, integer '0' as toll	INTO accountBalancetime60 FROM  accountBalancerequests as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.time-60 >= tollAccAlerts.time GROUP BY t2.carid, t2.time, t2.qid ORDER BY t2.carid, t2.time;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Deleting from above calculation any cars already appearing in answer");
$dbquery="DELETE FROM accountBalancetime60 WHERE accountBalancetime60.qid=accountBalancetime0.qid;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DELETE FROM accountBalancetime60 WHERE accountBalancetime60.qid=accountBalancetime10.qid;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DELETE FROM accountBalancetime60 WHERE accountBalancetime60.qid=accountBalancetime2.qid;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Calculating tolls for the times where carids do not have tollAccAlerts in the 60 second window prior to a type 2 request.");
$dbquery="SELECT t2.carid, t2.querytime as querytime, t2.resulttime as resulttime, t2.qid, sum(tollAccAlerts.toll) AS toll INTO accountBalanceancient	FROM  accountBalancetime60 as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.resulttime > tollAccAlerts.time GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Calculating queries that have no tollAccAlerts.");
$dbquery="DELETE FROM accountBalancetime60 WHERE accountBalanceancient.qid=accountBalancetime60.qid;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Setting balance to zero for carids that have a type 2 request but no previous tollAccAlerts.");
$dbquery="SELECT t2.carid, t2.time as querytime, min (tollAccAlerts.time) as resulttime,  t2.qid,  integer '0' AS toll INTO accountBalancequeryatenterance FROM  accountBalancerequests as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid	GROUP BY t2.carid, t2.time, t2.qid ORDER BY t2.carid, t2.time;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DELETE FROM accountBalancequeryatenterance WHERE querytime<>resulttime;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

$dbquery="DELETE FROM accountBalancetime2 WHERE accountBalancelast.qid=accountBalancetime2.qid;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Creating answer in accountBalanceanswer table.");
$dbquery="SELECT * into accountBalanceanswer from accountBalancenow UNION select * from accountBalancemiddle UNION select * from accountBalancelast UNION select * from accountBalanceancient UNION select * from accountBalancequeryatenterance UNION select * FROM accountBalancetime2 UNION select * from accountBalancetime60;";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;
writeToLog ( $logfile, $logvar, "Type 2: Finished Calculating accountBalanceanswer except for special cases too expensive to calculate.");

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
