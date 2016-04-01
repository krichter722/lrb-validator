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
use Log::Log4perl qw(:easy);

Log::Log4perl->easy_init($DEBUG);
my $logger = Log::Log4perl->get_logger('lrb_validator.accountbalanceanswer');

# Process arguments
my @arguments = @ARGV;
my $dbname = shift(@arguments);
my $dbhost = shift(@arguments);
my $dbuser = shift(@arguments);
my $dbpassword = shift(@arguments);
my $logfile = shift(@arguments);
my $logvar = shift(@arguments);

my $dbquery;
my $sth;
my $dbh  = DBI->connect(
            "DBI:Pg:dbname=$dbname;host=$dbhost", "$dbuser", "$dbpassword",
            {PrintError => 1}
          ) || $logger->logdie("Could not connect to database:  $DBI::errstr");

$logger->info( "Calculating type 2 answer.");
$logger->info( "Generating table accountBalancetimeEq with times for carids with tollAccAlerts at same time as type 2 request. (resulttime=querytime)");
$dbquery="SELECT t2.carid, t2.qid,  t2.time as querytime, t2.time as resulttime INTO accountBalancetimeEq FROM  accountBalancerequests as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.time = tollAccAlerts.time GROUP BY t2.carid, t2.time, t2.qid ORDER BY t2.carid, t2.time;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$logger->info( "Using table accountBalancetimeEq to generate balances for carids with tollAccAlerts at same time as type 2 requests.");
$dbquery="SELECT t2.carid, t2.querytime, t2.resulttime, t2.qid, sum(tollAccAlerts.toll) AS toll INTO accountBalancenow FROM  accountBalancetimeEq as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.resulttime > tollAccAlerts.time GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid ORDER BY t2.carid, t2.querytime;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$logger->info( "Finding max times of a tollalert for all carids having querytime>=resulttime1.");
$dbquery="SELECT t2.carid, t2.qid,  t2.time as querytime, max (tollAccAlerts.time) as resulttime INTO accountBalancetime0 FROM  accountBalancerequests as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.time >=tollAccAlerts.time and t2.time-30<tollAccAlerts.time GROUP BY t2.carid, t2.time, t2.qid ORDER BY t2.carid, t2.time;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$logger->info( "Calculating maximum times where querytime>resulttime1 (second possible answer time).");
$dbquery="SELECT t2.carid, t2.qid,  t2.querytime, max(tollAccAlerts.time) AS resulttime INTO accountBalancetime1 FROM  accountBalancetime0 as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.resulttime > tollAccAlerts.time GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid ORDER BY t2.carid, t2.resulttime;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$logger->info( "Compensating for cars having t-30 as their last tollalert.");
$dbquery="SELECT t2.carid, t2.querytime as querytime, t2.resulttime as resulttime, t2.qid, sum(tollAccAlerts.toll) AS toll INTO accountBalancemiddle FROM  accountBalancetime1 as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.resulttime > tollAccAlerts.time GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$logger->info( "Calculating possible resulttime2 thats different from querytime for all carids having resulttime2<Resulttime1<querytime.");
$dbquery="SELECT t2.carid, t2.qid,  t2.time as querytime, min (tollAccAlerts.time) as resulttime INTO accountBalancetime10 FROM  accountBalancerequests as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.time >=tollAccAlerts.time and t2.time-60<tollAccAlerts.time GROUP BY t2.carid, t2.time, t2.qid ORDER BY t2.carid, t2.time;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$logger->info( "Calculating balances for resulttime2.");
$dbquery="SELECT t2.carid, t2.querytime, max(tollAccAlerts.time) AS resulttime, t2.qid, integer '0' as toll INTO accountBalancetime2 FROM  accountBalancetime10 as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.resulttime >= tollAccAlerts.time GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid ORDER BY t2.carid, t2.resulttime;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$logger->info( "Compensating for cars having t-60 as their last tollalert.");
$dbquery="SELECT t2.carid, t2.querytime as querytime, t2.resulttime as resulttime, t2.qid, sum(tollAccAlerts.toll) AS toll INTO accountBalancelast FROM  accountBalancetime2 as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.resulttime > tollAccAlerts.time GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$logger->info( "Calculating max times for cars not having any tollAccAlerts in the 60 second window prior to a type 2 request.");
$dbquery="SELECT t2.carid, t2.time as querytime, max(tollAccAlerts.time) as resulttime,  t2.qid, integer '0' as toll	INTO accountBalancetime60 FROM  accountBalancerequests as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.time-60 >= tollAccAlerts.time GROUP BY t2.carid, t2.time, t2.qid ORDER BY t2.carid, t2.time;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$logger->info( "Deleting from above calculation any cars already appearing in answer");
$dbquery="DELETE FROM accountBalancetime60 WHERE accountBalancetime60.qid=accountBalancetime0.qid;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$dbquery="DELETE FROM accountBalancetime60 WHERE accountBalancetime60.qid=accountBalancetime10.qid;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$dbquery="DELETE FROM accountBalancetime60 WHERE accountBalancetime60.qid=accountBalancetime2.qid;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$logger->info( "Calculating tolls for the times where carids do not have tollAccAlerts in the 60 second window prior to a type 2 request.");
$dbquery="SELECT t2.carid, t2.querytime as querytime, t2.resulttime as resulttime, t2.qid, sum(tollAccAlerts.toll) AS toll INTO accountBalanceancient	FROM  accountBalancetime60 as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid and t2.resulttime > tollAccAlerts.time GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$logger->info( "Calculating queries that have no tollAccAlerts.");
$dbquery="DELETE FROM accountBalancetime60 WHERE accountBalanceancient.qid=accountBalancetime60.qid;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$logger->info( "Setting balance to zero for carids that have a type 2 request but no previous tollAccAlerts.");
$dbquery="SELECT t2.carid, t2.time as querytime, min (tollAccAlerts.time) as resulttime,  t2.qid,  integer '0' AS toll INTO accountBalancequeryatenterance FROM  accountBalancerequests as t2, tollAccAlerts WHERE  t2.carid=tollAccAlerts.carid	GROUP BY t2.carid, t2.time, t2.qid ORDER BY t2.carid, t2.time;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$dbquery="DELETE FROM accountBalancequeryatenterance WHERE querytime<>resulttime;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$dbquery="DELETE FROM accountBalancetime2 WHERE accountBalancelast.qid=accountBalancetime2.qid;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

$logger->info( "Creating answer in accountBalanceanswer table.");
$dbquery="SELECT * into accountBalanceanswer from accountBalancenow UNION select * from accountBalancemiddle UNION select * from accountBalancelast UNION select * from accountBalanceancient UNION select * from accountBalancequeryatenterance UNION select * FROM accountBalancetime2 UNION select * from accountBalancetime60;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;
$logger->info( "Type 2: Finished Calculating accountBalanceanswer except for special cases too expensive to calculate.");
