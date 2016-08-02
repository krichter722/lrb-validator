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
my $logger = Log::Log4perl->get_logger('lrb_validator.accountbalancevalidation');

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

#Creating wrong answers table, just a copy of all output at the moment
$dbquery="SELECT * into accountBalancewronganswers FROM outputaccountBalance;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

#Deleting all of the right answers from wrong answer table
$dbquery="DELETE FROM accountBalancewronganswers WHERE accountBalancewronganswers.qid=accountBalanceanswer.qid and accountBalancewronganswers.bal=accountBalanceanswer.toll and accountBalancewronganswers.resulttime=accountBalanceanswer.resulttime;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;


#Selecting for validation
$dbquery="SELECT qid, resulttime, bal FROM accountBalancewronganswers LIMIT 50;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

my @accountBalancecomparison = $sth->fetchrow_array;

if (!@accountBalancecomparison){
	$dbquery="select i.carid, i.time, i.qid  into temp1 from input as i, accountBalancewronganswers as wr where i.qid=wr.qid and i.time=wr.time;";
	$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
	$sth->execute;

	$dbquery="select temp1.carid, temp1.time, temp1.qid into temp2 from temp1, input where temp1.carid=input.carid and input.time=temp1.time and input.lane=4;";
	$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
	$sth->execute;

	$dbquery="delete from accountBalancewronganswers where accountBalancewronganswers.qid=temp2.qid and temp2.time=accountBalancewronganswers.resulttime";
	$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
	$sth->execute;

       $dbh->do("DROP TABLE temp1;");
       $dbh->do("DROP TABLE temp2;");
}
$dbquery="SELECT qid, resulttime, bal FROM accountBalancewronganswers LIMIT 50;";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute;

@accountBalancecomparison = $sth->fetchrow_array;

# If wrong answer table isn't empty, it'll print to log
if ( !@accountBalancecomparison){

	$logger->info("Account Balance Validation failed All incorrect results are stored in accountBalancewronganswers table.");
	$logger->info("Following are some incorrect Account Balanc results. Fields listed are: qid, resulttime, balance.");
	$logger->info(join( ',', @accountBalancecomparison));
	while (@accountBalancecomparison = $sth->fetchrow_array)  {
		$logger->info(join( ',', @accountBalancecomparison));
	}
	exit (0);
}else {
	$dbquery="DROP TABLE accountBalancewronganswers;";
	$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
	$sth->execute;
}
#$dbquery="DROP TABLE accountBalanceancient;DROP TABLE accountBalancelast;DROP TABLE accountBalancemiddle;DROP TABLE accountBalancenow;
#	DROP TABLE accountBalancequeryatenterance;DROP TABLE accountBalancetime0;
#	DROP TABLE accountBalancetime1;DROP TABLE accountBalancetime10;DROP TABLE accountBalancetime2;DROP TABLE accountBalancetime60;
#	DROP TABLE accountBalancetimeeq;";
#	$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
#	$sth->execute;

       $dbh->do("DROP TABLE accountBalanceancient;");
       $dbh->do("DROP TABLE accountBalancelast;");
       $dbh->do("DROP TABLE accountBalancemiddle;");
       $dbh->do("DROP TABLE accountBalancenow;");
       $dbh->do("DROP TABLE accountBalancequeryatenterance;");
       $dbh->do("DROP TABLE accountBalancetime0;");
       $dbh->do("DROP TABLE accountBalancetime1;");
       $dbh->do("DROP TABLE accountBalancetime10;");
       $dbh->do("DROP TABLE accountBalancetime2;");
       $dbh->do("DROP TABLE accountBalancetime60;");
       $dbh->do("DROP TABLE accountBalancetimeeq;");


	$logger->info("Account Balance Validition Completed Successfully!");

$dbh->disconnect;
