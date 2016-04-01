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
my $logger = Log::Log4perl->get_logger('lrb_validator.dropalltables');

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
