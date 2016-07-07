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
my $logger = Log::Log4perl->get_logger('lrb_validator.import');

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

## Compare counts to make sure they are the same.
## If counts aren't the same, the delete can still delete all from the wrong answer table--despite wrong answer.
$logger->info( "Comparing output and answer table sizes for type 3.");
	$dbquery="SELECT Count(*) AS CountOfqueryid FROM dailyExpenditureanswer;";
	$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
	$sth->execute;
my @answerCount = $sth->fetchrow_array;
	$dbquery="SELECT Count(*) AS CountOfqueryid FROM outputdailyExpenditure;";
	$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
	$sth->execute;
my @outputCount = $sth->fetchrow_array;


if ($answerCount[0]!=$outputCount[0] and $answerCount[0] ne $outputCount[0] ) {
	$logger->info( "Daily Expenditure validation failed! Your output has: $outputCount[0] tuples. The answer has: $answerCount[0] tuples.");
	exit(0);
}else {
	$logger->info( "Daily Expenditure count comparison ok. Total tuples in answer: $answerCount[0]. ");
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

	$logger->logdie( "Daily Expenditure validation failed! Wrong answers stored in dailyExpenditurewronganswers table.");
} else {
	$logger->info( "Daily Expenditure Validition Completed Successfully!");
	$dbquery="DROP TABLE dailyExpenditurewronganswers;";
	$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
	$sth->execute;
}
