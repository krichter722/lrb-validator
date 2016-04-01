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
# Date  	:	2004
# Purposes	:
# Modified history :
#      Name        Date           Comment
#      -------     -----------    ---------------------------------
#      Nga         8/31/04        Pass arguments
####################################################################


use strict;
use warnings;
use DBI;
use FileHandle;
use Log::Log4perl qw(:easy);

Log::Log4perl->easy_init($DEBUG);
my $logger = Log::Log4perl->get_logger('lrb_validator.import');

@ARGV == 1 or die ("Usage: import.pl [PROPERTIES FILE with path]");
my $propertyfile=$ARGV[0];
my $currLine;
my @currProp;

my $carDataInput;
my $accountBalance;
my $dailyExpenditure;
my $completeHistory;
my $tollAlerts;
my $accidentalerts;
my $logvar;
my $dbname;
my $dbhost;
my $dbuser;
my $dbpassword;
my $logfile;

my $input_table_name = "input";
my $account_balance_table_name = "outputAccountBalance";
my $daily_expenditure_table_name = "outputDailyExpenditure";
my $complete_history_table_name = "completehistory";
my $toll_alters_table_name = "tollalerts";
my $accident_alters_table_name = "accidentalerts";

#BEGIN {
#	open (STDERR, ">execution.log");
#}

#******************** Import properties
open( PROPERTIES , "$propertyfile") || $logger->logdie("Could not open configuration file '$propertyfile': $!");
while (  $currLine = <PROPERTIES>){
	chomp ( $currLine );

        if (!$currLine){
	    next;
	}

	@currProp=split( /=/, $currLine  );

	if ( $currProp[0]  eq "cardatainput") {
		$carDataInput=$currProp[1];
        $logger->info("using car data file input file $carDataInput");
	}
	if ( $currProp[0] eq "dailyexpenditure") {
		$dailyExpenditure=$currProp[1];
        $logger->info("using daily expenditure file $dailyExpenditure");
	}
	if ( $currProp[0] eq "accountbalance") {
		$accountBalance=$currProp[1];
        $logger->info("using account balance file $accountBalance");
	}
	if ( $currProp[0] eq "completehistory") {
		$completeHistory=$currProp[1];
        $logger->info("using complete history file $completeHistory");
	}
	if ( $currProp[0] eq "outputtollalerts") {
		$tollAlerts=$currProp[1];
        $logger->info("using toll alters file $tollAlerts");
	}
	if ( $currProp[0] eq "keeplog") {
		$logvar=$currProp[1];
        $logger->info("keeplog flag is $logvar");
	}
	if ( $currProp[0] eq "logfile") {
		$logfile=$currProp[1];
        $logger->info("using logfile $logfile");
	}
	if ( $currProp[0] eq "databasename") {
		$dbname=$currProp[1];
        $logger->info("using database name $dbname");
	}
	if ( $currProp[0] eq "databaseusername") {
        $dbuser=$currProp[1];
        $logger->info("using database user $dbuser");
	}
	if ( $currProp[0] eq "databasepassword") {
		$dbpassword=$currProp[1];
        $logger->info("using database password $dbpassword");
	}
	if ( $currProp[0] eq "outputaccidentalerts") {
        $accidentalerts=$currProp[1];
        $logger->info("using accident alters input file $accidentalerts");
    }
    if ( $currProp[0] eq "databasehost") {
        $dbhost=$currProp[1];
        $logger->info("using database host $dbhost");
    }
}
close ( PROPERTIES );

my $dbquery;
my $sth;

my $dbh  = DBI->connect(
            "DBI:Pg:dbname=$dbname;host=$dbhost", "$dbuser", "$dbpassword",
            {PrintError => 1}
          ) || $logger->logdie("Could not connect to database:  $DBI::errstr");

my $daily_expenditure_present = 1;
my $complete_history_present = 1;

# check all files (if they are present) prior to continueing...just open and close
open( TEMP , "$carDataInput") || do {
    close( TEMP ); # avoid leaking of file descriptors
    $logger->logdie("Could not open car data input file '$carDataInput': $!");
};
close( TEMP );
open( TEMP , "$dailyExpenditure") || do { $logger->warn("Could not open daily expenditure file '$dailyExpenditure': $!, skipping because it's a historical query only");
    $daily_expenditure_present = 0;
};
close( TEMP );
open( TEMP , "$accountBalance") || do {
    close( TEMP );
    $logger->logdie("Could not open account balance file '$accountBalance': $!");
};
close( TEMP );
open( TEMP , "$completeHistory") || do { $logger->warn("Could not open complete history file '$completeHistory': $!, skipping because it's a historical query only");
    $complete_history_present = 0;
};
close( TEMP );
open( TEMP , "$tollAlerts") || do {
    close( TEMP );
    $logger->logdie("Could not open toll alters file '$tollAlerts': $!");
};
close( TEMP );
open( TEMP , "$accidentalerts") || do {
    close( TEMP );
    $logger->logdie("Could not open accident alters file '$accidentalerts': $!");
};
close( TEMP );


## Import all of the files we need into postgres (first create the table)

# A sub to wrap the `copy [table] from [file path]` which is only available for
# user `root` and needs to be done by reading from stdin and putting line by
# line with `$dbh->pg_putcopydata`.
sub copy_file {
    my $dbh = shift;
    my $table = shift;
    my $file = shift;
    my $dbquery="copy $table from stdin using delimiters ','";
    #$dbquery="copy input from '$carDataInput' using delimiters ','"; # Using `COPY` with `from [file path]` is only allowed for `root`
    $dbh->do("$dbquery") or $logger->logdie($DBI::errstr);

    open my $info, $file or $logger->logdie("Could not open car data input file '$file': $!");
    while( my $line = <$info>)  {
        $dbh->pg_putcopydata($line) # $line already contains \n read from file
            or $logger->logdie("handling of line $line with COPY failed");
    }
    close $info;
    $dbh->pg_putcopyend() or $logger->logdie("finalization of COPY import failed");
}

##Import the input cardatapoints used to feed database
$logger->info ("Creating input table '$input_table_name'");
$dbquery="CREATE TABLE $input_table_name ( type integer, time integer, carid integer, speed integer, xway integer, lane integer, dir integer, seg integer, pos integer, qid integer, m_init integer, m_end integer, dow integer, tod integer, day integer );";
$sth=$dbh->prepare("$dbquery") or $logger->logdie($DBI::errstr);
$sth->execute or $logger->logdie("statement '$dbquery' failed");

$logger->info ("Importing input table data from '$carDataInput'.");
copy_file($dbh, $input_table_name, $carDataInput);

## Import the results of the database taking the benchmark
$logger->info ("Creating output table '$account_balance_table_name' for type 2. This table contains output of type 2 from db running the benchmark.");
$dbquery="CREATE TABLE $account_balance_table_name ( type integer, time integer, emit integer, qid integer, ResultTime integer, bal integer );";
$sth=$dbh->prepare("$dbquery") or $logger->logdie ($DBI::errstr);
$sth->execute or $logger->logdie("statement '$dbquery' failed");

$logger->info ("Importing output of account balance data from '$accountBalance'.");
copy_file($dbh, $account_balance_table_name, $accountBalance);

# daily expenditure
if ( $daily_expenditure_present == 0) {
    $logger->warn("skipping daily expenditure validation because file isn't present");
} else {
    $logger->info ( "Creating output table '$daily_expenditure_table_name' for type 3. This table contains output of type 3 from db running the benchmark.");
    $dbquery="CREATE TABLE $daily_expenditure_table_name ( type integer, time integer, emit text, qid integer, bal integer );";
    $sth=$dbh->prepare("$dbquery") or $logger->logdie ($DBI::errstr);
    $sth->execute or $logger->logdie("statement '$dbquery' failed");

    $logger->info ("Importing output of daily expenditure data from '$dailyExpenditure'.");
    copy_file($dbh, $daily_expenditure_table_name, $dailyExpenditure);
}

## Import History
if ($complete_history_present == 0) {
    $logger->warn("skipping complete history validation because file isn't present");
} else {
    $logger->info ("Creating complete history table '$complete_history_table_name'.");
    $dbquery="CREATE TABLE $complete_history_table_name ( carid integer, day integer, xway integer, toll integer );";
    $sth=$dbh->prepare("$dbquery") or $logger->logdie ($DBI::errstr);
    $sth->execute or $logger->logdie("statement '$dbquery' failed");

    $logger->info ("Importing complete history data from '$completeHistory'.");
    copy_file($dbh, $complete_history_table_name, $completeHistory);
}

## Import toll alerts table
$logger->info ("Creating tollalerts table '$toll_alters_table_name'.");
$dbquery="CREATE TABLE $toll_alters_table_name ( type integer, carid integer, time integer, emit text, lav integer, toll integer );";
$sth=$dbh->prepare("$dbquery") or $logger->logdie ($DBI::errstr);
$sth->execute or $logger->logdie("statement '$dbquery' failed");

$logger->info ("Importing output for toll alerts from '$tollAlerts'.");
copy_file($dbh, $toll_alters_table_name, $tollAlerts);

## Import accident alerts table
$logger->info ("Creating accidentalerts table '$accident_alters_table_name'.");
$dbquery="CREATE TABLE $accident_alters_table_name ( type integer, time integer, emit text, carid integer, seg integer );";
$sth=$dbh->prepare("$dbquery") or $logger->logdie ($DBI::errstr);
$sth->execute or $logger->logdie("statement '$dbquery' failed");

$logger->info ("Importing output for accident alerts from '$accidentalerts'.");
copy_file($dbh, $accident_alters_table_name, $accidentalerts);

$logger->info ("Import complete.");
