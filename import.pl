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

use DBI;
use strict;
use FileHandle;

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
my $dbuser;
my $dbpassword;
my $logfile;

#BEGIN {
#	open (STDERR, ">execution.log");
#}

#******************** Import properties
open( PROPERTIES , "$propertyfile") || die("Could not open file: $!");
while (  $currLine = <PROPERTIES>){
	chomp ( $currLine );
        
        if (!$currLine){
	    next;
	}

	@currProp=split( /=/, $currLine  );

	if ( $currProp[0]  eq "cardatainput") {
		$carDataInput=$currProp[1];
#                print "$carDataInput\n";
	}
	if ( $currProp[0] eq "dailyexpenditure") {
		$dailyExpenditure=$currProp[1];
#                 print "$dailyExpenditure\n";
	}
	if ( $currProp[0] eq "accountbalance") {
		$accountBalance=$currProp[1];
#                 print "$accountBalance\n";
	}
	if ( $currProp[0] eq "completehistory") {
		$completeHistory=$currProp[1];
#                 print "$completeHistory\n";
	}	
	if ( $currProp[0] eq "outputtollalerts") {
		$tollAlerts=$currProp[1];
#                 print "$tollAlerts\n";
	}
	if ( $currProp[0] eq "keeplog") {
		$logvar=$currProp[1];
#                 print "$logvar\n";
	}
	if ( $currProp[0] eq "logfile") {
		$logfile=$currProp[1];
#                print "$logfile\n";
	}
	if ( $currProp[0] eq "databasename") {
		$dbname=$currProp[1];
#                 print "$dbname\n";
	}	
	if ( $currProp[0] eq "databaseusername") {
		$dbuser=$currProp[1];
#                 print "$dbuser\n";
	}
	if ( $currProp[0] eq "databasepassword") {
		$dbpassword=$currProp[1];
#                 print "$dbpassword\n";
	}
	if ( $currProp[0] eq "outputaccidentalerts") {
	         $accidentalerts=$currProp[1];
#                 print "$accidentalerts\n";
        }
}
close ( PROPERTIES );

my $dbquery;
my $sth;    

my $dbh  = DBI->connect(
            "DBI:PgPP:$dbname", "$dbuser", "$dbpassword",
            {PrintError => 1}
          ) || die "Could not connect to database:  $DBI::errstr";


# check all files (if they are present) prior to continueing...just open and close
open( TEMP , "$carDataInput") || die("Could not open file: $!");
close( TEMP );
open( TEMP , "$dailyExpenditure") || die("Could not open file: $!");
close( TEMP );
open( TEMP , "$accountBalance") || die("Could not open file: $!");
close( TEMP );
open( TEMP , "$completeHistory") || die("Could not open file: $!");
close( TEMP );
open( TEMP , "$tollAlerts") || die("Could not open file: $!");
close( TEMP );
		

## Import all of the files we need into postgres (first create the table)
##Import the input cardatapoints used to feed database

writeToLog ( $logfile, $logvar, "Creating input table.");
$dbquery="CREATE TABLE input ( type integer, time integer, carid integer, speed integer, xway integer, lane integer, dir integer, seg integer, pos integer, qid integer, m_init integer, m_end integer, dow integer, tod integer, day integer );";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Importing input table data.");
$dbquery="copy input from '$carDataInput' using delimiters ','";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

## Import the results of the database taking the benchmark
writeToLog ( $logfile, $logvar, "Creating output table for type 2. This table contains output of type 2 from db running the benchmark.");
$dbquery="CREATE TABLE outputAccountBalance ( type integer, time integer, emit integer, qid integer, ResultTime integer, bal integer );";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Importing output of accountBalance data.");
$dbquery="COPY outputAccountBalance FROM '$accountBalance' USING delimiters ','";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Creating output table for type 3. This table contains output of type 3 from db running the benchmark.");
$dbquery="CREATE TABLE outputDailyExpenditure ( type integer, time integer, emit text, qid integer, bal integer );";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Importing output of dailyExpenditure data.");
$dbquery="COPY outputDailyExpenditure FROM '$dailyExpenditure' USING delimiters ','";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

## Import History
writeToLog ( $logfile, $logvar, "Creating complete history table.");
$dbquery="CREATE TABLE completehistory ( carid integer, day integer, xway integer, toll integer );";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Importing complete history data.");
$dbquery="COPY completehistory FROM '$completeHistory' USING delimiters ','";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

## Import toll alerts table
writeToLog ( $logfile, $logvar, "Creating tollalerts table.");
$dbquery="CREATE TABLE tollalerts ( type integer, carid integer, time integer, emit text, lav integer, toll integer );";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

## Import toll alerts table
writeToLog ( $logfile, $logvar, "Creating accidentalerts table.");
$dbquery="CREATE TABLE accidentAlerts ( type integer, time integer, emit text, carid integer, seg integer );";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Importing output for accident alerts.");
$dbquery="copy accidentAlerts from '$accidentalerts' using delimiters ','";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;

writeToLog ( $logfile, $logvar, "Importing output for tollalerts.");
$dbquery="copy tollAlerts from '$tollAlerts' using delimiters ','";
$sth=$dbh->prepare("$dbquery") or die $DBI::errstr;
$sth->execute;


writeToLog ( $logfile, $logvar, "Importing complete.");


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
