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
#      Nga         8/27/04        Split expressways
####################################################################

# Since the original script uses a lot of `system` calls and called scripts
# they return with `exit` if they fail because `die` only makes sense if
# code is moved into functions and modules which are invoked as functions
# (in try-catch blocks or something similar).

use strict;
use warnings;
use DBI;
use FileHandle;

use Getopt::Long;
use Log::Log4perl qw(:easy);

Log::Log4perl->easy_init($DEBUG);
my $logger = Log::Log4perl->get_logger('lrb_validator.validate');

my $verbose = 'Print verbose informtion';	# option variable with default value (false)
my $debug = 'Print debug information (implies verbose)';	# option variable with default value (false)
my $propertyfile = ''; # Path to properties file

# somehow declaring sub arguments fails because the argument names are not declared (-> sense?)
sub properties_file_arg {
    $propertyfile = $_[0];
}

# apparently non-option arguments can only be specified using subroutine being
# associated with '<>'<ref>http://perldoc.perl.org/Getopt/Long.html</ref>
GetOptions ('verbose' => \$verbose, 'debug' => \$debug, '<>' => \&properties_file_arg)
or die("Option parsing failed due to previously indicated error"); # GetOptions writes error messages
    # with warn() and die(), so they should be definitely displayed

if ( $propertyfile eq '' ) {
    die "A path to a properties file has to be specified as non-option argument";
}

my $currLine;
my @currProp;

my $dbhost;
my $dbname;
my $dbuser;
my $dbpassword;
my $logfile;
my $logvar;

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

	if ( $currProp[0] eq "keeplog") {
		$logvar=$currProp[1];
	}
	if ( $currProp[0] eq "logfile") {
		$logfile=$currProp[1];
	}
	if ( $currProp[0] eq "databasename") {
		$dbname=$currProp[1];
	}
	if ( $currProp[0] eq "databaseusername") {
		$dbuser=$currProp[1];
	}
	if ( $currProp[0] eq "databasepassword") {
		$dbpassword=$currProp[1];
	}
    if ( $currProp[0] eq "databasehost") {
        $dbhost=$currProp[1];
        $logger->info("using database host $dbhost");
    }

}
close ( PROPERTIES );

#print "$dbname\n";
#print "$dbuser\n";
#print "$dbpassword\n";
#print "$logfile\n";
#print "$logvar\n";
#print "$carDataInput\n";
#print "$accountbalance\n";
#print "$dailyexpenditure\n";
#print "$completeHistory\n";
#print "$tollAlerts\n";
#print "$accidentalerts\n";

my $startTime = time;

system ("perl dropalltables.pl $dbname $dbhost $dbuser $dbpassword $logfile $logvar") == 0 or $logger->logdie("dropalltables.pl failed (see preceeding output for details)");
$logger->info("Drop table done");

system ("perl import.pl $propertyfile") == 0 or $logger->logdie("import.pl failed (see preceeding output for details)");
$logger->info( "Import done");

system ("perl indexes.pl $dbname $dbhost $dbuser $dbpassword $logfile $logvar") == 0 or $logger->logdie("indexes.pl failed (see preceeding output for details)");
$logger->info( "Indexes done");

# Generate alerts
system ("perl xwayLoop.pl $dbname $dbhost $dbuser $dbpassword $logfile $logvar") == 0 or $logger->logdie("xwayLoop.pl failed (see preceeding output for details)");
$logger->info( "Loop done");

#--> IGOR:  All this stuff should move to xwayLoop.pl if you want to split, otherwise it is OK
# Split types
system("perl splitbytype.pl $dbname $dbhost $dbuser $dbpassword $logfile $logvar") == 0 or $logger->logdie("splitbytype.pl failed (see preceeding output for details)");
$logger->info( "split by type done");

system ("perl accountBalanceAnswer.pl $dbname $dbhost $dbuser $dbpassword $logfile $logvar") == 0 or $logger->logdie("accountBalanceAnswer.pl failed (see preceeding output for details)");
$logger->info( "account Balance done");

system ("perl dailyExpenditureAnswer.pl $dbname $dbhost $dbuser $dbpassword $logfile $logvar") == 0 or $logger->logdie("dailyExpenditureAnswer.pl failed (see preceeding output for details)");
$logger->info( "Daily expenditure done");


# Validation
system("perl compareAlerts.pl  $dbname $dbhost $dbuser $dbpassword $logfile $logvar") == 0 or $logger->logdie("validation in compareAlters.pl failed (see preceeding output for details)");
$logger->info( "compare alerts table done");

system ("perl accountBalanceValidation.pl $dbname $dbhost $dbuser $dbpassword $logfile $logvar") == 0 or $logger->logdie("validation in accountBalanceValidation.pl failed (see preceeding output for details)");
$logger->info( "accountBalanceValidation.pl done");

system ("perl dailyExpenditureValidation.pl $dbname $dbhost $dbuser $dbpassword $logfile $logvar") == 0 or $logger->logdie("validation in dailyExpenditureValidation.pl failed (see preceeding output for details)");
$logger->info( "dailyExpenditureValidation.pl done");


my $runningTime = time - $startTime;
$logger->info( "Total running time: $runningTime");
