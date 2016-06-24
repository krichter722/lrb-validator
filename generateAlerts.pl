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
# Author	:	Nga Tran
# Date          :	Aug, 2004
# Purposes	:
#	. Generate toll alerts and accident alerts
# Modified history :
#      Name        Date           Comment
#      -------     -----------    ---------------------------------
#      Nga         8/24/04        Generate for only 1 expresss way
####################################################################


use strict;
use DBI qw(:sql_types);
use FileHandle;
use Log::Log4perl qw(:easy);

Log::Log4perl->easy_init($DEBUG);
my $logger = Log::Log4perl->get_logger('lrb_validator.import');

# Arguments
my @arguments = @ARGV;
my $dbname = shift(@arguments);
my $dbhost = shift(@arguments);
my $dbuser = shift(@arguments);
my $dbpassword = shift(@arguments);
my $logFile =  shift(@arguments);
my $logVar =  shift(@arguments);

my $startTime = time;

#BEGIN {
#    open (STDERR, ">>execution.log");
#}

$logger->info("generateAlerts in progess ...");

system ("perl runDdl.pl $dbname $dbhost $dbuser $dbpassword $logFile $logVar") == 0 or $logger->logdie("runDdl.pl failed (see preceeding output for details)");
system ("perl extractAccidents.pl $dbname $dbhost $dbuser $dbpassword $logFile $logVar") == 0 or $logger->logdie("extractAccidents.pl failed (see preceeding output for details)");
system ("perl insertStatistics.pl  $dbname $dbhost $dbuser $dbpassword $logFile $logVar") == 0 or $logger->logdie("insertStatistics.pl failed (see preceeding output for details)");
system ("perl extractNumVehicles.pl $dbname $dbhost $dbuser $dbpassword $logFile $logVar") == 0 or $logger->logdie("extractNumVehicles.pl failed (see preceeding output for details)");
system ("perl extractLavs.pl  $dbname $dbhost $dbuser $dbpassword $logFile $logVar") == 0 or $logger->logdie("extractLavs.pl failed (see preceeding output for details)");
system ("perl calculateTolls.pl  $dbname $dbhost $dbuser $dbpassword $logFile $logVar") == 0 or $logger->logdie("calculateTolls.pl failed (see preceeding output for details)");
system ("perl createAlerts.pl  $dbname $dbhost $dbuser $dbpassword $logFile $logVar") == 0 or $logger->logdie("createAlerts.pl failed (see preceeding output for details)");

my $runningTime = time - $startTime;
$logger->info("Total generateAlerts running time: $runningTime seconds");

exit(0);
