#!/usr/bin/perl -w

####################################################################
# Author	:	Igor Pendan
# Date  	:	2004
# Purposes	:
# Modified history :
#      Name        Date           Comment
#      -------     -----------    ---------------------------------
#      Nga         8/27/04        Split expressways
####################################################################
use DBI;
use strict;
use FileHandle;

@ARGV == 1 or die ("Usage: validate.pl [PROPERTIES FILE with path]");
my $propertyfile=$ARGV[0];
my $currLine;
my @currProp;

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

system ("perl dropalltables.pl $dbname $dbuser $dbpassword $logfile $logvar");
print "Drop table done\n";

system ("perl import.pl $propertyfile");
print "Import done\n";

system ("perl indexes.pl $dbname $dbuser $dbpassword $logfile $logvar");
print "Indexes done\n";

# Generate alerts
system ("perl xwayLoop.pl $dbname $dbuser $dbpassword $logfile $logvar");
print "Loop done\n";

#--> IGOR:  All this stuff should move to xwayLoop.pl if you want to split, otherwise it is OK
# Split types
system("perl splitbytype.pl $dbname $dbuser $dbpassword $logfile $logvar");
print "split by type done\n";

system ("perl accountBalanceAnswer.pl $dbname $dbuser $dbpassword $logfile $logvar");
print "account Balance done\n";

system ("perl dailyExpenditureAnswer.pl $dbname $dbuser $dbpassword $logfile $logvar");
print "Daily expenditure done\n";


# Validation
system("perl compareAlerts.pl  $dbname $dbuser $dbpassword $logfile $logvar");
print "compare alerts table done\n";

system ("perl accountBalanceValidation.pl $dbname $dbuser $dbpassword $logfile $logvar");
print "accountBalanceValidation.pl done\n";

system ("perl dailyExpenditureValidation.pl $dbname $dbuser $dbpassword $logfile $logvar");
print "dailyExpenditureValidation.pl done\n";


my $runningTime = time - $startTime;
print "Total running time: $runningTime\n";


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
