#!/usr/bin/perl -w

####################################################################
# Author	:	Nga Tran
# Date  	:	Aug, 2004
# Purposes	:
#	. Extract numbver of vehicles from input table
# Modified history :
#      Name        Date           Comment
#      -------     -----------    ---------------------------------
#      Nga         8/24/04        Generate for only 1 expresss way
####################################################################


use strict;
use DBI qw(:sql_types);
use FileHandle;

#BEGIN {
#    open (STDERR, ">>execution.log");
#}

# Process arguments
my @arguments = @ARGV;
my $dbName = shift(@arguments);
my $userName = shift(@arguments);
my $password = shift(@arguments);
my $logFile = shift(@arguments);
my $logVar = shift(@arguments);

writeToLog($logFile, $logVar, "extractNumVehicles in progress ...\n");

# Connect to Postgres database
my $dbh = DBI->connect(
            "DBI:PgPP:$dbName", "$userName", "$password",
            {PrintError => 1, AutoCommit => 0}
          ) || die "Could not connect to database:  $DBI::errstr";


eval {
   my $startTime = time;

   createTmpTables($dbh);
   preVehicles($dbh);
   indexPreVehicles($dbh);
   vehicles($dbh);
   noVehicles($dbh);
   dropTmpTables($dbh);   


   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "Total extractNumVehicles running time:  $runningTime\n\n");   
};
print $@;   # Print out errors
$dbh->disconnect;
exit(0);


#------------------------------------------------------------------------
# Create all temporary tables
#------------------------------------------------------------------------

sub createTmpTables
{
   my ($dbh) = @_;  
          
#   $dbh->do("
#          CREATE TABLE preVehicle(
#               xway integer,
#               dir  integer,
#               seg  integer,
#               minute integer,
#               numVehicles  integer);");               

   $dbh->do("
          CREATE TABLE preVehicle(
               dir  integer,
               seg  integer,
               minute integer,
               numVehicles  integer);");               
           
    $dbh->commit;
}

#------------------------------------------------------------------------
# Drop all temporary tables
#------------------------------------------------------------------------

sub dropTmpTables
{
   my ($dbh) = @_;
   $dbh->do("DROP TABLE  preVehicle;");   
   $dbh->commit;
}

#------------------------------------------------------------------------
# Calculate numVehicles for (xway, dir, seg) fro previous minute
# This function is for performance purpose
#------------------------------------------------------------------------
sub preVehicles
{
   my ($dbh) = @_;

   my $startTime = time;

  # Extract number of vehicles in an
  # (express way, direction, segment, minute)

#   my $sql =  "INSERT INTO  preVehicle(xway, dir, seg, minute, numvehicles)
#              SELECT   xway,
#                       dir,
#                       seg,
#                       trunc(time/60) + 2,
#                       count(distinct carid)
#              FROM     input
#              WHERE    type = 0
#              GROUP BY xway, dir, seg, trunc(time/60);";

   my $sql =  "INSERT INTO  preVehicle(dir, seg, minute, numvehicles)
              SELECT   dir,
                       seg,
                       trunc(time/60) + 2,
                       count(distinct carid)
              FROM     input
              WHERE    type = 0
              GROUP BY dir, seg, trunc(time/60);";


   my $statement = $dbh->prepare($sql);
   $statement->execute;      
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar,"     preVehicles running time:  $runningTime\n");
}


#------------------------------------------------------------------------
# Create index for preVehicle Table
#------------------------------------------------------------------------

sub indexPreVehicles
{
   my ($dbh) = @_;

   my $startTime = time;
   
#   $dbh->do("CREATE UNIQUE INDEX preVehicleIdx1
#             ON preVehicle (xway, dir, seg, minute);");

   $dbh->do("CREATE UNIQUE INDEX preVehicleIdx1
             ON preVehicle (dir, seg, minute);");

   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     indexPreVehicles running time:  $runningTime\n");
}

#------------------------------------------------------------------------
# Update numVehicles for statistics table
#------------------------------------------------------------------------
sub vehicles
{
   my ($dbh) = @_; 

   my $startTime = time;

#   my $sql =  "UPDATE statistics
#               SET    numVehicles = (SELECT numVehicles
#                             FROM   preVehicle
#                             WHERE  statistics.xway = preVehicle.xway AND
#                                    statistics.dir = preVehicle.dir AND
#                                    statistics.seg = preVehicle.seg AND
#                                    statistics.minute = preVehicle.minute);";

   my $sql =  "UPDATE statistics
               SET    numVehicles = (SELECT numVehicles
                             FROM   preVehicle
                             WHERE  statistics.dir = preVehicle.dir AND
                                    statistics.seg = preVehicle.seg AND
                                    statistics.minute = preVehicle.minute);";

   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     vehicles running time:  $runningTime\n");
}


#------------------------------------------------------------------------
# Update numVehicles for statistics table
#------------------------------------------------------------------------
sub noVehicles
{
   my ($dbh) = @_; 

   my $startTime = time;

   my $sql =  "UPDATE statistics
               SET    numVehicles = 0
               WHERE  numVehicles IS NULL;";


   my $statement = $dbh->prepare($sql);
   $statement->execute;
   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     no vehicles running time:  $runningTime\n");
}
#--------------------------------------------------------------------------------

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
