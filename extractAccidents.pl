#!/usr/bin/perl -w

####################################################################
# Author	:	Nga Tran
# Date		:	Aug, 2004
# Purposes	:
#	. Extract accidents and put them into the ACCIDENT table
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

writeToLog($logFile, $logVar, "exractAccidents IN PROGRESS\n");

# Constants
my $EMITTED_DURATION = 30;	# 30 seconds
my $ACCIDENT_EMITTED_TIME = 4;  # 4 times
my $ACCIDENT_DURATION = $EMITTED_DURATION * $ACCIDENT_EMITTED_TIME;

# Connect to Postgres database
my $dbh = DBI->connect(
            "DBI:PgPP:$dbName", "$userName", "$password",
            {PrintError => 0, AutoCommit => 0}
          ) || die "Could not connect to database:  $DBI::errstr";
eval {

   my $startTime = time;

   initData($dbh);
   createTmpTables($dbh);
   cutDownTuples($dbh);
   createIndexes($dbh);
   extractAccidentTuples($dbh);
   getAccidentsInfo($dbh);   
   dropTmpTables($dbh);

   my $endTime = time;
   my $runningTime =  $endTime - $startTime;
   writeToLog($logFile, $logVar, "Total extractAccidents running time:  $runningTime seconds\n\n");
};
print $@;   # Print out errors

$dbh->disconnect;

exit(0);

#------------------------------------------------------------------------

sub initData
{
   my ($dbh) = @_;

   # Initialize data
   # Delete all data in the ACCIDENT table
   $dbh->do("TRUNCATE TABLE accident;");
   $dbh->commit;
}

#------------------------------------------------------------------------

sub createTmpTables
{
   my ($dbh) = @_;  
 
#   $dbh->do("
#          CREATE TABLE preAccident1(
#               time integer,
#               carid integer,
#               xway integer,
#               lane integer,
#               dir integer,
#               seg integer,
#               pos integer
#           ); ");
           
#    $dbh->do("           
#           CREATE TABLE preAccident2(
#               carid1 integer,
#               carid2 integer,
#               time integer,
#               xway integer,
#               dir integer,
#               seg integer,
#               pos integer
#             ); ");
             
   $dbh->do("
          CREATE TABLE preAccident1(
               time integer,
               carid integer,
                lane integer,
               dir integer,
               seg integer,
               pos integer
           ); ");
           

    $dbh->do("           
           CREATE TABLE preAccident2(
               carid1 integer,
               carid2 integer,
               time integer,
                dir integer,
               seg integer,
               pos integer
             ); ");

    $dbh->commit;
}

#------------------------------------------------------------------------

sub dropTmpTables
{
   my ($dbh) = @_;
   $dbh->do("DROP TABLE preAccident1;");
   $dbh->do("DROP TABLE preAccident2;");
   $dbh->commit;
}

#------------------------------------------------------------------------
# Extract all tuples with speed = 0 and type = 0 and put them into
# "preAccident1" table. The tuples were reported from cars that may
# cause accidents. The preAccident is a temporary table and will be
# deleted at the end of this process
#------------------------------------------------------------------------

sub cutDownTuples
{
   my ($dbh) = @_;

   my $startTime = time;
   
#   my $sql =  "INSERT INTO preAccident1
#               SELECT  time,
#                       carid,
#                       xway,
#                       lane,
#                       dir,
#                       seg,
#                       pos
#               FROM    input
#               WHERE   speed = 0 AND                       
#                       type = 0;";

  my $sql =  "INSERT INTO preAccident1
               SELECT  time,
                       carid,
                       lane,
                       dir,
                       seg,
                       pos
               FROM    input
               WHERE   speed = 0 AND                       
                       type = 0;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;

   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     cutDownTuples running time:  $runningTime\n");
}
#------------------------------------------------------------------------
# Create Indexes for preAccident1
#------------------------------------------------------------------------

sub createIndexes
{
   my ($dbh) = @_;

   my $startTime = time;
   
#    $dbh->do("CREATE INDEX preAcc1Idx1
#             ON preAccident1 (carid, pos, xway, lane, dir);");

#    $dbh->do("CREATE INDEX preAcc1Idx2
#             ON preAccident1 (carid, pos, xway, lane, dir, time);");

#    $dbh->do("CREATE INDEX preAcc1Idx3
#             ON preAccident1 (pos, xway, lane, dir);");

#    $dbh->do("CREATE INDEX preAcc1Idx4
#             ON preAccident1 (pos, xway, lane, dir, time);");

    $dbh->do("CREATE INDEX preAcc1Idx1
             ON preAccident1 (carid, pos, lane, dir);");

    $dbh->do("CREATE INDEX preAcc1Idx2
             ON preAccident1 (carid, pos, lane, dir, time);");

    $dbh->do("CREATE INDEX preAcc1Idx3
             ON preAccident1 (pos, lane, dir);");

    $dbh->do("CREATE INDEX preAcc1Idx4
             ON preAccident1 (pos, lane, dir, time);");


   $dbh->commit;

   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     createIndexes running time:  $runningTime\n");
}
#------------------------------------------------------------------------
# Extract tuples of accident cars from "preAccident1" table
# This tuple from 2 different cars emitted same xway, lane, dir,
# seg and pos
# On extract tuples of real accident (after report same 
# position in 4 consecutive reports)
#------------------------------------------------------------------------

sub extractAccidentTuples
{
   my ($dbh) = @_;

   my $startTime = time;

#   my $sql =    "INSERT INTO preAccident2
#                 SELECT  in1.carid,
#                       in2.carid,
#                       in2.time,
#                       in1.xway,
#                       in1.dir,
#                       in1.seg,
#                       in1.pos
#               FROM    preAccident1 AS in1,
#                       preAccident1 AS in2,
#                       preAccident1 AS in11,
#                       preAccident1 AS in22
#               WHERE   in2.carid <> in1.carid AND
#                       in2.pos = in1.pos AND
#                       in2.xway = in1.xway AND
#                       in2.lane = in1.lane AND
#                       in2.dir = in1.dir AND
#                       in2.time >= in1.time AND 
#                       in2.time <= in1.time + $EMITTED_DURATION AND
#                       in11.carid = in1.carid AND
#                       in11.pos = in1.pos AND
#                       in11.xway = in1.xway AND
#                       in11.lane = in1.lane AND
#                       in11.dir = in1.dir AND
#                       in11.time = in1.time + $ACCIDENT_DURATION AND
#                       in22.carid = in2.carid AND
#                       in22.pos = in1.pos AND
#                       in22.xway = in1.xway AND
#                       in22.lane = in1.lane AND
#                       in22.dir = in1.dir AND
#                       in22.time = in2.time + $ACCIDENT_DURATION;";

   my $sql =    "INSERT INTO preAccident2
                 SELECT  in1.carid,
                       in2.carid,
                       in2.time,
                       in1.dir,
                       in1.seg,
                       in1.pos
               FROM    preAccident1 AS in1,
                       preAccident1 AS in2,
                       preAccident1 AS in11,
                       preAccident1 AS in22
               WHERE   in2.carid <> in1.carid AND
                       in2.pos = in1.pos AND                       
                       in2.lane = in1.lane AND
                       in2.dir = in1.dir AND
                       in2.time >= in1.time AND 
                       in2.time <= in1.time + $EMITTED_DURATION AND
                       in11.carid = in1.carid AND
                       in11.pos = in1.pos AND                       
                       in11.lane = in1.lane AND
                       in11.dir = in1.dir AND
                       in11.time = in1.time + $ACCIDENT_DURATION AND
                       in22.carid = in2.carid AND
                       in22.pos = in1.pos AND                       
                       in22.lane = in1.lane AND
                       in22.dir = in1.dir AND
                       in22.time = in2.time + $ACCIDENT_DURATION;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;

   $dbh->commit;
   
   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     extractAccidentTuples running time:  $runningTime\n");

}

#------------------------------------------------------------------------
# Extract tuples of accident cars from "preAccident2" table
# and put them into the "accident" table
# This tuples from 2 accident cars emitted same xway, lane, dir,
# seg and pos
# min(time) + 90: accident start time (after 4 consecutive reports)
# max(time) + 120: Last minute of the accident + 1
#------------------------------------------------------------------------

sub getAccidentsInfo
{
   my ($dbh) = @_;

   my $startTime = time;
   
#   my $sql =  "INSERT INTO accident
#               SELECT   min(carid1),
#                        max(carid2),
#                        trunc((min(time) + 90)/60) + 1, 
#                        trunc((max(time) + $ACCIDENT_DURATION)/60) + 1,
#                        xway,
#                        dir,
#                        seg, 
#                        pos
#               FROM     preAccident2
#               GROUP BY
#                        xway,
#                        dir,
#                        seg,
#                        pos;";

   my $sql =  "INSERT INTO accident
               SELECT   min(carid1),
                        max(carid2),
                        trunc((min(time) + 90)/60) + 1, 
                        trunc((max(time) + $ACCIDENT_DURATION)/60) + 1,
                        dir,
                        seg, 
                        pos
               FROM     preAccident2
               GROUP BY                        
                        dir,
                        seg,
                        pos;";

   my $statement = $dbh->prepare($sql);
   $statement->execute;

   $dbh->commit;
   
   my $runningTime =  time - $startTime;
   writeToLog($logFile, $logVar, "     getAccidentsInfo running time:  $runningTime\n");
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
