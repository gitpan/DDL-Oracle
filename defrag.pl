#! /usr/bin/perl -w

# $Id: defrag.pl,v 1.7 2000/12/02 14:06:20 rvsutherland Exp $
#
# Copyright (c) 2000 Richard Sutherland - United States of America
#
# See COPYRIGHT section in pod text below for usage and distribution rights.
#

use Cwd;
use DBI;
use DDL::Oracle;
use English;
use Getopt::Long;

use strict;

my %args;
my %uniq;

my @export_objects;
my @export_temps;
my @sizing_array;

my $add_temp_log;
my $add_temp_sql;
my $alttblsp;
my $aref;
my $create_ndx_ddl;
my $create_tbl_ddl;
my $create_temp_ddl;
my $date;
my $dbh;
my $drop_ddl;
my $drop_temp_ddl;
my $drop_temp_log;
my $drop_temp_sql;
my $exchange_query;
my $expdir;
my $gzip;
my $home = $ENV{HOME}
        || $ENV{LOGDIR}
        || ( getpwuid($REAL_USER_ID) )[7]
        || die "\nCan't determine HOME directory.\n";
my $index_query;
my $iot_query;
my $logdir;
my $obj;
my $other_constraints;
my $partitions;
my $prefix;
my $prttn_exp_log;
my $prttn_exp_par;
my $prttn_exp_text;
my $prttn_imp_log;
my $prttn_imp_par;
my $prttn_imp_text;
my $row;
my $script;
my $shell;
my $sqldir;
my $sth;
my $stmt;
my $table_query;
my $tblsp;
my $text;
my $user = getlogin
        || scalar getpwuid($REAL_USER_ID)
        || undef;


########################################################################

set_defaults();

if (
         @ARGV    == 0
     or  $ARGV[0] eq "?"
     or  $ARGV[0] eq "-?"
     or  $ARGV[0] eq "-h"
     or  $ARGV[0] eq "--help"
   ) 
{
  print_help();
  exit 0;
}
else 
{
  get_args();
}

print "\n$0 is being executed by $user\non ", scalar localtime,"\n\n";

#
# Display user options, and save them in .defrag.rc
#

delete $args{ sid } if $args{ sid } eq "";
open RC, ">$home/.defragrc" or die "Can't open .defrag.rc:  $!\n";
KEY: foreach my $key ( sort keys %args ) 
{
  next KEY unless (
                       $key eq "sid"
                    or $key eq "logdir"
                    or $key eq "sqldir"
                    or $key eq "prefix"
                    or $key eq "expdir"
                    or $key eq "resize"
                  );
  print "$key = $args{ $key }\n";
  print RC "$key = $args{ $key }\n";
}
close RC or die "Can't close .defrag.rc:  $!\n";
print "\n";

########################################################################

#
# Now we're ready -- start dafriggin' defraggin'
#

print "Generating files to defrag Tablespace $tblsp.\n",
      "Using Tablespace $alttblsp for partition operations.\n\n";

print "Working...\n\n";

initialize_queries();

# The 9 steps below issue queries mostly comprised of 4 main queries,
# sometimes doing UNIONs and/or MINUSes among them.
# See sub 'initialize_queries' for the queries and their descriptions.
#

# Step 1 - Export the stray partitions -- those in our tablespace whose
#          table also has partitions in at least one other tablespace.
#          Using this option, there will be 2 exports.  After the first
#          export, for each such partition:
#            a) Create a Temp table mirroring the partition.
#            b) Create indexes on the Temp table matching the LOCAL 
#               indexes on the partitioned table.
#            c) Create a PK matching the PK of the partitioned table,
#               if any.
#            d) EXCHANGE the Temp table with the partition.
#
#          With the data now in the Temp table, the Temp table gets 
#          treated the same as other regular tables in our tablespace
#          (see Steps 2 - 9), but has added operations following the
#          creation of its indexes (same as the LOCAL indexes on the 
#          partition) and the addition of its PK (if any).
#
#            a) the Temp table does an EXCHANGE PARTITION so that the
#               data (which was imported into the Temp table) rejoins
#               the partitioned table.
#            b) the [now empty] Temp table is DROPped.
#
#            c) REBUILD all Global indexes (if any) on the partitioned
#               table(s).
#
#          NOTE:  Two 'fall back' scripts are created which are to be
#                 used ONLY in the event that problems occur during
#                 the CTAS step (Shell #2 when using this option).
#
#                  ***  DO NOT PROCEED IF Shell #2 HAS ERRORS ***
#
#                 Shells #8 and #9  will restore the data to the original
#                 condition Their Steps are:
#                   a) DROP the Temp table(s).
#                   b) TRUNCATE the partitions
#                   c) MOVE the partitions back to our tablespace
#                   d) Import the data back into the partitions.
#

$sth = $dbh->prepare( $exchange_query );
$sth->execute;
$aref = $sth->fetchall_arrayref;

foreach $row ( @$aref )
{
  my ( $owner, $table, $partition, $type ) = @$row;

  $obj = DDL::Oracle->new(
                           type => 'exchange table',
                           list => [
                                     [
                                       "$owner",
                                       "$table:$partition",
                                     ]
                                   ],
                         );
  my $create_tbl = $obj->create;
  # Remove REM lines created by DDL::Oracle
  $create_tbl = ( join "\n",grep !/^REM/,split /\n/,$create_tbl )."\n\n";

  my $temp = "${tblsp}_${date}_" . unique_nbr();

  push @export_temps,   "\L$owner.$table:$partition";
  push @export_objects, "\L$owner.$temp";

  # Change the CREATE TABLE statement to create the temp
  $create_tbl =~ s|\L$owner.$table|\L$owner.$temp|g;

  my $exchange = index_and_exchange( $temp, @$row );

  $obj = DDL::Oracle->new(
                           type => 'table',
                           list => [
                                     [
                                       "$owner",
                                       "$temp",
                                     ]
                                   ],
                         );
  my $drop_tbl = $obj->drop;
  # Remove REM lines created by DDL::Oracle
  $drop_tbl = ( join "\n", grep !/^REM/, split /\n/, $drop_tbl ) . "\n\n";

  my $drop_temp = $drop_tbl .
                  trunc( @$row ) .
                  move ( @$row, $tblsp );

  $create_temp_ddl  = group_header( 1 )   unless $create_temp_ddl;
  $create_temp_ddl .= $create_tbl .
                      $exchange .
                      move ( @$row, $alttblsp );

  $drop_ddl         = group_header( 2 )   unless $drop_ddl;
  $drop_ddl        .= $drop_tbl;

  $create_tbl_ddl   = group_header( 7 )   unless $create_tbl_ddl;
  $create_tbl_ddl  .= $create_tbl;

  $create_ndx_ddl   = group_header( 9 )   unless $create_ndx_ddl;
  $create_ndx_ddl  .= $exchange .
                      $drop_tbl;      

  $drop_temp_ddl    = group_header( 15 )  unless $drop_temp_ddl;
  $drop_temp_ddl   .= $drop_temp;
}

#
# Step 2 - Drop all Foreign Keys referenceing our tables and IOT's or
#          referenceing the tables of our other indexes.  NOTE:  our
#          indexes may not be the target of a foreign key, but for 
#          simplicity purposes if the index's table holds said target
#          (i.e., its index is in some other tablespace but it belongs
#          to the same table as our index), we'll drop the FK anyway --
#          it won't hurt anything and we promise to put it back.
#

$stmt =
      "
       SELECT
              c.owner
            , c.constraint_name
       FROM
              dba_constraints  c
            , dba_constraints  r
       WHERE
                  c.constraint_type      = 'R'
              AND c.r_owner              = r.owner
              AND c.r_constraint_name    = r.constraint_name
              AND (
                      r.owner
                    , r.table_name
                  ) IN (
                         SELECT
                                owner
                              , table_name
                         FROM
                              (
                                $table_query
                                UNION ALL
                                $iot_query
                                UNION ALL
                                SELECT
                                       owner
                                     , table_name
                                FROM
                                     (
                                       $index_query
                                     )
                              )
                       )
       ORDER
          BY
              1, 2
      ";

$sth = $dbh->prepare( $stmt );
$sth->execute;
my $fk_aref = $sth->fetchall_arrayref;

$obj = DDL::Oracle->new(
                         type => 'constraint',
                         list => $fk_aref,
                       );

$drop_ddl .= group_header( 3 ) .
             $obj->drop            if @$fk_aref;

#
# Step 3 - Drop and create the tables.  NOTE:  the DROP statements are in
#          one file followed by coalesce tablespace statements, and the
#          CREATE statements are put in a separate file.  The assumption
#          here is that the user will verify that the drop and coalesce
#          statements executed OK before executing the CREATE tables file.
#

$stmt =
      "
       SELECT DISTINCT
              owner
            , table_name
       FROM
            (
              $table_query
              UNION ALL
              $iot_query
            )
       ORDER
          BY
              1, 2
      ";

$sth = $dbh->prepare( $stmt );
$sth->execute;
$aref = $sth->fetchall_arrayref;

if ( @$aref )
{
  foreach $row ( @$aref )
  {
    push @export_objects, "\L@$row->[0].@$row->[1]";
  }

  $obj = DDL::Oracle->new(
                           type => 'table',
                           list => $aref,
                         );

  $drop_ddl       .= group_header( 4 ) .
                     $obj->drop;

  $create_tbl_ddl .= group_header( 8 ) .
                     $obj->create;
}

#
# Step 4 - Drop all Primary Key, Unique and Check constraints on the tables
#          of our indexes (those on our tables disappeared with the DROP
#          TABLE statements).
#

$stmt =
      "
       SELECT
              owner
            , constraint_name
       FROM
              dba_constraints
       WHERE
                  constraint_type     IN ('P','U','C')
              AND (
                      owner
                    , table_name
                  ) IN (
                         SELECT
                                owner
                              , table_name
                         FROM
                              (
                                SELECT
                                       owner
                                     , table_name
                                FROM
                                     (
                                       $index_query
                                     )
                                MINUS
                                (
                                  $table_query
                                  UNION ALL
                                  $iot_query
                                )
                              )
                       )
       ORDER
          BY
              1, 2
      ";

$sth = $dbh->prepare( $stmt );
$sth->execute;
$aref = $sth->fetchall_arrayref;

$obj = DDL::Oracle->new(
                         type => 'constraint',
                         list => $aref,
                       );

$drop_ddl .= group_header( 5 ) .
             $obj->drop          if @$aref;

#
# Step 5 - Drop all of our indexes, unless they are the supporting index
#          of a Primary Key or Unique constraint -- these disappeared in
#          the preceding step.  NOTE:  This will generate DROP INDEX
#          statements for PK/UK's if the constraint name differs from the
#          index name (e.g., system generated names).  It won't cause any
#          harm, but it WILL get an error in SQL*Plus.  Maybe we'll fix
#          this someday.
#

$stmt =
      "
       SELECT DISTINCT
              owner
            , index_name
       FROM 
            (
              $index_query
            ) i
       WHERE
             NOT EXISTS   (
                            SELECT
                                   null
                            FROM
                                   dba_constraints
                            WHERE
                                       owner           = i.owner
                                   AND constraint_name = i.index_name
                          )
             AND (
                     owner
                   , table_name
                 ) NOT IN (
                            $table_query
                            UNION ALL
                            $iot_query
                          )
       ORDER
          BY
              1, 2
      ";

$sth = $dbh->prepare( $stmt );
$sth->execute;
$aref = $sth->fetchall_arrayref;

$obj = DDL::Oracle->new(
                         type => 'index',
                         list => $aref,
                       );

$drop_ddl .= group_header( 6 ) .
             $obj->drop           if @$aref;

#
# Step 6 - Create ALL indexes.
#

$stmt =
      "
       SELECT DISTINCT
              owner
            , index_name
       FROM 
            (
              $index_query
            )
       ORDER
          BY
              1, 2
      ";

$sth = $dbh->prepare( $stmt );
$sth->execute;
$aref = $sth->fetchall_arrayref;

$obj = DDL::Oracle->new(
                         type => 'index',
                         list => $aref,
                       );

$create_ndx_ddl .= group_header( 10 ) .
                   $obj->create         if @$aref;

#
# Step 7 - Create all Primary Key, Unique and Check constraints on our
#          tables and on the tables of our indexes.  NOTE:  do not create
#          the constraints for the IOT tables -- their primary keys were
#          defined in the CREATE TABLE statements.
#

$stmt =
      "
       SELECT
              owner
            , constraint_name
            , constraint_type
            , search_condition
       FROM
              dba_constraints
       WHERE
                  constraint_type     IN ('P','U','C')
              AND (
                      owner
                    , table_name
                  ) IN (
                         $table_query
                         UNION ALL
                         SELECT
                                owner
                              , table_name
                         FROM
                              (
                                $index_query
                              )
                       )
       ORDER
          BY
              1, 2
      ";

$dbh->{ LongReadLen } = 8192;    # Allows SEARCH_CONDITION length of 8K
$dbh->{ LongTruncOk } = 1;

$sth = $dbh->prepare( $stmt );
$sth->execute;
$aref = $sth->fetchall_arrayref;

my @constraints;
foreach $row ( @$aref )
{
  my ( $owner, $constraint_name, $cons_type, $condition, ) = @$row;

  if ( $cons_type ne 'C' )
  {
    push @constraints, [ $owner, $constraint_name ];
  }
  elsif ( $condition !~ /IS NOT NULL/ )  # NOT NULL is part of CREATE TABLE
  {
    push @constraints, [ $owner, $constraint_name ];
  }
}

$obj = DDL::Oracle->new(
                         type => 'constraint',
                         list => \@constraints,
                       );

$create_ndx_ddl .= group_header( 11 ) .
                   $obj->create          if @constraints;

#
# Step 8 - Create all Check constraints on our IOT tables (their PK was
#          part of the CREATE TABLE, and they can't have any other indexes,
#          thus no UK's)
#

$stmt =
      "
       SELECT
              owner
            , constraint_name
            , constraint_type
            , search_condition
       FROM
              dba_constraints
       WHERE
                  constraint_type = 'C'
              AND (
                      owner
                    , table_name
                  ) IN (
                         $iot_query
                       )
       ORDER
          BY
              1, 2
      ";

$dbh->{ LongReadLen } = 8192;    # Allows SEARCH_CONDITION length of 8K
$dbh->{ LongTruncOk } = 1;

$sth = $dbh->prepare( $stmt );
$sth->execute;
$aref = $sth->fetchall_arrayref;

@constraints = ();
foreach $row ( @$aref )
{
  my ( $owner, $constraint_name, $cons_type, $condition, ) = @$row;

  if ( $condition !~ /IS NOT NULL/ )  # NOT NULL is part of CREATE TABLE
  {
    push @constraints, [ $owner, $constraint_name ];
  }
}

$obj = DDL::Oracle->new(
                         type => 'constraint',
                         list => \@constraints,
                       );

$create_ndx_ddl .= group_header( 12 ) .
                   $obj->create           if @constraints;

#
# Step 9 - Recreate all Foreign Keys referenceing our tables and IOT's or
#          referenceing the tables of our other indexes.  Use the same list
#          used in Step 2 to drop them ($fk_aref).
#

$obj = DDL::Oracle->new(
                         type => 'constraint',
                         list => $fk_aref,
                       );

$create_ndx_ddl .= group_header( 13 ) .
                   $obj->create          if @$fk_aref;

#
# Step 10 - REBUILD all UNUSABLE indexes/index [sub]partitions.  Most likely,
#           these are non-partitioned or Global partitioned indexes on THE
#           PARTITIONS.
#

$stmt =
      "
       SELECT
              owner
            , index_name
       FROM
              dba_indexes
       WHERE
                  status = 'UNUSABLE'
       UNION ALL
       SELECT
              index_owner
            , index_name || ':' || partition_name
       FROM
              dba_ind_partitions
       WHERE
              status = 'UNUSABLE'
       UNION ALL
       SELECT
              index_owner
            , index_name || ':' || subpartition_name
       FROM
              dba_ind_subpartitions
       WHERE
              status = 'UNUSABLE'
       ORDER
          BY
              1, 2
      ";

$sth = $dbh->prepare( $stmt );
$sth->execute;
$aref = $sth->fetchall_arrayref;

$obj = DDL::Oracle->new(
                         type => 'index',
                         list => $aref,
                       );

$create_ndx_ddl .= group_header( 14 ) .
                   $obj->resize         if @$aref;

#
# It's hard to believe, but maybe they gave us an empty tablespace
# to practice on.
#

die "\n***Error:  Tablespace $tblsp is empty. 
           Doest thou take me for a fool?\n\n"
     unless $create_tbl_ddl . $create_ndx_ddl;

#
# OK, we're ligit.  Coalesce all data/index tablespaces
#

$stmt =
      "
       SELECT
              LOWER(tablespace_name)
       FROM
              dba_tablespaces  t
       WHERE
                  status            = 'ONLINE'
              AND contents         <> 'TEMPORARY'
              AND tablespace_name  <> 'SYSTEM'
              AND extent_management = 'DICTIONARY'
              AND NOT EXISTS        (
                                      SELECT
                                             null
                                      FROM
                                             dba_segments  s
                                      WHERE
                                                 s.segment_type    = 'ROLLBACK'
                                             AND s.tablespace_name =
                                                 t.tablespace_name
                                    )
       ORDER
          BY
              tablespace_name
      ";

$sth = $dbh->prepare( $stmt );
$sth->execute;
$aref = $sth->fetchall_arrayref;

foreach $row ( @$aref )
{
  $drop_ddl .= "PROMPT ALTER TABLESPACE @$row->[0] COALESCE\n\n" .
               "ALTER TABLESPACE @$row->[0] COALESCE ;\n\n",
}

#
# Wrap it up -- open, write and close all files
#

if ( $create_temp_ddl )
{
  $add_temp_sql = "$sqldir/$prefix${tblsp}_add_temp.sql";
  $add_temp_log = "$sqldir/$prefix${tblsp}_add_temp.log";
  print "Create temps            : $add_temp_sql\n";
  write_file( $add_temp_sql, $create_temp_ddl, 'REM' );

  $drop_temp_sql = "$sqldir/$prefix${tblsp}_drop_temp.sql";
  $drop_temp_log = "$sqldir/$prefix${tblsp}_drop_temp.log";
  print "Drop temps              : $drop_temp_sql\n";
  write_file( $drop_temp_sql, $drop_temp_ddl, 'REM' );
}

my $drop_all_sql = "$sqldir/$prefix${tblsp}_drop_all.sql";
my $drop_all_log = "$sqldir/$prefix${tblsp}_drop_all.log";
print "Drop objects            : $drop_all_sql\n";
write_file( $drop_all_sql, $drop_ddl, 'REM' );

my $add_tbl_sql = "$sqldir/$prefix${tblsp}_add_tbl.sql";
my $add_tbl_log = "$sqldir/$prefix${tblsp}_add_tbl.log";
print "Create tables           : $add_tbl_sql\n";
write_file( $add_tbl_sql, $create_tbl_ddl, 'REM' );

my $add_ndx_sql = "$sqldir/$prefix${tblsp}_add_ndx.sql";
my $add_ndx_log = "$sqldir/$prefix${tblsp}_add_ndx.log";
print "Create indexes          : $add_ndx_sql\n\n";
write_file( $add_ndx_sql, $create_ndx_ddl, 'REM' );

my $pipefile = "$expdir/$prefix$tblsp.pipe";
unlink $pipefile;
eval { system ("mknod $pipefile p") };

if ( $create_temp_ddl )
{
  $prttn_exp_par  = "$expdir/prttn_$prefix${tblsp}_exp.par";
  $prttn_exp_log  = "$logdir/prttn_$prefix${tblsp}_exp.log";
  $prttn_exp_text = "log         = $prttn_exp_log\n" .
                    "file        = $pipefile\n" .
                    "rows        = y\n" .
                    "grants      = y\n" .
                    "#direct      = y\n" .   # Has bug on Import??
                    "buffer      = 65536\n" .
                    "indexes     = n\n" .
                    "compress    = n\n" .
                    "triggers    = y\n" .
                    "constraints = n\n" .
                    "tables      = (\n" .
                    "                  " .
                    join ( "\n                , ", @export_temps ) .
                    "\n              )\n\n";

  print "Partition Export parfile: $prttn_exp_par\n";
  print "Partition Export logfile: $prttn_exp_log\n";
  write_file( $prttn_exp_par, $prttn_exp_text, '#' );

  $prttn_imp_par  = "$expdir/prttn_$prefix${tblsp}_imp.par";
  $prttn_imp_log  = "$logdir/prttn_$prefix${tblsp}_imp.log";
  $prttn_imp_text = "log         = $prttn_imp_log\n" .
                    "file        = $pipefile\n" .
                    "rows        = y\n" .
                    "commit      = y\n" .
                    "ignore      = y\n" .
                    "buffer      = 65536\n" .
                    "analyze     = n\n" .
                    "full        = y\n\n" .
                    "#tables      = (\n" .
                    "#                  " .
                    join ( "\n#                , ", @export_temps ) .
                    "\n#              )\n\n";

  print "Partition Import parfile: $prttn_imp_par\n";
  print "Partition Import logfile: $prttn_imp_log\n\n";
  write_file( $prttn_imp_par, $prttn_imp_text, '#' );
}

my $exp_par  = "$expdir/$prefix${tblsp}_exp.par";
my $exp_log  = "$logdir/$prefix${tblsp}_exp.log";
my $exp_text = "log         = $exp_log\n" .
               "file        = $pipefile\n" .
               "rows        = y\n" .
               "grants      = y\n" .
               "#direct      = y\n" .   # Has bug on Import??
               "buffer      = 65536\n" .
               "indexes     = n\n" .
               "compress    = n\n" .
               "triggers    = y\n" .
               "constraints = n\n" .
               "tables      = (\n" .
               "                  " .
               join ( "\n                , ", @export_objects ) .
               "\n              )\n\n";

print "Table Export parfile    : $exp_par\n";
print "Table Export logfile    : $exp_log\n";
write_file( $exp_par, $exp_text, '#' );

my $imp_par  = "$expdir/$prefix${tblsp}_imp.par";
my $imp_log  = "$logdir/$prefix${tblsp}_imp.log";
my $imp_text = "log         = $imp_log\n" .
               "file        = $pipefile\n" .
               "rows        = y\n" .
               "commit      = y\n" .
               "ignore      = y\n" .
               "buffer      = 65536\n" .
               "analyze     = n\n" .
               "full        = y\n\n" .
               "#tables      = (\n" .
               "#                  " .
               join ( "\n#                , ", @export_objects ) .
               "\n#              )\n\n";

print "Table Import parfile    : $imp_par\n";
print "Table Import logfile    : $imp_log\n\n";
write_file( $imp_par, $imp_text, '#' );

print "Export FIFO pipe        : $pipefile\n\n";

#
# And, finally, the little shell scripts to help with the driving
#

my $i = 0;
print "\n";

$shell = "$sqldir/$prefix$tblsp.sh";

if ( $create_temp_ddl )
{
  $gzip  = "$expdir/prttn_$prefix$tblsp.dmp.gz";

  $script = $shell . ++$i;
  $text =
    "# Step $i -- Export the partitions in Tablespace $tblsp\n\n" .
    "nohup cat $pipefile | gzip -c \\\n" .
    "        > $gzip &\n\n" .
    "exp / parfile = $prttn_exp_par\n\n";
  create_shell( $script, $text );

  $script = $shell . ++$i;
  $text =
    "# Step $i -- Use SQL*Plus to run $add_temp_sql\n" .
    "#           which will create temp tables for partitions " .
    "in tablespace $tblsp\n\n" .
    "sqlplus -s / << EOF\n\n" .
    "   SPOOL $add_temp_log\n\n" .
    "   @ $add_temp_sql\n\n" .
    "EOF\n\n";
  create_shell( $script, $text );
}

$gzip  = "$expdir/$prefix$tblsp.dmp.gz";

$script = $shell . ++$i;
$text =
  "# Step $i -- Export the tables in Tablespace $tblsp\n\n" .
  "nohup cat $pipefile | gzip -c \\\n" .
  "        > $gzip &\n\n" .
  "exp / parfile = $exp_par\n\n";
create_shell( $script, $text );

$script = $shell . ++$i;
$text =
  "# Step $i -- Use SQL*Plus to run $drop_all_sql\n" .
  "#           which will drop all objects in tablespace $tblsp\n\n" .
  "sqlplus -s / << EOF\n\n" .
  "   SPOOL $drop_all_log\n\n" .
  "   @ $drop_all_sql\n\n" .
  "EOF\n\n";
create_shell( $script, $text );

$script = $shell . ++$i;
$text =
  "# Step $i -- Use SQL*Plus to run $add_tbl_sql\n".
  "#           which will recreate all tables in tablespace $tblsp\n\n" .
  "sqlplus -s / << EOF\n\n" .
  "   SPOOL $add_tbl_log\n\n" .
  "   @ $add_tbl_sql\n\n" .
  "EOF\n\n";
create_shell( $script, $text );

$script = $shell . ++$i;
$text =
  "# Step $i -- Import the tables back into Tablespace $tblsp\n\n" .
  "nohup gunzip -c $gzip \\\n" .
  "              > $pipefile &\n\n" .
  "imp / parfile = $imp_par\n\n";
create_shell( $script, $text );

$script = $shell . ++$i;
$text =
  "# Step $i -- Use SQL*Plus to run $add_ndx_sql\n" .
  "#           which will recreate all indexes/constraints " .
  "in tablespace $tblsp\n\n" .
  "sqlplus -s / << EOF\n\n" .
  "   SPOOL $add_ndx_log\n\n" .
  "   @ $add_ndx_sql\n\n" .
  "EOF\n\n";
create_shell( $script, $text );

if ( $create_temp_ddl )
{
  $gzip  = "$expdir/prttn_$prefix$tblsp.dmp.gz";

  print "\n*** The following 2 scripts ARE FOR FALLBACK PURPOSES ONLY!!\n" .
        "*** Use these scripts ONLY IF Shell #2 HAD ERRORS.\n\n";

  $script = $shell . ++$i;
  $text =
    "# USE FOR FALLBACK PURPOSES ONLY\n\n" .
    "# Use SQL*Plus to run $drop_temp_sql\n" .
    "# which will drop the temp tables holding data for partitions " .
    "in tablespace $tblsp\n\n" .
    "sqlplus -s / << EOF\n\n" .
    "   SPOOL $drop_temp_log\n\n" .
    "   @ $drop_temp_sql\n\n" .
    "EOF\n\n";
  create_shell( $script, $text );

  $script = $shell . ++$i;
  $text =
    "# USE FOR FALLBACK PURPOSES ONLY\n\n" .
    "#Import the tables back into the partitions in " .
    "Tablespace $tblsp\n\n" .
    "echo\n" .
    "echo \"**************** NOTICE ***************\"\n" .
    "echo\n" .
    "echo Ignore warnings about missing partitions -- because not\n" .
    "echo all partitions were exported, and thus not all partitions\n" .
    "echo need be re-imported.\n" .
    "echo The error to be ignored is:\n" .
    "echo\n" .
    "echo \"   IMP-00057: Warning: Dump file may not contain data of all partitions...\"\n" .
    "echo\n" .
    "echo \"************ END OF NOTICE ************\"\n\n" .
    "nohup gunzip -c $gzip \\\n" .
    "              > $pipefile &\n\n" .
    "imp / parfile = $prttn_imp_par\n\n";
  create_shell( $script, $text );
}

my @shells = glob("$sqldir/$prefix$tblsp.sh?");
chmod( 0754, @shells ) == @shells or die "\nCan't chmod some shells: $!\n";

print "\n$0 completed successfully\non ", scalar localtime,"\n\n";

exit 0;

#################### Subroutines (alphabetically) ######################

# sub connect_to_oracle
#
# Requires both "user" and "password", or neither.  If "user" is supplied
# but not "password", will prompt for a "password".  On Unix systems, a
# system call to "stty" is made before- and after-hand to control echoing
# of keystrokes.  [How do we do this on Windows?]
#
sub connect_to_oracle
{
  if ( $args{ user } and not $args{ password } )
  {
    print "Enter password: ";
    eval{ system("stty -echo" ); };
    chomp( $args{ password } = <STDIN> );
    print "\n";
    eval{ system( "stty echo" ); };
  }

  $args{ sid }      = "" unless $args{ sid };
  $args{ user }     = "" unless $args{ user };
  $args{ password } = "" unless $args{ password };

  $dbh = DBI->connect(
                       "dbi:Oracle:$args{ sid }",
                       "$args{ user }",
                       "$args{ password }",
                       {
                         PrintError => 0,
                         RaiseError => 1,
                       }
                     );

  DDL::Oracle->configure(
                          dbh    => $dbh,
                          view   => 'DBA',
                          schema => 1,
                          resize => $args{ resize } || 1,
                        );
}

# sub create_shell
#
# Opens, writes $text, closes the named shell script
#
sub create_shell
{
  my ( $script, $text ) = @_;

  print "Shell #$i is $script\n";
  open SHELL, ">$script"     or die "Can't open $script: $!\n";
  write_header( \*SHELL, $script, '# ' );
  print SHELL $text . "#  --- END OF FILE ---\n\n";
  close SHELL                  or die "Can't close $script: $!\n";
}

# sub get_args
#
# Uses supplied module Getopt::Long to place command line options into the
# hash %args.  Ensures that at least the mandatory argument --tablespace
# was supplied.  Also verifies directory arguments and connects to Oracle.
#
sub get_args
{
  #
  # Get options from command line and store in %args
  #
  GetOptions(
              \%args,
              "alttablespace:s",
              "expdir:s",
              "logdir:s",
              "password:s",
              "prefix:s",
              "sid:s",
              "resize:s",
              "sqldir:s",
              "tablespace:s",
              "user:s",
            );

  #
  # If there is anything left in @ARGV, we have a problem
  #
  die "\n***Error:  unrecognized argument",
      ( @ARGV == 1 ? ":  " : "s:  " ),
      ( join " ",@ARGV ),
      "\n$0 aborted,\n\n" ,
    if @ARGV;
  
  #
  # Validate arguments (maybe they type as bad as we do!
  #

  $sqldir = ( $args{ sqldir } eq "." ) ? cwd : $args{ sqldir };
  die "\n***Error:  sqldir defined as '$sqldir', which is not a Directory\n",
      "$0 aborted,\n\n"
    unless -d $sqldir;

  $logdir = ( $args{ logdir } eq "." ) ? cwd : $args{ logdir };
  die "\n***Error:  logdir defined as '$logdir', which is not a Directory\n",
      "$0 aborted,\n\n"
    unless -d $logdir;

  $expdir = ( $args{ expdir } eq "." ) ? cwd : $args{ expdir };
  die "\n***Error:  expdir defined as '$expdir', which is not a Directory\n",
      "$0 aborted,\n\n"
    unless -d $expdir;

  $tblsp = uc( $args{ tablespace } ) or
  die "\n***Error:  You must specify --tablespace=<NAME>\n",
      "$0 aborted,\n\n";

  $alttblsp = uc( $args{ alttablespace } );

  $prefix = $args{ prefix };

  connect_to_oracle();      # Will fail unless sid, user, password are OK

  # Confirm the tablespace exists
  $stmt =
      "
       SELECT
              'EXISTS'
       FROM
              dba_tablespaces  t
       WHERE
                  tablespace_name   = '$tblsp'
              AND status            = 'ONLINE'
              AND contents         <> 'TEMPORARY'
              AND extent_management = 'DICTIONARY'
              AND NOT EXISTS(
                              SELECT
                                     null
                              FROM
                                     dba_segments  s
                              WHERE
                                         s.segment_type    = 'ROLLBACK'
                                     AND s.tablespace_name =
                                         t.tablespace_name
                            )
      ";

  $sth = $dbh->prepare( $stmt );
  $sth->execute;
  $row = $sth->fetchrow_array;

  die "\n***Error:  Tablespace \U$tblsp",
      " does not exist\n",
      "           or is not ONLINE\n",
      "           or is managed LOCALLY\n",
      "           or is a TEMPORARY tablespace\n",
      "           or contains ROLLBACK segments.\n\n"
    unless $row;

  # First row returned is valid tablespace, and is $alttblsp.
  # Since we know $tblsp is good, we're guaranteed at least one row.
  $stmt =
      "
       SELECT
              tablespace_name
       FROM
              dba_tablespaces  t
       WHERE
                  tablespace_name   = '$alttblsp'
              AND status            = 'ONLINE'
              AND contents         <> 'TEMPORARY'
              AND extent_management = 'DICTIONARY'
              AND NOT EXISTS(
                              SELECT
                                     null
                              FROM
                                     dba_segments  s
                              WHERE
                                         s.segment_type    = 'ROLLBACK'
                                     AND s.tablespace_name =
                                         t.tablespace_name
                            )
       UNION ALL
       SELECT
              tablespace_name
       FROM
              dba_tablespaces  t
       WHERE
                  tablespace_name   = 'USERS'
              AND status            = 'ONLINE'
              AND contents         <> 'TEMPORARY'
              AND extent_management = 'DICTIONARY'
              AND NOT EXISTS(
                              SELECT
                                     null
                              FROM
                                     dba_segments  s
                              WHERE
                                         s.segment_type    = 'ROLLBACK'
                                     AND s.tablespace_name =
                                         t.tablespace_name
                            )
       UNION ALL
       SELECT
              tablespace_name
       FROM
              dba_tablespaces
       WHERE
                  tablespace_name   = '$tblsp'
      ";

  $sth = $dbh->prepare( $stmt );
  $sth->execute;
  $aref = $sth->fetchall_arrayref;

  $alttblsp = ( shift @$aref )->[0];

  my ( undef,undef,undef,$day,$month,$year,undef,undef,undef ) = localtime;
  $date = $year + 1900 . $month + 1 . $day;
}

# sub group_header
#
# Returns a Remark to identify the ensuing DDL statements
#
sub group_header
{
  my ( $nbr ) = @_;

  return 'REM ' . '#' x 60 . "\n" .
         "REM\n" .
         "REM                      Statement Group $nbr\n" .
         "REM\n" .
         'REM ' . '#' x 60 . "\n\n";
}

# sub index_and_exchange
#
# Generate the DDL to:
#
# 1.  Create an index on named temp table equal to every LOCAL index on the
#     named partitioned table.
# 2.  Create a PK for the temp table equal to the PK of the partitioned table,
#     if any.
# 3.  Exchange the temp table with the named partition.
#
sub index_and_exchange
{
  my ( $temp, $owner, $table, $partition, $type ) = @_;

  my $sql;
  my $text;

  # Get partitioned, local indexes
  $stmt =
      "
       SELECT DISTINCT
              index_name
       FROM
              dba_segments  s
            , dba_indexes   i
       WHERE
                  i.owner           = UPPER('$owner')
              AND i.table_name      = UPPER('$table')
              AND s.owner           = i.owner
              AND s.segment_name    = i.index_name
              AND s.segment_type LIKE 'INDEX%PARTITION'
              AND NOT EXISTS (
                               SELECT
                                      null
                               FROM
                                      dba_part_indexes
                               WHERE
                                          owner      = i.owner
                                      AND index_name = i.index_name
                                      AND locality   = 'GLOBAL'
                             )
      ";

  $sth = $dbh->prepare( $stmt );
  $sth->execute;
  $aref = $sth->fetchall_arrayref;

  foreach $row( @$aref )
  {
    my $index = @$row->[0];

    $obj = DDL::Oracle->new(
                             type => 'exchange index',
                             list => [
                                       [
                                         "$owner",
                                         "$index:$partition",
                                       ]
                                     ],
                           );
    my $sql = $obj->create;
    # Remove REM lines created by DDL::Oracle
    $sql =  ( join "\n", grep !/^REM/, split /\n/, $sql ) . "\n\n";

    my $indx =  "${tblsp}_${date}_" . unique_nbr();
    $sql     =~ s|\L$owner.$index|\L$owner.$indx|g;
    $sql     =~ s|\L$owner.$table|\L$owner.$temp|g;

    $text .= $sql;
  }

  $stmt =
      "
       SELECT
              constraint_name
       FROM
              dba_constraints
       WHERE
                  owner      = UPPER('$owner')
              AND table_name = UPPER('$table')
              AND constraint_type = 'P'
      ";

  $sth = $dbh->prepare( $stmt );
  $sth->execute;
  my @row = $sth->fetchrow_array;

  if ( @row )
  {
    my ( $constraint ) = @row;

    $obj = DDL::Oracle->new(
                             type => 'constraint',
                             list => [
                                       [
                                         "$owner",
                                         "$constraint",
                                       ]
                                     ],
                           );
    my $sql = $obj->create;
    # Remove REM lines created by DDL::Oracle
    $sql =  ( join "\n", grep !/^REM/, split /\n/, $sql ) . "\n\n";

    my $cons =  "${tblsp}_${date}_" . unique_nbr();
    $sql     =~ s|\L$owner.$table|\L$owner.$temp|g;
    $sql     =~ s|\L$constraint|\L$cons|g;

    $text .= $sql;
  }

  $text .= "PROMPT " .
           "ALTER TABLE \L$owner.$table \UEXCHANGE $type \L$partition\n\n" .
           "ALTER TABLE \L$owner.$table\n" .
           "   \UEXCHANGE $type \L$partition \UWITH TABLE \L$temp\n" .
           "   INCLUDING INDEXES\n".
           "   WITHOUT VALIDATION ;\n\n";

  return $text;
}

# sub initialize_queries
#
# Initializes the 3 driving queries used to
# retrieve object names involved in the defrag.
#
sub initialize_queries
{
  # This query produces a list of THE PARTITIONS, which are the partitions
  # in THE TABLESPACE belonging to tables which have at least one partition
  # in some other tablespace.  These will be the target of ALTER TABLE
  # EXCHANGE [SUB]PARTITION statements with "temp" tables.
  #
  $exchange_query =
      "
       SELECT
              owner
            , segment_name
            , partition_name
            , SUBSTR(segment_type,7)       AS segment_type
       FROM
              dba_segments  s
       WHERE
                  segment_type LIKE 'TABLE%PARTITION'
              AND tablespace_name = '$tblsp'
              AND EXISTS (
                           SELECT
                                  null
                           FROM
                                  dba_segments
                           WHERE
                                      segment_type  LIKE 'TABLE%PARTITION'
                                  AND tablespace_name <> '$tblsp'
                                  AND owner            = s.owner
                                  AND segment_name     = s.segment_name
                         )
       ORDER
          BY
              1, 2, 3
      ";

  # This query produces a list of THE INDEXES (and their tables) -- those
  # non-partitioned indexes which reside in THE TABLESPACE, plus indexes 
  # which have at least one partition in THE TABLESPACE.  These indexes are
  # on tables other than the tables of THE PARTITIONS but may me on THE
  # TABLES.
  #
  $index_query =
      "
       SELECT
              owner
            , index_name
            , table_name
       FROM
            (
              SELECT
                     owner
                   , index_name
                   , table_name
              FROM
                     dba_indexes
              WHERE
                         tablespace_name = '$tblsp'
                     AND index_type     <> 'IOT - TOP'
              UNION ALL
              SELECT
                     i.owner
                   , i.index_name
                   , i.table_name
              FROM
                     dba_indexes         i
                   , dba_ind_partitions  p
              WHERE
                         p.tablespace_name = '$tblsp'
                     AND i.owner           = p.index_owner
                     AND i.index_name      = p.index_name
                     AND i.index_type     <> 'IOT - TOP'
              UNION ALL
              SELECT
                     i.owner
                   , i.index_name
                   , i.table_name
              FROM
                     dba_indexes            i
                   , dba_ind_subpartitions  p
              WHERE
                         p.tablespace_name = '$tblsp'
                     AND i.owner           = p.index_owner
                     AND i.index_name      = p.index_name
                     AND i.index_type      <> 'IOT - TOP'
            )
       WHERE
             (
                 owner
               , table_name
             ) NOT IN (
                        SELECT
                               owner
                             , segment_name
                        FROM
                             (
                               $exchange_query
                             )
                      )
       ORDER
          BY
              1, 2, 3
      ";

  # This query produces a list of THE IOTS -- non-partition index organized
  # tables which reside in THE TABLESPACE or partitioned index organized
  # tables which have at least one partition in THE TABLESPACE.
  # 
  $iot_query =
      "
       SELECT
              owner
            , table_name
       FROM
              dba_indexes
       WHERE
                  tablespace_name = '$tblsp'
              AND index_type      = 'IOT - TOP'
       UNION ALL
       SELECT
              i.owner
            , i.table_name
       FROM
              dba_indexes         i
            , dba_ind_partitions  p
       WHERE
                  p.tablespace_name = '$tblsp'
              AND i.index_type      = 'IOT - TOP'
              AND i.owner           = p.index_owner
              AND i.table_name      = p.index_name
       UNION ALL
       SELECT
              i.owner
            , i.table_name
       FROM
              dba_indexes            i
            , dba_ind_subpartitions  p
       WHERE
                  p.tablespace_name = '$tblsp'
              AND i.index_type      = 'IOT - TOP'
              AND i.owner           = p.index_owner
              AND i.table_name      = p.index_name
      ";

  # This query produces a list of THE TABLES -- non-partitioned tables which
  # reside in THE TABLESPACE or partitioned tables which have at least one
  # partition in THE TABLESPACE.
  #
  $table_query =
      "
       SELECT
              owner
            , table_name
       FROM
              dba_tables
       WHERE
              tablespace_name   = '$tblsp'
       UNION ALL
       SELECT DISTINCT
              table_owner
            , table_name
       FROM
              dba_tab_partitions  t
       WHERE
                  tablespace_name   = '$tblsp'
              AND NOT EXISTS (
                               SELECT
                                      null
                               FROM
                                      dba_tab_partitions
                               WHERE
                                          table_owner      = t.table_owner
                                      AND table_name       = t.table_name
                                      AND tablespace_name <> '$tblsp'
                               UNION ALL
                               SELECT
                                      null
                               FROM
                                      dba_tab_subpartitions
                               WHERE
                                          table_owner      = t.table_owner
                                      AND table_name       = t.table_name
                                      AND tablespace_name <> '$tblsp'
                             )
       UNION ALL
       SELECT DISTINCT
              table_owner
            , table_name
       FROM
              dba_tab_subpartitions  t
       WHERE
                  tablespace_name   = '$tblsp'
              AND NOT EXISTS (
                               SELECT
                                      null
                               FROM
                                      dba_tab_subpartitions
                               WHERE
                                          table_owner      = t.table_owner
                                      AND table_name       = t.table_name
                                      AND tablespace_name <> '$tblsp'
                             )
      ";
}

# sub move
# 
# Formats an ALTER TABLE MOVE [SUB]PARTITION statement
#
sub move
{
  my ( $owner, $table, $partition, $type, $tblsp ) = @_;

  return "PROMPT " .
         "ALTER TABLE \L$owner.$table \UMOVE $type \L$partition\n\n" .
         "ALTER TABLE \L$owner.$table \UMOVE $type \L$partition\n" .
         "TABLESPACE \L$tblsp ;\n\n";
}

# sub print_help
#
# Displays a description of each argument.
#
sub print_help
{
  print "
  Usage:  defrag.pl [OPTION] [OPTION]...

  ?, -?, -h, --help   Prints this help.

  --tablespace=TABLESPACE

           Drop/recreate all objects in the named tablespace -- tables,
           table partitions, non-partitioned indexes and indexes which
           have even one partition in the named tablespace.

           This argument is REQUIRED.

  --alttablespace=TABLESPACE

           If table partition(s) is(are) part of the defrag, a
           substitute, placeholder partition is created in this
           tablespace.  If not given, tablespace USERS will be used if
           present, otherwise the named tablespace.  If the argument
           is not given, and if there are partitioned tables in the
           named tablespace, and if there is not a USERS tablespace,
           the placeholder partitions will probably prevent a complete
           coalesce of the named tablesapace.  This argument is highly
           recommended.

  --expdir=PATH *

           Directory to place the import/export .par files.  Defaults to
           environment variable DBA_EXP, or to the current directory.

  --logdir=PATH *

           Directory to place the import/export .log files.  Defaults to
           environment variable DBA_LOG, or to the current directory.

  --password=PASSWORD

           User's password.  Not required if user is authenticated
           externally.  Respresents a security risk on Unix systems.

           If USER is given and PASSWORD is not, program will prompt
           for PASSWORD.  This would be preferable, since the password
           will then not be visible in a 'ps' command.

  --prefix=STRING *

           The leading portion of all filenames.  Defaults to 'defrag_',
           and may be '' (in which case filenames will begin with the
           name of the tablespace).

  --sid=SID *

           The SID or service used to connect to Oracle.  If omitted,
           the connection will be to the instance identified in
           environment variable ORACLE_SID.

  --resize=STRING *

           In the CREATE statement, objects are given INITIAL and NEXT
           extent sizes, appropriate for objects having the number of
           blocks used.  This is a colon delimited string consisting
           of n sets of LIMIT:INITIAL:NEXT.  LIMIT is expressed in
           Database Blocks.  The highest LIMIT may contain the string
           'UNLIMITED', and in any event will be forced to be so by
           DDL::Oracle..

  --sqldir=PATH *

           Directory to place the SQL files (which will have extensions
           of .sql, .tbl, .ndx, or .con).  Defaults to environment
           variable DBA_SQL, or to the current directory.

  --user=USERNAME

           Connects to Oracle as this user.  Defaults to operating
           system username.

  *  Items marked with '*' are saved in a file named .defragrc,
     stored in the user's HOME directory.  If omitted in subsequent
     usages of defrag.pl, these entries will be reused unless a
     new entry is assigned at that time.

  ";

  $text = "
  Program 'defrag.pl' uses 4 main SQL statements to retrieve record sets which
  form the basis of generated DDL.  They are sometimes UNIONed, sometimes 
  MINUSed, etc., to refine the record sets.  The queries are:

  THE TABLESPACE -- the Tablspace named by the '--tablespace=<name>' argument.

  THE TABLES -- provides a list of Owner/Table_name's which fully reside in
  THE TABLESPACE.  These are non-partitioned tables plus partitioned tables
  where every partition and subpartition reside in THE TABLESPACE.  This list 
  excludes IOT tables.

  THE IOTS -- provides a list of Owner/Table_name's which fully or partially
  reside in THE TABLESPACE.  In other words, if a partitioned IOT table has
  even one partition in THE TABLESPACE, it is included in this list.  Reasons
  these are in  a separate list from THE TABLES include the fact that their 
  Primary Key is part of the CREATE TABLE syntax, and there are never other 
  indexes on them,

  THE INDEXES -- provides a list of Owner/Index_name/Table_name's for indexes
  not belonging to THE TABLES but which fully or partially reside in THE
  TABLESPACE.  In other words, a partitioned index with even one partition in
  THE TABLESPACE is included in this list.

  The data in THE TABLES and THE IOTS will be exported, after which members of
  all 3 of the lists will be dropped before THE TABLESPACE is coalesced into
  as few as 1 extent per datafile.

  THE PARTITIONS -- provides Owner/Table_name/Partition_name/Segment_type's 
  for all partitions and subpartitions not belonging to THE TABLES nor to THE
  IOTS but which are located in THE TABLESPACE.  If any of these exist, the
  first step will be to perform a 'safety' export of their data directly from
  THE PARTITIONS.  Under normal circumstances, this export is not used.
  Rather, for each partition a corresponding 'temp' table is built matching
  the partition in structure, indexes and Primary Key.  The temp table is then
  EXCHANGED with the partition; this results in the temp table holding the
  data and the partition becoming empty.  The empty partition is moved to the
  alternate tablespace before the coalescing takes place.  The temp table is
  then treated like a member of THE TABLES (i.e., exported, dropped,
  recreated, indexed, imported, etc.).  After the temp table has its data
  imported, it is again EXCHANGED with its original partition, and thus the
  data once again becomes part of the table in its new, properly sized 
  segment.

  Note that nothing is done with indexes on the tables of THE PARTITIONS.  In
  the event that such an index or a partition thereof happens to reside in THE
  TABLESPACE, it will still be there after all other objects have been dropped 
  or moved eleehwhere.  Likewise, unless an alternate tablespace other than
  THE TABLESPACE is given (or if the named alternate tablespace does not
  exist), then the empty partition segments will also remain in THE TABLESPACE.
  If either of these conditions occurs, the THE TABLESPACE will not be
  completely empty when it is coalesced.  This is not necessarily a big
  problem, it is just not as clean as when THE TABLESPACE becomes completely
  empty before it is coalesced.

  The following descriptions of the 'Statement Groups' show the sequence of
  statments used to defragment THE TABLESPACE.  These DDL statements are in
  3 to 5 files.  Shell scripts are provided which perform the statements in
  the correct sequence, intermingled with the exports and imports.  The user
  should check the execution of each shell script for errors before continuing
  with the next step.  Within the SQL files, each group of statements is
  delineated by a header record which refers to a 'Statement Group Number'.
  These groups are defined below.
  
  EXPORT the data from THE PARTITIONS. (If all goes well, we won't use this.)
  
   1.  For each member of THE PARTITIONS:
         a.  Create a Temp table.
         b.  Add appropriate indexes.
         c.  Add a PK, if any.
         d.  EXCHANGE the Temp table with the partition.
         e.  MOVE the [now empty] Temp table to the alternate tablespace.
  
  EXPORT the data from THE TABLES, THE IOTS and the Temp tables.
  
   2.  DROP the Temp tables created in Group #1.
  
   3.  DROP all Foreign Keys referencing THE TABLES, THE IOTS or the tables
       of THE INDEXES.
  
   4.  DROP members of THE TABLES and THE IOTS.  Note: this DROPs all
       constrints on these tables.

   5.  DROP Primary Keys, Unique Constraints and Check Constraints on the
       tables of THE INDEXES. 

   6.  DROP members of THE INDEXES unless they enforce a Primay Key or Unique
       Constraint of the same name -- those that do disappeared in Group #5.
       Note: this will generate DROP INDEX statements for PK/UK's if the 
       Constraint name differs from the Index name (e.g., system generated
       names).  It won't cause any harm, but it will show an error in the log
       file spooled in SQL*Plus; these should be ignored.  Maybe we'll fix
       this someday.

   7.  CREATE the Temp tables.
  
   8.  CREATE members of THE TABLES and THE IOTS.

  IMPORT the data for THE TABLES, THE IOTS and the Temp tables.
  
   9.  CREATE indexes and PK's on the Temp tables.  EXCHANGE them with their
       corresponding partition, and DROP the now empty Temp tables.
  
  10.  CREATE indexes on THE TABLES, plus THE INDEXES themselves.

  11.  CREATE all Constraints on THE TABLES.

  12.  CREATE Check Cosntraints on THE IOTS.

  13.  CREATE Foreign Keys referencing THE TABLES, THE IOTS or the tables
       of THE INDEXES.

  14.  REBUILD non-partitioned or Global partitioned indexes on THE PARTITIONS
       (these were marked UNUSABLE during the partition EXCHANGE).

  ONLY IF PROBLEMS OCCURED DURING EXECUTION OF GROUP #1:

  15.  DROP the Temp tables.

  IMPORT the data for THE PARTITIONS.

  ";

  write_file( "./README.stmts", $text, '' );

  print "
  Also, see the 'README.stmts' which was just written in this directory
  for information about the DDL statements generated and their sequence.
  ";

  return;
}

# sub set_defaults
#
# If file HOME/.defragrc exists, reads its contents into hash %args.
# Otherwise, fill the hash with arbitrary defaults.
#
sub set_defaults
{
  if ( -e "$home/.defragrc" ) 
  {
    # We've been here before -- set up per .defragrc
    open RC, "<$home/.defragrc"      or die "Can't open\n";
    while ( <RC> ) 
    {
      chomp;                       # no newline
      s/#.*//;                     # no comments
      s/^\s+//;                    # no leading white space
      s/\s+$//;                    # no trailing white space
      next unless length;          # anything left? (or was blank)
      my ( $key, $value) = split( /\s*=\s*/, $_, 2 );
      $args{ $key } = $value;
    }
    close RC                         or die "Can't close defragrc:  $!\n";

    # Just in case they farkled the .defragrc file
    $args{ expdir } = '.'       unless $args{ expdir };
    $args{ sqldir } = '.'       unless $args{ sqldir };
    $args{ logdir } = '.'       unless $args{ logdir };
    $args{ prefix } = 'defrag_' unless $args{ prefix };
  }
  else 
  {
    # First time for this user
    $args{ expdir } = $ENV{ DBA_EXP }    || ".";
    $args{ sqldir } = $ENV{ DBA_SQL }    || ".";
    $args{ logdir } = $ENV{ DBA_LOG }    || $ENV{ LOGDIR } || ".";
    $args{ prefix } = "defrag_";
  }
  Getopt::Long::Configure( 'passthrough' );
}

# sub trunc
#
# Formats a TRUNCATE statement for the supplied [sub]partition
#
sub trunc
{
  my ( $owner, $table, $partition, $type ) = @_;

  return  "PROMPT " .
          "ALTER TABLE \L$owner.$table \UTRUNCATE $type \L$partition  \n\n" .
          "ALTER TABLE \L$owner.$table \UTRUNCATE $type \L$partition ;\n\n";
}

# sub unique_nbr
#
# Generates a unique number between 1 and 99999 for use in Temp Table names
#
sub unique_nbr
{
  my $nbr;

  while( 1 )
  {
    $nbr = int( rand 999999 ) + 1;
    $uniq{ $nbr }++;
    last unless $uniq{ $nbr } > 1;
  }

  return $nbr
}

# sub write_file
#
# Opens, writes, closes a .sql or .par file
#
sub write_file
{
  my ( $filename, $text, $remark ) = @_;

  open FILE, ">$filename"     or die "Can't open $filename: $!\n";
  write_header( \*FILE, $filename, $remark );
  print FILE $text,
             "$remark --- END OF FILE ---\n\n";
  close FILE                  or die "Can't close $filename: $!\n";
}

# sub write_header
#
# Creates a 7-line header in the supplied file, marked as comments.
#
sub write_header
{
  my ( $fh, $filename, $remark ) = @_;

  print $fh "$remark $filename\n",
            "$remark \n",
            "$remark Created by $0\n",
            "$remark on ", scalar localtime,"\n\n\n\n";
}

# $Log: defrag.pl,v $
# Revision 1.7  2000/12/02 14:06:20  rvsutherland
# Completed 'exchange' method for handling partitions,
# including REBUILD of UNUSABLE indexes.
# Removed 'resize' method for handling partitions.
#
# Revision 1.6  2000/11/26 20:10:54  rvsutherland
# Added 'exchange' method for handling partitions.  Will probably
# remove the 'resize' method next update.
#
# Revision 1.5  2000/11/24 18:36:00  rvsutherland
# Restructured file writes
# Revamped 'resize' method for handling partitions
#
# Revision 1.4  2000/11/19 20:08:58  rvsutherland
# Added 'resize' partitions option.
# Restructured file creation.
# Added shell scripts to simplify executing generated files.
# Modified selection of IOT tables (now handled same as indexes)
# Added validation of input arguments -- meaning we now check for
# hanging chad and pregnant votes  ;-)
#
# Revision 1.3  2000/11/17 21:35:53  rvsutherland
# Commented out Direct Path export -- Import has a bug (at least on Linux)
#
# Revision 1.2  2000/11/16 09:14:38  rvsutherland
# Major restructure to take advantage of DDL::Oracle.pm
#

__END__

########################################################################

=head1 NAME

defrag.pl -- Creates SQL*Plus command files to defragment a tablespace.

=head1 SYNOPSIS

[ ? | -? | -h | --help ]

--tablespace=TABLESPACE 

[--alttablespace=TABLESPACE]

[--expdir=PATH]

[--logdir=PATH]

[--resize=STRING]

[--sqldir=PATH]

[--user=USERNAME]

[--password=PASSWORD]

[--prefix=PREFIX]

[--sid=SID]

[--resize=STRING]

Note:  descriptions of each of these arguments are provided via 'help',
which may be displayed by entering 'defrag.pl' without any arguments.

=head1 DESCRIPTION

Creates command files to defragment (reorganize) an entire Oracle
Tablespace.  Arguments are specified on the command line.

A "defrag" is usually performed to recapture the little fragments of
unused (and unusable) space that tend to accumulate in Oracle
tablespaces when objects are repeatedly created and dropped.. To fix
this, data is first exported.  Objects are then dropped and the
tablespace is "coalesced" into one large extent of available space.  The
objects are then recreated using either the default sizing algorithm or a
user supplied algorithm, and the data is imported.  Space utilized is then
contiguous, and the unused free space has been captured for use.

The steps in the process are:

    1.  Export all objects in the tablespace (tables, indexes, partitions).
    2.  Drop all objects.
    3.  Coalesce the tablespace.
    4.  Create all tables and partitions, resized appropriately.
    5.  Import the data into the new structures.
    6.  Recreate the indexes.
    7.  Restore all constraints.

=head1 TO DO

=head1 BUGS

=head1 FILES

The names and number of files output varies according to the Tablespace
specified and the options selected.  All .sql and .log files and shell
scripts produced are displayed on STDOUT during the execution of the program.

Also, see 'README.stmts', which will be created when Help is displayed (by
entering 'defrag.pl' without any arguments).

=head1 AUTHOR

 Richard V. Sutherland
 rvsutherland@yahoo.com

=head1 COPYRIGHT

Copyright (c) 2000, Richard V. Sutherland.  All rights reserved.
This script is free software.  It may be used, redistributed,
and/or modified under the same terms as Perl itself.  See:

    http://www.perl.com/perl/misc/Artistic.html

=cut

