#! /usr/bin/perl -w

# $Id: defrag.pl,v 1.3 2000/11/17 21:35:53 rvsutherland Exp $
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

my @export_objects;
my @sizing_array;

my $aref;
my $create_ndx_ddl;
my $create_tbl_ddl;
my $cwd  = cwd;
my $dbh;
my $drop_ddl;
my $filename;
my $home = $ENV{HOME}
        || $ENV{LOGDIR}
        || ( getpwuid($REAL_USER_ID) )[7]
        || die "\nCan't determine HOME directory.\n";
my $index_query;
my $iot_query;
my $obj;
my $other_constraints;
my $sth;
my $table_query;
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

connect_to_oracle();

my $sqldir = ( $args{ sqldir } eq "." ) ? $cwd : $args{ sqldir };
my $tblsp  = uc( $args{ tablespace } );
my $prefix = $args{ prefix };

#
# Display user options, and save them in .defrag.rc
#

print "Generating files to defrag  Tablespace \U$tblsp.\n\n";

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

# Confirm the tablespace exists
my $stmt =
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
my $row = $sth->fetchrow_array;

if ( not $row ) 
{
  print STDERR "\n***Error:  Tablespace \U$tblsp",
               " does not exist\n",
               "           or is not ONLINE\n",
               "           or is managed LOCALLY\n",
               "           or is a TEMPORARY tablespace\n",
               "           or contains ROLLBACK segments.\n\n";
  exit 1;
}

#
# Now we're ready -- start dafriggin' defraggin'
#

print "Working...\n";

initialize_queries();

# The 8 steps below issue queries comprised of 3 main queries, sometimes
# doing UNIONs and/or MINUSes among them.  The queries are:.
#
#    $table_auery - retrieves Owner/Name of all tables entirely contained
#                   within the tablespace -- a non-partitioned table or a
#                   partitioned table with *every* partition residing in
#                   the tablespace.
#    $iot_query   - same as $table)_query, but for IOT's
#    $index_query - retrieves Owner/Index Name/Table Name for all indexes
#                   not belonging to the tables above with at least one
#                   segment within the tablespace.  Indexes do not have
#                   syntax for partition exchanges, so we're going to drop
#                   and recreate the entire index even if only one partition
#                   is in the tablespace.
# Note that for performance reasons, the queries consist of UNION ALLs and
# may return duplicates.  Therefore the wrapper query will perform SELECT
# DISTINCT's.
#
# The results of the generated DDL is stored in 1 or 2 of the variables
# which will be written to the 3 SQL files.  These are:
#
#     $drop_ddl       - Contains all of the DROP statements for Tables,
#                       Indexes and Constraints.  This is designed to run
#                       after the Export has been checked and OK'ed.
#                       Triggers are not dropped, but are restored by the
#                       Import.
#     $create_tbl_ddl - Contains all of the CREATE statements for Tables.
#                       This is run after the objects are dropped and the
#                       tablespace is coalesced.
#     $create_ndx_ddl - Contains all of the CREATE statemsnts for Indexes
#                       and Constraints.  This is executed after the Import
#                       (which allows the data to be imported into unindexed
#                       tables).

#
# Step 1 - Drop all Foreign Keys referenceing our tables and IOT's or
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

$drop_ddl .= $obj->drop if @$fk_aref;

#
# Step 2 - Drop and create the tables.  NOTE:  the DROP statements are in
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

foreach my $row ( @$aref )
{
  push @export_objects, "\L@$row->[0].@$row->[1]";
}

$obj = DDL::Oracle->new(
                         type => 'table',
                         list => $aref,
                       );

if ( @$aref )
{
  $drop_ddl       .= $obj->drop;
  $create_tbl_ddl .= $obj->create;
}

#
# Step 3 - Drop all Primary Key, Unique and Check constraints on the tables
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
                                )
                                MINUS
                                (
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

$drop_ddl .= $obj->drop if @$aref;

#
# Step 4 - Drop all of our indexes, unless they are the supporting index
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

$drop_ddl .= $obj->drop if @$aref;

#
# Step 5 - Create ALL indexes.
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

$create_ndx_ddl .= $obj->create if @$aref;

#
# Step 6 - Create all Primary Key, Unique and Check constraints on our
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
foreach my $row ( @$aref )
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

$create_ndx_ddl .= $obj->create if @constraints;

#
# Step 7 - Create all Check constraints on our IOT tables (their PK was
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
foreach my $row ( @$aref )
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

$create_ndx_ddl .= $obj->create if @constraints;

#
# Step 8 - And finally, recreate all Foreign Keys referenceing our tables
# and IOT's or referenceing the tables of our other indexes.  Use the 
# same list used in Step 1 to drop them ($fk_aref).
#

$obj = DDL::Oracle->new(
                         type => 'constraint',
                         list => $fk_aref,
                       );

$create_ndx_ddl .= $obj->create if @$fk_aref;

#
# Actually, it's not final.  We still have to deal with individual partitions.
#

#
# It's hard to believe, but maybe they gave us an empty tablespace
# to practice on.
#

die "\n***Error:  Tablespace $tblsp is empty. 
           Doest thou take me for a fool?\n\n"
     unless $create_tbl_ddl . $create_ndx_ddl;

#
# OK, so we're ligit.  Coalesce all data/index tablespaces
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

print "\n";

$filename = $sqldir . "/" . $prefix . $tblsp . "_drop_all.sql";
print "Drop objects  is   :  $filename\n";
open DELALL, ">$filename"     or die "Can't open $filename: $!\n";
write_header( \*DELALL, $filename, 'REM' );
print DELALL $drop_ddl;
print DELALL "\n\nREM  --- END OF FILE ---\n\n";
close DELALL                  or die "Can't close $filename: $!\n";

$filename = $sqldir . "/" . $prefix . $tblsp . "_add_tbl.sql";
print "Create tables is   :  $filename\n";
open ADDTBL, ">$filename"     or die "Can't open $filename: $!\n";
write_header( \*ADDTBL, $filename, 'REM' );
print ADDTBL $create_tbl_ddl;
print ADDTBL "\n\nREM  --- END OF FILE ---\n\n";
close ADDTBL                  or die "Can't close $filename: $!\n";

$filename = $sqldir . "/" . $prefix . $tblsp . "_add_ndx.sql";
print "Create indexes is  :  $filename\n";
open ADDNDX, ">$filename"     or die "Can't open $filename: $!\n";
write_header( \*ADDNDX, $filename, 'REM' );
print ADDNDX $create_ndx_ddl;
print ADDNDX "\n\nREM  --- END OF FILE ---\n\n";
close ADDNDX                  or die "Can't close $filename: $!\n";

my $logdir = ( $args{logdir} eq "." ) ? $cwd : $args{ logdir };
my $expdir = ( $args{expdir} eq "." ) ? $cwd : $args{ expdir };

my $pipefile = $expdir . "/" . $prefix . $tblsp . ".pipe";
unlink $pipefile;
print "Export FIFO pipe is:  $pipefile\n";
eval { system ("mknod $pipefile p") };

$filename = $expdir . "/" . $prefix . $tblsp . "_imp.par";
print "Import parfile is  :  $filename\n";
open IMPPAR, ">$filename"     or die "Can't open $filename: $!\n";
write_header(\*IMPPAR, $filename, '# ' );

my $logfile = $logdir . "/" . $prefix . $tblsp . "_imp.log";
print "Import logfile is  :  $logfile\n";

print IMPPAR "log         = $logfile\n",
             "file        = $pipefile\n",
             "rows        = y\n",
             "commit      = y\n",
             "ignore      = y\n",
             "buffer      = 65536\n",
             "analyze     = n\n",
             "full        = y\n\n",
             "#tables      = (\n",
             "#                  ",
             join ( "\n#                , ", @export_objects ),
             "\n#              )\n\n\n\n",
             "#  --- END OF FILE ---\n\n";
close IMPPAR                  or die "Can't close $filename: $!\n";

$filename = $expdir . "/" . $prefix . $tblsp . "_exp.par";
print "Export parfile is  :  $filename\n";
open EXPPAR, ">$filename"     or die "Can't open $filename: $!\n";
write_header(\*EXPPAR, $filename, '# ' );

my $logfile = $logdir . "/" . $prefix . $tblsp . "_exp.log";
print "Export logfile is  :  $logfile\n";

print EXPPAR "log         = $logfile\n",
             "file        = $pipefile\n",
             "rows        = y\n",
             "grants      = y\n",
             "#direct      = y\n",   # Has bug on Import??
             "buffer      = 65536\n",
             "indexes     = n\n",
             "compress    = n\n",
             "triggers    = y\n",
             "constraints = n\n",
             "tables      = (\n",
             "                  ",
             join ( "\n                , ", @export_objects ),
             "\n              )\n\n\n\n",
             "#  --- END OF FILE ---\n\n";
close EXPPAR                  or die "Can't close $filename: $!\n";

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

# sub get_args
#
# Uses supplied module Getopt::Long to place command line options into the
# hash %args.  Ensures that at least one of the mandatory arguments has
# been supplied, and sets the defrag type.
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
  if ( @ARGV ) 
  {
    print STDERR "\n***Error:  unrecognized argument";
    print  ( @ARGV == 1 ? ":  " : "s:  " );
    print ( join " ",@ARGV );
    print "\n\n$0 aborted,\n\n";
    exit 1;
  }
  #
  # Otherwise, abort unless we have minimum requirements
  #
  elsif ( not defined $args{ tablespace } ) 
  {
    print STDERR "\n***Error:  You must specify --tablespace\n\n",
                 "$0 aborted,\n\n";
    exit 1;
  }
}

# sub initialize_queries
#
# Initializes the 3 driving queries used to
# retrieve object names involved in the defrag.
#
sub initialize_queries
{
  $index_query =
      "
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
              AND index_type       <> 'IOT - TOP'
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
              AND index_type       <> 'IOT - TOP'
      ";

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
              AND NOT EXISTS (
                               SELECT
                                      null
                               FROM
                                      dba_ind_partitions
                               WHERE
                                          index_owner      = i.owner
                                      AND table_name       = i.index_name
                                      AND tablespace_name <> '$tblsp'
                               UNION ALL
                               SELECT
                                      null
                               FROM
                                      dba_ind_subpartitions
                               WHERE
                                          index_owner      = i.owner
                                      AND table_name       = i.index_name
                                      AND tablespace_name <> '$tblsp'
                             )
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
              AND NOT EXISTS (
                               SELECT
                                      null
                               FROM
                                      dba_ind_subpartitions
                               WHERE
                                          index_owner      = i.owner
                                      AND table_name       = i.index_name
                                      AND tablespace_name <> '$tblsp'
                             )
      ";

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
           and may be omitted (in which case filenames will begin with
           the name of the tablespace).

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
  return;
}

# sub set_defaults
#
# If file HOME/.defragrc exists, reads its contents into hash %args.
# Otherwise, it fills the hash with arbitrary defaults.
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
  $args{ expdir } = "."       unless $args{ expdir };
  $args{ sqldir } = "."       unless $args{ sqldir };
  $args{ logdir } = "."       unless $args{ logdir };
  $args{ prefix } = "defrag_" unless $args{ prefix };
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
# Revision 1.3  2000/11/17 21:35:53  rvsutherland
# Commented out Direct Path export -- Import has a bug (at least on Linux)
#
# Revision 1.2  2000/11/16 09:14:38  rvsutherland
# Major restructure to take advantage of DDL::Oracle.pm
#

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

=head1 TODO

=head1 BUGS

=head1 FILES

=head1 AUTHOR

 Richard V. Sutherland
 rvsutherland@yahoo.com

=head1 COPYRIGHT

Copyright (c) 2000, Richard V. Sutherland.  All rights reserved.
This script is free software.  It may be used, redistributed,
and/or modified under the same terms as Perl itself.  See:

    http://www.perl.com/perl/misc/Artistic.html

=cut

