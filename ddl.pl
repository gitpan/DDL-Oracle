#! /usr/bin/perl -w

# $Id: ddl.pl,v 1.5 2000/12/09 17:55:20 rvsutherland Exp $

use strict;

use DBI;
use DDL::Oracle;
use English;

my  $dbh = DBI->connect(
                        "dbi:Oracle:",
                        "",
                        "",
                        {
                         PrintError => 0,
                         RaiseError => 1
                        }
    );

DDL::Oracle->configure( 
                        dbh    => $dbh,
#                        resize => 0,
#                        view   => 'user',
                      );

my $user = getlogin
        || scalar getpwuid($REAL_USER_ID)
        || undef;

print STDERR "Enter Action [CREATE]: ";
chomp( my $action = <STDIN> );
$action = "create" unless $action;

print STDERR "Enter Type    [TABLE]: ";
chomp( my $type = <STDIN> );
$type = "TABLE" unless $type;

print STDERR "Enter Owner [\U$user]: ";
chomp( my $owner = <STDIN> );
$owner = $user unless $owner;
die "\nYou must specify an Owner.\n" unless $owner;

print STDERR "Enter Name           : ";
chomp( my $name = <STDIN> );
die "\nYou must specify an object.\n" unless $name;

print STDERR "\n";

my $obj = DDL::Oracle->new(
                            type  => $type,
                            list  => [
                                       [
                                         $owner,
                                         $name,
                                       ]
                                     ]
                          );

my $sql;

if ( $action eq "drop" ){
    $sql = $obj->drop;
}
elsif ( $action eq "create" ){
    $sql = $obj->create;
}
elsif ( $action eq "resize" ){
    $sql = $obj->resize;
}
else{
    die "\nDon't know how to '$action'.\n";
} ;

print $sql;

# $Log: Ddl.pl,v $
# Revision 1.5  2000/12/09 17:55:20  rvsutherland
# Re-added after CVS bug fixed.
#
# Revision 1.3  2000/11/11 07:48:59  rvsutherland
# Added CVS tags
#

