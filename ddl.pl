#! /usr/bin/perl -w

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

print "Enter Action [CREATE]: ";
chop( my $action = <STDIN> );
$action = "create" unless $action;

print "Enter Type    [TABLE]: ";
chop( my $type = <STDIN> );
$type = "TABLE" unless $type;

print "Enter Owner [\U$user]: ";
chop( my $owner = <STDIN> );
$owner = $user unless $owner;
die "\nYou must specify an Owner.\n" unless $owner;

print "Enter Name           : ";
chop( my $name = <STDIN> );
die "\nYou must specify an object.\n" unless $name;

print "\n";

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

