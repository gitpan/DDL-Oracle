#! /usr/bin/perl -w

# $Id: copy_user.pl,v 1.3 2000/11/11 07:48:59 rvsutherland Exp $

use strict;

use DBI;
use DDL::Oracle;

my $obj;
my $ddl;
my $old_sql;
my $new_sql;
my $old_user;
my $new_user;
my @users;

# This is a simple connection.  Modify it to suit your needs.
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
                      );

# Printing prompts to STDERR allows the output to be
# redirected to a file (a good idea, eh?) and still
# allows the user to see the prompts.

print STDERR "\nEnter Name of Existing User (the Template) : ";
chomp( $old_user = lc( <STDIN> ) );
die "\nYou must specify an Existing User.\n" unless $old_user;
print STDERR "\n";

$obj = DDL::Oracle->new(
                         type  => 'user',
                         list  => [
                                    [
                                      'n/a',
                                      $old_user,
                                    ]
                                  ]
                       );

$old_sql = $obj->create;   # Will FAIL unless $old_user exists!

while (1)
{
  print STDERR "Enter Name of New User or <ENTER> when done: ";
  chomp( $new_user = lc( <STDIN> ) );
  last unless $new_user;
  push @users, $new_user;
}
die "\nYou must specify at least one New User\n\n" unless @users;
print STDERR "\n";

foreach $new_user( @users )
{
  $new_sql = $old_sql;
  $new_sql =~ s/$old_user/$new_user/go;
  $new_sql =~ s/REM.*\n//go;

  {
    # If $old_user is a Passworded Account 
    # and if there is an arbitrary method of assigning
    # passwords to new users, this is a good place to
    # substitute the new password for the VALUES 'ABCDEF...'.

    # For example:
    my $password = $new_user . '123';
    $new_sql =~ s/VALUES \S+/$password/go;
  }

  $ddl .= $new_sql;
}

print $ddl;

# $Log: copy_user.pl,v $
# Revision 1.3  2000/11/11 07:48:59  rvsutherland
# Added CVS tags
#
# Revision 1.2  2000/11/11 00:20:42  rvsutherland
# Initial revision
#

