#!/tool/pandora64/bin/perl5.24.0
#===============================================================================
#
#     FILE:     skeleton.pl
#     $Author: midenney $
#     COMPANY:  aBiz
#     CREATED:  06/11/07
#     $Revision: 1.5 $
#     $Source: /tool/sysadmin/CVSROOT/makefs/makefs/bin/skeleton.pl,v $
#     $ID =      q$Id: skeleton.pl,v 1.5 2020/06/12 20:50:05 midenney Exp $;
#===============================================================================
# change here
use FindBin qw($RealBin);
use lib "$RealBin/../lib";

use strict;
use warnings;
use feature qw(say);
use DBI;
use Getopt::Long;
use Pod::Usage qw(pod2usage);
use Switch;
use Cwd;
use Net::SNMP qw(:snmp);
use POSIX qw(ceil);
use Data::Dumper;
use Pod::Usage qw(pod2usage);

use Database;
use Cmode;

use vars qw($verbose $debug $dry $help);

chdir($FindBin::Bin) || die "Unable to chdir to script bin\n";
GetOptions(
          'verbose|v'      => \$verbose,
          'debug|de'       => \$debug,
          'help|?'         => \$help,
          'dry|d'          => \$dry
) || pod2usage(2);
my $msg="";
if ($help) { pod2usage(-msg => $msg, -exitVal => 0 , -verbose => 2 , -output => \*STDOUT ); } ;
sub usage{
   my $exitv = 1;
   if ($help) { $exitv = 0};

   pod2usage(1) if $help;
   #pod2usage("$script: No files given.")  if ((@ARGV == 0) && (-t STDIN));
   pod2usage({
      -message => "Msg text here" ,
      -verbose => 2,
      -exitval => $exitv
   });
}

say "Hello world from skeleton.pl"

__END__

=head1 NAME

Skeleton.pl - Skeleton structure for new scripts

=head1 SYNOPSIS

skeleton.pl (-d|--dry) (-v|--verbose) (-h|--help)  (-de|--debug)

