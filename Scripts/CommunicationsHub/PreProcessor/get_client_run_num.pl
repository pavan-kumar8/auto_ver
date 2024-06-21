#!C:/Perl5.10.1/bin/perl.exe

use 5.010;

use strict;
use warnings;

use lib $ENV{CCS_RESOURCE};
use lib_CCS others => ["$ENV{CCS_RESOURCE}/Global/Std"];
use AardvarkServices;

open my $saveout, ">&STDOUT";
open STDOUT, '>', "/dev/null";

my $aa_web_svc = AardvarkServices->new();
my $run_number = $aa_web_svc->increment_contract_run_number( $ARGV[0] )
	or die "AardvarkServices: Failed to get run number: " . $aa_web_svc->get_error();

open STDOUT, ">&", $saveout;

print $run_number;

exit 0;
