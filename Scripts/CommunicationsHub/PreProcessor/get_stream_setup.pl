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
my $stream_setup_xml = $aa_web_svc->get_job_setup_xml( job_code => $ARGV[0] )
	or die "AardvarkServices: Failed to get job setup XML: " . $aa_web_svc->get_error();

open STDOUT, ">&", $saveout;

print $stream_setup_xml;

exit 0;
