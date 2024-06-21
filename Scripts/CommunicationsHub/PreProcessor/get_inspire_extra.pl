#!C:/Perl5.10.1/bin/perl.exe

use 5.010;

use strict;
use warnings;

use FindBin qw($Bin);
use POSIX qw(strftime);

use lib $ENV{CCS_RESOURCE};
use lib_CCS others => ["$ENV{CCS_RESOURCE}/Global/Std"];
use CcsCommon;

my $InspireJob = $ARGV[0];

open my $saveout, ">&STDOUT";
open STDOUT, '>', "/dev/null";

my $chub_env      = CcsCommon::get_setting( 'GENERAL', 'gold_env' );
my $chub_ini      = CcsCommon::ini2h("$Bin/PreProcessor.ini");
my $chub_contract = $chub_ini->{'general'}{'commhub_contract'};
my $override_file = $chub_ini->{'inspire_extra'}{'override_file'};

my $inspire_extra_file = (
	lc($chub_env) eq 'production'
	? $chub_ini->{'archive_base'}{'production'}
	: $chub_ini->{'archive_base'}{'non_prod'}
) . "/$chub_contract/$InspireJob/$override_file";

my $inspire_extra = '';
if ( -f $inspire_extra_file ) {
	open my $fh, '<', $inspire_extra_file or die "open: $!\n";
	local $/ = undef;
	$inspire_extra = <$fh>;
	close $fh;
}

open STDOUT, ">&", $saveout;

print $inspire_extra;

exit 0;
