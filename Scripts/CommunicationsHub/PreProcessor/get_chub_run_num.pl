#!C:/Perl5.10.1/bin/perl.exe

use 5.010;

use strict;
use warnings;

use FindBin qw($Bin);
use Fcntl qw(:flock);
use POSIX qw(strftime);

use lib $ENV{CCS_RESOURCE};
use lib_CCS others => ["$ENV{CCS_RESOURCE}/Global/Std"];
use AardvarkServices;

## -------------------------------------- BEGIN main() ------------------------------------------##

my ( $InspireJob, $ClientJob, $ClientRun ) = @ARGV;

open my $saveout, ">&STDOUT";
open STDOUT, '>', "/dev/null";

my $AaWebSvc = AardvarkServices->new();
my $PreprocEnv = CcsCommon::get_setting( 'GENERAL', 'gold_env' );

my $lock = get_lock();

my $InspireRun = $AaWebSvc->increment_contract_run_number($InspireJob)
	or die "AardvarkServices: Failed to get run number: " . $AaWebSvc->get_error();

unlock_it($lock);

open STDOUT, ">&", $saveout;

print $InspireRun;

exit 0;

## --------------------------------------- END main() -------------------------------------------##

sub get_lock {
	my $chub_ini      = CcsCommon::ini2h("$Bin/PreProcessor.ini");
	my $chub_contract = $chub_ini->{'general'}{'commhub_contract'};

	my $lockf = (
		  $PreprocEnv eq 'production'
		? $chub_ini->{'archive_base'}{'production'}
		: $chub_ini->{'archive_base'}{'non_prod'}
	) . "/$chub_contract/$InspireJob/RunNum.lock";

	open my $fh, '>>', $lockf or die "Failed to open '$lockf': $!\n";
	flock( $fh, LOCK_EX ) or die "Failed to lock '$lockf': $!\n";

	return $fh;
}

sub unlock_it {
	my $fh = shift;

	printf $fh (
		"[%s] %s %d => %s %d%s\n",
		strftime( '%Y%m%d %H%M%S', localtime ),
		$ClientJob,
		$ClientRun,
		$InspireJob,
		$InspireRun,
		{
			development => ' [DEV]',
			uat         => ' [QA]',
		}->{$PreprocEnv},
	);

	close $fh;
	return;
}
