# Version: 0.0.5
# Timestamp: 2024-06-21 21:31:21 +0530
# Author: pavan kumar

# change in 4 for 5

#!C:/Strawberry/perl/bin/perl.exe

use v5.32;

use strict;
use warnings;

## ------------------------------- BEGIN Program Description ------------------------------------##
#
# This script converts input datafile(s) using the specified communications hub job and converter.
# Converted output files are in .JSON format. Communications Hub contract starts with 800CH.
#
# The process invokes multiple helper scripts to obtain (see [helper] section in PreProcessor.ini)
#
# 1) get_ccs_setting.pl - get parameter value from [general] section from ccssite.ini
# 2) get_chub_run_num.pl - get run number from Aardvark for given communications hub job
# 3) get_client_run_num.pl - get run number from Aardvark for given client contract job
# 4) get_encompass_qco.pl - get JSON content from Encompass for given client contract job
# 5) get_stream_setup.pl - get stream setup from Aardvark for given client contract job
#
# Finally, it builds a startup file and invokes InspireRunJob.exe for doc composition.
#
# This application runs as follows:
# C:\Strawberry\perl\bin\perl.exe \
#   connect2_convert.pl           \
#     --contract   <ClientJob>    \
#     --converter  <Converter>    \
#     <InputFile>
#
# Alternatively, an Aardvark style --startupfile <StartupFile> can be used as follows:
# C:\Strawberry\perl\bin\perl.exe connect2_convert.pl --startupfile <StartupFile>
#
# where <StartupFile> contains the following key=value parameters:
# JobNumber=<ClientJob>
# DataFileName=<InputFile>
# Extras=--converter <Converter>
# StartupFile=<StartupFile>
# TraceFile=<TraceFile>
# ErrorFile=<ErrorFile>
#
## --------------------------------- END Program Description ------------------------------------##

use Data::Printer;
use Getopt::Long;
use FindBin qw($Bin);
use IPC::Run qw(run);
use File::Copy;
use File::Basename;
use Date::Format;
use JSON;

## --------------------------------- BEGIN Global variables -------------------------------------##

my $DataFileName;
my %PP_startup;
my %Options = (
	'startupfile=s'   => \my $StartupFile,
	'contract=s'      => \my $ClientJob,
	'converter=s'     => \my $Converter,
	'inspirejob=s'    => \my $InspireJob,
	'inspireextras=s' => \my $InspireExtras,
	'startuponly'     => \my $StartupOnly,
	'help'            => sub { usage(); exit 0 },
);

my $INI_file;
my $CHUB_ini;

my @InspireRunJobCmd;

## ---------------------------------- END Global variables --------------------------------------##
## -------------------------------------- BEGIN main() ------------------------------------------##

$INI_file = "$Bin/PreProcessor.ini";
$CHUB_ini = preprocessor_init($INI_file);
#say "Communications Hub init: ", np $CHUB_ini;

GetOptions(%Options);
$DataFileName = $ARGV[0];

if ( defined $StartupFile ) {
	%PP_startup = parse_startupfile($StartupFile);

	$DataFileName = $PP_startup{'DataFileName'};
	$ClientJob    = $PP_startup{'JobNumber'};

	if ( defined $PP_startup{'Extras'} ) {
		Getopt::Long::GetOptionsFromString( $PP_startup{'Extras'}, %Options );
	}

	say "Preprocessor startup: ", np %PP_startup;
}

die "No input file specified! Use --help for usage\n"
	if not defined $DataFileName;

die "Specify a contract number with --contract. Use --help for usage\n"
	if not defined $ClientJob;

die "Specify a converter with --converter. Use --help for usage\n"
	if not defined $Converter;

die "Missing Converter '$Converter' specification in '$INI_file'\n"
	if not exists $CHUB_ini->{'converter'}{$Converter};

$Converter = $CHUB_ini->{'converter'}{$Converter};

$InspireJob //= $Converter->[1];

die "Invalid Communications Hub contract code '$InspireJob'\n"
	if $InspireJob !~ /^$CHUB_ini->{'general'}{'commhub_contract'}\d+$/;

# build InspireRunJob startup file, then call InspireRunJob.exe with it
@InspireRunJobCmd = ( $CHUB_ini->{'general'}{'inspire_runjob'} );
push @InspireRunJobCmd, '--startupfile', build_startupfile();
say 'RUNNING: ' . join( ' ', @InspireRunJobCmd );

# exit if --startuponly is set; this is to test startup file creation only
die "Option --startuponly set. Exiting\n" if defined $StartupOnly;

run \@InspireRunJobCmd;

exit 0;

## --------------------------------------- END main() -------------------------------------------##

sub preprocessor_init {
	my $ini_file = shift;

	if ( not -f $ini_file ) {
		die "INI file '$ini_file' not found!\n";
	}

	open my $INIFH, '<', $ini_file
		or die "Failed to read INI file '$ini_file': $!\n";

	my $init;
	my $section = 'general';

	while (<$INIFH>) {
		s/\r?\n$//ms;
		s/\s+$//;

		next if /^[;#]/ || /^$/;

		if (/^\[(\w+)\]$/) {
			$section = $1;
			next;
		}

		my ( $key, $value ) = split /=/, $_, 2;

		$value = undef if '(null)' eq $value;
		$value = [ split( ',', $value ) ] if $value =~ /,/;

		$init->{$section}{$key} = $value;
	}

	close $INIFH;

	my $irjexe = $init->{'general'}{'inspire_runjob'};
	$irjexe =~ s/\%(\w+)\%/$ENV{$1}/e;
	$irjexe =~ s/\\/\//g;
	die "No such executable '$irjexe'\n" if not -x $irjexe;
	$init->{'general'}{'inspire_runjob'} = $irjexe;

	delete $init->{'inspire_extra'}{'override_file'};

	foreach my $key ( keys %{ $init->{'stack'} } ) {
		my $exe = $init->{'stack'}{$key};
		$exe =~ s/\%(\w+)\%/$ENV{$1}/e;
		$exe =~ s/\\/\//g;
		die "No such executable '$exe'\n" if not -x $exe;
		$init->{'stack'}{$key} = $exe;
	}

	# validate the converter harnesses
	foreach my $key ( keys %{ $init->{'harness'} } ) {
		my ( $stack, $script ) = @{ $init->{'harness'}{$key} };
		die "Invalid stack '$stack' for harness '$key'\n"
			if not exists $init->{'stack'}{$stack};
		$stack  = $init->{'stack'}{$stack};
		$script = "$Bin/$script";
		die "No such script '$script'\n" if not -f $script;
		$init->{'harness'}{$key} = [ $stack, $script ];
	}

	# validate the 'helper' scripts
	foreach my $key ( keys %{ $init->{'helper'} } ) {
		my ( $stack, $script ) = @{ $init->{'helper'}{$key} };
		die "Invalid stack '$stack' for helper '$key'\n"
			if not exists $init->{'stack'}{$stack};
		$stack  = $init->{'stack'}{$stack};
		$script = "$Bin/$script";
		die "No such script '$script'\n" if not -f $script;
		$init->{'helper'}{$key} = sub {
			my @command = ( $stack, $script, @_ );
			say 'RUNNING: ' . join( ' ', @command );
			run \@command, '>', \my $output;
			return $output;
		};
	}

	# validate the 'doc_type' entries
	foreach my $key ( keys %{ $init->{'doc_type'} } ) {
		my $contract = $init->{'doc_type'}{$key}[0];
		die "Invalid contract '$contract' for doc_type '$key'\n"
			if $contract !~ /^$init->{'general'}{'commhub_contract'}\d+$/;
		my $harness = $init->{'doc_type'}{$key}[1];
		die "Invalid harness '$harness' for doc_type '$key'\n"
			if not exists $init->{'harness'}{$harness};
		# Q2G module is optional; skip validation
	}

	# validate the 'converter' entries
	foreach my $key ( keys %{ $init->{'converter'} } ) {
		my $doc_type = $init->{'converter'}{$key};
		die "Invalid doc_type '$doc_type' for converter '$key'\n"
			if not exists $init->{'doc_type'}{$doc_type};
		$init->{'converter'}{$key} =
			[ $key, @{ $init->{'doc_type'}{$doc_type} } ];
	}

	return $init;
}

sub parse_startupfile {
	my $startupfile = shift;

	if ( not -f $startupfile ) {
		die "startup file '$startupfile' not found!\n";
	}

	my %parsed;

	open my $STARTUPFH, '<', $startupfile
		or die "Failed to read startup file '$startupfile': $!\n";

	while (<$STARTUPFH>) {
		s/\r?\n$//ms;
		s/\s+$//;
		my ( $key, $value ) = split /=/, $_, 2;

		# more Perl-y
		$value = undef if '(null)' eq $value;

		# handle generically
		if ( exists $parsed{$key} ) {
			if ( ref $parsed{$key} ) {
				push @{ $parsed{$key} }, $value;
			}
			else {
				$parsed{$key} = [ $parsed{$key}, $value ];
			}
		}
		else {
			$parsed{$key} = $value;
		}
	}

	close $STARTUPFH;

	if ( defined $parsed{'ErrorFile'} ) {
		say "REDIRECTING STDERR to '$parsed{ErrorFile}'";
		open STDERR, '>>', $parsed{'ErrorFile'}
			or die "ERROR redirecting STDERR: $!";
		STDERR->autoflush(1);
	}
	else {
		die "ERROR redirecting STDERR: $!";
	}

	if ( defined $parsed{'TraceFile'} ) {
		say "REDIRECTING STDOUT to '$parsed{TraceFile}'";
		open STDOUT, '>>', $parsed{'TraceFile'}
			or die "ERROR redirecting STDOUT: $!";
		STDOUT->autoflush(1);
	}
	else {
		die "ERROR redirecting STDOUT: $!";
	}

	return %parsed;
}

sub convert_input {
	my ( $client_job, $client_run ) = @_;

	my $filename = basename($DataFileName);
	move $DataFileName, '.'
		or die "Could not move '$filename' to current dir: $!\n";

	my $harness = $Converter->[2];

	# build the command for run()
	my @command = @{ $CHUB_ini->{'harness'}{$harness} };
	push @command, '--converter', $Converter->[0];
	push @command, '--contract',  $client_job;
	push @command, '--run',       $client_run;
	push @command, '--file',      $filename;

	say 'RUNNING: ' . join( ' ', @command );
	run \@command, '>', \my $output;
	say $output;

	my $jsonfile;
	while ( $output =~ /^(.+) -> (.+)$/mg ) {
		if ( $1 eq $filename ) {
			$jsonfile = $2;
			last;
		}
	}

	die "'$Converter->[0]' converter failed to convert '$filename'\n"
		if !( defined $jsonfile && -f $jsonfile && $jsonfile =~ /\.json$/i );

	return ( $filename, $jsonfile );
}

sub build_startupfile {
	my (
		$client_run,      $inspire_run,      $inputfile,         $outputfile,     $doc_type,
		$preproc_env,     $job_description,  $qco_json_file,     $qco_json_str,   $qco_fh,
		$qco_op_details,  $stream_setup_xml, $ssx_fh,            %startup_extras, $get_ccs_setting,
		$today_and_now,   $startup_filename, $progress_filename, $trace_filename, $error_filename,
		%inspire_startup, $startup_fh,
	);

	die "Missing Client Run Number in Preprocessor startupfile\n"
		if exists $PP_startup{'RunNumber'} && !defined $PP_startup{'RunNumber'};

	$client_run = $PP_startup{'RunNumber'} // $CHUB_ini->{'helper'}{'get_client_run_num'}->($ClientJob);

	die "Missing or Invalid Client Run Number '$client_run'\n"
		if not( defined $client_run && $client_run =~ /^\d+$/ );

	$inspire_run =
		$CHUB_ini->{'helper'}{'get_chub_run_num'}->( $InspireJob, $ClientJob, $client_run );

	die "Missing or Invalid Inspire Run Number '$inspire_run'\n"
		if !( defined $inspire_run && $inspire_run =~ /^\d+$/ );

	# convert the input data to JSON using harness and converter module
	( $inputfile, $outputfile ) = convert_input( $ClientJob, $client_run );
	$outputfile =~ /\.(\w+)\.json$/i and $doc_type = $1;

	$preproc_env = lc $CHUB_ini->{'helper'}{'get_ccs_setting'}->('gold_env');
	# the following overrides the 'gold_env' as DEV autoproces are executed using UAT Aardvark
	#hack $PP_startup{'JobDescription'} =~ / DEV / and $preproc_env = 'development';

	$job_description = "$InspireJob - Step 4 - Communications Hub ";
	$job_description .= { development => '[DEV] ', uat => '[QA]', }->{$preproc_env};
	$job_description .= "- $ClientJob Run #$client_run $doc_type - Run #$inspire_run";

	# build the additional encompass quadient content object JSON file
	$qco_json_file = "${ClientJob}.${client_run}.QCO.json";
	$qco_json_str  = $CHUB_ini->{'helper'}{'get_encompass_qco'}->($ClientJob);
	open $qco_fh, '>', $qco_json_file or die "Failed to open '$qco_json_file': $!\n";
	print $qco_fh $qco_json_str;
	close $qco_fh;

	$qco_op_details = decode_json($qco_json_str)->{'content'}{'data'}[0]{'operations_details'};

	# build the stream setup file aka fetch the client specific job bag from Aardvark
	$stream_setup_xml = "stream_setup_${ClientJob}_${client_run}.xml";
	open $ssx_fh, '>', $stream_setup_xml or die "Failed to open '$stream_setup_xml': $!\n";
	print $ssx_fh $CHUB_ini->{'helper'}{'get_stream_setup'}->($ClientJob);
	close $ssx_fh;

	# now, let's construct this very long Extras argument for the Inspire job
	%startup_extras = %{ $CHUB_ini->{'inspire_extra'} };

	$get_ccs_setting                       = $CHUB_ini->{'helper'}{'get_ccs_setting'};
	$startup_extras{'site'}                = $get_ccs_setting->('site');
	$startup_extras{'aardvarkAppAdminUri'} = $get_ccs_setting->('aardvark_web_svc_appadmin_proxy');

	$startup_extras{'streamSetupXML'} = $stream_setup_xml;
	$startup_extras{'jobConfigName'}  = "JobConfig_${doc_type}.xml";
	$startup_extras{'environment'}    = $CHUB_ini->{'inspire_env'}{$preproc_env};

	$InspireExtras //= $CHUB_ini->{'helper'}{'get_inspire_extra'}->($InspireJob);

	if ( defined $InspireExtras && length($InspireExtras) ) {
		say "Inspire startup extra override: $InspireExtras";
		my @inspireextras = split( ' ', $InspireExtras );

		foreach my $extra (@inspireextras) {
			if ( $extra =~ /(\w+)=(.+)/ ) {
				$startup_extras{$1} = $2;
			}
			else {
				die "Invalid --inspireextras option $InspireExtras";
			}
		}
	}

	say "Startup Extras: ", np %startup_extras;

	$today_and_now = time2str( '%j.%H%M%S', time );
	$startup_filename = "${InspireJob}.${inspire_run}.${today_and_now}.startup.txt";

	( $progress_filename = $startup_filename ) =~ s/startup/progress/;
	( $trace_filename    = $startup_filename ) =~ s/startup/trace/;
	( $error_filename    = $startup_filename ) =~ s/startup/error/;

	%inspire_startup = (
		JobNumber        => $InspireJob,
		ClientCode       => $CHUB_ini->{'general'}{'commhub_contract'},
		RunNumber        => $inspire_run,
		DataFileName     => [ $inputfile, $outputfile, $qco_json_file ],
		JobDescription   => $job_description,
		Extras           => join( ' ', map { "--$_ $startup_extras{$_}" } sort keys %startup_extras ),
		StartupFileName  => $startup_filename,
		ProgressFile     => $progress_filename,
		TraceFile        => $trace_filename,
		ErrorFile        => $error_filename,
		ProcessingScript => $CHUB_ini->{'general'}{'inspire_runjob'},
		JobQueueId     => $PP_startup{'JobQueueId'}     // '0',
		Product        => $PP_startup{'Product'}        // '(null)',
		SLAProcessCode => $PP_startup{'SLAProcessCode'} // 'All',
		# following custom keys are used during post-processing in Q2G step
		_requireSignOffYN => $qco_op_details->{'requires_approval_yn'} || 'No',
	);

	# _Q2GType is an optional post-processing module during q2g step
	$inspire_startup{'_Q2GType'} = $Converter->[3] if defined $Converter->[3];

	say "InspireRunJob startup: ", np %inspire_startup;

	open $startup_fh, '>', $startup_filename or die "Failed to open $startup_filename: $!\n";

	foreach my $key ( sort keys %inspire_startup ) {
		my $value = $inspire_startup{$key};
		$value = '(null)' if not defined $value;

		if ( ref($value) eq 'ARRAY' ) {
			foreach ( sort @{$value} ) {
				$_ = '(null)' if not defined $_;
				print $startup_fh "$key=$_\n";
			}
		}
		else {
			print $startup_fh "$key=$value\n";
		}
	}

	close $startup_fh;

	say "Created InspireRunJob startup file '$startup_filename'";
	return $startup_filename;
}

sub usage {
	my ($cmd) = $0 =~ /^.*?[\\\/]?([^\\\/]+?)(?:\.\w{1,4})?$/;

	print <<"EOF";
 $cmd: Convert an input file, then calls InspireRunJob to submit to Quadient

Options:
  --startupfile     <FILE>          Aardvark auto-proc startup file

Development Options:
  --contract        <CONTRACT>      Client job number
  --converter       <CONVERTER>     Converter name (see PreProcessor.ini file)
  --inspirejob      <CONTRACT>      Communications Hub contract code
  --inspireextras   <KEY=VALUE>     Add/update Inspire Extras parameter(s) (optional)

  --startuponly                	    build startupfile only; do not execute InspireRunJob
  --help                            print this usage message
EOF

	return;
}
