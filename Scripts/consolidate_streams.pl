#!C:/Perl5.10.1/bin/perl.exe

use 5.010;

use strict;
use warnings;

use Data::Dumper;
use Getopt::Long;
use File::Basename qw(basename dirname);
use File::Copy qw(copy move);
use File::Temp qw(tempdir);
use File::Path qw(rmtree);
use Cwd;

use lib $ENV{'CCS_RESOURCE'} . '/Global/Perl/5.10/site/lib';
use XML::Twig;

use lib $ENV{'CCS_RESOURCE'} . '/Global/Std';
use StreamMerge;
use Sort::GPD;
use AdmXml;
use CcsSmtp;
use CcsCommon;

=flow
args
	normal: directory with job/run-indicating trigger folders
	dev: a bunch of GPDs (maybe just in leftover @ARGV)
	alternate: list of GPD folders

from those folders, get a list of GPDs
	determine consolidation keys
	copy GPDs to consolidation target folders: 001, 002, etc.

rewrite GPDs
	change stock keys
	add set tags for recon
	mark streams as unsorted

merge GPDs
	call global module, concatenate GPDs

sort GPDs

write reports

send to queue

=cut

my @valid_additions = qw(micr color-plex);
my %exclude_workflows = map { $_ => 1 } qw(optical_disc email fax sms dss viewpoint);
my $print_site;
my $archive_folder = CcsCommon::ini2h( $ENV{'CCS_SETTINGS'} . '/ccssite.ini' )->{'general'}{'archive_folder'};
$archive_folder =~ s{\\}{/}g;

my $SEVENZ = $ENV{'CCS_RESOURCE'} . '/Global/Programs/7zip/16.02/7z.exe';

GetOptions(
	'trigger_dir=s' => \my $trigger_dir,
	'startupfile=s' => \my $startupfile,
	'gpd_dir=s'     => \my @gpd_dirs,
	'job=s'         => \my $consolidated_job,
	'run=i'         => \my $consolidated_run,
	'verbose!'      => \my $verbose,
	'pause'         => \my $pause,
	'criteria=s'    => \my @add_criteria,
	'delete!'       => \my $delete,
	'sort!'         => \my $sort_streams,
	'archive!'      => \my $archive,
	'stdout!'       => \my $stdout,
	'queue!'        => \my $queue,
	'help'          => sub { usage(0); },
) or usage(1);

if ( 'DEVELOPMENT' eq $ENV{'AREA'} ) {
	local $| = 1;
	# job/run default in development; otherwise, they're required
	$consolidated_job //= '139CO0015';
	$consolidated_run //= -1;

	# verbose on by default in dev
	if ( not defined $verbose ) {
		$verbose = 1;
		print "DEV MODE: enabling verbose mode\n";
	}

	# don't delete in dev, by default
	if ( not defined $delete ) {
		$delete = 0;
		print "DEV MODE: disabling automatic delete\n";
	}

	# don't archive
	if ( not defined $archive ) {
		$archive = 0;
		print "DEV MODE: disabling remote archiving\n";
	}

	# use STDOUT/STDERR
	if ( not defined $stdout ) {
		$stdout = 1;
		print "DEV MODE: using STDOUT instead of trace from startup\n";
	}

	# don't queue the run
	if ( not defined $queue ) {
		$queue = 0;
		print "DEV MODE: skipping the print queue\n";
	}

	# don't use the real archive
	$archive_folder = cwd();
}

$sort_streams //= 1;
$archive      //= 1;
$stdout       //= 0;
$queue        //= 1;

my $stream_source = 0;
$stream_source++ if $trigger_dir;
$stream_source++ if $startupfile;
$stream_source++ if @gpd_dirs;
if ( 1 < $stream_source ) {
	say 'We need just one of: --trigger_dir, --startupfile and --gpd_dir';
	usage(1);
}

my @trigger_files;
my ( $error, $trace );
my $init_failure = '';
if ($startupfile) {
	# get triggers, job and run from startup
	my %startup_details = get_startup_details($startupfile);

	$consolidated_job = $startup_details{'JobNumber'};
	$consolidated_run = $startup_details{'RunNumber'};

	@trigger_files = @{ $startup_details{'DataFileName'} };

	if ( not $stdout ) {
		$trace = $startup_details{'TraceFile'};
		$error = $startup_details{'ErrorFile'};
	}

	if ( not @trigger_files ) {
		$init_failure .= "No trigger files in startup!\n";
	}

	@trigger_files = grep { -f $_ } @trigger_files;

	if ( not @trigger_files ) {
		$init_failure .= "No trigger files found!\n";
	}
}

my $ERROR;
if ($error) {
	open $ERROR, '>>', $error or die "Cannot write to error file '$error': $!\n";
}
else {
	$ERROR = \*STDERR;
}
local $SIG{__WARN__} = sub { print $ERROR @_; };
local $SIG{__DIE__} = sub { print $ERROR @_; die; };

my $TRACE;
if ($trace) {
	open $TRACE, '>>', $trace or ( print $ERROR "Cannot write to trace file '$trace': $!\n" and exit 1 );
}
else {
	$TRACE = \*STDOUT;
}

# if we got a fail conditon before we set up ERROR
die $init_failure if $init_failure;

# select makes $TRACE the default file handle
# PBP doesn't like this, but that's b/c that behavior is seen as a bad side effect
# but it's exactly what we want, on a global scope
select $TRACE;    ## no critic InputOutput::ProhibitOneArgSelect

if ( not defined $delete ) {
	# delete/cleanup by default
	$delete = 1;
}

if ( not defined $consolidated_job or not defined $consolidated_run ) {
	print "ERROR: --job and --run required\n";
	usage(1);
}

foreach my $additional_criteria (@add_criteria) {
	my $found = 0;
	foreach my $valid (@valid_additions) {
		$found++ if $additional_criteria eq $valid;
	}

	if ( not $found ) {
		die "'$additional_criteria' isn't a valid field that can be added to consolidation criteria. Valid fields:\n"
			. join '', map { "  $_\n" } @valid_additions;
	}
}

my @gpds = ();
my %gpds = ();

if (@trigger_files) {
	# so we know where to stash single-run failures
	if ( not $trigger_dir ) {
		$trigger_dir = dirname( $trigger_files[0] );
		say "Determined trigger dir: $trigger_dir" if $verbose;
	}

	%gpds = process_triggers( list => \@trigger_files );
	@gpds = sort keys %gpds;
}
elsif ($trigger_dir) {
	%gpds = process_triggers( dir => $trigger_dir );
	@gpds = sort keys %gpds;
}
elsif (@gpd_dirs) {
	@gpds = process_gpd_dirs(@gpd_dirs);

	if ($verbose) {
		print "Found GPDs:\n";
		foreach my $gpd (@gpds) {
			print "  $gpd\n";
		}
	}
}
elsif (@ARGV) {
	foreach my $arg (@ARGV) {
		if ( $arg =~ /\.gpd$/i ) {
			push @gpds, $arg;
		}
		elsif ( $arg =~ /^(.+\.gpd)\.adm\.xml$/ ) {
			push @gpds, $1;
		}
		else {
			print "Unrecognized argument: '$arg'\n";
			usage(1);
		}
	}
}

if ( not @gpds ) {
	print "No GPDs!\n";

	if ( 'DEVELOPMENT' eq $ENV{'AREA'} ) {
		usage(1);
	}
	else {
		exit 0;
	}
}

my $consolidated_dir;
if ($delete) {
	$consolidated_dir = tempdir( 'consolidated_XXXX', DIR => $ENV{'TEMP'}, CLEANUP => 1 );
}
else {
	$consolidated_dir = tempdir( 'consolidated_XXXX', DIR => $ENV{'TEMP'} );
}

if (@trigger_files) {
	mkdir "$consolidated_dir/triggers" or die "Cannot create $consolidated_dir/triggers folder: $!";
	foreach my $trigger_file (@trigger_files) {
		if ( -f $trigger_file ) {
			copy $trigger_file, "$consolidated_dir/triggers" or die "Cannot copy trigger '$trigger_file': $!";
		}
		else {
			# we checked earlier, and we found all the files in @trigger_files
			# so this must have been moved aside in the process
			# continue on..
		}
	}
}

#******************************************************************************
print "About to stage GPDs to consolidation folder..\n";
pause() if $pause;
#******************************************************************************

# sort and copy to subfolders under job/run dir
my $staged = stage_streams(
	streams        => \%gpds,
	working_folder => $consolidated_dir,
	criteria       => \@add_criteria,
);

#******************************************************************************
print 'Staged ' . scalar(@gpds) . ' streams to ' . $staged->{'count'} . " folders in $consolidated_dir\n";
print "About to rewrite streams..\n";
pause() if $pause;
#******************************************************************************

# rewrite streams
my $rewrote = rewrite_streams($consolidated_dir);

#******************************************************************************
print "Rewrote $rewrote streams\n";
print "About to copy graphics..\n";
pause() if $pause;
#******************************************************************************

my $copied = copy_gpd_graphics($consolidated_dir);

#******************************************************************************
print "Copied $copied graphics\n";
print "About to merge streams..\n";
pause() if $pause;
#******************************************************************************

# merge GPDs by staged folder
my $merged = merge_folders($consolidated_dir);

#******************************************************************************
print "Merged $merged streams\n";

if ($sort_streams) {
	print "About to sort streams..\n";
	pause() if $pause;
	#**************************************************************************

	# sort!
	sort_gpds($consolidated_dir);

	#**************************************************************************
	print "Sorted!\n";
}

print "About to create and send reports..\n";
pause() if $pause;
#******************************************************************************

# reports
my $report_file = "StreamConsolidationReport_$consolidated_job" . "_run$consolidated_run.csv";
write_report( streams => $staged->{'streams'}, folder => $consolidated_dir, report => $report_file );

#******************************************************************************
print "Reported!\n";

my $upload_success = 1;
if ($queue) {
	print "About to send to the print queue..\n";
	pause() if $pause;
	#**************************************************************************

	if ( 'DEVELOPMENT' eq $ENV{'AREA'} ) {
		print "\nDEVELOPMENT: quit now?\n";
		pause();
	}

	# to queue
	$upload_success = send_to_print_queue( site => $print_site, folder => $consolidated_dir );

	#**************************************************************************
	if ($upload_success) {
		print "Queued!\n";
	}
	else {
		warn 'Print Queue Upload Failed!';
		exit 1;
	}
}

print "About to archive..\n";
pause() if $pause;
#******************************************************************************

# create zip and place in Gold archive
archive_consolidated_streams(
	job        => $consolidated_job,
	run        => $consolidated_run,
	tmp_folder => $consolidated_dir,
);

#******************************************************************************
print "Archived!\n";
print "About to clean up..\n";
pause() if $pause;
#******************************************************************************

if ($upload_success) {
	# cleanup
	finalize_triggers(
		delete           => $delete,
		consolidated_job => $consolidated_job,
		consolidated_run => $consolidated_run,
	) if 0;    # disabling b/c COCC file recon isn't needed immediately

	if ($delete) {
		#FIXME: quick and dirty
		unlink $_ for @trigger_files;
	}

	#**************************************************************************
	print "Consolidation process complete!\n";
	#**************************************************************************

	exit 0;
}
else {
	# triggers are left in place

	#**************************************************************************
	print "Consolidation process incomplete!\n";
	print "Address any apparent issues and re-queue.\n";
	#**************************************************************************

	exit 1;
}

sub usage {
	my $exit = shift;

	my ($cmd) = $0 =~ /^.*?[\\\/]?([^\\\/]+?)(?:\.\w{1,4})?$/;

	print <<"EOF";
 $cmd: Consolidate GPDs and send to the queue

Options:
  --startupfile   Aardvark auto-proc startup file
  --trigger_dir   folder full of per-run triggers
  --gpd_dir       location of GPDs; can specify several
  --job           consolidation target job
  --run           consolidation target run
  --nosort        do not sort consolidated streams
  --verbose       print more details
  --pause         pause after each phase
  --criteria      optional additional criteria
                     valid args: @valid_additions
  --nodelete      leave trigger files in the trigger folder
  --noarchive     don't move archive
  --stdout        print to STDOUT/STDERR instead of trace/error from startup
  --help          print this usage message
EOF

	exit $exit;
}

{
	my @triggers;
	my %job_run_by_stream;

	sub get_job_run_from_stream {
		my $stream = shift;

		if ( exists $job_run_by_stream{$stream} ) {
			return ( $job_run_by_stream{$stream}{'job'}, $job_run_by_stream{$stream}{'run'} );
		}
		else {
			return;
		}
	}

	sub process_triggers {
		my %args = @_;

		my $dir;
		my @found_triggers;

		if ( exists $args{'dir'} ) {
			$dir = $args{'dir'};
			$dir =~ s/\\/\//g;

			if ( not -d $dir ) {
				print "'$dir' isn't an accessible directory\n";
				usage(1);
			}

			if ( $dir !~ m{^[A-Z]:}i and $dir !~ m{^[\\/]} ) {
				print "'$dir' appears to be a relative path. Specify a full path instead.\n";
				usage(1);
			}

			@found_triggers = glob "$dir/consolidate.*.txt";
		}
		elsif ( exists $args{'list'} ) {
			@found_triggers = @{ $args{'list'} };
		}
		else {
			die 'Nothing useful passed to process_triggers()';
		}

		my @found = ();
		my %found = ();

	RUNS: foreach my $trigger (@found_triggers) {
			if ( $trigger =~ /consolidation_trigger\.txt$/ ) {
				# don't process, but still clean it up at the end
				push @triggers, $trigger;
				next;
			}

			# pull apart contract and run
			my ( $trigger_job, $trigger_run ) = $trigger =~ m{consolidate\.([^.]+)\.(\d+)\.txt$};

			if ( not defined $trigger_job or not defined $trigger_run ) {
				die "Could not extract job/run from trigger '$trigger'";
			}

			# for the trace file
			print "- $trigger_job/$trigger_run\n";

			# read streams from trigger
			# compare to gpds found
			my @trigger_list;
			my %qualified;
			my $archive_zip;

			open my $TRIGGER, '<', $trigger or do {
				single_run_error(
					trigger => $trigger,
					message => "Could not read trigger file '$trigger': $!",
				);
				next RUNS;
			};

			while ( my $line = <$TRIGGER> ) {
				$line =~ s/\r?\n$//ms;

				if ( $line =~ /^(\d+)$/ ) {
					my $stream_count = $1;
					if ( $stream_count != @trigger_list ) {
						close $TRIGGER;
						single_run_error(
							trigger => $trigger,
							message => "Count in trigger file doesn't match list of streams in '$trigger'",
						);
						next RUNS;
					}
				}

				my ( $name, $do ) = split ':', $line, 2;

				if ( 'ARCHIVE' eq $name ) {
					if ( -f $do ) {
						$archive_zip = $do;
					}
					elsif ( -d $do ) {
						# find specific zip under this folder
						if ( -f "$do\\Run$trigger_run.zip" ) {
							$archive_zip = "$do\\Run$trigger_run.zip";
						}
						else {
							close $TRIGGER;
							single_run_error(
								trigger => $trigger,
								message => "Could not find archive for run $trigger_run under $do",
							);
							next RUNS;
						}
					}
					else {
						close $TRIGGER;
						single_run_error(
							trigger => $trigger,
							message => "Could not find paths from trigger ARCHIVE line: $line",
						);
						next RUNS;
					}
				}
				else {
					$qualified{$name} = $do;
					push @trigger_list, $name;
				}
			}
			close $TRIGGER;

			$archive_zip //= find_archive_zip( job => $trigger_job, run => $trigger_run );

			if ( not defined $archive_zip ) {
				single_run_error(
					trigger => $trigger,
					message => "Archive zip issue for $trigger_job/$trigger_run; moving aside.. ",
				);
				next RUNS;
			}
			else {
				my @zipped_files = map  { "$archive_zip|$_" } get_files_from_zip($archive_zip);
				my @gpd_files    = grep { /\.gpd$/ } @zipped_files;

				push @found, @gpd_files;

				if ( @gpd_files != @trigger_list ) {
					single_run_error(
						trigger => $trigger,
						message =>
							"Stream count mismatch between trigger file and archive for $trigger_job/$trigger_run:\n"
							. Dumper( { trigger_list => \@trigger_list, gpds_in_zip => \@gpd_files, } ),
					);
					next RUNS;
				}

				my @comp_list = map { /^.*\|(.+)$/ } @gpd_files;

				foreach my $i ( 0 .. $#comp_list ) {
					if ( $comp_list[$i] ne $trigger_list[$i] ) {
						single_run_error(
							trigger => $trigger,
							message => "Stream mismatch between trigger file and archive for $trigger_job/$trigger_run",
						);
						next RUNS;
					}
				}

				if ( @gpd_files and $verbose ) {
					print "   $_\n" for @gpd_files;
				}

				# this is our list of good/handled trigger files
				push @triggers, $trigger if -f $trigger;

				# keys in %qualified are basenames
				# keys in %found are full paths
				foreach my $gpd (@gpd_files) {
					my ($base_stream) = $gpd =~ /.zip\|(.+)/i;

					$found{$gpd} = $qualified{$base_stream};

					$base_stream =~ s/\.gpd$//;

					if ( exists $job_run_by_stream{ basename($base_stream) } ) {
						die 'duplicate stream: ' . basename($base_stream);
					}

					$job_run_by_stream{ basename($base_stream) } = {
						job     => $trigger_job,
						run     => $trigger_run,
						trigger => $trigger,
					};
				}
			}
		}

		return %found;
	}

	sub single_run_error {
		my %args    = @_;
		my $trigger = $args{'trigger'} || '';
		my $stream  = $args{'stream'} || '';
		my $message = $args{'message'};

		# if trigger.. all set
		# else, we need stream/job/run
		if ( not $trigger ) {
			if ( not $stream ) {
				die 'need stream or trigger';
			}

			$trigger = $job_run_by_stream{$stream}{'trigger'};
		}

		print $message, "\n";

		# move trigger file out of the way
		if ( not -d "$trigger_dir/error" ) {
			mkdir "$trigger_dir/error" or die "Could not create $trigger_dir/error: $!";
		}
		move $trigger, "$trigger_dir/error" or die "Could not move $trigger => $trigger_dir/error: $!";

		print "OK\n";

		my @to      = ( 'crystal.daniels@computershare.com', 'brian.kelly@computershare.com' );
		my $from    = 'CCSReporting@computershare.com';
		my $subject = 'Consolidation: single run error';
		my $body    = "$message\t\n\t\nMoved $trigger aside.\t\n";

		my %email = (
			to          => \@to,
			sender      => $from,
			reply_to    => \@to,
			subject     => $subject,
			body        => [$body],
			attachments => [],
		);

		if ( 'DEVELOPMENT' eq $ENV{'AREA'} ) {
			print "DEV MODE: single run error:\n" . Dumper( \%email );
		}
		else {
			CcsSmtp::SendMail( \%email );
		}

		return;
	}

	# clean up processed triggers, but only once we're done
	sub finalize_triggers {
		my %args = @_;

		# add these into doc-comp archive zips?
		my $consolidated_file = "handled.$args{'consolidated_job'}.$args{'consolidated_run'}.txt";

		# if good, delete (archive?) triggers
		# no worries about cleaning up in the other scenarios
		my $unlink_errors = 0;
		foreach my $trigger (@triggers) {
			if ( $args{'delete'} ) {
				if ( not unlink $trigger ) {
					print "could not delete '$trigger': $!";
					$unlink_errors++;
				}
				else {
					print "Deleted $trigger ..\n" if $verbose;
				}
			}

			my ( $job, $run ) = $trigger =~ / consolidate \. ([A-Z0-9]+) \. (-?\d+) \.txt $/ix;
			#FIXME: add handled file to archive
			my $archive_path = find_archive_folder( job => $job, run => $run );

			open my $NOTE, '>', "$archive_path/Print_Files/$consolidated_file";
			close $NOTE;
		}

		if ($unlink_errors) {
			print "Could not delete some triggers!\n";
			exit 1;
		}

		return;
	}

	# delete the graphics folder and zip up everything that remains
	# stash it in the doc-comp archives
	sub archive_consolidated_streams {
		my %args       = @_;
		my $job        = $args{'job'};
		my $run        = $args{'run'};
		my $tmp_folder = $args{'tmp_folder'};

		chdir $tmp_folder;
		rmtree('graphics');

		my $archive_base = $archive_folder;
		my $job_folder;

		my ($client)        = $job =~ /^(\d+[A-Z]+)\d+$/i;
		my ($client_folder) = glob("$archive_base/*_$client");
		if ( not $client_folder or not -d $client_folder ) {
			$client_folder = "$archive_base/Consolidation_$client";
			if ( not mkdir $client_folder ) {
				die "Could not find or create client archive folder for '$client'";
			}
		}

		($job_folder) = glob("$client_folder/${job}_*");
		if ( not $job_folder or not -d $job_folder ) {
			$job_folder = "$client_folder/${job}_Consolidation";
			if ( not mkdir $job_folder ) {
				die "Could not find or create job archive folder for '$job'";
			}
		}

		my $new_zip = "$job_folder/Run$run.zip";

		if ( -f $new_zip ) {
			my $work_zip = $new_zip;
			$work_zip =~ s/\.zip$/.00.zip/;

			while ( -f $work_zip ) {
				# $work_zip exists
				$work_zip =~ /\.(\d+)\.zip$/;
				my $last_num = $1;

				my $file_name_end = sprintf( '.%02s.zip', $last_num + 1 );
				$work_zip =~ s/\.\d+\.zip$/$file_name_end/;

				# how about $work_zip + 1?
			}

			# $work_zip doesn't exist
			move $new_zip, $work_zip;
		}

		# $new_zip doesn't exist
		system("$SEVENZ a -tZip -r $new_zip *");

		return;
	}
}

sub process_gpd_dirs {
	my @dirs  = @_;
	my @found = ();

	foreach my $dir (@dirs) {
		if ( not -d $dir ) {
			print "'$dir' isn't an accessible directory\n";
			usage(1);
		}

		push @found, glob "$dir/*.gpd";
	}

	return @found;
}

sub get_gpd_consolidation_details {
	my $stream = shift;
	my $gpd    = $stream . '.gpd';
	my $adm    = $gpd . '.adm.xml';

	# return a string to indicate an error
	if ( not -f $gpd ) {
		return "GPD '$gpd' not found";
	}
	if ( not -f $adm ) {
		return "ADM '$adm' not found";
	}

	my $gpd_version = get_gpd_version($gpd);
	if ( $gpd_version =~ /\D/ ) {
		# if we have non-digits, it's an error message
		return $gpd_version;
	}

	my %details = ( version => $gpd_version );

	my ($adm_details) = AdmXml::xml_to_struct($adm);

	# maybe these will be handy later
	$details{'job'} = $adm_details->{'job'}{'code'};
	$details{'run'} = $adm_details->{'job'}{'run_number'};

	foreach my $section ( 'workflow', 'workflags' ) {
		my $section_hash = $adm_details->{$section};
		$details{$section} = { map { $_ => $section_hash->{$_} } grep { $section_hash->{$_} } keys %$section_hash };
	}

	# get stock details
	( $details{'stocks'} )  = get_gpd_stocks( 'stocks',  $adm_details );
	( $details{'inserts'} ) = get_gpd_stocks( 'inserts', $adm_details );

	$details{'envelope'} = '';
	foreach my $envelope ( @{ $adm_details->{'envelopes'} } ) {
		$details{'envelope'} = $envelope->{'code'};
	}

	$details{'micr'}   = 'yes' eq $adm_details->{'stream'}{'micr'}       ? 1 : 0;
	$details{'color'}  = $adm_details->{'stream'}{'total_images_color'}  ? 1 : 0;
	$details{'duplex'} = $adm_details->{'stream'}{'total_sheets_duplex'} ? 1 : 0;

	$details{'total_sets'}   = $adm_details->{'stream'}{'total_sets'};
	$details{'total_sheets'} = $adm_details->{'stream'}{'total_sheets'};
	$details{'print_site'}   = $adm_details->{'stream'}{'print_site'};

	# GOOD ENOUGH
	# g2aa sets this based on the site specified on its command line
	# but for COCC taxes, g2aa is called after the doc-comp archives are created
	# so the updated version isn't in archive
	# this won't be the case generally
	$details{'print_site'} ||= 'Edison';

	if ( $adm_details->{'custom_fields'} and $adm_details->{'custom_fields'}{'sales_tax'} ) {
		$details{'sales_tax'} = $adm_details->{'custom_fields'}{'sales_tax'};
	}
	else {
		$details{'sales_tax'} = '';
	}

	return \%details;
}

sub get_gpd_version {
	my $gpd_file = shift;

	open my $GPD, '<', $gpd_file or return "Cannot read '$gpd_file': $!";
	my $line = <$GPD>;
	close $GPD;

	my ($version) = $line =~ /^001(\d+\.\d\d *\d+)\s*/;
	$version =~ s/\D//g;

	return $version;
}

sub get_gpd_stocks {
	my $stock_type  = shift;
	my $adm_details = shift;

	my %details;

	foreach my $stock ( @{ $adm_details->{$stock_type} } ) {
		$details{ $stock->{'key'} } = $stock->{'code'};
	}

	my $string = join ',', map { "$_=$details{$_}" } sort keys %details;
	my $codes  = join ',', map { uc( $details{$_} ) } sort keys %details;

	return ( $codes, \%details );
}

sub stage_consolidation_groups {
	my %args = @_;

	my $cgs = $args{'consolidation_groups'};
	my $dir = $args{'working_folder'};
	my $map = $args{'stream_mapping'};

	my %sorted_streams;

	my $index = '000';
	foreach my $target ( sort keys %$cgs ) {
		if ($target) {
			$index++;
			print "$index: $target ($map->{$target})\n";

			my $sort_folder = "$dir/$index";
			if ( $map->{$target} ) {
				$sort_folder .= $map->{$target};
			}

			if ( not -d $sort_folder ) {
				mkdir $sort_folder;
			}

			open my $DETAILS, '>', "$sort_folder/details.txt" or die "Could not write $sort_folder/details.txt: $!";
			print $DETAILS "$target\n";
			close $DETAILS;

			foreach my $stream ( @{ $cgs->{$target} } ) {
				foreach my $ext (qw( .gpd .gpd.ofs .gpd.adm.xml )) {
					if ( -f $stream . $ext ) {
						print "\tCopying $stream$ext to $sort_folder ..";
						if ( copy "$stream$ext", $sort_folder ) {
							print " OK\n";
						}
						else {
							print " failed: $!\n";
						}
					}
					else {
						print "\t$stream$ext not found\n";
					}
				}

				$sorted_streams{$stream} = $sort_folder;
			}
		}
		else {
			# non-consolidated
			foreach my $stream ( @{ $cgs->{$target} } ) {
				# maybe just ignore these
				# any handling should happen earlier
				# maybe during that first call to g2aa
				print "NONCONS: $stream\n";

				$sorted_streams{$stream} = 'NONCONS';
			}
		}
	}

	return { count => +$index, sorted_streams => \%sorted_streams };
}

sub stage_streams {
	my %args           = @_;
	my $gpds           = $args{'streams'};
	my $working_folder = $args{'working_folder'};

	# can disable the use of some criteria
	# remove them from our criteria list here

	my @consolidation_criteria = qw(version workflow_string workflags_string stocks inserts envelope sales_tax);

	# additional: add elements from @{ $args{'criteria'} }
	if ( @{ $args{'criteria'} } ) {
		# already validated
		push @consolidation_criteria, @{ $args{'criteria'} };
	}

	my %stream_mapping;

	my $zip_tmp = "$working_folder/zip_tmp";
	mkdir $zip_tmp;

	my %streams;
	my %handled_zips;

	foreach my $gpd ( sort keys %$gpds ) {
		my ( $zip, $stream ) = $gpd =~ /^(.*Run\d+.zip)\|(.+)\.gpd$/i;

		my ( $job, $run ) = get_job_run_from_stream($stream);

		if ( not exists $handled_zips{$zip} ) {
			if ( not -f $zip ) {
				single_run_error(
					message => "Cannot find zip file '$zip'",
					stream  => $stream,
				);
			}

			my $local_zip = "$zip_tmp/$job." . basename($zip);
			copy $zip, $local_zip or die "Cannot copy $zip: $!";

			system("$SEVENZ e -o$zip_tmp/$job.$run $local_zip *.gpd *.gpd.ofs *.gpd.adm.xml graphics/*");
			$handled_zips{$zip} = 1;

			# We have what we need, delete the zip
			unlink $local_zip;

			# separate graphics extraction? or just glob for graphics?
			foreach my $extracted ( glob "$zip_tmp/$job.$run/*" ) {
				next if -d $extracted;
				next if $extracted =~ /\.gpd(?:\.ofs|\.adm.xml|)$/i;

				if ( not -d "$working_folder/graphics" ) {
					mkdir "$working_folder/graphics";
				}

				print "Moving $extracted => $working_folder/graphics .. ";
				move $extracted, "$working_folder/graphics" or die "move failed: $!";
				say 'OK';
			}
		}
		$stream = "$zip_tmp/$job.$run/$stream";

		print "$stream: " if $verbose;

		my $details = get_gpd_consolidation_details($stream);

		if ( not ref $details ) {
			# just a string? it's an error message
			single_run_error(
				message => $details,
				stream  => $stream,
			);

			next;
		}

		$streams{$stream} = $details;

		# confirm we have consistent print_site values
		if ( not defined $print_site ) {
			$print_site = $streams{$stream}{'print_site'};
		}
		elsif ( lc($print_site) ne lc( $streams{$stream}{'print_site'} ) ) {
			die "Changed print_site! $stream is set for '$streams{$stream}{'print_site'}' "
				. "but prior streams are set for '$print_site'\n";
		}
		# else, all is well

		if ( $gpds->{$gpd} ) {
			# build key string
			# any streams with the same key string will be consolidated together

			# workflags are simple
			$streams{$stream}{'workflags_string'} = join ',', sort keys %{ $streams{$stream}{'workflags'} };

			# in Legacy, we only care about non-e-comms workflows
			my @workflows;
			foreach my $workflow_key ( sort keys %{ $streams{$stream}{'workflow'} } ) {
				# weed out e-comms here
				if ( not exists $exclude_workflows{$workflow_key} ) {
					push @workflows, $workflow_key;
				}
			}
			$streams{$stream}{'workflow_string'} = join ',', @workflows;

			$streams{$stream}{'color-plex'} =
				  ( $streams{$stream}{'color'} and $streams{$stream}{'duplex'} ) ? 'ColDup'
				: $streams{$stream}{'color'} ? 'ColSim'
				:                              'Blk';

			$streams{$stream}{'key_string'} = join '-', map { $streams{$stream}{$_} } @consolidation_criteria;

			# remaining @optional_criteria will also be added to folder/stream name
			$stream_mapping{ $streams{$stream}{'key_string'} } = '.';
			foreach my $criteria ( @{ $args{'criteria'} } ) {
				if ( 'color-plex' eq $criteria ) {
					$stream_mapping{ $streams{$stream}{'key_string'} } .= $streams{$stream}{'color-plex'} . '.';
				}
				elsif ( 'micr' eq $criteria ) {
					$stream_mapping{ $streams{$stream}{'key_string'} } .=
						( $streams{$stream}{'micr'} ? 'micr.' : 'nomicr.' );
				}
				else {
					die "Criteria '$criteria' not supported in stream naming";
				}
			}
			$stream_mapping{ $streams{$stream}{'key_string'} } =~ s/\.$//;
		}
		else {
			$streams{$stream}{'key_string'} = '';
		}

		print $streams{$stream}{'key_string'}, "\n" if $verbose;
	}

	my %consolidation_groups;
	foreach my $k ( keys %streams ) {
		push @{ $consolidation_groups{ $streams{$k}{'key_string'} } }, $k;
	}

	my $results = stage_consolidation_groups(
		consolidation_groups => \%consolidation_groups,
		working_folder       => $working_folder,
		stream_mapping       => \%stream_mapping,
	);

	# We don't need this once we've staged streams to the group folders
	rmtree($zip_tmp);

	foreach my $stream ( keys %streams ) {
		$streams{$stream}{'consolidated_stream'} = $results->{'sorted_streams'}{$stream};
		$streams{$stream}{'consolidated_stream'} =~ s{^.*\Q$working_folder\E/}{};
		$streams{$stream}{'consolidated_stream'} =~ s{/}{-};
	}

	return { count => $results->{'count'}, streams => \%streams };
}

{
	# high-level details
	my $current_job;
	my $current_run;
	my $current_stream;
	my $current_client;

	# paper details
	my %stocks_old_code_by_stream_and_key;
	my %stocks_new_key_by_code;
	my %stocks_new_key_by_stream_and_key;
	my $stocks_count = 0;

	# insert details
	my %inserts_new_key_by_code;
	my %inserts_new_key_by_stream_and_key;
	my $inserts_count = 0;

	my %gpd_graphics;
	my $adm_graphics_folder = '';

	sub rewrite_streams {
		my $work_dir = shift;

		my $stream_count = 0;
		foreach my $stream_folder ( glob "$work_dir/*" ) {
			next if $stream_folder !~ m{/\d\d\d};
			next if not -d $stream_folder;

			foreach my $gpd ( glob "$stream_folder/*.gpd" ) {
				print "Rewriting $gpd ..\n";

				process_adm("$gpd.adm.xml");
				rewrite_gpd($gpd);
				rewrite_adm("$gpd.adm.xml");

				$stream_count++;
				$adm_graphics_folder = '';
			}
		}

		print Dumper {
			stocks  => \%stocks_new_key_by_stream_and_key,
			inserts => \%inserts_new_key_by_stream_and_key,
			}
			if $verbose;

		return $stream_count;
	}

	sub process_adm {
		my $adm = shift;

		($current_stream) = $adm =~ /^(.+)\.gpd\.adm\.xml$/i;

		my $adm_processing = XML::Twig->new(
			pretty_print => 'indented',    # pretty printing for debugging
			twig_roots   => {
				'stocks/stock'   => \&adm_stocks_handler,
				'job'            => \&adm_job_details,
				'stream'         => \&adm_stream_details,
				'inserts/insert' => \&adm_insert_handler,
			},
		);
		$adm_processing->parsefile($adm);

		return;
	}

	sub adm_stocks_handler {
		my ( $twig, $stock_section ) = @_;

		my $current_code;
		foreach my $child ( sort { $a->gi() cmp $b->gi() } $stock_section->children() ) {
			# take them alphabetically, so we get to 'code' before 'key'

			my $tag_name = $child->gi();

			if ( 'code' eq $tag_name ) {
				$current_code = $child->text();
			}
			elsif ( 'key' eq $tag_name ) {
				my $old_key = $child->text();

				if ( not exists $stocks_new_key_by_code{$current_code} ) {
					$stocks_new_key_by_code{$current_code} = 'merge' . ++$stocks_count;
				}

				if ( not exists $stocks_old_code_by_stream_and_key{$current_stream}{$old_key} ) {
					$stocks_old_code_by_stream_and_key{$current_stream}{$old_key} = $current_code;
				}

				if ( not exists $stocks_new_key_by_stream_and_key{$current_stream}{$old_key} ) {
					$stocks_new_key_by_stream_and_key{$current_stream}{$old_key} =
						$stocks_new_key_by_code{$current_code};
				}

				$child->set_text( $stocks_new_key_by_code{$current_code} );
			}
			# else, leave it
		}

		return;
	}

	sub adm_job_details {
		my ( $twig, $job_section ) = @_;

		foreach my $child ( $job_section->children() ) {
			my $tag_name = $child->gi();

			if ( 'code' eq $tag_name ) {
				$current_job = $child->text();
			}
			elsif ( 'run_number' eq $tag_name ) {
				$current_run = $child->text();
			}
			elsif ( 'client_code' eq $tag_name ) {
				$current_client = $child->text();
			}
			# else, not needed
			# sort_weight_digest?
		}

		return;
	}

	sub adm_stream_details {
		my ( $twig, $job_section ) = @_;

		foreach my $child ( $job_section->children() ) {
			my $tag_name = $child->gi();

			if ( 'graphics_folder' eq $tag_name ) {
				my $graphics_folder_value = $child->text();

				if ($graphics_folder_value) {
					$adm_graphics_folder = $graphics_folder_value;
				}
				# else, no need to reset
			}
			# else, not needed
		}

		return;
	}

	sub rewrite_gpd {
		my $gpd = shift;
		my ($stream) = $gpd =~ /^(.+)\.gpd$/i;

		# change stock key in 010 and 020 records
		# change insert key in 304 records
		# add set tags for job/run: merged_job and merged_run

		my $dirname  = dirname($gpd);
		my $basename = basename($gpd);

		my $new_gpd = "$dirname/$basename.new";
		( my $new_ofs = $new_gpd ) =~ s/\.gpd\.new$/.gpd.ofs.new/;

		open my $GPD, '<', $gpd     or die "Cannot read '$gpd': $!";
		open my $NEW, '>', $new_gpd or die "Cannot write '$new_gpd': $!";
		open my $OFS, '>', $new_ofs or die "Cannot write '$new_ofs': $!";

		# we don't need to mess with off-by-one newlines in our offsets if we set binmode
		binmode $GPD;
		binmode $NEW;

		my $records    = 0;
		my $offset     = 0;
		my $set_number = 1;

		my $current_set_sheets              = 0;
		my $current_set_images              = 0;
		my $current_set_sheet_count_set_tag = 0;
		my $current_set_image_count_set_tag = 0;
		my $found_first_graphic             = 0;

		while ( my $line = <$GPD> ) {
			$records++;
			my $record = substr( $line, 0, 3 );

			if ( '010' eq $record ) {
				# layout
				# INTEGER  SheetNumber         14
				# INTEGER  SetNumber           14
				# INTEGER  SetSheetNumber      14
				# INTEGER  TotalSheetsInSet    14
				# STRING   StockKey            6

				my ( $set_sheet_number, $stock_key ) = unpack( '@31A14 @59A6', $line );
				if ( not exists $stocks_old_code_by_stream_and_key{$stream}{$stock_key} ) {
					die "stock key '$stock_key' not found";
				}
				substr( $line, 59, 6 ) =
					$stocks_new_key_by_code{ $stocks_old_code_by_stream_and_key{$stream}{$stock_key} };

				if ( 1 == $set_sheet_number ) {
					print $OFS sprintf( '%14s', $offset ), "\n";

					if ( $current_set_sheets and $current_set_images ) {
						# this is the start of at least the second set
						# replace the placeholders in the prior set

						my $current_position = tell($NEW);

						seek( $NEW, $current_set_sheet_count_set_tag, 0 );
						printf $NEW "202SLD_sheet_count=%06d\n", $current_set_sheets;

						seek( $NEW, $current_set_image_count_set_tag, 0 );
						printf $NEW "202SLD_image_count=%06d\n", $current_set_images;

						seek( $NEW, $current_position, 0 );
					}

					$current_set_sheets = 0;
					$current_set_images = 0;

					# save our location before reading/writing this 010 record
					my $start_of_set_offset = tell($NEW);

					# throw down job and run set tags
					# simplest to do it right now
					$line .= "202merged_job=$current_job\n";
					$line .= "202merged_run=$current_run\n";
					$line .= "202merged_stream=$basename\n";
					$line .= "202merged_orig_set_number=$set_number\n";
					$line .= "202SLD_client=$current_client\n";
					$line .= "202comptag=$current_client\n";

					# stash this location so we can replace the placeholder once we know the sheet count
					$current_set_sheet_count_set_tag = $start_of_set_offset + length($line);
					$line .= "202SLD_sheet_count=______\n";

					# same placeholder business here
					$current_set_image_count_set_tag = $start_of_set_offset + length($line);
					$line .= "202SLD_image_count=______\n";

					$records += 8;
					$set_number++;
				}

				$current_set_sheets++;
				$current_set_images++;
			}
			elsif ( '011' eq $record ) {
				$current_set_images++;
			}
			elsif ( '020' eq $record ) {
				# layout
				# STRING   StockKey        6
				# STRING   was_StockNumber 14

				my ($stock_key) = unpack( '@3A6', $line );
				if ( not exists $stocks_old_code_by_stream_and_key{$stream}{$stock_key} ) {
					# bad
					die "Could not find replacement stock key for '$stock_key'";
				}
				substr( $line, 3, 6 ) =
					$stocks_new_key_by_code{ $stocks_old_code_by_stream_and_key{$stream}{$stock_key} };
			}
			elsif ( '002' eq $record ) {
				# layout
				# INTEGER  TotalRecords        14
				# INTEGER  TotalSets           14
				# INTEGER  TotalSheets         14
				# INTEGER  TotalImages         14
				# INTEGER  TotalSimplexSheets  14
				# INTEGER  TotalDuplexSheets   14

				substr( $line, 3, 14 ) = sprintf '%14s', $records;
			}
			elsif ( '304' eq $record ) {
				# layout
				# STRING   Inserts  unlimited

				my $inserts_string = substr( $line, 3 );
				foreach my $insert ( split ',', $inserts_string ) {
					my ( $key, $tray ) = split '=', $insert;
					my $replacement_key = $inserts_new_key_by_stream_and_key{$current_stream}{$key};
					$line =~ s/$key/$replacement_key/;
				}
			}
			elsif ( '003' eq $record ) {
				# layout
				# STRING   Name            254
				# BOOLEAN  DoComponentise  1

				my $graphic_name = substr( $line, 3, 254 );
				$graphic_name =~ s/\s*$//;

				# We're going to keep track of more information than we need for now.
				# This way, we will know about all the potentially unique graphics,
				# which can possibly be named the same across runs.
				# If we need to deal with this later, this is enough information to go on.
				if ($adm_graphics_folder) {
					$gpd_graphics{$graphic_name}{$adm_graphics_folder} = 1;
				}
				else {
					#TODO: rethinking graphics staging..
					$gpd_graphics{$graphic_name}{"$current_job.$current_run"} = 1;
				}

				# turn off componentization, since GV is done at this point
				substr( $line, 257, 1 ) = '0';

				if ( not $found_first_graphic ) {
					# replace placeholders for final set's count set tags
					$found_first_graphic = 1;

					my $current_position = tell($NEW);

					seek( $NEW, $current_set_sheet_count_set_tag, 0 );
					printf $NEW "202SLD_sheet_count=%06d\n", $current_set_sheets;

					seek( $NEW, $current_set_image_count_set_tag, 0 );
					printf $NEW "202SLD_image_count=%06d\n", $current_set_images;

					seek( $NEW, $current_position, 0 );
				}
			}
			elsif ( '202' eq $record ) {
				# set tags are just keys and values
				my ( $key, $value ) = $line =~ /^202 ([^=]+) = (.+) \r?\n? $/xms;
				if ( 'orig_set_number' eq $key ) {
					# drop this line entirely
					$records--;
					next;
				}
			}

			print $NEW $line;
			$offset += length($line);
		}

		close $GPD;
		close $NEW;
		close $OFS;

		# overwrite originals
		move $new_gpd, $gpd       or die "Could not overwrite '$new_gpd' -> '$gpd' : $!";
		move $new_ofs, "$gpd.ofs" or die "Could not overwrite '$new_ofs' -> '$gpd.ofs' : $!";

		return;
	}

	# process ADM line-by-line
	sub rewrite_adm {
		my $adm = shift;

		my $dirname  = dirname($adm);
		my $basename = basename($adm);
		my $new_adm  = "$dirname/$basename.new";

		open my $ADM, '<:encoding(UTF-8)', $adm     or die "Cannot read ADM '$adm': $!";
		open my $NEW, '>:encoding(UTF-8)', $new_adm or die "Cannot write '$new_adm': $!";

		my $xpath = '';
		while ( my $line = <$ADM> ) {
			my $base_path;
			my $dont_print = 0;

			if ( $line =~ /<(\w+)>/ ) {
				$base_path = $1;
				$xpath .= "/$base_path";
			}
			elsif ($xpath) {
				($base_path) = $xpath =~ m{/(\w+)$};
			}
			else {
				$base_path = '';
			}

			if ( '/admin/stream/file_size' eq $xpath ) {
				# GPD has already been overwritten, so there's no gpd.new file anymore
				my ($new_gpd) = $new_adm =~ /^(.+\.gpd)\.adm\.xml\.new$/i;
				my $new_gpd_size = -s $new_gpd;

				$line =~ s{<file_size>\d+</file_size>}{<file_size>$new_gpd_size</file_size>};
			}
			elsif ( '/admin/stocks/stock/key' eq $xpath ) {
				my ($current_key) = $line =~ m{<key>(\w+)</key>};
				if ( exists $stocks_new_key_by_stream_and_key{$current_stream}{$current_key} ) {
					my $new_code = $stocks_new_key_by_stream_and_key{$current_stream}{$current_key};
					$line =~ s{<key>$current_key</key>}{<key>$new_code</key>};
				}
				else {
					die "Could not find replacement stock code for '$current_key' in stream '$current_stream'";
				}
			}
			elsif ( '/admin/inserts/insert/key' eq $xpath ) {
				my ($current_key) = $line =~ m{<key>(\w+)</key>};
				if ( exists $inserts_new_key_by_stream_and_key{$current_stream}{$current_key} ) {
					my $new_code = $inserts_new_key_by_stream_and_key{$current_stream}{$current_key};
					$line =~ s{<key>$current_key</key>}{<key>$new_code</key>};
				}
				else {
					die "Could not find replacement insert code for '$current_key' in stream '$current_stream'";
				}
			}
			elsif ( '/admin/stream/sort_status' eq $xpath ) {
				if ( not $sort_streams ) {
					$line =~ s{<sort_status>[^<]*<}{<sort_status>not_sortable<};
				}
				elsif ( not $line =~ s{<sort_status>sorted<}{<sort_status>not sorted<} ) {
					# if there are ANY that aren't set for sorting, don't sort anything
					$line =~ s{<sort_status>[^<]*<}{<sort_status>not_sortable<};
					$sort_streams = 0;
				}
			}
			elsif ( $xpath =~ m{^/admin/batch_breaks/} ) {
				# delete batch breaks
				$dont_print = 1;
			}
			elsif ( '/admin/stream/spec_inst_print' eq $xpath ) {
				# manage spec_inst_print
				# maybe a few other fields..
				$line =~ s{<spec_inst_print>.+</spec_inst_print>}{<spec_inst_print></spec_inst_print>};
			}
			elsif ( '/admin/stream/spec_inst_mail' eq $xpath ) {
				$line =~ s{<spec_inst_mail>.+</spec_inst_mail>}{<spec_inst_mail></spec_inst_mail>};
			}
			elsif ( '/admin/job/code' eq $xpath ) {
				$line =~ s{<code>.+</code>}{<code>$consolidated_job</code>};
			}
			elsif ( '/admin/job/name' eq $xpath ) {
				$line =~ s{<name>.+</name>}{<name>Consolidated $consolidated_job</name>};
			}
			elsif ( '/admin/job/client_code' eq $xpath ) {
				my ($client_code) = $consolidated_job =~ /^(\d+[A-Z]+)\d+$/;
				$line =~ s{<client_code>.+</client_code>}{<client_code>$client_code</client_code>};
			}
			elsif ( '/admin/job/client_name' eq $xpath ) {
				$line =~ s{<client_name>.+</client_name>}{<client_name>Post-Process Consolidation</client_name>};
			}
			elsif ( '/admin/job/run_number' eq $xpath ) {
				$line =~ s{<run_number>.+</run_number>}{<run_number>$consolidated_run</run_number>};
			}
			elsif ( '/admin/job/work_order' eq $xpath ) {
				$line =~ s{<work_order>.+</work_order>}{<work_order></work_order>};
			}
			elsif ( '/admin/stream/desc' eq $xpath ) {
				# this is the folder name
				my $desc = basename($dirname);
				$line =~ s{<desc>.+</desc>}{<desc>$desc</desc>};
			}
			elsif ( '/admin/stream/graphics_folder' eq $xpath ) {
				$line =~ s{<graphics_folder>.+</graphics_folder>}{<graphics_folder></graphics_folder>};
			}
			elsif ( $xpath =~ m{^/admin/workflow/(.+)} ) {
				my $workflow = $1;

				if ( exists $exclude_workflows{$workflow} ) {
					$line =~ s{<$workflow>[0-9]+</$workflow>}{<$workflow>0</$workflow>};
				}
			}

			if ( $line =~ m{</$base_path>} ) {
				$xpath =~ s{/$base_path$}{};
			}

			print $NEW $line if not $dont_print;
		}

		close $ADM;
		close $NEW;

		# overwrite original
		move $new_adm, $adm or die "Could not overwrite '$new_adm' -> '$adm' : $!";

		return;
	}

	sub adm_insert_handler {
		my ( $twig, $stock_section ) = @_;

		my ( $code, $key );
		foreach my $child ( sort { $a->gi() cmp $b->gi() } $stock_section->children() ) {
			my $tag_name = $child->gi();
			my $payload  = $child->text();

			if ( 'code' eq $tag_name ) {
				$code = $payload;
			}
			elsif ( 'key' eq $tag_name ) {
				$key = $payload;
			}
		}

		if ( not exists $inserts_new_key_by_code{$code} ) {
			$inserts_new_key_by_code{$code} = 'merge' . ++$inserts_count;
		}

		$inserts_new_key_by_stream_and_key{$current_stream}{$key} = $inserts_new_key_by_code{$code};

		return;
	}

	# stage the graphics needed for this job to local Graphics folder, so they end up in the ZIP
	# no option to stage at print site, since adm/stream/graphics_folder doesn't work anymore
	sub copy_gpd_graphics {
		my $work_dir     = shift;
		my $graphics_dir = "$work_dir/Graphics";

		if ( not -d $graphics_dir ) {
			mkdir $graphics_dir or die "Could not create Graphics folder '$graphics_dir': $!";
		}

		foreach my $g ( sort keys %gpd_graphics ) {
			if ( $g !~ /\.\w{1,5}$/ ) {
				# no extension? it's a PCX
				$g .= '.pcx';
			}

			# we already copied this over
			next if -f "$graphics_dir/$g";

			# We're just going to take the first instance for now.
			# If we get into non-uniquely named graphics later, we'll need to revisit this.
			my ($graphics_source) = sort keys %{ $gpd_graphics{$g} };
			if ( $graphics_source =~ /^\w/ ) {
				$graphics_source = "$work_dir/zip_tmp/$graphics_source";
			}

			print "Copying '$g' to $graphics_dir/ .. " if $verbose;
			copy "$graphics_source/$g", $graphics_dir or die "Could not copy $graphics_source/$g to $graphics_dir: $!";
			print "OK\n" if $verbose;
		}

		return scalar keys %gpd_graphics;
	}
}

sub merge_folders {
	my $cons_folder = shift;

	opendir my $CONSOLIDATED_FOLDER, $cons_folder or die "Cannot open folder '$cons_folder': $!";
	my @folders = grep { /^\d\d\d/ and -d "$cons_folder/$_" } readdir($CONSOLIDATED_FOLDER);
	closedir $CONSOLIDATED_FOLDER;

	chdir $cons_folder;

	my $merged_count = 0;

	foreach my $folder (@folders) {
		if ( not $folder or not -d $folder ) {
			die 'Gimme a folder!';
		}

		my @streams      = glob "$folder/*.gpd";
		my $stream_count = @streams;
		print "Consolidating $stream_count ";
		print( 1 == $stream_count ? 'stream' : 'streams' );
		print " in $folder ..\n";

		my $s = StreamMerge->new();

		my $stream_hash = $s->create_streams(
			folders => [$folder],
			prefix  => "$consolidated_job.$consolidated_run",
			suffix  => "-$folder",
			regex   => q/^.*/,
		);

		$s->merge_streams();

		$merged_count += $stream_count;
	}

	chdir '..';

	return $merged_count;
}

sub sort_gpds {
	my $cons_folder = shift;

	# GPDs in $cons_folder
	chdir $cons_folder;

	my @streams = map { /^ (.+) \.gpd $/x; $1; } glob '*.gpd';

	foreach my $stream (@streams) {
		Sort::GPD->run($stream);
	}

	chdir '..';

	return;
}

sub pause {
	print "Press ENTER to continue. Ctrl-C to stop here.\n";
	<STDIN>;

	return;
}

sub send_to_print_queue {
	my %args = @_;

	if ( 'DEVELOPMENT' eq $ENV{'AREA'} ) {
		print "No g2aa in dev..\n";
		return;
	}

	my $site   = $args{'site'};
	my $folder = $args{'folder'};

	chdir $folder;

	my $G2AA = $ENV{'CCS_RESOURCE'} . '/Aardvark/scripts/regional/g2aa.cmd';

	my $attempts = 0;
	my $success  = 0;

	while ( not $success ) {
		$attempts++;
		my $out = system("$^X $G2AA --site $site");

		if ( 0 == $out ) {
			$success = 1;
		}
		else {
			# error message
			print "Aardvark upload attempt #$attempts failed!";
		}

		if ( $attempts > 2 ) {
			last;
		}

		sleep 5;
	}

	chdir '..';

	return $success;
}

sub write_report {
	my %args = @_;

	my $streams = $args{'streams'};
	my $folder  = $args{'folder'};
	my $report  = $args{'report'};

	open my $REPORT, '>', "$folder/$report" or die "Cannot write consolidation report: $!";

	print $REPORT "JOB,RUN,SETS,SHEETS,INPUT STREAM,CONSOLIDATED STREAM,CONSOLIDATION KEY\n";

	foreach my $stream ( sort keys %$streams ) {
		my ($base) = $stream =~ m{^(?:.*/)?(.+)$};

		print $REPORT join( ',',
			$streams->{$stream}{'job'},        $streams->{$stream}{'run'},
			$streams->{$stream}{'total_sets'}, $streams->{$stream}{'total_sheets'},
			$base,                             $streams->{$stream}{'consolidated_stream'},
			qq{"$streams->{$stream}{'key_string'}"} ),
			"\n";
	}

	close $REPORT;

	return;
}

sub find_archive_folder {
	my %args = @_;

	my $contract_archive = "R:/Archives/Contract_Runs/$args{'job'}";
	my $run0             = $contract_archive . sprintf( '/Run%04d', $args{'run'} );
	my $run_             = "$contract_archive/Run_$args{'run'}";

	my $archive_path =
		  -d $run0 ? $run0
		: -d $run_ ? $run_
		:            undef();

	return $archive_path;
}

sub find_archive_zip {
	my %args = @_;
	my $job  = $args{'job'};
	my $run  = $args{'run'};

	my ($client) = $job =~ /^(\d+[A-Z]+)\d+$/;

	# get archive folder from CcsCommon
	my $archive_base = CcsCommon::ini2h( $ENV{'CCS_SETTINGS'} . '/ccssite.ini' )->{'general'}{'archive_folder'};
	$archive_base =~ s{\\}{/}g;

	# we might have more than one folder to look through
	my @client_folders = glob "$archive_base/*_$client";

	my @run_zips_found;
	my $run_zip = sprintf( 'Run%04d.zip', $run );
	foreach my $client_folder (@client_folders) {
		my @contract_folders = glob "$client_folder/$job*";
		foreach my $contract_folder (@contract_folders) {
			push @run_zips_found, "$contract_folder/$run_zip" if -f "$contract_folder/$run_zip";
		}
	}

	# if more than one.. sad trombone
	if ( not @run_zips_found ) {
		# none found
		say "$job/$run: No archive zip found";
		return;
	}
	elsif ( 1 != @run_zips_found ) {
		# too many found
		say "$job/$run: Too many zips found\n" . Dumper( \@run_zips_found );
		return;
	}
	# else, all is well

	return $run_zips_found[0];
}

sub get_files_from_zip {
	my $zip = shift;

	my @files_in_zip = map {
		/^
			[-0-9]+ [ ] # Date
			[:0-9]+ [ ] # Time
			..... \s+   # Attr
			[0-9]+ \s+  # Size
			[0-9]+ \s+  # Compressed
			(.+)        # return file name
		$/x;
	} grep { /^[-0-9]+ / } `$SEVENZ l $zip`;

	return @files_in_zip;
}

sub get_startup_details {
	my $startup = shift;

	my %details;
	open my $STARTUP, '<', $startup or die "Cannot read startup '$startup': $!";

	while ( my $line = <$STARTUP> ) {
		$line =~ s/\r?\n$//ms;
		$line =~ s/\s+$//;
		my ( $key, $value ) = split /=/, $line, 2;

		if ( '(null)' eq $value ) {
			$value = '';
		}

		if ( exists $details{$key} ) {
			my $type = ref( $details{$key} );
			if ($type) {
				if ( 'ARRAY' eq $type ) {
					push @{ $details{$key} }, $value;
				}
				else {
					die "Unrecognized structure in startup handling:\n" . Dumper( $details{$key} );
				}
			}
			else {
				$details{$key} = [ $details{$key}, $value ];
			}
		}
		else {
			$details{$key} = $value;
		}
	}
	close $STARTUP;

	if ( exists $details{'DataFileName'} and not ref $details{'DataFileName'} ) {
		$details{'DataFileName'} = [ $details{'DataFileName'} ];
	}

	return %details;
}

END {
	# get out of the temp folder, so File::Temp can clean up
	chdir $ENV{'TEMP'};
}
