#!C:/Strawberry/perl/bin/perl.exe

use v5.32;

use strict;
use warnings;

use Data::Dumper;
use Getopt::Long;
use File::Basename;
use List::Util qw(any);
use FindBin qw($Bin);

use lib "$Bin/Modules";

# compiling and using a a module based on the command line
# means we need stringy eval
## no critic BuiltinFunctions::ProhibitStringyEval

GetOptions(
	'file=s'      => \my @files,
	'converter=s' => \my $converter,
	'contract=s'  => \my $contract,
	'run=i'       => \my $run,
	'out=s'       => \my $output_zip,
	'trace=s'     => \my $trace,
	'help'        => sub { usage(0); },
);

if ( $ENV{'DEVELOPMENT'} and 'ON' eq $ENV{'DEVELOPMENT'} ) {
	$contract //= '999ZZ0001';
	$run      //= 9999;
}
else {
	if ( not $run ) {
		say '--run required';
		usage(1);
	}
	if ( not $contract ) {
		say '--contract required';
		usage(1);
	}
}

# hold on to STDOUT
#my $ORIG_OUT = select();
#select $TRACE;

if (@ARGV) {
	push @files, @ARGV;
}

if ( not @files ) {
	say 'Specify files to convert with --file';
	usage(1);
}

my @converters =
	grep { 'Base' ne $_ } map { s/\.pm$//r } map { basename($_) } glob "$Bin/Modules/Converters/*";

if ( not $converter ) {
	say 'Specify a converter with --converter';
	say $_ for map { "  $_" } @converters;
	print "\n";

	usage(1);
}

if ( not any { $converter eq $_ } @converters ) {
	say "Converter '$converter' not found. Available converters:";
	say $_ for map { "  $_" } @converters;
	print "\n";

	usage(1);
}

$converter = "Converters::$converter";
eval "use $converter;";
if ($@) {
	die "Could not compile '$converter': $@";
}

my $converter_new    = eval "sub { ${converter}->new(\@_) }";
my $converter_object = $converter_new->(
	connect_product => 'Notice',
	document_type   => 'Notice',
	contract_number => $contract,
	run_number      => $run,
	trace           => $trace,
);

my $exit_status = 0;

foreach my $file (@files) {
	my $success = $converter_object->processFile($file);

	# output this status line to the original STDOUT
	#select($ORIG_OUT);

	if ($success) {
		say "$file -> " . $converter_object->getNewFileName();
	}
	else {
		$exit_status = 1;
		say "$file -> failed processing";
	}

	# more output should go back to the trace file
	#select($TRACE);
}

exit $exit_status;

sub usage {
	my $exit = shift;

	my ($cmd) = $0 =~ /^.*?[\\\/]?([^\\\/]+?)(?:\.\w{1,4})?$/;

	print <<"EOF";
 $cmd: Perl harness for converting client input into Connect2 JSON

Options:
  --file FILE                file to convert (multiples allowed)
  --converter CONVERTER      which converter to use
  --contract  CONTRACT       CCS contract code
  --run       RUN#           contract run number
  --out       ZIP NAME       output zip file name
  --help                     print this usage message
EOF

	exit $exit;
}

