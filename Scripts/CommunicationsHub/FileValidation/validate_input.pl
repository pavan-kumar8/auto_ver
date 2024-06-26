#!C:/perl5.10.1/bin/perl.exe

use v5.10;

use strict;
use warnings;

#--------------------------------------------------------------------------------------------------
# This script validates input datafiles.
# Files that are valid are moved to be processed.
# Invalid files are quarantined to a bad folder,
# Client gets an Email listing the validation error.
#
# This application runs in Aardvark using AardvarkAutoprocessingScriptSupervisor.pl as follows:
# cmd /c C:\perl5.10.1\bin\perl.exe
# %CCS_RESOURCE%\Regional\NA\Scripts\AardvarkAutoprocessingScriptSupervisor.pl
# \\csavcdsfpd1\GE\UAT\GitResource\Regional\US\Scripts\CommunicationsHub\PreProcessor\validate_input.pl
# --base  \\csavcdsfpd1\GE\UAT\DataIn\CommunicationsHub
# --good  Preprocess
# --bad   Invalid
# --type  XSD
# --std   COCC\COCC_Notices_XML_Schema.xsd
# --email 'computersharecyclenotifications@cocc.com'
# \\csavcdsfpd1\GE\UAT\DataIn\CommunicationsHub\In\TEST_20230301_1078_CO_ADDCHG_NOTICE.XML
#--------------------------------------------------------------------------------------------------

use Data::Dumper;
use Getopt::Long;
use FindBin qw($Bin);
use File::Copy qw(move);
use File::Basename;

use lib $ENV{CCS_RESOURCE};
use lib_CCS;
use CcsSmtp;

my $DefaultSenderName  = q{#NA CTG Operations&Support};
my $DefaultSenderEmail = q{nactgoperations&support@computershare.com};
my $DefaultRecipient   = q{!USCSBURProgramming@computershare.com};
my $EmailMessage       = q{
The input file  !FILENAME!  failed validation against the current XSD.

!VALIDATION_ERROR!
File processing has been aborted. Please correct the issue and send another file.

Thank You.
};
my $ErrorMessage;

my %Options = (
	'startupfile=s' => \my $StartupFile,
	'base=s'        => \my $BaseDir,
	'good=s'        => \my $GoodDir,
	'bad=s'         => \my $BadDir,
	'type=s'        => \my $FileType,
	'std=s'         => \my $ValidSTDFile,
	'email=s'       => \my $Recipient,
	'pass'          => \my $PassThrough,
	'help'          => sub { usage(); exit 0 },
);

sub usage {
	my ($cmd) = $0 =~ /^.*?[\\\/]?([^\\\/]+?)(?:\.\w{1,4})?$/;

	print <<"EOF";
 $cmd: validates each XML file against specified XSD

Options:
  --base  <FOLDER>  base folder for file moves
  --good  <FOLDER>  valid XML file sub-folder
  --bad   <FOLDER>  invalid XML file sub-folder
  --type  <TYPE>    file type validation e.g. XSD
  --std   <FILE>    path to validation schema file
  --email <EMAIL>   recipient(s) for error email
  --pass            pass thru file to processing
  --help            print this usage message

EOF

	return;
}

sub isValidFile {
	my ( $input_file, $schema_file ) = @_;

	# build the command for run()
	my @command = ('C:/Strawberry/perl/bin/perl.exe');
	push @command, "$Bin/${FileType}_validation.pl";
	push @command, '--xsd', $schema_file, $input_file;

	# put the command line in the trace first
	say 'RUNNING: ' . join( ' ', @command );

	my $output = `@command`;
	( $ErrorMessage = $output ) =~ s/^VALIDATION?:.+$//mg;

	$output =~ /^VALIDATION:? (PASS|FAIL)$/mi;
	return { PASS => 1, FAIL => 0 }->{ uc $1 };
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

sub sendEmail {
	my %email = @_;

	$email{'name'} //= $email{'sender'};

	say "From:    $email{'name'}";
	say "         $email{'sender'}";
	say "To:      $email{'recipient'}";
	say "Subject: $email{'subject'}";
	say "\n$email{'body'}";

	CcsSmtp::SendMail(
		{
			fromdispname => $email{'name'},
			from         => $email{'sender'},
			to           => [ split /;/, $email{'recipient'} ],
			subject      => $email{'subject'},
			body         => [ $email{'body'} ],
		}
	);

	say "Email sent!";
	return;
}

## -------------------------------------- BEGIN main() ------------------------------------------##

my %PP_startup;

GetOptions(%Options);
my $DataFileName = $ARGV[0];

if ( defined $StartupFile ) {
	%PP_startup = parse_startupfile($StartupFile);

	$DataFileName = $PP_startup{'DataFileName'};

	if ( defined $PP_startup{'Extras'} ) {
		Getopt::Long::GetOptionsFromString( $PP_startup{'Extras'}, %Options );
	}
}

if (
	not(   defined $BaseDir
		&& defined $GoodDir
		&& defined $BadDir
		&& defined $FileType
		&& defined $ValidSTDFile
		&& defined $DataFileName )
	)
{
	usage();
	die "Specify --base --good --bad folders, and --type --std file options.\n";
}

if ( not -d $BaseDir ) {
	die "No such --base dir '$BaseDir'\n";
}

mkdir "$BaseDir/$GoodDir" if not -d "$BaseDir/$GoodDir";
mkdir "$BaseDir/$BadDir"  if not -d "$BaseDir/$BadDir";

my $schema_file = "$BaseDir/$FileType/$ValidSTDFile";
if ( not -f $schema_file ) {
	die "No such validation schema file '$schema_file'\n";
}

if ( defined $PassThrough ) {
	say "XML Validation skipped, moving input file '$DataFileName' to '$BaseDir/$GoodDir'";
	move( $DataFileName, "$BaseDir/$GoodDir" ) or die "Failed to move '$DataFileName': $!\n";
}
else {
	say "Validating '$DataFileName' with '$FileType' Schema file '$schema_file'";
	if ( isValidFile( $DataFileName, $schema_file ) ) {
		say "Input file '$DataFileName' is valid; moving to '$BaseDir/$GoodDir'";
		move( $DataFileName, "$BaseDir/$GoodDir" ) or die "Failed to move '$DataFileName': $!\n";
	}
	else {
		say "Input file '$DataFileName' is invalid; moving to '$BaseDir/$BadDir'";
		move( $DataFileName, "$BaseDir/$BadDir" ) or die "Failed to move '$DataFileName': $!\n";

		my $filename = basename($DataFileName);
		$EmailMessage =~ s/!FILENAME!/$filename/o;
		$EmailMessage =~ s/!VALIDATION_ERROR!/$ErrorMessage/o;

		sendEmail(
			name      => $DefaultSenderName,
			sender    => $DefaultSenderEmail,
			recipient => $Recipient // $DefaultRecipient,
			subject   => "Input file '$filename' validation failure",
			body      => $EmailMessage,
		);
	}
}

exit 0;

## --------------------------------------- END main() -------------------------------------------##
