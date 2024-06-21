package Converters::Base;

use v5.32;

use strict;
use warnings;

#
# Perl Libraries
#
use Data::Dumper;
use IO::File;
use File::stat;
use File::Copy;
use JSON::XS;

use open qw( :std :encoding(UTF-8) );

#---------------------------------------------------------------------------------------------------

=pod
	The constructor method. It sets up some formatting for the text files.
	Opens the file, writes out the headings.

	@param filename, the output filename of the RunQuantity report
=cut

sub new {
	my ( $class, %args ) = @_;

	my $this = {
		_zipFileName           => $args{'zip_file_name'},
		_contractNumber        => uc( $args{'contract_number'} ),
		_runNumber             => $args{'run_number'},
		_companyDetails        => $args{'company_details'},
		_development           => $args{'development'},
		_set_limit             => $args{'set_limit'} || 0,
		_processing_mode       => $args{'processing_mode'},
		_process_object        => $args{'process_object'},
		_connectProduct        => $args{'connect_product'},
		_documentType          => $args{'document_type'},
		_client                => $args{'contract_number'} =~ s/^(\d+[A-Z]+)\d+/$1/r,
		_fine                  => 1,
		_periodEndingDate      => '',
		_last_percent_progress => '',
		_preset_total          => '',
		json_writer            => JSON::XS->new()->pretty(),
		brands                 => {},
	};

	bless $this, $class;

	#
	# Get the date and time that the report is generated (now)
	#
	( $this->{'_dateString'}, $this->{'_timeString'} ) = $this->_formatDateTimes();

	$this->setStatementCount(0);

	return $this;
}

sub developmentOn {
	my ($this) = @_;
	return $this->{'_development'};
}

sub get_processing_mode {
	my ($this) = @_;
	return $this->{'_processing_mode'};
}

#
# This method is called by the process to create the file, this and the constructor
# are the only methods that are expected by the process.
#
sub writeFile {
	my ($this) = @_;

	# to make the XML tag order consistent, run-to-run
	$ENV{'PERL_HASH_SEED'} = 0;    ## no critic Variables::RequireLocalizedPunctuationVars

	# is the file actually present?
	if ( not -f $this->getOriginalFileName() ) {
		say 'file not found';
		die q{file '} . $this->getOriginalFileName() . q{' not found};
	}

	#
	# Get the date last modified from the file
	#
	my $stats = stat( $this->getOriginalFileName() );
	( $this->{'_dateReceived'}, $this->{'_timeReceived'} ) = $this->_formatDateTimes( $stats->mtime );

	if ( $this->shouldRunFile() ) {
		$this->setNewFileName();
		$this->startFile();
		$this->processStatements();
		$this->finishFile();
	}
	else {
		$this->setNewFileName('not processed');
	}

	return $this->isFine();
}

#
# This allows for processor files to be ignored if necessary
#
sub shouldRunFile {
	my ($this) = @_;

	return 1;
}

sub processFile {
	my ( $this, $file ) = @_;

	$this->setStatementCount(0);
	$this->setOriginalFileName($file);

	return $this->writeFile();
}

sub finishFile {
	my ($this) = @_;

	my $FH = $this->{'_fileHandle'};

	# Add the header into the file
	$this->insertHeader();

	# Add the rest into final file
	open( my $TEMP_FILE, '<', $this->{'_tempFileName'} ) or die $!;
	while ( my $line = <$TEMP_FILE> ) {
		print $FH ($line);
	}
	close($TEMP_FILE) or die $!;

	# for balance { [
	print $FH qq(\n]\n});

	#
	# Get rid of the temp file
	#
	unlink( $this->{'_tempFileName'} );
	$this->{'_tempFileName'} = undef;

	#
	# Finish up
	#
	close($FH) or die $!;

	#
	# Must have a period ending date && a non-zero statment count
	# output as much of the intermediate file as possible before failing
	#
	if ( not $this->getStatementCount() ) {
		die q{no statement count defined; no statements were found in file '} . $this->getOriginalFileName() . q{'};
	}
	if ( $this->getStatementCount() < 1 ) {
		# some converters can end up with -1, which really means 0
		die q{statement count is 0; no statements were found in file '} . $this->getOriginalFileName() . q{'};
	}
	if ( not $this->getPeriodEndingDate() ) {
		die q{no statement end date found in file '} . $this->getOriginalFileName() . q{'};
	}

	if ( $this->developmentOn() ) {
		local $| = 1;
		print 'Converting statement ' . $this->getStatementCount() . " -- FILE CONVERTED\n";
	}

	$this->{'brands'} = {};

	return 1;
}

sub processStatements {
	my ($this) = @_;

	$this->runStatementData();

	close $this->{'_tempFileHandle'};

	return 1;
}

sub start_statement {
	my ($this) = @_;

	$this->incrementStatementCount();

	return;
}

sub finish_statement {
	my ($this) = @_;

	$this->{'brands'}{ $this->{'_statement'}{'BRAND_CODE'} }++;

	#
	# write out the file
	#
	$this->addStatement( $this->{'_statement'} );
	$this->clear_statement();

	return;
}

sub clear_statement {
	my ($this) = @_;

	#
	# Clears the memory object that holds one statement
	#
	$this->{'_statement'} = {};

	return;
}

sub addStatement {
	my ( $this, $dataStructure ) = @_;

	# hash-ref arg to encode creates an extra layer of depth
	my $jsonString = $this->{'json_writer'}->encode($dataStructure);
	$jsonString =~ s/.\z//ms;

	my $FH = $this->{'_tempFileHandle'};

	if ( 1 == $this->getStatementCount() ) {
		print $FH ("$jsonString");
	}
	else {
		print $FH (",\n$jsonString");
	}

	if ( $this->developmentOn() ) {
		local $| = 1;
		print 'Converting statement ' . $this->getStatementCount() . "\r";
	}

	my $preset_total = $this->getStatementPresetTotal();
	if ($preset_total) {
		# report every 10%
		my $percent_progress = sprintf( '%02d', $this->getStatementCount() / $preset_total * 100 );
		if ( $percent_progress % 10 == 0 && $percent_progress ne $this->{'_last_percent_progress'} ) {
			$this->{'_last_percent_progress'} = $percent_progress;
			$this->job_message( 'Converting '
					. $this->getOriginalFileName() . ': '
					. $this->getStatementCount()
					. " out of $preset_total; $percent_progress%" );
		}
		elsif ( 1 == $this->getStatementCount() ) {
			$this->{'_last_percent_progress'} = '0';
			$this->job_message( 'Converting '
					. $this->getOriginalFileName() . ': '
					. $this->getStatementCount()
					. " out of $preset_total; $percent_progress%" );
		}
	}
	elsif ( not( $this->getStatementCount() % 100 ) ) {
		# report every 100 statements
		$this->job_message( 'Converting '
				. $this->getOriginalFileName() . ': '
				. $this->getStatementCount()
				. ' statements converted' );
	}

	return;
}

sub startFile {
	my ($this) = @_;

	open( my $JSONHANDLE, '>', $this->getNewFileName() ) or die $!;
	$this->{'_fileHandle'} = $JSONHANDLE;

	$this->{'_tempFileName'} = $this->getNewFileName() . '_statement_temp';

	open( my $STATEMENT_TEMP, '>:encoding(UTF-8)', $this->{'_tempFileName'} ) or die $!;
	$this->{'_tempFileHandle'} = $STATEMENT_TEMP;

	return 1;
}

sub insertHeader {
	my ($this) = @_;

	#
	# This information is available without needing to know what processor we are
	# using
	#
	my $fileInformation = {
		'ORIGINAL_FILE' => {
			'FILE_NAME'     => $this->getOriginalFileName(),
			'DATE_RECEIVED' => $this->{'_dateReceived'},
			'TIME_RECEIVED' => $this->{'_timeReceived'},
		},
		'DERIVATIVE_FILE' => {
			'FILE_NAME'    => $this->getNewFileName(),
			'DATE_CREATED' => $this->{'_dateString'},
			'TIME_CREATED' => $this->{'_timeString'},
		},
		'RECORD_COUNT'    => $this->getStatementCount(),
		'CONNECT_PRODUCT' => $this->getConnectProduct(),
		'DOCUMENT_TYPE'   => $this->getDocumentType(),
		'CONTRACT'        => $this->getContractNumber(),
		'CCS_CLIENT_CODE' => $this->getClientCode(),
		'RUN_NUMBER'      => $this->getRunNumber(),
	};

	if ( defined $this->{'_extra_graphic_stages'} ) {
		foreach ( @{ $this->{'_extra_graphic_stages'} } ) {
			push @{ $fileInformation->{'EXTRA_GRAPHIC_STAGES'}[0]{'GRAPHIC_STAGE'} }, $_;
		}
	}

	my $jsonString = $this->{'json_writer'}->encode( { HEADER => $fileInformation } );

	my $FH = $this->{'_fileHandle'};

	print $FH print_json_header($jsonString);

	return 1;
}

sub print_json_header {
	my $file_info = shift;

	# remove a trailing newline
	$file_info =~ s/.\z//ms;

	# remove leading and closing curly braces, so this embeds better
	$file_info =~ s/^ *{.//ms;
	$file_info =~ s/}\z//ms;

	# remove another trailing newline
	$file_info =~ s/.\z//ms;

	# out-dent once
	$file_info =~ s/^   //gms;

	return <<EOF;
{
$file_info,
"RECORDS" : [
EOF
}

sub runStatementData {
	my ($this) = @_;
	#
	# Overwrite for base data
	#

	return;
}

#
# Accessor methods
#
sub getNewFileName {
	my ($this) = @_;

	return $this->{'_newFile'};
}

{
	my $file_count;

	sub setNewFileName {
		my ( $this, $name ) = @_;

		if ( not $name ) {
			$file_count //= '00';
			$file_count++;

			$this->{'_newFile'} = join '.',
				$this->{'_contractNumber'}, $this->{'_runNumber'}, $file_count, $this->{'_documentType'}, 'json';
		}
		else {
			$this->{'_newFile'} = $name;
		}

		return 1;
	}
}

sub getOriginalFileName {
	my ($this) = @_;

	return $this->{'_originalFile'};
}

sub setOriginalFileName {
	my ( $this, $name ) = @_;

	$this->{'_originalFile'} = $name;

	return 1;
}

sub getZipFileName {
	my ($this) = @_;

	return $this->{'_zipFileName'};
}

sub getContractNumber {
	my ($this) = @_;

	return $this->{'_contractNumber'};
}

sub getClientCode {
	my ($this) = @_;

	return $this->{'_client'};
}

sub getRunNumber {
	my ($this) = @_;

	return $this->{'_runNumber'};
}

sub getCompanyDetails {
	my ($this) = @_;

	return $this->{'_companyDetails'};
}

sub getPeriodEndingDate {
	my ($this) = @_;

	return $this->{'_periodEndingDate'};
}

sub setPeriodEndingDate {
	my ( $this, $value ) = @_;

	$this->{'_periodEndingDate'} = $value;

	return 1;
}

sub getStatementCount {
	my ($this) = @_;

	return $this->{'_statementCount'};
}

sub setStatementCount {
	my ( $this, $value ) = @_;

	$this->{'_statementCount'} = $value;

	return 1;
}

sub incrementStatementCount {
	my ($this) = @_;

	$this->{'_statementCount'}++;

	return 1;
}

sub isFine {
	my ($this) = @_;

	return $this->{'_fine'};
}

sub makeFine {
	my ($this) = @_;

	$this->{'_fine'} = 1;

	return 1;
}

sub notFine {
	my ($this) = @_;

	$this->{'_fine'} = 0;

	return;
}

#
# This function takes a time (in epoch seconds) and gets it's local time
# formatted our way. If no time is passed in it gets the time now
#
sub _formatDateTimes {
	my ( $this, $timeToFormat ) = @_;

	my ( $sec, $min, $hour, $day, $month, $year );
	if ( defined($timeToFormat) ) {
		( $sec, $min, $hour, $day, $month, $year ) = ( localtime($timeToFormat) )[ 0, 1, 2, 3, 4, 5 ];
	}
	else {
		( $sec, $min, $hour, $day, $month, $year ) = (localtime)[ 0, 1, 2, 3, 4, 5 ];
	}

	$year  = sprintf( '%04d', $year + 1900 );
	$month = sprintf( '%02d', $month + 1 );
	$day   = sprintf( '%02d', $day );

	$sec  = sprintf( '%02d', $sec );
	$min  = sprintf( '%02d', $min );
	$hour = sprintf( '%02d', $hour );

	return ( "$year-$month-$day", "$hour:$min:$sec" );
}

sub setDDA {
	my ( $this, $dda ) = @_;

	$this->{'_dda'} = $dda;

	return 1;
}

sub job_message {
	my ( $this, $msg ) = @_;

	#TODO: update the trace, maybe conditionally?

	return 1;
}

sub getConnectProduct {
	my ($this) = @_;

	return $this->{'_connectProduct'};
}

sub getDocumentType {
	my ($this) = @_;

	return $this->{'_documentType'};
}

sub getStatementPresetTotal {
	my ($this) = @_;

	return $this->{'_preset_total'};
}

sub setStatementPresetTotal {
	my ( $this, $total ) = @_;

	$this->{'_preset_total'} = $total;

	return 1;
}

sub DESTROY {
	my ($this) = @_;
	close $this->{'_fileHandle'}
		if defined $this->{'_fileHandle'};
	close $this->{'_tempFileHandle'}
		if defined $this->{'_tempFileHandle'};

	return 1;
}

1;
