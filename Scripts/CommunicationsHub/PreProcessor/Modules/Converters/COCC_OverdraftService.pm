package Converters::COCC_OverdraftService;

use v5.32;

use strict;
use warnings;

use parent qw(Converters::Base);

use Utils::COCC qw(:all);

#
# Perl Libraries
#
use Data::Printer;

use Date::Calc qw(Today);
use XML::Twig;

#
# Module Globals
#
my $DOCUMENT_TYPE   = q(OverdraftService);        # used for output JSON file postfix
my $FILENAME_SUFFIX = q(DP_REGE_CONFM_NOTICE);    # used for input XML file postfix

#---------------------------------------------------------------------------------------------------

=pod
=cut

sub new {
	my ( $pkg, %args ) = @_;

	# set document type to override generic default
	$args{'document_type'} = $DOCUMENT_TYPE;

	#FIXME: leave it alone, revisit when we truss this up
	my $class = ref($pkg) || $pkg;
	my $this = $class->SUPER::new(%args);

	return $this;
}

sub shouldRunFile {
	my ($this) = @_;

	my $file_name = $this->getOriginalFileName();

	if ( $file_name =~ /\d{8}_(\d{4})_${FILENAME_SUFFIX}\.XML$/i ) {
		$this->{'fi'} = $1;
		return 1;
	}
	else {
		warn "File '$file_name' does not match the naming convention for '$this->{_documentType}'\n";
		return 0;
	}
}

sub runStatementData {
	my ($this) = @_;

	my $file_name = $this->getOriginalFileName();

	my ( $year, $month, $day ) = Today();
	$this->{'date'} = sprintf( '%04d-%02d-%02d', $year, $month, $day );

	# XML::Twig, iterate through all statements
	my $t = XML::Twig->new(
		twig_handlers => {
			Pack => sub { $this->handle_pack(@_) },
		}
	);
	$t->parsefile($file_name);

	if ($@) {
		if ( "SAMPLE\n" eq $@ ) {
			print "\nSample quantity met..\n";
			return;
		}
		else {
			warn "\n";
			die $@;
		}
	}

	return;
}

sub handle_pack {
	my ( $this, $t, $xml_node ) = @_;

	if ( $this->{'_set_limit'} > 0 and $this->getStatementCount() >= ( $this->{'_set_limit'} - 1 ) ) {
		# die with a keyword we can recognize after the eval in runStatementData()
		$t->purge();
		$t->dispose();
		die "SAMPLE\n";
	}

	my $xs = $xml_node->simplify( forcearray => 1 );
	$xs->{_node_name} = $xml_node->gi();

	$t->purge();

	$this->start_statement();

	$this->setPeriodEndingDate( $this->{'date'} );
	$this->{'_statement'}{'STATEMENT_IDENTIFICATION'}{'STATEMENT_TO_DATE'} =
		$xs->{'File_Information'}[0]{'Notice_Date'}[0];
	$this->{'_statement'}{'STATEMENT_IDENTIFICATION'}{'ACCOUNT_NUMBER'} =
		$xs->{'Account_Information'}[0]{'Account_Number'}[0];

	$this->{'_statement'}{'ADDRESS'}        = handleCOCCAddress( $xs->{'Addressee'}[0],      'Address_Line_' );
	$this->{'_statement'}{'RETURN_ADDRESS'} = handleCOCCAddress( $xs->{'FI_Information'}[0], 'FI_Address' );

	$this->{'_statement'}{'EXTERNAL_CLIENT_CODE'} = $xs->{'FI_Information'}[0]{'FI_Number'}[0];
	$this->{'_statement'}{'CLIENT_NAME'}          = $xs->{'FI_Information'}[0]{'FI_Name'}[0];

	if ( exists $xs->{'FI_Information'}[0]{'FI_Brand'} and not ref $xs->{'FI_Information'}[0]{'FI_Brand'}[0] ) {
		$this->{'_statement'}{'BRAND_CODE'} = $xs->{'FI_Information'}[0]{'FI_Brand'}[0];
	}
	else {
		$this->{'_statement'}{'BRAND_CODE'} = 'default';
	}

	# maybe this can be generic across all of COCC?
	# if so, this sub should be moved to a Util module (COCC_Utils or something)
	$this->{'_statement'}{'WORKFLOW'} = processCOCCWorkflow( $xs->{'File_Information'}[0] );

	$this->{'_statement'}{'DOCUMENTS'} = [ $this->handle_document_details($xs) ];

	$this->finish_statement();

	return;
}

# client-generic, but document-specific fields
sub handle_document_details {
	my ( $this, $set ) = @_;

	# most data should be in returned structure
	# some things might be too COCC specific, and should be added to CUSTOM_FIELDS directly

	my $doc_details = setCOCCDocumentDetails(
		$set,
		{
			#	CONFIRMATION_DETAILS => {
			#		START_DATE => 'Account_Information/Start_Date',
			#		OPT_IN     => 'Account_Information/RegE_OptIn_YN',
			#	},
			'CONFIRMATION_DETAILS/START_DATE' => 'Account_Information/Start_Date',
			'CONFIRMATION_DETAILS/OPT_IN'     => 'Account_Information/RegE_OptIn_YN',
			DELIVERY_METHOD                   => 'File_Information/Document_Delivery_Method',
		}
	);

	my $custom_fields = setCOCCFileInformation($set);
	setCOCCCustomField( $custom_fields, $set, 'Product_Information', 'Minor_Description' );

	$doc_details->{'TEMPLATE'}      = 'Overdraft Service Opt In/Out Notice';
	$doc_details->{'CUSTOM_FIELDS'} = setCOCCFileInformation($set);

	return $doc_details;
}

1;
