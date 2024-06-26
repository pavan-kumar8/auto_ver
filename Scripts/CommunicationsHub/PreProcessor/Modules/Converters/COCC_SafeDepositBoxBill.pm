package Converters::COCC_SafeDepositBoxBill;

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
my $DOCUMENT_TYPE   = q(SafeDepositBoxBill);    # used for output JSON file postfix
my $FILENAME_SUFFIX = q(SDBBILLS_NOTICE);       # used for input XML file postfix

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

	$this->{'_statement'}{'ADDRESS'} = handleCOCCAddress( $xs->{'Addressee'}[0], 'Address_Line_' );
	$this->{'_statement'}{'RETURN_ADDRESS'} = handleCOCCAddress( $xs->{'FI_Information'}[0], 'FI_Address', 1 );

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
			VERSION         => 'Account_Information/Allot_Text',
			BOX_NUMBER      => 'Account_Information/Box_Number',
			BOX_TYPE        => 'Account_Information/Box_Type',
			BRANCH_NAME     => 'Account_Information/Branch_Name',
			DUE_DATE        => 'Account_Information/Payment_DueDate',
			START_DATE      => 'Account_Information/Start_Date',
			THROUGH_DATE    => 'Account_Information/Thru_Date',
			AMOUNT_DUE      => 'Account_Information/Total_Amount_Due',
			LATE_DATE       => 'Account_Information/Grace_Date',
			LATE_FEE        => 'Account_Information/Late_Charges',
			DELIVERY_METHOD => 'File_Information/Document_Delivery_Method',
		}
	);

	my $file_info     = setCOCCFileInformation($set);
	my $reg_text      = setCOCCAccountRegText($set);
	my $custom_fields = { %$file_info, %$reg_text };
	setCOCCCustomField( $custom_fields, $set, 'Account_Information', 'Branch_Name' );

	$doc_details->{'TEMPLATE'}      = 'SAFE DEPOSIT BOX BILL';
	$doc_details->{'TITLE'}         = 'Safe Deposit Box';
	$doc_details->{'CUSTOM_FIELDS'} = $custom_fields;

	my $table = {};
	if (   exists $set->{'Form_Loops'}
		&& exists $set->{'Form_Loops'}[0]
		&& exists $set->{'Form_Loops'}[0]{'Form_Loop1'}
		&& exists $set->{'Form_Loops'}[0]{'Form_Loop1'}[0] )
	{
		$table = $this->handle_form_loop( $set->{'Form_Loops'}[0]{'Form_Loop1'} );
	}

	return { %$doc_details, %$table };
}

# client-generic, but document-specific form-loops
sub handle_form_loop {
	my ( $this, $form_loop1 ) = @_;

	my @rows;

	foreach my $transaction ( @{$form_loop1} ) {
		push @rows,
			setCOCCFormLoopToRow(
			$transaction,
			{
				CELL01 => 'Post_Date',
				CELL02 => 'Tran_Desc',
				CELL03 => 'Charge_Amt',
				CELL04 => 'Payment_Amt',
			}
			);
	}

	my %table = ();

	$table{'TABLE01'}{'NAME'} = 'Transactions';
	$table{'TABLE01'}{'ROWS'} = \@rows if @rows;

	return \%table;
}

1;
