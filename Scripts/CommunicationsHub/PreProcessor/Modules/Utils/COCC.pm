package Utils::COCC;

use v5.32;

use strict;
use warnings;
use lib $ENV{'CCS_RESOURCE'} . '/Regional';
use Data::Printer;

use NA::Std::CountryCodes;

use Exporter qw(import);
our @EXPORT_OK = qw(
	processCOCCWorkflow
	handleCOCCAddress
	setCOCCDocumentDetails
	setCOCCFileInformation
	setCOCCFormLoopToRow
	setCOCCAccountRegText
	setCOCCCustomField
);
our %EXPORT_TAGS = ( all => \@EXPORT_OK );

# determine template from COCC file name?

# this is going to be common across COCC XMLs, right?
sub processCOCCWorkflow {
	my $base_struct = shift;

	my %workflow;

	if ( 'Mail' eq $base_struct->{'Document_Delivery_Method'}[0] ) {
		$workflow{'PRINT'}    = 1;
		$workflow{'INSERT'}   = 1;
		$workflow{'LODGE'}    = 1;
		$workflow{'DISPATCH'} = 0;
	}
	elsif ( 'DoNotMail' eq $base_struct->{'Document_Delivery_Method'}[0] ) {
		$workflow{'PRINT'}    = 1;
		$workflow{'INSERT'}   = 0;    # confirm insertion logic
		$workflow{'LODGE'}    = 0;
		$workflow{'DISPATCH'} = 1;
	}
	# is there a no-print option?

	$workflow{'VIEWPOINT'} = ( $base_struct->{'GV_Delivery'}[0] =~ /Upload/ ) ? 1 : 0;

	# email is "notifciation" not delivery, right? odd to put it in GV_Delivery field
	$workflow{'EMAIL'} = ( $base_struct->{'GV_Delivery'}[0] =~ /NoEmail/ ) ? 0 : 1;

	return \%workflow;
}

sub handleCOCCAddress {
	my ( $address_section, $field_base, $remit_coupon ) = @_;

	my %address_struct = ( ADDRESS_LINES => [] );
	for my $i ( 1 .. 10 ) {
		if ( exists $address_section->{"$field_base$i"} and not ref $address_section->{"$field_base$i"}[0] ) {
			push @{ $address_struct{'ADDRESS_LINES'} }, $address_section->{"$field_base$i"}[0];

			# if notice uses remittance coupon, return_addr_line_N set tags will be generated from the following:
			$address_struct{"ADDRESS_LINE$i"} = $address_section->{"$field_base$i"}[0] if defined $remit_coupon;
		}
	}

	if ( exists $address_section->{'City'} and $address_section->{'City'}[0] and not ref $address_section->{'City'}[0] )
	{
		$address_struct{'CITY'} = $address_section->{'City'}[0];
	}

	if (    exists $address_section->{'State'}
		and $address_section->{'State'}[0]
		and not ref $address_section->{'State'}[0] )
	{
		$address_struct{'TERRITORY'} = $address_section->{'State'}[0];
	}
	elsif ( exists $address_section->{'Province'}
		and $address_section->{'Province'}[0]
		and not ref $address_section->{'Province'}[0] )
	{
		$address_struct{'TERRITORY'} = $address_section->{'Province'}[0];
	}

	if (    exists $address_section->{'ZipCode'}
		and $address_section->{'ZipCode'}[0]
		and not ref $address_section->{'ZipCode'}[0] )
	{
		$address_struct{'POSTCODE'} = $address_section->{'ZipCode'}[0];
	}
	elsif ( exists $address_section->{'PostCode'}
		and $address_section->{'PostCode'}[0]
		and not ref $address_section->{'PostCode'}[0] )
	{
		$address_struct{'POSTCODE'} = $address_section->{'PostCode'}[0];
	}

	if (    exists $address_section->{'Country'}
		and $address_section->{'Country'}[0]
		and not ref $address_section->{'Country'}[0] )
	{
		if ( $address_section->{'Country'}[0] =~ /^USA?$/ ) {
			$address_struct{'COUNTRY'} = $address_section->{'Country'}[0];
		}
		else {
			$address_struct{'COUNTRY'} = &_getFullCountryName( $address_section->{'Country'}[0] )
				// $address_section->{'Country'}[0];
		}
	}

	return \%address_struct;
}

sub _getFullCountryName {
	my $input_country_code = shift;
	my $country_code =
		  ( length($input_country_code) == 2 ) ? ( NA::Std::CountryCodes::iso2to3($input_country_code) )
		: ( length($input_country_code) == 3 ) ? ($input_country_code)
		:                                        '';

	my $countryln =
		  ($country_code)
		? ( NA::Std::CountryCodes::ISOCountryCodeToFullCountryName($country_code) )
		: ($input_country_code);

	return $countryln;
}

sub setCOCCDocumentDetails {
	my ( $set, $mapping ) = @_;

	my %document;

	foreach my $key ( keys %{$mapping} ) {
		my $acct_key = $mapping->{$key};
		if ( not ref $acct_key ) {
			my ( $info, $field ) = split '/', $acct_key, 2;
			if (   exists $set->{$info}[0]{$field}
				&& exists $set->{$info}[0]{$field}[0] )
			{
				$document{$key} = ref $set->{$info}[0]{$field}[0] ? qq() : $set->{$info}[0]{$field}[0];
			}
		}
		elsif ( 'HASH' eq ref($acct_key) ) {
			$document{$key} = setCOCCDocumentDetails( $set, $acct_key );
		}
		else {
			die "Unrecognized mapping for key '$key'";
		}
	}

	return \%document;
}

sub setCOCCFormLoopToRow {
	my ( $transaction, $mapping ) = @_;

	my %row;

	foreach my $field ( keys %{$mapping} ) {
		my $form_loop_key = $mapping->{$field};
		if ( not ref $form_loop_key ) {
			if (   exists $transaction->{$form_loop_key}
				&& exists $transaction->{$form_loop_key}[0] )
			{
				$row{$field} = ref $transaction->{$form_loop_key}[0] ? qq() : $transaction->{$form_loop_key}[0];
			}
		}
		elsif ( 'HASH' eq ref($form_loop_key) ) {
			$row{$field} = setCOCCFormLoopToRow( $transaction, $form_loop_key );
		}
		else {
			die "Unrecognized mapping for field '$field'";
		}
	}

	return \%row;
}

sub setCOCCFileInformation {
	my $set = shift;

	my %file_info;

	foreach my $field ( keys %{ $set->{'File_Information'}[0] } ) {
		setCOCCCustomField( \%file_info, $set, 'File_Information', $field );
	}

	return \%file_info;
}

sub setCOCCCustomField {
	my ( $custom_fields, $set, $info, $field ) = @_;
	if (   exists $set->{$info}[0]{$field}
		&& exists $set->{$info}[0]{$field}[0] )
	{
		$custom_fields->{"$info/$field"} = ref $set->{$info}[0]{$field}[0] ? qq() : $set->{$info}[0]{$field}[0];
	}
	return;
}

sub setCOCCAccountRegText {
	my $set = shift;

	my %reg_text;

	# stash COCC's Reg_Text# fields into CUSTOM_FIELDS
	my $reg_index = 1;
	while (exists $set->{'Account_Information'}[0]{"Reg_Text$reg_index"}
		&& exists $set->{'Account_Information'}[0]{"Reg_Text$reg_index"}[0] )
	{
		if ( !ref $set->{'Account_Information'}[0]{"Reg_Text$reg_index"}[0] ) {
			$reg_text{"Account_Information/Reg_Text$reg_index"} =
				$set->{'Account_Information'}[0]{"Reg_Text$reg_index"}[0];
		}
		$reg_index++;
	}

	return \%reg_text;
}

1;
