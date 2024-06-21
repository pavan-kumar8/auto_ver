#!C:/Strawberry/perl/bin/perl.exe

use v5.32;

use strict;
use warnings;

#--------------------------------------------------------------------------------------------------
# This script validates XML datafiles based upon the most recent XSD file.
# C:/Strawberry/perl/bin/perl.exe
# \\csavcdsfpd1\GE\UAT\GitResource\Regional\US\Scripts\CommunicationsHub\PreProcessor\xsd_validation.pl
# --xsd \\csavcdsfpd1\GE\UAT\DataIn\CommunicationsHub\XSD\COCC\COCC_Notices_XML_Schema.xsd
# \\csavcdsfpd1\GE\UAT\DataIn\CommunicationsHub\In\TEST_20230301_1078_CO_ADDCHG_NOTICE.XML
#--------------------------------------------------------------------------------------------------

use Data::Printer;
use Getopt::Long;
use XML::LibXML;
use Try::Tiny qw(try catch);

GetOptions(
	'xsd=s' => \my $XSDFilePath,
	'help'  => sub { usage(); exit 0 },
);

sub usage {
	my ($cmd) = $0 =~ /^.*?[\\\/]?([^\\\/]+?)(?:\.\w{1,4})?$/;

	print <<"EOF";
 $cmd: validates each XML file against specified XSD

Options:
  --xsd   <FILE>    path to XSD file location

  --help            print this usage message

EOF

	return;
}

sub isValidXML {
	my ( $xml_file, $xsd_file ) = @_;

	my $xml_doc = XML::LibXML->load_xml( location => $xml_file );
	my $xsd_doc = XML::LibXML::Schema->new( location => $xsd_file );

	my $is_xml_valid = try {
		not $xsd_doc->validate($xml_doc);
	}
	catch {
		# output error message to stdout
		s/^\S+?:0://mg;
		print;
		return 0;
	};

	return $is_xml_valid;
}

## -------------------------------------- BEGIN main() ------------------------------------------##

if ( not defined $XSDFilePath ) {
	usage();
	exit 1;
}

if ( not -f $XSDFilePath ) {
	die "No such XSD file '$XSDFilePath'\n";
}

if ( 0 == @ARGV ) {
	die "No input file specified.\n";
}

foreach my $xml_file (@ARGV) {
	say "VALIDATION: '$xml_file' current XSD '$XSDFilePath'";
	my $is_xml_valid = isValidXML( $xml_file, $XSDFilePath );
	# following message is parsed in validate_input.pl script
	say "VALIDATION: " . ( $is_xml_valid ? "PASS" : "FAIL" );
}

exit 0;

## --------------------------------------- END main() -------------------------------------------##
