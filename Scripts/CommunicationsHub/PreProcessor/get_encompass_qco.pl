#!C:/Perl5.10.1/bin/perl.exe

use 5.010;

use strict;
use warnings;

use FindBin qw($Bin);
use POSIX qw(strftime);

use lib $ENV{CCS_RESOURCE};
use lib_CCS others => [
	"$ENV{CCS_RESOURCE}/Global/Std", "$ENV{CCS_RESOURCE}/Global/enCompass/Modules/DAL",
	"$ENV{CCS_RESOURCE}/Regional/NA/Perl/5.10/site/lib",
];

use CcsCommon;
use SOAP::Lite;
use JSON::API;
use DAL;

## -------------------------------------- BEGIN main() ------------------------------------------##

open my $saveout, ">&STDOUT";
open STDOUT, '>', "/dev/null";

my $qco_data = get_encompass_json( $ARGV[0] );

open STDOUT, ">&", $saveout;

print JSON->new()->encode($qco_data);

exit 0;

## --------------------------------------- END main() -------------------------------------------##

sub connect_encompass {
	my $dal = DAL->new();
	my $dsn =
		SOAP::Lite->proxy( CcsCommon::get_setting( 'ENCOMPASS', 'encompass2_appadmin' ), timeout => 60 )
		->on_action( sub { $_[0] . $_[1] } )->on_fault( sub { shift; die Dumper( \@_ ) } )
		->call( SOAP::Data->name('GetConnectionString')->attr( { xmlns => "http://tempuri.org/" } ) =>
			( SOAP::Data->name( 'siteName' => 'Toronto' ), SOAP::Data->name( 'appName' => 'encompass2' ) ) )->result();
	my %dsnDetails = map { /(.*)=(.*)/ ? ( lc $1 => $2 ) : () } split( ';', $dsn );

	$dal->set_username_and_password( $dsnDetails{'user id'}, $dsnDetails{'password'} );
	$dal->create_db_connection( $dsnDetails{'data source'}, $dsnDetails{'initial catalog'} );

	return $dal;
}

sub get_encompass_json {
	( my $client = shift ) =~ s/\d+$//;

	my $dal = connect_encompass();

	my $chub_ini = CcsCommon::ini2h("$Bin/PreProcessor.ini");
	my $node     = $chub_ini->{'encompass'}{'node'};

	my $bc_root = $dal->get_node_type_id( "${node}_Structure", $node );

	my $content = {
		version     => 1,
		author      => '',
		description => '',
		keywords    => '',
		type        => $node,
		title       => $client,
		key         => $client,
	};

	foreach my $fi ( @{ $dal->get_all_child_node_details($bc_root) } ) {
		my $fi_info = get_product_details( $dal, $fi->{id} );
		my $ccs_client_code = $fi_info->{'client_details'}{'ccs_client_code'};

		next if not( defined $ccs_client_code && $ccs_client_code eq $client );

		foreach my $brand ( @{ $dal->get_all_child_node_details( $fi->{id} ) } ) {
			my $brand_info = get_product_details( $dal, $brand->{id} );

			foreach my $doc ( @{ $dal->get_all_child_node_details( $brand->{id} ) } ) {
				my $doc_info = get_product_details( $dal, $doc->{id} );
				$brand_info->{documents}{ $doc->{name} } = $doc_info;
			}

			$fi_info->{brands}{ $brand->{name} } = $brand_info;
		}

		$content->{data} = [$fi_info];
		last;
	}

	return { content => $content };
}

sub get_product_details {
	my ( $dal, $id ) = @_;

	my $result;
	my $dbdata = $dal->get_product_node_details( $id, 0, undef );

	while ( my ( $section, $properties ) = each %{ $dbdata->{$id} } ) {
		next unless ref $properties;
		$result->{$section} = $properties->{0};
	}

	return $result;
}
