#!C:/Perl5.10.1/bin/perl.exe

use 5.010;

use strict;
use warnings;

use lib $ENV{CCS_RESOURCE};
use lib_CCS others => ["$ENV{CCS_RESOURCE}/Global/Std"];
use CcsCommon;

print CcsCommon::get_setting( 'GENERAL', $ARGV[0] );

exit 0;
