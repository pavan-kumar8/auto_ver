# Version: 0.0.2
# Timestamp: 2024-06-26 19:03:34 +0530
# Author: pavan kumar

#!C:/Perl5.10.1/bin/perl.exe

# change on 1 for 2

use 5.010;

use strict;
use warnings;

use lib $ENV{CCS_RESOURCE};
use lib_CCS others => ["$ENV{CCS_RESOURCE}/Global/Std"];
use CcsCommon;

print CcsCommon::get_setting( 'GENERAL', $ARGV[0] );

exit 0;
