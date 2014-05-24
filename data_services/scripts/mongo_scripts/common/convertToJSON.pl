#!/usr/bin/perl
use POSIX;

use utf8;

use Getopt::Long;
use Scalar::Util qw(looks_like_number);

sub now {
        return strftime '%a %b %e %H:%M:%S %Y', localtime;
}

my @ARGV0 = @ARGV;
my $opt_h;
my $opt_i = '';
my $opt_o = '';

GetOptions("h" => \$opt_h,
	"i=s" => \$opt_i,
	"o=s" => \$opt_o);

if ( scalar @ARGV0 == 0 || $opt_i eq '' || $opt_o eq '' ) {
	print "Usage $0 [-h] -i <input file> -o <output file>\n";
	print "You used '$0 ";
	foreach $av (@ARGV0) {
		print $av . " ";
	}
	print "'\n";
	exit 1;
}


open(INPUT, "<", $opt_i)
        or die "Error: Cannot open $opt_i for processing!";
open(OUTPUT, ">", $opt_o)
        or die "Error: Cannot open $opt_o for processing!";

my $count = 0;
my $skipped = 0;
print OUTPUT "var values = [\n";

NEXT: while ($line = <INPUT>) {
	if ( $count == 0 && $opt_h ) {
		$count++;
		next NEXT;
	}
	$count++;
        my @columns = split(/\t/, $line);
        my $uid = $columns[0];
        my $val = $columns[1];
	$val =~ s/\s+$//;
	if ( $uid eq '' && $val eq '' ) {
		$skipped++;
		next NEXT;
	}
	my $quote = looks_like_number($val) ? "" : "'";

	print OUTPUT "{uid:'" . "$uid',value:" . $quote . $val . $quote . "},\n";
}

print OUTPUT "]\n";

print now() . " Processed $count records. Skipped $skipped empty lines.\n";
close INPUT;
close OUTPUT;

