#!/m1/shared/bin/perl -w

use strict;
use lib "/usr/local/bin/voyager/perl";
use MARC::Batch;
use UCLA_Batch; #for UCLA_Batch::safenext to better handle data errors

if ($#ARGV != 0) {
  print "\nUsage: $0 infile\n";
  exit 1;
}

my $infile = $ARGV[0];
my ($filename, $extension) = split_filename($infile);
#print "$filename === $extension\n";

my $batch = MARC::Batch->new('USMARC', $infile);

# Turn off strict validation - otherwise, all records after error are lost
$batch->strict_off();

while (my $record = UCLA_Batch::safenext($batch)) {
  my $field = $record->field('952');
  my $loc = "yr"; #default
  # Getting a variety of locations; split into files based on the "unit"
  # (first 2 characters of the location), not the whole loc code.
  if ($field) {
    $loc = substr($field->subfield('a'), 0, 2);
  }
  my $outfilename = $filename . '_' . $loc . $extension;
  open OUT, '>>:utf8', $outfilename or die "Cannot open output file: $!\n";
  print OUT $record->as_usmarc();
  close OUT;
}

exit 0;

sub split_filename {
  my $filename = shift;
  # Split: extension = final .*, basename = the rest; set defaults if no period in filename
  my ($basename, $extension) = ($filename, "");
  if ( $filename =~ m/(.*)(\.[^.]*)$/ ) {
    ($basename, $extension) = ($1, $2);
  }
  return ($basename, $extension);
}

