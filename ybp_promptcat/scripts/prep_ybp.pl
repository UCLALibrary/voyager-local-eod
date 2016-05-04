#!/m1/shared/bin/perl -w

use strict;
use lib "/opt/local/perl";
use MARC::Batch;
use UCLA_Batch; #for UCLA_Batch::safenext to better handle data errors
use DBI;

if ($#ARGV != 3) {
  print "\nUsage: $0 infile outfile schema schema_pwd\n";
  exit 1;
}

my $infile = $ARGV[0];
my $outfile = $ARGV[1];
my $schema = $ARGV[2];
my $password = $ARGV[3];

# YBP records are in MARC-8, not UTF-8
my $batch = MARC::Batch->new('USMARC', $infile);
open OUT, '>', $outfile or die "Cannot open output file: $!\n";

# Make database connection
my $dsn = "dbi:Oracle:host=localhost;sid=VGER";
my $dbh = DBI->connect($dsn, $schema, $password);

# SQL to get account-specific info
my $sql = "
  select location_code, fund_code from vger_support.ybp_account_lookup
  where account_number = ?
";
my $sth = $dbh->prepare($sql);
    
# Turn off strict validation - otherwise, all records after error are lost
$batch->strict_off();

while (my $record = UCLA_Batch::safenext($batch)) {
  # Get 001 (OCLC record number)
  my $oclc = $record->field('001')->data();

  # Get 982 $b YBP account
  my $f982 = $record->field('982');
  my $account = "599030"; # default YRL account
  if ($f982) {
    $account = $f982->subfield('b') if defined($f982->subfield('b'));
  }

  # Change (or add) location code in 952 $a based on YBP account
  my $loc = getLoc($account);
  # Loc is mandatory, set a default if not defined
  if (! defined($loc)) {
    print "ERROR: $oclc has no location for $account, using yr\n";
    $loc = "yr";
  }
  my $f952 = $record->field('952');
  if ($f952) {
    $f952->update(a => $loc);
  }
  else {
    $f952 = MARC::Field->new('952', '', '', 'a', $loc);
    $record->append_fields($f952);
  }

  # Change (or add) fund code in 981 $b based on YBP account
  my $fund = getFund($account);
  # Fund is optional, only set it if defined
  if (defined($fund)) {
    my $f981 = $record->field('981');
    if ($f981) {
      $f981->update(b => $fund);
    }
    else {
      $f981 = MARC::Field->new('981', '', '', 'b', $fund);
      $record->append_fields($f981);
    }
  } # $fund

  print OUT $record->as_usmarc();
}

# Clean up
$sth->finish();
$dbh->disconnect();
close OUT;
exit 0;

sub getFund {
  my $account = shift;
  my $result = $sth->execute($account) || die $sth->errstr;
  my ($loc, $fund) = $sth->fetchrow_array();
  return $fund;
}

sub getLoc {
  my $account = shift;
  my $result = $sth->execute($account) || die $sth->errstr;
  my ($loc, $fund) = $sth->fetchrow_array();
  return $loc;
}

