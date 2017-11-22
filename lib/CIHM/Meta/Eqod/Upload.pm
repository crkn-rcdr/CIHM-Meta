package CIHM::Meta::Eqod::Upload;

use common::sense;
use Data::Dumper;
use Storable qw(freeze thaw);
use MooseX::App::Command;
use Try::Tiny;
use CIHM::Eqod;


extends qw(CIHM::Meta::Eqod);

parameter 'file' => (
  is => 'rw',
  isa => 'Str',
  required => 1,
  documentation => q[Filename of the metadata .xml file],
);

command_short_description 'Parses EQOD data and uploads to Externalmeta DB';

sub run {
	my ($self) = @_;
	warn $self->configpath;
	die;
	my $eqod = CIHM::Eqod->new($self->configpath);
	
	my $eqod_csv = $self->file;
	warn $file;
	die;
	if (! -f $eqod_csv) {
      print STDERR "Eqod file not found: $eqod_csv\n";
      return;
	}    
    $eqod->upload_eqod($self->file);
	
}
1;
