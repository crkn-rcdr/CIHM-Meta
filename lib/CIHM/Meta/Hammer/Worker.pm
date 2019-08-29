package CIHM::Meta::Hammer::Worker;

use strict;
use Carp;
use AnyEvent;
use Try::Tiny;
use JSON;
use CIHM::TDR::TDRConfig;
use CIHM::TDR::REST::ContentServer;
use CIHM::Meta::REST::filemeta;
use CIHM::Meta::REST::internalmeta;
use CIHM::Meta::Hammer::Process;

our $self;

sub initworker {
    my $configpath = shift;
    our $self;

    AE::log debug => "Initworker ($$): $configpath";

    $self = bless {};

    $self->{config} = CIHM::TDR::TDRConfig->instance($configpath);
    $self->{logger} = $self->{config}->logger;
    my %confighash = %{$self->{config}->get_conf};


    # Undefined if no <filemeta> config block
    if (exists $confighash{filemeta}) {
        $self->{filemeta} = new CIHM::Meta::REST::filemeta (
            server => $confighash{filemeta}{server},
            database => $confighash{filemeta}{database},
            type   => 'application/json',
            conf   => $configpath,
            clientattrs => {timeout => 3600},
            );
    } else {
        croak "Missing <filemeta> configuration block in config\n";
    }

    # Undefined if no <internalmeta> config block
    if (exists $confighash{internalmeta}) {
        $self->{internalmeta} = new CIHM::Meta::REST::internalmeta (
            server => $confighash{internalmeta}{server},
            database => $confighash{internalmeta}{database},
            type   => 'application/json',
            conf   => $configpath,
            clientattrs => {timeout => 3600},
            );
    } else {
        croak "Missing <internalmeta> configuration block in config\n";
    }

    my %cosargs = (
        jwt_payload => '{"uids":[".*"]}',
        conf => $configpath
        );
    $self->{cos} = new CIHM::TDR::REST::ContentServer (\%cosargs);
    if (!$self->cos) {
        croak "Missing ContentServer configuration\n";
    }
}


# Simple accessors for now -- Do I want to Moo?
sub log {
    my $self = shift;
    return $self->{logger};
}
sub config {
    my $self = shift;
    return $self->{config};
}
sub cos {
    my $self = shift;
    return $self->{cos};
}
sub filemeta {
    my $self = shift;
    return $self->{filemeta};
}
sub internalmeta {
    my $self = shift;
    return $self->{internalmeta};
}

sub warnings {
    my $warning = shift;
    our $self;
    my $aip="unknown";

    # Strip wide characters before  trying to log
    (my $stripped=$warning) =~ s/[^\x00-\x7f]//g;

    if ($self) {
        $self->{message} .= $warning;
        $aip = $self->{aip};
        $self->log->warn($aip.": $stripped");
    } else {
        say STDERR "$warning\n";
    }
}


sub swing {
  my ($aip,$metspath,$manifestdate,$configpath) = @_;
  our $self;

  # Capture warnings
  local $SIG{__WARN__} = sub { &warnings };

  if (!$self) {
      initworker($configpath);
  }

  # Debugging: http://lists.schmorp.de/pipermail/anyevent/2017q2/000870.html
#  $SIG{CHLD} = 'IGNORE';

  $self->{aip}=$aip;
  $self->{message}='';

  $self->log->info("Processing $aip");

  AE::log debug => "$aip Before ($$)";

  my $status;

  # Handle and record any errors
  try {
      $status = 1;
      new  CIHM::Meta::Hammer::Process(
          {
              aip => $aip,
              metspath => $metspath,
              configpath => $configpath,
              log => $self->log,
              cos => $self->cos,
              filemeta => $self->filemeta,
              internalmeta => $self->internalmeta,
          })->process;
  } catch {
      $status = 0;
      $self->log->error("$aip: $_");
      $self->{message} .= "Caught: " . $_;
  };
  $self->postResults($aip,$status,$self->{message},$manifestdate,$metspath);

  AE::log debug => "$aip After ($$)";

  return ($aip);
}

sub postResults {
    my ($self,$aip,$status,$message,$manifestdate,$metspath) = @_;

    $self->internalmeta->update_basic($aip,{ 
        "hammer" => encode_json({
            "status" => $status,
            "message" => $message,
            "manifestdate" => $manifestdate,
            "metspath" => $metspath
                                   })
    });
}

1;
