package CIHM::Meta::SipValidate::Process;

use 5.014;
use strict;
use Try::Tiny;
use JSON;
use Switch;
use POSIX qw(strftime);
use CIHM::TDR::SIP;
use Data::Dumper;

=head1 NAME

CIHM::Meta::SipValidate::Process - Handles the processing of individual AIPs for CIHM::Meta::SipValidate

=head1 SYNOPSIS

    my $t_repo = CIHM::TDR::SipValidate::Process->new($args);
      where $args is a hash of arguments.

=cut


sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::TDR::Replication->new() not a hash\n";
    };
    $self->{args} = $args;

    if (!$self->aip) {
        die "Parameter 'aip' is mandatory\n";
    }
    if (!$self->wipmeta) {
        die "CIHM::TDR::REST::wipmeta instance parameter is mandatory\n";
    }
    if (!$self->tdr) {
        die "CIHM::TDR instance parameter is mandatory\n";
    }
    if (!$self->log) {
        die "log object parameter is mandatory\n";
    }
    if (!$self->hostname) {
        die "hostname parameter is mandatory\n";
    }

    $self->{aipdata}=$self->wipmeta->get_aip($self->aip);
    if (!$self->aipdata) {
        die "Failed retrieving AIP data\n";
    }
    return $self;
}

# Simple accessors for now -- Do I want to Moo?
sub args {
    my $self = shift;
    return $self->{args};
}
sub aip {
    my $self = shift;
    return $self->args->{aip};
}
sub aipdata {
    my $self = shift;
    return $self->{aipdata};
}
sub hostname {
    my $self = shift;
    return $self->args->{hostname};
}
sub log {
    my $self = shift;
    return $self->args->{log};
}
sub wipmeta {
    my $self = shift;
    return $self->args->{wipmeta};
}
sub tdr {
    my $self = shift;
    return $self->args->{tdr};
}
sub tempdir {
    my $self = shift;
    return $self->args->{tempdir};
}


sub process {
    my ($self) = @_;

    $self->{job} = $self->aipdata->{'processReq'}[0];

    $self->log->info($self->aip.": Accepted job. processReq = ". encode_json($self->{job}));

    my ($depositor, $identifier) = split(/\./,$self->aip);
    my $aippath = $self->tdr->repo->find_aip_pool($depositor, $identifier);
    if (!$aippath) {
        die $self->aip." not found in TDR\n";
    }
    my $path = join("/",$aippath,"data","sip");
    my $sip = CIHM::TDR::SIP->new($path);
    $sip->validate(1,$self->tempdir);
    $self->log->info($self->aip.": Successfully validated.");
    return {};
}

1;
