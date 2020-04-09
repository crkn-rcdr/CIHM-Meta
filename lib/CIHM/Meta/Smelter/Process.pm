package CIHM::Meta::Smelter::Process;

use 5.014;
use strict;
use Try::Tiny;
use JSON;
use Switch;
use URI::Escape;

=head1 NAME

CIHM::Meta::Smelter::Process - Handles the processing of individual AIPs for CIHM::Meta::Smelter

=head1 SYNOPSIS

    my $process = CIHM::Meta::Smelter::Process->new($args);
      where $args is a hash of arguments.

=cut

sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::Meta::Hammer::Process->new() not a hash\n";
    };
    $self->{args} = $args;

    if (!$self->log) {
        die "Log::Log4perl object parameter is mandatory\n";
    }
    if (!$self->dipstaging) {
        die "dipstaging object parameter is mandatory\n";
    }
    if (!$self->aip) {
        die "Parameter 'aip' is mandatory\n";
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
sub log {
    my $self = shift;
    return $self->args->{log};
}
sub dipstaging {
    my $self = shift;
    return $self->args->{dipstaging};
}


sub process {
    my ($self) = @_;

    #die "This is a failure\n";
    #warn "This is a warning\n";
}

1;
