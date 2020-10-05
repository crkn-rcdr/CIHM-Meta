package CIHM::Meta::Dmdtask::Process;

use 5.014;
use strict;
use CIHM::METS::parse;
use XML::LibXML;
use Try::Tiny;
use JSON;
use Switch;
use URI::Escape;
use Data::Dumper;
use CIHM::Meta::dmd::flatten qw(normaliseSpace);
use List::MoreUtils qw(uniq);

=head1 NAME

CIHM::Meta::Dmdtask::Process - Handles the processing of individual tasks

=head1 SYNOPSIS

    my $process = CIHM::Meta::Dmdtask::Process->new($args);
      where $args is a hash of arguments.

=cut

sub new {
    my ( $class, $args ) = @_;
    my $self = bless {}, $class;

    if ( ref($args) ne "HASH" ) {
        die "Argument to CIHM::Meta::Hammer2::Process->new() not a hash\n";
    }
    $self->{args} = $args;

    if ( !$self->log ) {
        die "Log::Log4perl object parameter is mandatory\n";
    }
    if ( !$self->dmdtaskdb ) {
        die "dmdtaskdb object parameter is mandatory\n";
    }

    return $self;
}

# Simple accessors for now -- Do I want to Moo?
sub args {
    my $self = shift;
    return $self->{args};
}

sub taskid {
    my $self = shift;
    return $self->args->{taskid};
}

sub type {
    my $self = shift;
    return $self->args->{type};
}

sub log {
    my $self = shift;
    return $self->args->{log};
}

sub dmdtaskdb {
    my $self = shift;
    return $self->args->{dmdtaskdb};
}

# Top method
sub process {
    my ($self) = @_;

    if ($self->type eq "split") {
        $self->doSplit();
    } else {
        $self->doStore();
    }
}

sub doSplit {
    my ($self) = @_;

    warn "Nothing to split here\n";
}

sub doStore {
    my ($self) = @_;

    warn "Nothing to store here\n";
}

1;
