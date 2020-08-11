package CIHM::Meta::Hammer2::Process;

use 5.014;
use strict;
use CIHM::METS::parse;
use XML::LibXML;
use Try::Tiny;
use JSON;
use Switch;
use URI::Escape;
use Data::Dumper;

=head1 NAME

CIHM::Meta::Hammer2::Process - Handles the processing of individual manifests

=head1 SYNOPSIS

    my $process = CIHM::Meta::Hammer2::Process->new($args);
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
    if ( !$self->swift ) {
        die "swift object parameter is mandatory\n";
    }
    if ( !$self->collectiondb ) {
        die "collectiondb object parameter is mandatory\n";
    }
    if ( !$self->manifestdb ) {
        die "manifestdb object parameter is mandatory\n";
    }
    if ( !$self->cantaloupe ) {
        die "cantaloupe object parameter is mandatory\n";
    }
    if ( !$self->noid ) {
        die "Parameter 'noid' is mandatory\n";
    }

    $self->{updatedoc}            = {};
    $self->{pageinfo}             = {};
    $self->pageinfo->{count}      = 0;
    $self->pageinfo->{dimensions} = 0;

    return $self;
}

# Simple accessors for now -- Do I want to Moo?
sub args {
    my $self = shift;
    return $self->{args};
}

sub noid {
    my $self = shift;
    return $self->args->{noid};
}

sub log {
    my $self = shift;
    return $self->args->{log};
}

sub swift {
    my $self = shift;
    return $self->args->{swift};
}

sub container {
    my $self = shift;
    return $self->args->{swiftcontainer};
}

sub manifestdb {
    my $self = shift;
    return $self->args->{manifestdb};
}

sub collectiondb {
    my $self = shift;
    return $self->args->{collectiondb};
}

sub type {
    my $self = shift;
    return $self->args->{type};
}

sub cantaloupe {
    my $self = shift;
    return $self->args->{cantaloupe};
}

sub updatedoc {
    my $self = shift;
    return $self->{updatedoc};
}

sub pageinfo {
    my $self = shift;
    return $self->{pageinfo};
}

sub process {
    my ($self) = @_;

    if ( $self->type eq "manifest" ) {
        $self->processManifest();
    }
    else {
        $self->processCollection();
    }
}

sub processManifest {
      my ($self) = @_;

      die "Nothing here for Manifests\n";
}

sub processCollection {
      my ($self) = @_;

      die "Nothing here for Collections\n";
}


1;
