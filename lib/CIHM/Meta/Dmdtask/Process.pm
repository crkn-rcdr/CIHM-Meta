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

    $self->{items} = [];
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

sub items {
    my $self = shift;
    return $self->{items};
}

sub doc {
    my $self = shift;
    return $self->{doc};
}

# Top method
sub process {
    my ($self) = @_;

    $self->{doc} = $self->dmdtaskdb->get_document( $self->taskid );
    if ( !( $self->doc ) ) {
        die "Couldn't load document\n";
    }
    if ( $self->type eq "split" ) {
        $self->doSplit();
    }
    else {
        $self->doStore();
    }
}

sub doSplit {
    my ($self) = @_;

    # Just a test.
    @{$self->items} = (
        {
            "id"          => "testid1",
            "accessidfound" => "oocihm.testid1",
            "validated"   => JSON::true,
            "message"     => ''
        },
        {
            "id"          => "testid2",
            "accessidfound" => "oocihm.testid2",
            "validated"   => JSON::false,
            "message"     => 'It is broke'
        },
        {
            "id"          => "testid3",
            "accessfound" => "oocihm.testid3",
            "validated"   => JSON::true,
            "message"     => 'Still chatty'
        }
    );
    warn "This is only a test at the moment.";
    $self->storeItems();
}

sub doStore {
    my ($self) = @_;

    print Dumper ( $self->doc );
    warn "Nothing to store here\n";
}

sub storeItems {
    my ($self) = @_;

    $self->dmdtaskdb->update(
        $self->taskid,
        {
            "items" => encode_json( $self->items )
        }
    );
}

1;
