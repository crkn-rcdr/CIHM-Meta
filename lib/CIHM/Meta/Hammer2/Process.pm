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
use CIHM::Meta::dmd::flatten;

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

    $self->{flatten} = CIHM::Meta::dmd::flatten->new;

    $self->{updatedoc}            = {};
    $self->{pageinfo}             = {};
    $self->{attachment}           = [];
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

sub access_metadata {
    my $self = shift;
    return $self->args->{access_metadata};
}

sub access_files {
    my $self = shift;
    return $self->args->{access_files};
}

sub preservation_files {
    my $self = shift;
    return $self->args->{preservation_files};
}

sub manifestdb {
    my $self = shift;
    return $self->args->{manifestdb};
}

sub collectiondb {
    my $self = shift;
    return $self->args->{collectiondb};
}

sub internalmetadb {
    my $self = shift;
    return $self->args->{internalmetadb};
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

sub document {
    my $self = shift;
    return $self->{document};
}

sub flatten {
    my $self = shift;
    return $self->{flatten};
}

sub attachment {
    my $self = shift;
    return $self->{attachment};
}

# Top method
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

    $self->{document} =
      $self->manifestdb->get_document( uri_escape_utf8( $self->noid ) );
    die "Missing Manifest Document\n" if !( $self->document );

    if ( !exists $self->document->{'dmdType'} ) {
        die "Missing dmdType\n";
    }

    if ( !exists $self->document->{'slug'} ) {
        die "Missing slug\n";
    }

    my ( $depositor, $objid ) = split( /\./, $self->document->{'slug'} );

    my $object =
      $self->noid . '/dmd' . uc( $self->document->{'dmdType'} ) . '.xml';
    my $r = $self->swift->object_get( $self->access_metadata, $object );
    if ( $r->code != 200 ) {
        die( "Accessing $object returned code: " . $r->code . "\n" );
    }
    my $xmlrecord = $r->content;

## First attachment array element is the item

    # Fill in dmdSec information first
    $self->attachment->[0] = $self->flatten->byType(
        $self->document->{'dmdType'},
        utf8::is_utf8($xmlrecord)
        ? Encode::encode_utf8($xmlrecord)
        : $xmlrecord
    );
    undef $r;
    undef $xmlrecord;

    # Prefix depositor to parent key...
    $self->attachment->[0]->{'pkey'} =
      $depositor . "." . $self->attachment->[0]->{'pkey'};
    $self->attachment->[0]->{'depositor'}  = $depositor;
    $self->attachment->[0]->{'type'}       = 'document';
    $self->attachment->[0]->{'key'}        = $self->document->{'slug'};
    $self->attachment->[0]->{'identifier'} = [$objid];
    $self->attachment->[0]->{'label'} =
      $self->getIIIFText( $self->document->{'label'} );

    if ( exists $self->document->{'ocrPdf'} ) {
        $self->attachment->[0]->{'canonicalDownload'} =
          $self->document->{'ocrPdf'}->{'path'};
        $self->attachment->[0]->{'canonicalDownloadSize'} =
          $self->document->{'ocrPdf'}->{'size'};
    }

## All other attachment array elements are components

    # Grab the data from the old attachment
    my $hammerdata =
      $self->internalmetadb->get_aip(
        $self->document->{'slug'} . "/hammer.json" );

    die "Can't load internalmeta attachment\n" if ( !$hammerdata );

    # Testing
    delete $hammerdata->[0]->{'canonicalDownloadMime'};

    if ( ( keys %{ $hammerdata->[0] } ) != keys %{ $self->attachment->[0] } ) {
        print Dumper ( $hammerdata->[0], $self->attachment->[0] );

        die "Key length mismatch: "
          . encode_json( $hammerdata->[0] )
          . "          "
          . encode_json( $self->attachment->[0] ) . "\n";
    }

    my $success = 1;
    foreach my $key ( keys %{ $hammerdata->[0] } ) {
        my $hd = $hammerdata->[0]->{$key};
        if ( ref($hd) eq 'ARRAY' ) {
            $hd = encode_json( sort( @{$hd} ) );
        }

        my $at = $self->attachment->[0]->{$key};
        if ( ref($at) eq 'ARRAY' ) {
            $at = encode_json( sort( @{$at} ) );
        }

        if ( $hd ne $at ) {
            warn "Key:$key   $hd  ne $at\n";
            $success = 0;
        }
    }
    die "Not matched!\n" if ( !$success );
}

sub processCollection {
    my ($self) = @_;

    $self->{document} =
      $self->collectiondb->get_document( uri_escape_utf8( $self->noid ) );
    die "Missing Collection Document\n" if !( $self->document );

    die "Nothing here for Collections\n";
}

sub getIIIFText {
    my ( $self, $text ) = @_;

    foreach my $try ( "none", "en", "fr" ) {
        if ( exists $text->{$try} ) {
            return $text->{$try}->[0];
        }
    }
}

1;
