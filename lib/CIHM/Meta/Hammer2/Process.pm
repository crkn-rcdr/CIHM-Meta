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
use CIHM::Meta::dmd::flatten qw(normaliseSpace);
use List::MoreUtils qw(uniq);

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

sub canvasdb {
    my $self = shift;
    return $self->args->{canvasdb};
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
        $self->{document} =
          $self->manifestdb->get_document( uri_escape_utf8( $self->noid ) );
        die "Missing Manifest Document\n" if !( $self->document );
    }
    else {    # This is a collection
        $self->{document} =
          $self->collectiondb->get_document( uri_escape_utf8( $self->noid ) );
        die "Missing Collection Document\n" if !( $self->document );

        if (   !( exists $self->document->{'ordered'} )
            || !( $self->document->{'ordered'} ) )
        {
            warn "Nothing to do for an unordered collection\n";
            return;
        }
    }

    if ( !exists $self->document->{'slug'} ) {

        # TODO: Look up noid in view and unapprove if it previously had a noid.
        if ( exists $self->document->{'public'} ) {

            # Noids without slugs shouldn't be public!
            warn "Noids without slugs shouldn't be marked public\n";
            return;
        }
        else {
            # If private, then this is a silent success
            return;
        }
    }
    my $slug = $self->document->{'slug'};

    if ( !exists $self->document->{'dmdType'} ) {
        die "Missing dmdType\n";
    }

    my ( $depositor, $objid ) = split( /\./, $slug );

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

    $self->attachment->[0]->{'depositor'} = $depositor;
    if ( $self->type eq "manifest" ) {
        $self->attachment->[0]->{'type'} = 'document';
    }
    else {
        $self->attachment->[0]->{'type'} = 'series';
    }
    $self->attachment->[0]->{'key'}  = $slug;
    $self->attachment->[0]->{'noid'} = $self->noid;

    my %identifier = ( $objid => 1 );
    if ( exists $self->attachment->[0]->{'identifier'} ) {
        foreach my $identifier ( @{ $self->attachment->[0]->{'identifier'} } ) {
            $identifier{$identifier} = 1;
        }
    }
    @{ $self->attachment->[0]->{'identifier'} } = keys %identifier;

    $self->attachment->[0]->{'label'} =
      $self->getIIIFText( $self->document->{'label'} );

    $self->attachment->[0]->{'label'} =~
      s/^\s+|\s+$//g;    # Trim spaces from beginning and end of label
    $self->attachment->[0]->{'label'} =~ s/\s+/ /g;    # Remove extra spaces

    if ( exists $self->document->{'ocrPdf'} ) {
        $self->attachment->[0]->{'canonicalDownload'} =
          $self->document->{'ocrPdf'}->{'path'};
        $self->attachment->[0]->{'canonicalDownloadSize'} =
          $self->document->{'ocrPdf'}->{'size'};
    }
    elsif ( exists $self->document->{'master'} ) {
        $self->attachment->[0]->{'canonicalDownload'} =
          $self->document->{'master'}->{'path'};
        $self->attachment->[0]->{'canonicalDownloadSize'} =
          $self->document->{'master'}->{'size'};
    }

## All other attachment array elements are components

    if ( $self->document->{'canvases'} ) {
        my @canvasids;
        foreach my $i ( 0 .. ( @{ $self->document->{'canvases'} } - 1 ) ) {
            push @canvasids, $self->document->{'canvases'}->[$i]->{'id'};
            $self->attachment->[ $i + 1 ]->{'noid'} =
              $self->document->{'canvases'}->[$i]->{'id'};
            $self->attachment->[ $i + 1 ]->{'label'} =
              $self->getIIIFText(
                $self->document->{'canvases'}->[$i]->{'label'} );
            $self->attachment->[ $i + 1 ]->{'type'}      = 'page';
            $self->attachment->[ $i + 1 ]->{'seq'}       = $i + 1;
            $self->attachment->[ $i + 1 ]->{'depositor'} = $depositor;
            $self->attachment->[ $i + 1 ]->{'identifier'} =
              [ $objid . "." . ( $i + 1 ) ];
            $self->attachment->[ $i + 1 ]->{'pkey'} = $slug;
            $self->attachment->[ $i + 1 ]->{'key'} = $slug . "." . ( $i + 1 );
        }
        my @canvases = @{ $self->canvasdb->get_documents( \@canvasids ) };
        die "Array length mismatch\n" if ( @canvases != @canvases );

        foreach my $i ( 0 .. ( @canvases - 1 ) ) {
            if ( defined $canvases[$i]{'master'} ) {
                my %master = %{ $canvases[$i]{'master'} };
                my %ocrPdf = %{ $canvases[$i]{'ocrPdf'} };

                $self->attachment->[ $i + 1 ]->{'canonicalMasterHeight'} =
                  $master{height}
                  if ( defined $master{height} );
                $self->attachment->[ $i + 1 ]->{'canonicalMasterWidth'} =
                  $master{width}
                  if ( defined $master{width} );
                $self->attachment->[ $i + 1 ]->{'canonicalMaster'} =
                  $master{path}
                  if ( defined $master{path} );
                $self->attachment->[ $i + 1 ]->{'canonicalMasterSize'} =
                  $master{size}
                  if ( defined $master{size} );
                $self->attachment->[ $i + 1 ]->{'canonicalMasterMime'} =
                  $master{mime}
                  if ( defined $master{mime} );
                $self->attachment->[ $i + 1 ]->{'canonicalDownload'} =
                  $ocrPdf{path}
                  if ( defined $ocrPdf{path} );
            }

            if ( defined $canvases[$i]{'ocrType'} ) {
                my $noid = $self->attachment->[ $i + 1 ]->{'noid'};
                my $object =
                  $noid . '/ocr' . uc( $canvases[$i]{'ocrType'} ) . '.xml';
                my $r =
                  $self->swift->object_get( $self->access_metadata, $object );
                if ( $r->code != 200 ) {
                    die(
                        "Accessing $object returned code: " . $r->code . "\n" );
                }
                my $xmlrecord = $r->content;

                # Add Namespace if missing
                $xmlrecord =~
s|<txt:txtmap>|<txtmap xmlns:txt="http://canadiana.ca/schema/2012/xsd/txtmap">|g;
                $xmlrecord =~ s|</txt:txtmap>|</txtmap>|g;

                my $ocr;
                my $xml = XML::LibXML->new->parse_string($xmlrecord);
                my $xpc = XML::LibXML::XPathContext->new($xml);
                $xpc->registerNs( 'txt',
                    'http://canadiana.ca/schema/2012/xsd/txtmap' );
                $xpc->registerNs( 'alto',
                    'http://www.loc.gov/standards/alto/ns-v3' );
                if (   $xpc->exists( '//txt:txtmap', $xml )
                    || $xpc->exists( '//txtmap', $xml ) )
                {
                    $ocr = $xml->textContent;
                }
                elsif (
                       $xpc->exists( '//alto', $xml )
                    || $xpc->exists('//alto:alto'),
                    $xml
                  )
                {
                    $ocr = '';
                    foreach
                      my $content ( $xpc->findnodes( '//*[@CONTENT]', $xml ) )
                    {
                        $ocr .= " " . $content->getAttribute('CONTENT');
                    }
                }
                else {
                    die "Unknown XML schema for noid=$noid\n";
                }
                $self->attachment->[ $i + 1 ]->{'tx'} = [ normaliseSpace($ocr) ]
                  if $ocr;
            }
        }
    }

    # Schema doesn't allow both 'canvases' (Scanned images)
    # and 'masterPages' (Born Digital PDF)
    if ( $self->document->{'masterPages'} ) {
        foreach my $i ( 0 .. ( @{ $self->document->{'masterPages'} } - 1 ) ) {
            $self->attachment->[ $i + 1 ]->{'label'} =
              $self->getIIIFText( $self->document->{'masterPages'}->[$i] );
            $self->attachment->[ $i + 1 ]->{'type'}      = 'page';
            $self->attachment->[ $i + 1 ]->{'seq'}       = $i + 1;
            $self->attachment->[ $i + 1 ]->{'depositor'} = $depositor;
            $self->attachment->[ $i + 1 ]->{'identifier'} =
              [ $objid . "." . ( $i + 1 ) ];
            $self->attachment->[ $i + 1 ]->{'pkey'} = $slug;
            $self->attachment->[ $i + 1 ]->{'key'} = $slug . "." . ( $i + 1 );
        }
    }

## Build update document and attachment

    $self->updatedoc->{'type'} = 'aip';

    # Manifest is a 'document', ordered collection is a 'series'
    $self->updatedoc->{'sub-type'} = $self->attachment->[0]->{'type'};

# If not public, then not approved in old system (clean up cosearch/copresentation docs)
    if ( exists $self->document->{'public'} ) {
        $self->updatedoc->{'approved'} = JSON::true;
    }
    else {
        $self->updatedoc->{'approved'} = JSON::false;

    }

    # We may not care about these any more, but will decide later...
    foreach my $field ( 'label', 'pubmin', 'pubmax', 'canonicalDownload' ) {
        if ( defined $self->attachment->[0]->{$field} ) {
            $self->updatedoc->{$field} = $self->attachment->[0]->{$field};
        }
    }

## Determine what collections this manifest or collection is in
    $self->{collections}        = {};
    $self->{orderedcollections} = {};

    $self->findCollections( $self->noid );

    # Ignore parent key from issueinfo records.
    # Concept of 'parent' going away as part of retiring 'issueinfo' records.
    delete $self->attachment->[0]->{'pkey'};
    my @parents = keys %{ $self->{orderedcollections} };
    if (@parents) {
        if ( @parents != 1 ) {
            warn "A member of more than a single ordered collection\n";
        }
        my $parent = shift @parents;
        if ($parent) {

            # Old platform didn't include 'series' records in collections.
            delete $self->{collections}->{$parent};
            $self->attachment->[0]->{'pkey'} = $parent;
            $self->updatedoc->{'parent'} = $parent;
        }
    }

    # Always set collection -- will be '' if no collections.
    $self->updatedoc->{collectionseq} =
      join( ',', keys %{ $self->{collections} } );


    # Create document if it doesn't already exist
    $self->internalmetadb->update_basic_full( $slug, {} );

    my $return = $self->internalmetadb->put_attachment(
        $slug,
        {
            type      => "application/json",
            content   => encode_json $self->attachment,
            filename  => "hammer.json",
            updatedoc => $self->updatedoc
        }
    );
    if ( $return != 201 ) {
        die "Return code $return for internalmetadb->put_attachment($slug)\n";
    }
}

sub findCollections {
    my ( $self, $noid ) = @_;

    foreach my $collection ( @{ $self->collectiondb->getCollections($noid) } ) {

        if (   exists $collection->{value}
            && exists $collection->{value}->{slug} )
        {
            my $slug = $collection->{value}->{slug};
            if ( !exists $self->{collections}->{$slug} ) {
                $self->{collections}->{$slug} = 1;

# TODO: Just for checking match with old                $self->findCollections( $collection->{'id'} );
            }
            if ( $collection->{value}->{ordered} ) {
                $self->{orderedcollections}->{$slug} = 1;
            }
        }
    }
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
