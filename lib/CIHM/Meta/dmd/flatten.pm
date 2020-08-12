package CIHM::Meta::dmd::flatten;

use strict;
use Switch;
use CIHM::METS::parse;    # Borrow some things until we replace it.
use XML::LibXML;

sub new {
    my ( $class, $args ) = @_;
    my $self = bless {}, $class;

    return $self;
}

sub byType {
    my ( $self, $type, $xmlin ) = @_;

    switch ( lc($type) ) {
        case "issueinfo" { return $self->issueinfo($xmlin) }
        case "marc"      { return $self->marc($xmlin) }
        case "dc"        { return $self->dc($xmlin) }
        else             { die "Unknown DMD type: $type\n" }
    }

}

sub issueinfo {
    my ( $self, $xmlin ) = @_;

    my %flat;
    my $xml = XML::LibXML->new->parse_string($xmlin);
    my $xpc = XML::LibXML::XPathContext->new;
    $xpc->registerNs( 'issueinfo',
        "http://canadiana.ca/schema/2012/xsd/issueinfo" );

    my @nodes = $xpc->findnodes( "//*", $xml );
    foreach my $node (@nodes) {
        my $content = $node->textContent;
        switch ( lc( $node->nodeName ) ) {
            case "issueinfo" {    #  Skip top level
            }
            case "published" {
                # TODO: min and max is interesting...
                $flat{'pubmin'} = $content;
                $flat{'pubmax'} = $content;
            }
            case "series" {
                $flat{'pkey'} = $content;
            }
            case "sequence" {
                $flat{'seq'} = $content
            }
            case "title" {
                if ( !exists $flat{'ti'} ) {
                    $flat{'ti'} = [];
                }
                push @{ $flat{'ti'} }, $content;
            }
            case [ "pubstatement", "source", "note", "identifier" ] {
                # cmr:description -- Unused? TODO
            } else {
                warn "Unknown issueinfo node name: ".$node->nodeName."\n";
            }
        }
    }

    return \%flat;
}

sub marc {
    my ( $self, $xmlin ) = @_;

    die "MARC not yet supported\n";
}

sub dc {
    my ( $self, $xmlin ) = @_;

    die "DC not yet supported\n";
}

1;
