package CIHM::Meta::dmd::flatten;

use strict;
use Switch;
use XML::LibXML;
use CIHM::Normalise;
use Data::Dumper;

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
        $content =~ s/^\s+|\s+$//g;    # Trim space at end and beginning.
        $content =~ s/\s+/ /g;         # Remove extra spaces

        if ( length($content) ) {
            switch ( lc( $node->nodeName ) ) {
                case "issueinfo" {     #  Skip top level
                }
                case "published" {
                    my $pubmin;
                    my $pubmax;
                    switch ( length($content) ) {
                        case 4 {
                            $pubmin = $content . "-01-01";
                            $pubmax = $content . "-12-31";
                        }
                        case 7 {
                            $pubmin = $content . "-01";
                            switch ( int( substr $content, 5 ) ) {
                                case [2] {
                                    $pubmax = $content . "-28";
                                }
                                case [ 1, 3, 5, 7, 8, 10, 12 ] {
                                    $pubmax = $content . "-31";
                                }
                                case [ 4, 6, 9, 11 ] {
                                    $pubmax = $content . "-30";
                                }
                            }
                        }
                        case 10 {
                            $pubmin = $content;
                            $pubmax = $content;
                        }
                    }
                    if ($pubmin) {
                        $pubmin = iso8601( $pubmin, 0 )
                          unless ( $pubmin =~
                            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/ );
                    }
                    if ($pubmax) {
                        $pubmax = iso8601( $pubmax, 1 )
                          unless ( $pubmax =~
                            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/ );
                    }
                    if ( $pubmin && $pubmin !~ /^0000/ ) {
                        $flat{'pubmin'} = $pubmin;
                    }
                    if ( $pubmax && $pubmax !~ /^0000/ ) {
                        $flat{'pubmax'} = $pubmax;
                    }
                }
                case "series" {
                    $flat{'pkey'} = $content;
                }
                case "sequence" {
                    $flat{'seq'} = $content
                }
                case "title" {
                    $content =~ s/-+$//g;     # Trim dashes
                    $content =~ s/\/+$//g;    # Trim odd slashes
                    $content =~
                      s/^\s+|\s+$//g;  # Trim space at end and beginning in case

                    if ( !exists $flat{'ti'} ) {
                        $flat{'ti'} = [];
                    }
                    push @{ $flat{'ti'} }, $content;
                }
                case "language" {
                    my @lang = normalise_lang($content);
                    if (@lang) {
                        if ( !exists $flat{'lang'} ) {
                            $flat{'lang'} = [];
                        }
                        push @{ $flat{'lang'} }, @lang;
                    }
                }
                case "note" {
                    if ( !exists $flat{'no'} ) {
                        $flat{'no'} = [];
                    }
                    push @{ $flat{'no'} }, $content;
                }
                case "source" {
                    if ( !exists $flat{'no_source'} ) {
                        $flat{'no_source'} = [];
                    }
                    push @{ $flat{'no_source'} }, $content;
                }
                case "pubstatement" {
                    if ( !exists $flat{'pu'} ) {
                        $flat{'pu'} = [];
                    }
                    push @{ $flat{'pu'} }, $content;
                }
                case "identifier" {
                    if ( !exists $flat{'identifier'} ) {
                        $flat{'identifier'} = [];
                    }
                    push @{ $flat{'identifier'} }, $content;
                }
                case "coverage" {

                    # TODO: We aren't using Coverage?
                }
                else {
                    warn "Unknown issueinfo node name: "
                      . $node->nodeName . "\n";
                }
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

    my %flat;
    my @dates;

    # Add Namespace if missing
    $xmlin =~
      s|<simpledc>|<simpledc xmlns:dc="http://purl.org/dc/elements/1.1/">|g;

    my $xml = XML::LibXML->new->parse_string($xmlin);
    my $xpc = XML::LibXML::XPathContext->new;
    $xpc->registerNs( 'dc', 'http://purl.org/dc/elements/1.1/' );

    my @nodes = $xpc->findnodes( "//*", $xml );
    foreach my $node (@nodes) {
        my $content = $node->textContent;
        $content =~ s/^\s+|\s+$//g;    # Trim space at end and beginning.
        $content =~ s/\s+/ /g;         # Remove extra spaces

        if ( length($content) ) {
            my $nodename = lc( $node->nodeName );
            $nodename =~ s|dc:||g;     # Strip namespace if it exists

            switch ($nodename) {
                case "simpledc" {      #  Skip top level
                }
                case "date" {
                    push @dates, $content;
                }

                case "language" {
                    my @lang = normalise_lang($content);
                    if (@lang) {
                        if ( !exists $flat{'lang'} ) {
                            $flat{'lang'} = [];
                        }
                        push @{ $flat{'lang'} }, @lang;
                    }
                }
                case "creator" {
                    if ( !exists $flat{'au'} ) {
                        $flat{'au'} = [];
                    }
                    push @{ $flat{'au'} }, $content;
                }
                case "description" {
                    if ( !exists $flat{'ab'} ) {
                        $flat{'ab'} = [];
                    }
                    push @{ $flat{'ab'} }, $content;
                }
                case "identifier" {
                    if ( !exists $flat{'identifier'} ) {
                        $flat{'identifier'} = [];
                    }
                    push @{ $flat{'identifier'} }, $content;
                }
                case "publisher" {
                    if ( !exists $flat{'pu'} ) {
                        $flat{'pu'} = [];
                    }
                    push @{ $flat{'pu'} }, $content;
                }
                case [ "source", "contributor" ] {
                    if ( !exists $flat{'no_source'} ) {
                        $flat{'no_source'} = [];
                    }
                    push @{ $flat{'no_source'} }, $content;
                }
                case "subject" {
                    if ( !exists $flat{'su'} ) {
                        $flat{'su'} = [];
                    }
                    push @{ $flat{'su'} }, $content;
                }
                case "title" {
#                    $content =~ s/\-\-$//g;     # Trim double
#                    $content =~ s/\/+$//g;    # Trim odd slashes
#                    $content =~
#                      s/^\s+|\s+$//g;  # Trim space at end and beginning in case

                    if ( !exists $flat{'ti'} ) {
                        $flat{'ti'} = [];
                    }
                    push @{ $flat{'ti'} }, $content;
                }
                case "type" {
                    if ( !exists $flat{'no'} ) {
                        $flat{'no'} = [];
                    }
                    push @{ $flat{'no'} }, $content;
                }
                case "format" {    #Not used?
                }
                else {
                    warn "Unknown Dublin Core node name: "
                      . $node->nodeName . "\n";
                }
            }
        }
    }

  # TODO: This is what we have been doing, but should be doing something better.
    if (@dates) {
        if ( int(@dates) == 1 ) {
            $flat{'pubmin'} = iso8601( $dates[0], 0 );
            $flat{'pubmax'} = iso8601( $dates[0], 1 );
        }
        else {
            $flat{'pubmin'} = iso8601( $dates[0], 0 );
            $flat{'pubmax'} = iso8601( $dates[1], 1 );
        }

        # Currently if either date was unreable, it was left blank.
        if ( !( $flat{'pubmin'} ) || !( $flat{'pubmax'} ) ) {
            delete $flat{'pubmin'};
            delete $flat{'pubmax'};
        }
    }

    return \%flat;
}

1;
