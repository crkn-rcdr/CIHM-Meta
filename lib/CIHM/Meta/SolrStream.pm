package CIHM::Meta::SolrStream;

use strict;
use Carp;
use Config::General;
use Log::Log4perl;
use CIHM::Meta::REST::cosearch;
use Role::REST::Client;
use Try::Tiny;
use Data::Dumper;
use JSON;

=head1 NAME

CIHM::Meta::SolrStream - Stream cosearch from CouchDB to Solr.

=head1 SYNOPSIS

    my $solr = CIHM::Meta::SolrStream->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is as used by Config::General
      $args->{localdocument} is the couchdb local document the past sequence number is saved into and read from next iteration.
      $args->{since} is a numeric sequence number

      
=cut

{

    package CIHM::Meta::SolrStream::Solr;

    use Moo;
    with 'Role::REST::Client';

}

sub new {
    my ( $class, $args ) = @_;
    my $self = bless {}, $class;

    if ( ref($args) ne "HASH" ) {
        die "Argument to CIHM::Meta::SolrStream->new() not a hash\n";
    }
    $self->{args} = $args;

    Log::Log4perl->init_once("/etc/canadiana/tdr/log4perl.conf");
    $self->{logger} = Log::Log4perl::get_logger("CIHM::TDR");

    my %confighash =
      new Config::General( -ConfigFile => $self->configpath, )->getall;

    # Undefined if no <cosearch> config block
    if ( exists $confighash{cosearch} ) {
        $self->{cosearch} = new CIHM::Meta::REST::cosearch(
            server      => $confighash{cosearch}{server},
            database    => $confighash{cosearch}{database},
            type        => 'application/json',
            conf        => $self->configpath,
            clientattrs => { timeout => 3600 }
        );
    }
    else {
        croak "Missing <cosearch> configuration block in config\n";
    }

    # Undefined if no <cosolr> config block
    if ( exists $confighash{cosolr} ) {
        $self->{cosolr} = new CIHM::Meta::SolrStream::Solr(
            server      => $confighash{cosolr}{server},
            type        => 'application/json',
            clientattrs => { timeout => 3600 }
        );
        $self->{cosolrdb} = $confighash{cosolr}{database};
    }
    else {
        croak "Missing <cosolr> configuration block in config\n";
    }
    return $self;
}

# Simple accessors for now -- Do I want to Moo?
sub args {
    my $self = shift;
    return $self->{args};
}

sub configpath {
    my $self = shift;
    return $self->{args}->{configpath};
}

sub since {
    my $self = shift;
    return $self->{args}->{since};
}

sub limit {
    my $self = shift;
    return $self->{args}->{limit};
}

sub localdocument {
    my $self = shift;
    return $self->{args}->{localdocument};
}

sub log {
    my $self = shift;
    return $self->{logger};
}

sub cosearch {
    my $self = shift;
    return $self->{cosearch};
}

sub cosolr {
    my $self = shift;
    return $self->{cosolr};
}

sub cosolrdb {
    my $self = shift;
    return $self->{cosolrdb};
}

sub process {
    my ($self) = shift;

    my $since = $self->getSince();
    $since = $self->since if defined $self->since;
    if ( !defined $since ) {
        $since = 0;
    }

    $self->log->info( "conf=" . $self->configpath . " since=" . $since );

    my $startseq = $since;
    while (1) {
        my ( $lastseq, $stream ) = $self->getNextStream($startseq);
        last if !$lastseq;

        my $poststream   = [];
        my $deletestream = [];

        foreach my $doc ( @{$stream} ) {
            next if substr( $doc->{id}, 0, 1 ) eq '_';

            if ( $doc->{deleted} ) {
                push @{$deletestream}, $doc->{id};
            }
            else {
                delete $doc->{doc}->{'_rev'};
                delete $doc->{doc}->{'_id'};
                push @{$poststream}, $doc->{doc};
            }
        }
        if ( scalar @{$deletestream} ) {
            $self->postSolrStream( { delete => $deletestream }, $startseq );
        }
        if ( scalar @{$poststream} ) {
            $self->postSolrStream( $poststream, $startseq );
        }
        $self->putSince($lastseq);
        $self->log->info("seq=$lastseq");
        $startseq = $lastseq;
    }

    # Only commit if we have made changes...
    if ( $startseq != $since ) {
        $self->postSolrStream( { commit => {} }, $startseq );
    }
}

sub getNextStream {
    my ( $self, $since ) = @_;

    $self->cosearch->type("application/json");
    my $res = $self->cosearch->get(
        "/"
          . $self->cosearch->database
          . "/_changes?include_docs=true&since=$since&limit="
          . $self->limit,
        {},
        { deserializer => 'application/json' }
    );
    if ( $res->code == 200 ) {
        if ( exists $res->data->{results}
            && scalar( @{ $res->data->{results} } ) )
        {
            return ( $res->data->{last_seq}, $res->data->{results} );
        }
    }
    else {
        die "_changes GET return code: " . $res->code . "\n";
    }
}

sub postSolrStream {
    my ( $self, $stream, $startseq ) = @_;

    $self->cosolr->type("application/json");
    my $res =
      $self->cosolr->post( "solr/" . $self->cosolrdb . "/update", $stream );
    if ( $res->code != 201 && $res->code != 200 ) {
        warn "postSolrStream($startseq) return code: " . $res->code . "\n";
    }
    return;
}

sub getSince {
    my ($self) = @_;

    my $since;
    $self->cosearch->type("application/json");
    my $res = $self->cosearch->get(
        "/" . $self->cosearch->{database} . "/_local/" . $self->localdocument,
        {}, { deserializer => 'application/json' } );
    if ( $res->code == 200 ) {
        $self->{localdocrev} = $res->data->{"_rev"};
        $since = $res->data->{"since"};
    }
    return $since;
}

sub putSince {
    my ( $self, $since ) = @_;

    my $newdoc = { since => $since };
    if ( $self->{localdocrev} ) {
        $newdoc->{"_rev"} = $self->{localdocrev};
    }

    $self->cosearch->type("application/json");
    my $res = $self->cosearch->put(
        "/" . $self->cosearch->{database} . "/_local/" . $self->localdocument,
        $newdoc );
    if ( $res->code != 201 && $res->code != 200 ) {
        warn "_local/"
          . $self->localdocument
          . " PUT return code: "
          . $res->code . "\n";
    }
    else {
        $self->{localdocrev} = $res->data->{"rev"};
    }
    return;
}

1;
