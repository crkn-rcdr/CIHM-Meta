package CIHM::Meta::Hammer2;

use strict;
use Carp;
use Config::General;
use Log::Log4perl;

use CIHM::Meta::REST::manifest;
use CIHM::Meta::REST::collection;
use CIHM::Meta::Hammer2::Worker;

use Coro::Semaphore;
use AnyEvent::Fork;
use AnyEvent::Fork::Pool;

use Try::Tiny;
use JSON;
use Data::Dumper;

=head1 NAME

CIHM::Meta::Hammer2 - Normalize metadata from new access platform databases and file store, and post to "internalmeta2"


=head1 SYNOPSIS

    my $hammer = CIHM::Meta::Hammer2->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is as used by Config::General

=cut

sub new {
    my ( $class, $args ) = @_;
    my $self = bless {}, $class;

    if ( ref($args) ne "HASH" ) {
        die "Argument to CIHM::Meta::Hammer2->new() not a hash\n";
    }
    $self->{args} = $args;

    $self->{skip} = delete $args->{skip};

    $self->{maxprocs} = delete $args->{maxprocs};
    if ( !$self->{maxprocs} ) {
        $self->{maxprocs} = 3;
    }

    # Set up for time limit
    $self->{timelimit} = delete $args->{timelimit};
    if ( $self->{timelimit} ) {
        $self->{endtime} = time() + $self->{timelimit};
    }

    # Set up in-progress hash (Used to determine which AIPs which are being
    # processed by a slave so we don't try to do the same AIP twice.
    $self->{inprogress} = {};

    $self->{limit} = delete $args->{limit};
    if ( !$self->{limit} ) {
        $self->{limit} = ( $self->{maxprocs} ) * 2 + 1;
    }

    Log::Log4perl->init_once("/etc/canadiana/tdr/log4perl.conf");
    $self->{logger} = Log::Log4perl::get_logger("CIHM::TDR");

    my %confighash =
      new Config::General( -ConfigFile => $args->{configpath}, )->getall;

    if ( exists $confighash{manifest} ) {
        $self->{manifestdb} = new CIHM::Meta::REST::manifest(
            server      => $confighash{manifest}{server},
            database    => $confighash{manifest}{database},
            type        => 'application/json',
            conf        => $args->{configpath},
            clientattrs => { timeout => 3600 },
        );
    }
    else {
        croak "Missing <manifest> configuration block in config\n";
    }

    if ( exists $confighash{collection} ) {
        $self->{collectiondb} = new CIHM::Meta::REST::collection(
            server      => $confighash{collection}{server},
            database    => $confighash{collection}{database},
            type        => 'application/json',
            conf        => $args->{configpath},
            clientattrs => { timeout => 3600 },
        );
    }
    else {
        croak "Missing <collection> configuration block in config\n";
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

sub skip {
    my $self = shift;
    return $self->{skip};
}

sub maxprocs {
    my $self = shift;
    return $self->{maxprocs};
}

sub limit {
    my $self = shift;
    return $self->{limit};
}

sub endtime {
    my $self = shift;
    return $self->{endtime};
}

sub log {
    my $self = shift;
    return $self->{logger};
}

sub manifestdb {
    my $self = shift;
    return $self->{manifestdb};
}

sub collectiondb {
    my $self = shift;
    return $self->{collectiondb};
}

sub hammer {
    my ($self) = @_;

    $self->log->info( "Hammer2 time: conf="
          . $self->configpath
          . " skip="
          . $self->skip
          . " limit="
          . $self->limit
          . " maxprocs="
          . $self->maxprocs
          . " timelimit="
          . $self->{timelimit} );

    my $pool =
      AnyEvent::Fork->new->require("CIHM::Meta::Hammer2::Worker")
      ->AnyEvent::Fork::Pool::run(
        "CIHM::Meta::Hammer2::Worker::swing",
        max        => $self->maxprocs,
        load       => 2,
        on_destroy => ( my $cv_finish = AE::cv ),
      );

    # Semaphore keeps us from filling the queue with too many AIPs before
    # some are processed.
    my $sem = new Coro::Semaphore( $self->maxprocs * 2 );
    my $somework;

    my %dblist = (
        "manifest" => $self->manifestdb,

        #        "collection" => $self->collectiondb
    );

    foreach my $type ( keys %dblist ) {
        my $thisdb = $dblist{$type};
        while ( my $noid = $self->getNextTestNOID($thisdb) ) {
            $somework = 1;
            $self->{inprogress}->{$noid} = 1;
            $sem->down;
            $pool->(
                $noid, $type,
                $self->configpath,
                sub {
                    my $noid = shift;
                    $sem->up;
                    delete $self->{inprogress}->{$noid};
                }
            );
        }
    }
    undef $pool;
    if ($somework) {
        $self->log->info("Waiting for child processes to finish");
    }
    $cv_finish->recv;
    if ($somework) {
        $self->log->info("Finished.");
    }
}

sub getNextTestNOID {
    my ( $self, $thisdb ) = @_;

    if ( !exists $self->{testnoids} ) {

        $thisdb->type("application/json");
        my $url = "/"
          . $thisdb->database
          . "/_design/metadatabus/_view/dmdType?reduce=false&descending=true&key=\"issueinfo\"";

        my $res =
          $thisdb->get( $url, {}, { deserializer => 'application/json' } );
        if ( $res->code == 200 ) {
            if ( exists $res->data->{rows} ) {
                $self->{testnoids} = [];
                foreach my $hr ( @{ $res->data->{rows} } ) {
                    my $noid = $hr->{id};
                    push @{ $self->{testnoids} }, $noid;
                }

                print Dumper ( $self->{testnoids} );
            }
        }
        else {
            warn $url . " GET return code: " . $res->code . "\n";
        }
    }

    if ( exists $self->{testnoids} ) {
        return pop @{ $self->{testnoids} };
    }
    return;
}

sub getNextNOID {
    my ( $self, $thisdb ) = @_;

    return if $self->endtime && time() > $self->endtime;

    my $skipparam = '';
    if ( $self->skip ) {
        $skipparam = "&skip=" . $self->skip;
    }

    $thisdb->type("application/json");
    my $res = $thisdb->get(
        "/"
          . $thisdb->database
          . "/_design/metadatabus/_view/updateinternalmetaq?reduce=false&descending=true&limit="
          . $self->limit
          . $skipparam,
        {},
        { deserializer => 'application/json' }
    );
    if ( $res->code == 200 ) {
        if ( exists $res->data->{rows} ) {
            foreach my $hr ( @{ $res->data->{rows} } ) {
                my $noid = $hr->{id};
                if ( !exists $self->{inprogress}->{$noid} ) {
                    return $noid;
                }
            }
        }
    }
    else {
        warn "_view/updateinternalmetaq on "
          . $thisdb->database
          . " GET return code: "
          . $res->code . "\n";
    }
    return;
}

1;
