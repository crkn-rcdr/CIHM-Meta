package CIHM::Meta::Smelter;

use strict;
use Carp;
use Config::General;
use Log::Log4perl;

use CIHM::Meta::REST::dipstaging;
use CIHM::Meta::Smelter::Worker;

use Coro::Semaphore;
use AnyEvent::Fork;
use AnyEvent::Fork::Pool;

use Try::Tiny;
use JSON;
use Data::Dumper;

=head1 NAME

CIHM::Meta::Smelter - Extract metadata from CIHM repository and add to 2020+ design access platform


=head1 SYNOPSIS

    my $smelter = CIHM::Meta::Smelter->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is as used by Config::General

=cut

sub new {
    my ( $class, $args ) = @_;
    my $self = bless {}, $class;

    if ( ref($args) ne "HASH" ) {
        die "Argument to CIHM::Meta::Smelter->new() not a hash\n";
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

    # Undefined if no <internalmeta> config block
    if ( exists $confighash{dipstaging} ) {
        $self->{dipstaging} = new CIHM::Meta::REST::dipstaging(
            server      => $confighash{dipstaging}{server},
            database    => $confighash{dipstaging}{database},
            type        => 'application/json',
            conf        => $args->{configpath},
            clientattrs => { timeout => 3600 },
        );
    }
    else {
        croak "Missing <dipstaging> configuration block in config\n";
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

sub dipstaging {
    my $self = shift;
    return $self->{dipstaging};
}

sub smelter {
    my ($self) = @_;

    $self->log->info( "Smelter: conf="
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
      AnyEvent::Fork->new->require("CIHM::Meta::Smelter::Worker")
      ->AnyEvent::Fork::Pool::run(
        "CIHM::Meta::Smelter::Worker::smelt",
        max        => $self->maxprocs,
        load       => 2,
        on_destroy => ( my $cv_finish = AE::cv ),
      );

    # Semaphore keeps us from filling the queue with too many AIPs before
    # some are processed.
    my $sem = new Coro::Semaphore( $self->maxprocs * 2 );
    my $somework;

# Dimensions from filemeta
#TODO    my ($aip,$metspath,$manifestdate) = ("oocihm.8_04957_172", "data/sip/data/metadata.xml", "2017-04-21T02:08:40Z"); {
# Dimensions from mets
# my $next={aip => "ooga.NRCan_114", metspath => "data/sip/data/metadata.xml", manifestdate => "2017-04-26T14:00:32Z"}; {
# First AIP with ALTO XML data
# my $next={aip => "oocihm.lac_reel_h3040", metspath => "data/sip/data/metadata.xml", manifestdate => "2017-05-25T15:20:59Z"}; {
# No dimensions (mets or filemeta)
# my $next={aip => "ooga.NRCan_108", metspath => "data/sip/data/metadata.xml", manifestdate => "2017-04-26T14:00:32Z"}; {
# Has item PDF
#my $next={aip => "oop.debates_CDC2501_20", metspath => "data/sip/data/metadata.xml", manifestdate => "2013-08-28T12:07:47Z"}; {

    while ( my $next = $self->getNextAIP ) {
        $somework = 1;
        my $aip = $next->{aip};
        $self->{inprogress}->{$aip} = 1;
        $sem->down;
        $pool->(
            $aip,
            $self->configpath,
            sub {
                my $aip = shift;
                $sem->up;
                delete $self->{inprogress}->{$aip};
            }
        );
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

sub getNextAIP {
    my $self = shift;

    return if $self->endtime && time() > $self->endtime;

    my $skipparam = '';
    if ( $self->skip ) {
        $skipparam = "&skip=" . $self->skip;
    }

    $self->dipstaging->type("application/json");
    my $res = $self->dipstaging->get(
        "/"
          . $self->dipstaging->database
          . "/_design/sync/_view/smeltq?reduce=false&limit="
          . $self->limit
          . $skipparam,
        {},
        { deserializer => 'application/json' }
    );
    if ( $res->code == 200 ) {
        if ( exists $res->data->{rows} ) {
            foreach my $hr ( @{ $res->data->{rows} } ) {
                my $uid = $hr->{id};
                if ( !exists $self->{inprogress}->{$uid} ) {
                    return (
                        {
                            aip          => $uid,
                            metspath     => $hr->{value}->{path},
                            manifestdate => $hr->{value}->{manifestdate}
                        }
                    );
                }
            }
        }
    }
    else {
        warn "_view/smeltq GET return code: " . $res->code . "\n";
    }
    return;
}

1;
