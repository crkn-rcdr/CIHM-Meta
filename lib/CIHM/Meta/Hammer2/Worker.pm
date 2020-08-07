package CIHM::Meta::Hammer2::Worker;

use strict;
use Carp;
use AnyEvent;
use Try::Tiny;
use JSON;
use Config::General;
use Log::Log4perl;

use CIHM::Swift::Client;
use CIHM::Meta::REST::cantaloupe;
use CIHM::Meta::REST::manifest;
use CIHM::Meta::REST::collection;
use CIHM::Meta::Hammer2::Process;

our $self;

sub initworker {
    my $configpath = shift;
    our $self;

    AE::log debug => "Initworker ($$): $configpath";

    $self = bless {};

    Log::Log4perl->init_once("/etc/canadiana/tdr/log4perl.conf");
    $self->{logger} = Log::Log4perl::get_logger("CIHM::TDR");

    my %confighash = new Config::General( -ConfigFile => $configpath, )->getall;

    # Undefined if no <cantaloupe> config block
    if ( exists $confighash{cantaloupe} ) {
        $self->{cantaloupe} = new CIHM::Meta::REST::cantaloupe(
            url         => $confighash{cantaloupe}{url},
            key         => $confighash{cantaloupe}{key},
            password    => $confighash{cantaloupe}{password},
            jwt_payload => '{"uids":[".*"]}',
            type        => 'application/json',
            conf        => $configpath,
            clientattrs => { timeout => 3600 },
        );
    }
    else {
        croak "Missing <cantaloupe> configuration block in config\n";
    }

    # Undefined if no <manifest> config block
    if ( exists $confighash{manifest} ) {
        $self->{manifestdb} = new CIHM::Meta::REST::manifest(
            server      => $confighash{manifest}{server},
            database    => $confighash{manifest}{database},
            type        => 'application/json',
            conf        => $configpath,
            clientattrs => { timeout => 3600 },
        );
    }
    else {
        croak "Missing <manifest> configuration block in config\n";
    }

    # Undefined if no <collection> config block
    if ( exists $confighash{collection} ) {
        $self->{collectiondb} = new CIHM::Meta::REST::collection(
            server      => $confighash{collection}{server},
            database    => $confighash{collection}{database},
            type        => 'application/json',
            conf        => $configpath,
            clientattrs => { timeout => 3600 },
        );
    }
    else {
        croak "Missing <collection> configuration block in config\n";
    }

    # Undefined if no <swift> config block
    if ( exists $confighash{swift} ) {
        my %swiftopt = ( furl_options => { timeout => 120 } );
        foreach ( "server", "user", "password", "account", "furl_options" ) {
            if ( exists $confighash{swift}{$_} ) {
                $swiftopt{$_} = $confighash{swift}{$_};
            }
        }
        $self->{swift}     = CIHM::Swift::Client->new(%swiftopt);
        $self->{container} = $confighash{swift}{container};
    }
    else {
        croak "No <swift> configuration block in " . $self->configpath . "\n";
    }
}

# Simple accessors for now -- Do I want to Moo?
sub log {
    my $self = shift;
    return $self->{logger};
}

sub swift {
    my $self = shift;
    return $self->{swift};
}

sub container {
    my $self = shift;
    return $self->{container};
}

sub cantaloupe {
    my $self = shift;
    return $self->{cantaloupe};
}

sub manifestdb {
    my $self = shift;
    return $self->{manifestdb};
}

sub collectiondb {
    my $self = shift;
    return $self->{collectiondb};
}

sub warnings {
    my $warning = shift;
    our $self;
    my $noid = "unknown";

    # Strip wide characters before  trying to log
    ( my $stripped = $warning ) =~ s/[^\x00-\x7f]//g;

    if ($self) {
        $self->{message} .= $warning;
        $noid = $self->{noid};
        $self->log->warn( $noid . ": $stripped" );
    }
    else {
        say STDERR "$warning\n";
    }
}

sub swing {
    my ( $noid, $type, $configpath ) = @_;
    our $self;

    # Capture warnings
    local $SIG{__WARN__} = sub { &warnings };

    if ( !$self ) {
        initworker($configpath);
    }

    # Debugging: http://lists.schmorp.de/pipermail/anyevent/2017q2/000870.html
    #  $SIG{CHLD} = 'IGNORE';

    $self->{noid}    = $noid;
    $self->{message} = '';

    $self->log->info("Processing $noid");

    AE::log debug => "$noid Before ($$)";

    my $status;

    # Handle and record any errors
    try {
        $status = 1;
        new CIHM::Meta::Hammer2::Process(
            {
                noid           => $noid,
                type           => $type,
                log            => $self->log,
                swift          => $self->swift,
                swiftcontainer => $self->container,
                cantaloupe     => $self->cantaloupe,
                manifestdb     => $self->manifestdb,
                collectiondb   => $self->collectiondb,
            }
        )->process;
    }
    catch {
        $status = 0;
        $self->log->error("$noid: $_");
        $self->{message} .= "Caught: " . $_;
    };
    $self->postResults( $noid, $type, $status, $self->{message} );

    AE::log debug => "$noid After ($$)";

    return ($noid);
}

sub postResults {
    my ( $self, $noid, $type, $status, $message ) = @_;

    my $thisdb;
    if ( $type eq "manifest" ) {
        $thisdb = $self->manifestdb;
    }
    else {
        $thisdb = $self->collectiondb;
    }
    $thisdb->update_basic(
        $noid,
        {
            "updateInternalmeta" => encode_json(
                {
                    "succeeded" => $status,
                    "message"   => $message,
                }
            )
        }
    );
}

1;
