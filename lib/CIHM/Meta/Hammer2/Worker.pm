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
use CIHM::Meta::REST::access;
use CIHM::Meta::REST::canvas;
use CIHM::Meta::REST::internalmeta;
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
            jwt_secret  => $confighash{cantaloupe}{password},
            jwt_payload => '{"uids":[".*"]}',
            type        => 'application/json',
            conf        => $configpath,
            clientattrs => { timeout => 3600 },
        );
    }
    else {
        croak "Missing <cantaloupe> configuration block in config\n";
    }

    # Undefined if no <access> config block
    if ( exists $confighash{access} ) {
        $self->{accessdb} = new CIHM::Meta::REST::access(
            server      => $confighash{access}{server},
            database    => $confighash{access}{database},
            type        => 'application/json',
            conf        => $configpath,
            clientattrs => { timeout => 3600 },
        );
    }
    else {
        croak "Missing <access> configuration block in config\n";
    }

    # Undefined if no <canvas> config block
    if ( exists $confighash{canvas} ) {
        $self->{canvasdb} = new CIHM::Meta::REST::canvas(
            server      => $confighash{canvas}{server},
            database    => $confighash{canvas}{database},
            type        => 'application/json',
            conf        => $configpath,
            clientattrs => { timeout => 3600 },
        );
    }
    else {
        croak "Missing <canvas> configuration block in config\n";
    }

    # Undefined if no <swift> config block
    if ( exists $confighash{swift} ) {
        my %swiftopt = ( furl_options => { timeout => 120 } );
        foreach ( "server", "user", "password", "account", "furl_options" ) {
            if ( exists $confighash{swift}{$_} ) {
                $swiftopt{$_} = $confighash{swift}{$_};
            }
        }
        $self->{swift}              = CIHM::Swift::Client->new(%swiftopt);
        $self->{preservation_files} = $confighash{swift}{container};
        $self->{access_metadata}    = $confighash{swift}{access_metadata};
        $self->{access_files}       = $confighash{swift}{access_files};
    }
    else {
        croak "No <swift> configuration block in " . $self->configpath . "\n";
    }

    # Undefined if no <internalmeta2> config block
    if ( exists $confighash{internalmeta2} ) {
        $self->{internalmetadb} = new CIHM::Meta::REST::internalmeta(
            server      => $confighash{internalmeta2}{server},
            database    => $confighash{internalmeta2}{database},
            type        => 'application/json',
            conf        => $configpath,
            clientattrs => { timeout => 3600 },
        );
    }
    else {
        croak "Missing <internalmeta2> configuration block in config\n";
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

sub cantaloupe {
    my $self = shift;
    return $self->{cantaloupe};
}

sub accessdb {
    my $self = shift;
    return $self->{accessdb};
}

sub canvasdb {
    my $self = shift;
    return $self->{canvasdb};
}

sub internalmetadb {
    my $self = shift;
    return $self->{internalmetadb};
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
    my ( $noid, $configpath ) = @_;
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
        $status = JSON::true;
        new CIHM::Meta::Hammer2::Process(
            {
                noid               => $noid,
                log                => $self->log,
                swift              => $self->swift,
                preservation_files => $self->{preservation_files},
                access_metadata    => $self->{access_metadata},
                access_files       => $self->{access_files},
                cantaloupe         => $self->cantaloupe,
                accessdb           => $self->accessdb,
                canvasdb           => $self->canvasdb,
                internalmetadb     => $self->internalmetadb,
            }
        )->process;
    }
    catch {
        $status = JSON::false;
        $self->log->error("$noid: $_");
        $self->{message} .= "Caught: " . $_;
    };
    $self->postResults( $noid, $status, $self->{message} );

    AE::log debug => "$noid After ($$)";

    return ($noid);
}

sub postResults {
    my ( $self, $noid, $status, $message ) = @_;

    $self->accessdb->hammerResult(
        $noid,
        {
            "succeeded" => $status,
            "message"   => $message,
        }
    );
}

1;
