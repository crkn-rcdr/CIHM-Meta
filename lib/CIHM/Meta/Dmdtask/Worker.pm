package CIHM::Meta::Dmdtask::Worker;

use strict;
use Carp;
use AnyEvent;
use Try::Tiny;
use JSON;
use Config::General;
use Log::Log4perl;

use CIHM::Swift::Client;
use CIHM::Meta::REST::dmdtask;
use CIHM::Meta::Dmdtask::Process;
use Data::Dumper;

our $self;

sub initworker {
    my $configpath = shift;
    our $self;

    AE::log debug => "Initworker ($$): $configpath";

    $self = bless {};

    Log::Log4perl->init_once("/etc/canadiana/tdr/log4perl.conf");
    $self->{logger} = Log::Log4perl::get_logger("CIHM::TDR");

    my %confighash = new Config::General( -ConfigFile => $configpath, )->getall;

    # Undefined if no <dmdtask> config block
    if ( exists $confighash{dmdtask} ) {
        $self->{dmdtaskdb} = new CIHM::Meta::REST::dmdtask(
            server      => $confighash{dmdtask}{server},
            database    => $confighash{dmdtask}{database},
            type        => 'application/json',
            conf        => $configpath,
            clientattrs => { timeout => 3600 },
        );
    }
    else {
        croak "Missing <dmdtask> configuration block in config\n";
    }

}

# Simple accessors for now -- Do I want to Moo?
sub log {
    my $self = shift;
    return $self->{logger};
}

sub dmdtaskdb {
    my $self = shift;
    return $self->{dmdtaskdb};
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

sub task {
    my ( $taskid, $type, $configpath ) = @_;
    our $self;

    # Capture warnings
    local $SIG{__WARN__} = sub { &warnings };

    if ( !$self ) {
        initworker($configpath);
    }

    # Debugging: http://lists.schmorp.de/pipermail/anyevent/2017q2/000870.html
    #  $SIG{CHLD} = 'IGNORE';

    $self->{taskid}  = $taskid;
    $self->{message} = '';

    $self->log->info("Processing $taskid");

    AE::log debug => "$taskid Before ($$)";

    my $status;

    # Handle and record any errors
    try {
        $status = 1;
        new CIHM::Meta::Dmdtask::Process(
            {
                taskid    => $taskid,
                type      => $type,
                log       => $self->log,
                dmdtaskdb => $self->dmdtaskdb,
            }
        )->process;
    }
    catch {
        $status = 0;
        $self->log->error("$taskid: $_");
        $self->{message} .= "Caught: " . $_;
    };
    $self->postResults( $taskid, $type, $status, $self->{message} );

    AE::log debug => "$taskid After ($$)";

    return ($taskid);
}

sub postResults {
    my ( $self, $taskid, $type, $status, $message ) = @_;

    $self->dmdtaskdb->update(
        $taskid,
        {
            "${type}res" => encode_json(
                {
                    "succeeded" => $status,
                    "message"   => $message,
                }
            )
        }
    );
}

1;
