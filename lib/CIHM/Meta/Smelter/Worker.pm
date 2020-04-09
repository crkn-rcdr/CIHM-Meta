package CIHM::Meta::Smelter::Worker;

use strict;
use Carp;
use AnyEvent;
use Try::Tiny;
use JSON;
use Config::General;
use Log::Log4perl;

use CIHM::Meta::REST::dipstaging;
use CIHM::Meta::Smelter::Process;

our $self;

sub initworker {
    my $configpath = shift;
    our $self;

    AE::log debug => "Initworker ($$): $configpath";

    $self = bless {};

    Log::Log4perl->init_once("/etc/canadiana/tdr/log4perl.conf");
    $self->{logger} = Log::Log4perl::get_logger("CIHM::TDR");

    my %confighash = new Config::General(
        -ConfigFile => $configpath,
    )->getall;


    # Undefined if no <dipstaging> config block
    if (exists $confighash{dipstaging}) {
        $self->{dipstaging} = new CIHM::Meta::REST::dipstaging (
            server => $confighash{dipstaging}{server},
            database => $confighash{dipstaging}{database},
            type   => 'application/json',
            conf   => $configpath,
            clientattrs => {timeout => 3600},
            );
    } else {
        croak "Missing <dipstaging> configuration block in config\n";
    }

}


# Simple accessors for now -- Do I want to Moo?
sub log {
    my $self = shift;
    return $self->{logger};
}
sub dipstaging {
    my $self = shift;
    return $self->{dipstaging};
}

sub warnings {
    my $warning = shift;
    our $self;
    my $aip="unknown";

    # Strip wide characters before  trying to log
    (my $stripped=$warning) =~ s/[^\x00-\x7f]//g;

    if ($self) {
        $self->{message} .= $warning;
        $aip = $self->{aip};
        $self->log->warn($aip.": $stripped");
    } else {
        say STDERR "$warning\n";
    }
}


sub smelt {
    my ($aip,$configpath) = @_;
    our $self;

    # Capture warnings
    local $SIG{__WARN__} = sub { &warnings };

    if (!$self) {
        initworker($configpath);
    }

    $self->{aip}=$aip;
    $self->{message}='';

    $self->log->info("Processing $aip");

    AE::log debug => "$aip Before ($$)";

    my $status;

    # Handle and record any errors
    try {
        $status = 1;
        new  CIHM::Meta::Smelter::Process(
        {
            aip => $aip,
            configpath => $configpath,
            log => $self->log,
            dipstaging => $self->dipstaging,
        })->process;
    } catch {
        $status = 0;
        $self->log->error("$aip: $_");
        $self->{message} .= "Caught: " . $_;
    };
    $self->postResults($aip,$status,$self->{message});

    AE::log debug => "$aip After ($$)";

    return ($aip);
}

sub postResults {
    my ($self,$aip,$status,$message) = @_;

    $self->dipstaging->update_basic($aip,{ 
        "smelt" => encode_json({
            "succeeded" => ($status ? JSON::true : JSON::false),
            "message" => $message,
        })
    });
}

1;
