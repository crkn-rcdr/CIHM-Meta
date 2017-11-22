package CIHM::Meta::Press;

use strict;
use Carp;
use CIHM::TDR::TDRConfig;
use CIHM::TDR::REST::internalmeta;
use CIHM::TDR::REST::cosearch;
use CIHM::TDR::REST::copresentation;
use CIHM::Meta::Press::Process;
use Try::Tiny;
use Data::Dumper;
use JSON;

=head1 NAME

CIHM::Meta::Press - Build cosearch and copresentation documents from
normalized data in "internalmeta" database.

Makes use of a CouchDB _view which emits based on checking if metadata has
been modified since the most recent date this tool has processed a document.

=head1 SYNOPSIS

    my $t_repo = CIHM::TDR::Replication->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is as defined in CIHM::TDR::TDRConfig

=cut


sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::TDR::Replication->new() not a hash\n";
    };
    $self->{args} = $args;

    $self->{config} = CIHM::TDR::TDRConfig->instance($self->configpath);
    $self->{logger} = $self->{config}->logger;

    # Confirm there is a named repository block in the config
    my %confighash = %{$self->{config}->get_conf};

    $self->{dbconf}={};
    # Undefined if no <internalmeta> config block
    if (exists $confighash{internalmeta}) {
        $self->{internalmeta} = new CIHM::TDR::REST::internalmeta (
            server => $confighash{internalmeta}{server},
            database => $confighash{internalmeta}{database},
            type   => 'application/json',
            conf   => $self->configpath,
            clientattrs => {timeout => 3600}
        );
    } else {
        croak "Missing <internalmeta> configuration block in config\n";
    }
    # Undefined if no <cosearch> config block
    if (exists $confighash{cosearch}) {
        $self->{cosearch} = new CIHM::TDR::REST::cosearch (
            server => $confighash{cosearch}{server},
            database => $confighash{cosearch}{database},
            type   => 'application/json',
            conf   => $self->configpath,
            clientattrs => {timeout => 3600}
        );
    } else {
        croak "Missing <cosearch> configuration block in config\n";
    }
    # Undefined if no <copresentation> config block
    if (exists $confighash{copresentation}) {
        $self->{copresentation} = new CIHM::TDR::REST::copresentation (
            server => $confighash{copresentation}{server},
            database => $confighash{copresentation}{database},
            type   => 'application/json',
            conf   => $self->configpath,
            clientattrs => {timeout => 3600}
        );
    } else {
        croak "Missing <internalmeta> configuration block in config\n";
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
    return $self->{args}->{skip};
}
sub config {
    my $self = shift;
    return $self->{config};
}
sub log {
    my $self = shift;
    return $self->{logger};
}
sub internalmeta {
    my $self = shift;
    return $self->{internalmeta};
}
sub cosearch {
    my $self = shift;
    return $self->{cosearch};
}
sub copresentation {
    my $self = shift;
    return $self->{copresentation};
}



sub Press {
    my ($self) = @_;

    $self->log->info("Press time: conf=".$self->configpath." skip=".$self->skip);

    # Scope of variables for warnings() requires these be in object,
    # and initialized with each new AIP being processed.
    $self->{message}='';
    $self->{aip} = 'none';

    # Capture warnings
    sub warnings {
        my $warning = shift;
        $self->log->warn($self->{aip} . ": $warning");
        $self->{message} .= $warning . "\n";
    }
    local $SIG{__WARN__} = sub { &warnings };


    while (1) 
    {
        my ($aip,$pressme) = $self->getNextAIP;
        last if !$aip;

#    my ($aip,$pressme) = ("oocihm.8_06490_123",1); {
#    my ($aip,$pressme) = ("oocihm.8_06490",1); {


        $self->log->info("Processing $aip");

        my $status;
        # Handle and record any errors
        try {
            # Initialize variables used by warnings() for each AIP
            $self->{message}='';
            $self->{aip} = $aip;

            # Initialize status
            $status = 1;
            new  CIHM::Meta::Press::Process(
                {
                    aip => $aip,
                    configpath => $self->configpath,
                    config => $self->config,
                    internalmeta => $self->internalmeta,
                    cosearch => $self->cosearch,
                    copresentation => $self->copresentation,
                    pressme => $pressme
                })->process;
        } catch {
            $status = 0;
            $self->log->error("$aip: $_");
            $self->{message} .= "Caught: " . $_;
        };
        $self->postResults($aip,$status,$self->{message});
    }
}

sub getNextAIP {
    my $self = shift;

    my $skipparam = '';
    if ($self->skip) {
        $skipparam="&skip=".$self->skip;
    }

    $self->internalmeta->type("application/json");
    my $res = $self->internalmeta->get("/".$self->internalmeta->database."/_design/tdr/_view/pressq?reduce=false&limit=1".$skipparam,{}, {deserializer => 'application/json'});
    if ($res->code == 200) {
        if (exists $res->data->{rows}) {
            return ($res->data->{rows}[0]->{id},$res->data->{rows}[0]->{value});
        }
        return;
    }
    else {
        die "_view/pressq GET return code: ".$res->code."\n"; 
    }
    return;
}

sub postResults {
    my ($self,$aip,$status,$message) = @_;

    $self->internalmeta->update_basic($aip,{ 
        "press" => encode_json({
            "status" => $status,
            "message" => $message
                                   })
    });
}

1;
