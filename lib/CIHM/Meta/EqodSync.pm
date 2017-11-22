package CIHM::Meta::EqodSync;

use strict;
use Carp;
use CIHM::TDR::TDRConfig;
use CIHM::TDR::REST::externalmeta;
use CIHM::TDR::REST::internalmeta;
use Archive::BagIt::Fast;
use Try::Tiny;
use JSON;
use Data::Dumper;

=head1 NAME

CIHM::Meta::RepoSync - Normalize metadata from
"externalmeta" and post to "internalmeta" databases

=head1 SYNOPSIS

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

    # Undefined if no <externalmeta> config block
    if (exists $confighash{externalmeta}) {
        $self->{externalmeta} = new CIHM::TDR::REST::externalmeta (
            server => $confighash{externalmeta}{server},
            database => $confighash{externalmeta}{database},
            type   => 'application/json',
            conf   => $self->configpath,
            clientattrs => {timeout => 3600},
            );
    } else {
        croak "Missing <externalmeta> configuration block in config\n";
    }
    # Undefined if no <internalmeta> config block
    if (exists $confighash{internalmeta}) {
        $self->{internalmeta} = new CIHM::TDR::REST::internalmeta (
            server => $confighash{internalmeta}{server},
            database => $confighash{internalmeta}{database},
            type   => 'application/json',
            conf   => $self->configpath,
            clientattrs => {timeout => 3600},
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
sub config {
    my $self = shift;
    return $self->{config};
}
sub log {
    my $self = shift;
    return $self->{logger};
}
sub externalmeta {
    my $self = shift;
    return $self->{externalmeta};
}
sub internalmeta {
    my $self = shift;
    return $self->{internalmeta};
}
sub since {
    my $self = shift;
    return $self->{args}->{since};
}
sub localdocument {
    my $self = shift;
    return $self->{args}->{localdocument};
}

sub eqodsync {
    my ($self) = @_;

    $self->log->info("Synchronizing \"externalmeta\" and \"internalmeta\" data...");
   
    #Read list of eqod aips to process from file
    my $file = "/Users/julienne/Documents/eqod/test_eqod_aips.txt";
    open (FH, "< $file") or die "Can't open $file for read: $!";
	my @reels;
	while (<FH>) {
		#warn $_;
    	push (@reels, $_);
	}
	close FH or die "Cannot close $file: $!";

    # Loop through all the page attachments, grab data, normalize, then update "internalmeta"    
    foreach my $reel (@reels) {
    	my $eqod_data = $self->externalmeta->get_aip($reel);
    	warn $eqod_data;
    	
    	#);
		#get aip couch document
		#get eqod attachments
		#process each eqod attachment
		#create json attachment for 
		die;
    }
}

1;
