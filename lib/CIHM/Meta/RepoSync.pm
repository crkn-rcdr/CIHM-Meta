package CIHM::Meta::RepoSync;

use strict;
use Carp;
use CIHM::TDR::TDRConfig;
use CIHM::TDR::REST::tdrepo;
use CIHM::TDR::REST::internalmeta;
use CIHM::TDR::REST::wipmeta;
use CIHM::TDR::REST::ContentServer;
use JSON;
use Date::Parse;
use DateTime;
use Data::Dumper;


=head1 NAME

CIHM::Meta::RepoSync - Synchronize specific data between 
"tdrepo" and "internalmeta" databases

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

    # Undefined if no <tdrepo> config block
    if (exists $confighash{tdrepo}) {
        $self->{tdrepo} = new CIHM::TDR::REST::tdrepo (
            server => $confighash{tdrepo}{server},
            database => $confighash{tdrepo}{database},
            repository => "", # Blank repository needs to be set
            type   => 'application/json',
            conf   => $self->configpath,
            clientattrs => {timeout => 3600},
            );
    } else {
        croak "Missing <tdrepo> configuration block in config\n";
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
    }
    # Undefined if no <wipmeta> config block
    if (exists $confighash{wipmeta}) {
        $self->{wipmeta} = new CIHM::TDR::REST::internalmeta (
            server => $confighash{wipmeta}{server},
            database => $confighash{wipmeta}{database},
            type   => 'application/json',
            conf   => $self->configpath,
            clientattrs => {timeout => 3600},
            );
    }
    $self->{dbs}=[];
    if ($self->internalmeta) {
        push $self->dbs, $self->internalmeta;
    }
    if ($self->wipmeta) {
        push $self->dbs, $self->wipmeta;
    }
    if (! @{$self->dbs}) {
        croak "No output databases defined\n";
    }

    my %cosargs = (
        jwt_payload => '{"uids":[".*"]}',
        conf => $self->configpath
        );
    $self->{cos} = new CIHM::TDR::REST::ContentServer (\%cosargs);
    if (!$self->cos) {
        croak "Missing ContentServer configuration\n";
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
sub tdrepo {
    my $self = shift;
    return $self->{tdrepo};
}
sub internalmeta {
    my $self = shift;
    return $self->{internalmeta};
}
sub wipmeta {
    my $self = shift;
    return $self->{wipmeta};
}
sub cos {
    my $self = shift;
    return $self->{cos};
}
sub dbs {
    my $self = shift;
    return $self->{dbs};
}
sub since {
    my $self = shift;
    return $self->{args}->{since};
}
sub localdocument {
    my $self = shift;
    return $self->{args}->{localdocument};
}


sub reposync {
    my ($self) = @_;

    $self->log->info("Synchronizing \"tdrepo\" data...");

    my $newestaips = $self->tdrepo->get_newestaip({
        date => $self->since,
        localdocument => $self->localdocument
                                                  });

    if (!$newestaips || !scalar(@$newestaips)) {
        # print STDERR "Nothing new....";
        return;
    }

 
    # Loop through all the changed AIPs, filter for public AIPs, then
    # update all the DBs
    foreach my $thisaip (@$newestaips) {
        my $aip = $thisaip->{key};
        my $manifestdate = $thisaip->{value}[0];
        my @repos = @{$thisaip->{value}[1]};

        my $updatedoc = {
            "repos" => encode_json(\@repos),
            "manifestdate" => $manifestdate
        };
        foreach my $db (@{$self->dbs}) {
            my $r=$db->update_basic_full($aip,$updatedoc);
            if (exists $r->{METSmatch} && ! $r->{METSmatch}) {
                my $mets=$self->getMETS($aip,$manifestdate);
                if ($mets) {
                    $updatedoc->{METS}=encode_json($mets);
                    $db->update_basic_full($aip,$updatedoc);
                }
            }
        }
    }
}

sub getMETS {
    my ($self,$aip,$manifestdate) = @_;

    my @mets;

    my $file = $aip."/manifest-md5.txt";
    my $r = $self->cos->get("/$file");
    if ($r->code == 200) {
        my $dt = DateTime->from_epoch(epoch => str2time($r->response->header('last-modified')));
        if ($dt) {
            my $cosmanifestdate = $dt->datetime. "Z";
            if ($cosmanifestdate eq $manifestdate) {
                my @metadata = grep {/\sdata\/(sip\/data|revisions\/[^\/\.]*\/data|revisions\/[^\/]*\.partial)\/metadata\.xml$/} split(/\n/gm,$r->response->content);
                my @mets;
                my %metshash;
                foreach my $md (@metadata) {
                    my ($md5,$path) = split(' ',$md);
                    $metshash{$path}=$md5;
                }
                foreach my $path (sort keys %metshash) {
                    push @mets , { md5 => $metshash{$path}, path => $path};
                }
                $self->log->info("Retrieved ".scalar(@mets)." manifests for $aip");
                return \@mets;
            } else {
                $self->log->error("$cosmanifestdate from COS != $manifestdate");
                return;
            }
        } else {
            $self->log->error("Error parsing date from header:" . $r->response->header('last-modified'));
            return;
        }
    } else {
        $self->log->error("Accessing $file returned code: " . $r->code);
        return;
    }
}

1;
