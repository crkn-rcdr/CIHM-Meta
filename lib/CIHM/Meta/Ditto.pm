package CIHM::Meta::Ditto;

use strict;
use Carp;
use CIHM::TDR::TDRConfig;
use CIHM::TDR::REST::tdrepo;
use CIHM::TDR::REST::tdrmeta;
use CIHM::TDR::REST::ContentServer;
use Try::Tiny;
use JSON;
use Date::Parse;
use DateTime;
use Digest::MD5;
use Data::Dumper;

=head1 NAME

CIHM::Meta::Ditto - Synchronize metadata.xml from TDR file repository and
"tdrmeta" database

=head1 SYNOPSIS

    my $ditto = CIHM::Meta::Ditto->new($args);
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
    # Undefined if no <tdrmeta> config block
    if (exists $confighash{tdrmeta}) {
        $self->{tdrmeta} = new CIHM::TDR::REST::tdrmeta (
            server => $confighash{tdrmeta}{server},
            database => $confighash{tdrmeta}{database},
            type   => 'application/json',
            conf   => $self->configpath,
            clientattrs => {timeout => 3600},
            );
    } else {
        croak "Missing <tdrmeta> configuration block in config\n";
    }

    my %cosargs = (
        jwt_payload => '{"uids":[".*"]}',
        conf => $self->configpath
        );
   $self->{cos} = new CIHM::TDR::REST::ContentServer (\%cosargs);
    if (!$self->{cos}) {
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
sub tdrmeta {
    my $self = shift;
    return $self->{tdrmeta};
}
sub cos {
    my $self = shift;
    return $self->{cos};
}
sub since {
    my $self = shift;
    return $self->{args}->{since};
}
sub localdocument {
    my $self = shift;
    return $self->{args}->{localdocument};
}
sub all {
    my $self = shift;
    return $self->{args}->{all};
}
sub ignoredate {
    my $self = shift;
    return $self->{args}->{ignoredate};
}


sub run {
    my ($self) = @_;

    $self->log->info("conf=".$self->configpath." since=".$self->since." localdocument=".$self->localdocument);

    my $gnaip = {};
    if (!$self->all) {
        $gnaip = {
            date => $self->since,
            localdocument => $self->localdocument
        }
    }
    my $newestaips = $self->tdrepo->get_newestaip($gnaip);

    if (!$newestaips || !scalar(@$newestaips)) {
        # print STDERR "Nothing new....";
        return;
    }

#=head
    my @aiplist;
    # Loop through all the changed AIPs, find out which need update
    foreach my $thisaip (@$newestaips) {
        my $aip = $thisaip->{key};
        my $manifestdate = $thisaip->{value}[0];

        # If ignoredate flag enabled, parse every AIP regardless if manifestdate
        # matches
        if ($self->ignoredate) {
            push @aiplist,$aip;
        } else {
            # Otherwise, skip AIPs where the manifest date from tdrepo
            # matches date in tdrmeta.
            my $du = $self->update_ditto($aip,{manifestdate => $manifestdate});
            if ($du->{return} && $du->{return} ne 'date match') {
                push @aiplist,$aip;
            }
        }
    }
    undef $newestaips;
#=cut

    #my @aiplist=("oocihm.03628","oocihm.12611","oop.debates_CDC3501_11","oop.debates_CDC3501_12","oocihm.lac_reel_t4504");

    # loop through each AIP that may have updates.
    foreach my $aip (@aiplist) {
        my ($manifestdate,$manifestInfo) = $self->getManifest($aip);
        next if !$manifestdate || !$manifestInfo;

        my $du = $self->update_ditto($aip,{
            manifestdate => $manifestdate,
            attachInfo => encode_json $manifestInfo
                                     });

        #print "$aip -> $manifestdate\n" . Dumper($manifestInfo,$du)."\n";

        # If there are missing attachments, then attach them
        if ($du && defined $du->{missing}) {
            foreach my $path (@{$du->{missing}}) {
                $self->putAttachment($aip,$path);
            }

            $du = $self->update_ditto($aip,{
                manifestdate => $manifestdate,
                attachInfo => encode_json $manifestInfo
                                         });
            if ($du->{return} ne 'attach match') {
                $self->log->error("Problem after uploading attachments for $aip : ".encode_json($du));
            }
        }
    }
}

sub update_ditto {
  my ($self, $uid, $updatedoc) = @_;
  my ($res, $code, $data);

  # This encoding makes $updatedoc variables available as form data
  $self->tdrmeta->type("application/x-www-form-urlencoded");
  $res = $self->tdrmeta->post("/".$self->tdrmeta->{database}."/_design/tdr/_update/ditto/".$uid, $updatedoc, {deserializer => 'application/json'});

  if ($res->code != 201 && $res->code != 200) {
      warn "_update/basic/$uid POST return code: " . $res->code . "\n";
  }
  if ($res->data) {
      return $res->data;
  }
}

sub getManifest {
    my ($self,$aip) = @_;

    my $file = $aip."/manifest-md5.txt";
    my $r = $self->cos->get("/$file");
    if ($r->code == 200) {
        my $dt = DateTime->from_epoch(epoch => str2time($r->response->header('last-modified')));
        if ($dt) {
            my $manifestdate = $dt->datetime. "Z";

            my $manifestinfo = {};

            # Now parse Manifest
            my @manifestlines = split(/\n/gm,$r->response->content);

            my $manifesthash={};
            foreach my $manifestline (@manifestlines) {
                my ($md5,$path) = split(' ',$manifestline);
                next if substr($path,-13) ne '/metadata.xml';
                $manifesthash->{$path}=$md5;
            }
            foreach my $path (sort(keys $manifesthash)) {
                my $md5 = $manifesthash->{$path};

                if (exists $manifestinfo->{$md5}) {
                    push $manifestinfo->{$md5}->{'paths'}, $path;
                } else {
                    $manifestinfo->{$md5}->{'paths'}=[$path];
                }
                if ($path =~ m/revisions\/(\d\d\d\d)(\d\d)(\d\d)T(\d\d)(\d\d)(\d\d)/) {
                    # For older files the timestamp is unreliable, and thus
                    # we use the revision time in the pathname.
                    # This works only because this time will always be 
                    # earlier than the date of the xml in the SIP, even 
                    # if only by a few minutes.
                    $manifestinfo->{$md5}->{'pathDate'}="$1-$2-$3T$4:$5:$6Z";
                } elsif ($path =~ m/^data\/sip\/data\//) {
                    delete $manifestinfo->{$md5}->{'pathDate'};
                }
            }
            return ($manifestdate,$manifestinfo);
        } else {
            warn "Error parsing date from header:" . $r->response->header('last-modified') . "\n";
            return;
        }
    } else {
        warn "Accessing $file returned code: " . $r->code . "\n";
        return;
    }
}


sub putAttachment {
    my ($self,$aip,$path) = @_;

    my $file = $aip."/$path";
    my $r = $self->cos->get("/$file");
    if ($r->code == 200) {
        my ($keydate,$filedate);

        my $dt = DateTime->from_epoch(epoch => str2time($r->response->header('last-modified')));
        if ($dt) {
            $filedate = $dt->datetime. "Z";
            my $md5=Digest::MD5->new->add($r->response->content)->hexdigest;

            my $updatedoc = {
                upload => $md5,
                uploadinfo => encode_json ({
                    fileDate => $filedate
                                           })
            };
            my $return = $self->tdrmeta->put_attachment($aip,{
                type => "application/xml",
                content => $r->response->content,
                filename => $md5,
                updatedoc => $updatedoc
                                                     });
            if ($return != 201) {
                warn "Return code $return for $aip\n";
            }
        } else {
            warn "Error parsing date for $file\n";
        }
    } else {
        warn "Accessing $file returned code: " . $r->code . "\n";
    }
}

1;
