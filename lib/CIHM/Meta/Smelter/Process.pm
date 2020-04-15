package CIHM::Meta::Smelter::Process;

use 5.014;
use strict;
use Try::Tiny;
use JSON;
use Switch;
use URI::Escape;
use XML::LibXML;
use XML::LibXSLT;
use Data::Dumper;

=head1 NAME

CIHM::Meta::Smelter::Process - Handles the processing of individual AIPs for CIHM::Meta::Smelter

=head1 SYNOPSIS

    my $process = CIHM::Meta::Smelter::Process->new($args);
      where $args is a hash of arguments.

=cut

sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::Meta::Hammer::Process->new() not a hash\n";
    };
    $self->{args} = $args;

    if (!$self->log) {
        die "Log::Log4perl object parameter is mandatory\n";
    }
    if (!$self->swift) {
        die "swift object parameter is mandatory\n";
    }
    if (!$self->dipstaging) {
        die "dipstaging object parameter is mandatory\n";
    }
    if (!$self->cantaloupe) {
        die "cantaloupe object parameter is mandatory\n";
    }
    if (!$self->aip) {
        die "Parameter 'aip' is mandatory\n";
    }
    $self->{divs}=[];
    $self->{manifest}={};
    $self->{canvases}=[];
    return $self;
}

# Simple accessors for now -- Do I want to Moo?
sub args {
    my $self = shift;
    return $self->{args};
}
sub aip {
    my $self = shift;
    return $self->args->{aip};
}
sub log {
    my $self = shift;
    return $self->args->{log};
}
sub dipstaging {
    my $self = shift;
    return $self->args->{dipstaging};
}
sub cantaloupe {
    my $self = shift;
    return $self->args->{cantaloupe};
}
sub swift {
    my $self = shift;
    return $self->args->{swift};
}
sub access_metadata {
    my $self = shift;
    return $self->args->{access_metadata};
}
sub access_files {
    my $self = shift;
    return $self->args->{access_files};
}
sub preservation_files {
    my $self = shift;
    return $self->args->{preservation_files};
}
sub xml {
    my $self = shift;
    return $self->{xml};
}
sub xpc {
    my $self = shift;
    return $self->{xpc};
}
sub divs {
    my ($self) = @_;
    return $self->{divs};
}
sub manifest {
    my ($self) = @_;
    return $self->{manifest};
}
sub canvases {
    my ($self) = @_;
    return $self->{canvases};
}


sub process {
    my ($self) = @_;

    $self->parse_mets();
    my $pagecount = (scalar @{$self->divs})-1;
    my $borndigital=$self->borndigital();


    if ($borndigital) {
        self->manifest->{'masterPages'}=$self->getpagelabels();
    } else {
        my @canvases;
        my $pagelabels=$self->getpagelabels();
        for my $index (0..(scalar @{$pagelabels})-1) {
            $canvases[$index]->{label}->{none}=$pagelabels->[$index];
            $canvases[$index]->{id}="Noid for $index";
        }
        $self->manifest->{'canvases'}=\@canvases;
    }

    print Dumper($self->manifest,$pagecount, $self->divs);

    #die "This is a failure\n";
    #warn "This is a warning\n";
}


# Called also from CIHM::METS::parse
sub get_metadata {
    my ($self,$file) = @_;

    # Will retry for a second time.
    my $count=2;

    my $object=$self->aip."/$file";
    while ($count--) {
        my $r = $self->swift->object_get($self->preservation_files,$object);
        if ($r->code == 200) {
            return $r->content;
        } elsif ($r->code == 599) {
            warn("Accessing $object returned code (trying again): " . $r->code."\n");
        } else {
            die("Accessing $object returned code: " . $r->code."\n");
        }
    }
}

sub parse_mets {
    my ($self) = @_;


    my $metsdir = 'data/sip/data';
    my $metspath = $metsdir.'/metadata.xml';
    my $type= 'physical';

    $self->{xml}=XML::LibXML->new->parse_string($self->get_metadata($metspath));
    $self->{xpc}=XML::LibXML::XPathContext->new;
    $self->xpc->registerNs('mets', "http://www.loc.gov/METS/");
    $self->xpc->registerNs('xlink', "http://www.w3.org/1999/xlink");


    my @nodes = $self->xpc->findnodes("descendant::mets:structMap[\@TYPE=\"$type\"]",$self->xml);
    if (scalar(@nodes) != 1) {
        die "Found ".scalar(@nodes)." structMap(TYPE=$type)\n";
    }
    foreach my $div ($self->xpc->findnodes('descendant::mets:div',$nodes[0])) {
        my $index=scalar @{$self->divs};
        my $type=$div->getAttribute('TYPE');
        if (!$index && ($type ne 'document') && ($type ne 'issue')) {
            die "First DIV of METS isn't type=document|issue| , but type=$type\n";
        }
        if ($index && ($type ne 'page')) {
            die "Not-first DIV of METS isn't type=page, but type=$type\n";
        }

        my %attr;
        $attr{'label'}=$div->getAttribute('LABEL');
        my $dmdid=$div->getAttribute('DMDID');
        if ($dmdid) {
            my @dmdsec=$self->xpc->findnodes("descendant::mets:dmdSec[\@ID=\"$dmdid\"]",$self->xml);
            if (scalar(@dmdsec) != 1) {
                die "Found ".scalar(@dmdsec)." dmdSec for ID=$dmdid\n";
            }
            my @md=$dmdsec[0]->nonBlankChildNodes();
            if (scalar(@md) != 1) {
                die "Found ".scalar(@md)." children for dmdSec ID=$dmdid\n";
            }
            my @types=split(/:/,$md[0]->nodeName);
            my $type=pop(@types);

            $attr{'dmd.id'}=$dmdid;
            $attr{'dmd.type'}=$type;
            $attr{'dmd.mime'}=$md[0]->getAttribute('MIMETYPE');
            $attr{'dmd.mdtype'}=$md[0]->getAttribute('MDTYPE');
            if ($attr{'dmd.mdtype'} eq 'OTHER') {
                $attr{'dmd.mdtype'}=$md[0]->getAttribute('OTHERMDTYPE');
            }
        }

        foreach my $fptr ($self->xpc->findnodes('mets:fptr',$div)) {
            my $fileid=$fptr->getAttribute('FILEID');

            my @file=$self->xpc->findnodes("descendant::mets:file[\@ID=\"$fileid\"]",$self->xml);
            if (scalar(@file) != 1) {
                die "Found ".scalar(@file)." for file ID=$fileid\n";
            }
            my $use=$file[0]->getAttribute('USE');

            # If the file doesn't have USE=, check parent fileGrp
            if (! $use) {
                my $filegrp=$file[0]->parentNode;
                $use=$filegrp->getAttribute('USE');
                if (! $use) {
                    die "Can't find USE= attribute for file ID=$fileid\n";
                }
            }

            # never used...
            next if $use eq 'canonical';

            my $mimetype = $file[0]->getAttribute('MIMETYPE');

            if ($use eq 'derivative') {
                if ($mimetype eq 'application/xml') {
                    $use = 'ocr';
                } elsif ($mimetype eq 'application/pdf') {
                    $use = 'distribution';
                }
            }

            my @flocat=$self->xpc->findnodes("mets:FLocat",$file[0]);
            if (scalar(@flocat) != 1) {
                die "Found ".scalar(@flocat)." FLocat file ID=$fileid\n";
            }

            $attr{$use.'.mimetype'}=$mimetype;
            $attr{$use.'.flocat'}=$self->aipfile($metsdir,'FLocat',$flocat[0]->getAttribute('LOCTYPE'),$flocat[0]->getAttribute('xlink:href'));
        }

        push @{$self->divs}, \%attr;
    }
}

sub aipfile {
    my ($self,$metsdir,$type,$loctype,$href) = @_;

    if ($loctype eq 'URN') {
        if ($type eq 'FLocat') {
            $href="files/$href";
        } else {
            $href="metadata/$href";
        }
    }
    return substr(File::Spec->rel2abs($href,'//'.$metsdir),1);
}

sub borndigital {
    my ($self) = @_;

    # It is born digital if the page divs have only dmd information (txtmap made from PDF) and a label.
    for my $index (1..(scalar @{$self->divs})-1) {
        foreach my $key (keys %{$self->divs->[$index]}) {
            if (($key ne 'label') && (substr($key,0,4) ne 'dmd.')) {
                return 0;
            }
        }
    }
    return 1;
}

sub getpagelabels {
    my ($self) = @_;

    my @labels;

    for my $index (1..(scalar @{$self->divs})-1) {
        my $label=$self->divs->[$index]->{label};

        if (!$label || $label eq '') {
            $label = "Image $index";
        }
        push @labels, $label;
    }
    return \@labels;
}
1;
