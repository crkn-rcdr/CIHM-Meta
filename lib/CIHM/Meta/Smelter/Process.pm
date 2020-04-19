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
    if (!$self->dipstagingdb) {
        die "dipstagingdb object parameter is mandatory\n";
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
sub noidsrv {
    my $self = shift;
    return $self->args->{noidsrv};
}
sub canvasdb {
    my $self = shift;
    return $self->args->{canvasdb};
}
sub dipstagingdb {
    my $self = shift;
    return $self->args->{dipstagingdb};
}
sub manifestdb {
    my $self = shift;
    return $self->args->{manifestdb};
}
sub slugdb {
    my $self = shift;
    return $self->args->{slugdb};
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
sub filemetadata {
    my $self = shift;
    return $self->{filemetadata};
}


sub process {
    my ($self) = @_;

    # TODO: Confirm slug isn't used.

    $self->loadFileMeta();

    $self->parseMETS();
    my $pagecount = (scalar @{$self->divs})-1;
    my $borndigital=$self->bornDigital();

    $self->buildManifest();
    if ($borndigital) {
        $self->manifest->{'type'}='pdf';
        $self->manifest->{'masterPages'}=$self->getPageLabels();
        # Move from the default position used in buildManifest(), as born digital is 'special'
        $self->manifest->{'master'}=$self->manifest->{'ocrPdf'};
        delete $self->manifest->{'ocrPdf'};
    } else {
        $self->manifest->{'type'}='multicanvas';
        $self->buildCanvases();
    }

    $self->assignNoids();

    print Dumper($self->manifest,$self->canvases, $pagecount, $self->divs);

    #$self->writeDocuments();

    # TODO: Set Slug for manifest noid.
}


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

sub parseMETS {
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

sub bornDigital {
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

sub getPageLabels {
    my ($self) = @_;

    my @labels;

    for my $index (1..(scalar @{$self->divs})-1) {
        my $label=$self->divs->[$index]->{label};

        if (!$label || $label eq '') {
            die "Label missing for index $index\n";
        }
        push @labels, { none => [$label]};
    }
    return \@labels;
}


sub buildManifest {
    my ($self) = @_;

    # Item is in div 0
    my $div = $self->divs->[0];
    my $label=$div->{label};

    if (!$label || $label eq '') {
        die "Missing item label\n";
    }
    $self->manifest->{label}->{none}=[$label];

    if (defined $div->{'distribution.flocat'}) {
        warn "Distribution not PDF" if ($div->{'distribution.mimetype'} ne 'application/pdf');
        $self->manifest->{'ocrPdf'}= {
            'path' => $self->aip."/".$div->{'distribution.flocat'},
            'size' => $self->filemetadata->{$div->{'distribution.flocat'}}->{'bytes'}
        }
    }
}


sub buildCanvases {
    my ($self) = @_;

    my @canvases;
    my @mancanvases;

    my $pagecount = (scalar @{$self->divs})-1;
    for my $index (0..$pagecount-1) {
        # Components in div 1+
        my $div = $self->divs->[$index+1];
        $mancanvases[$index]->{label}->{none}=[$div->{label}];
        my $master=$div->{'master.flocat'};
        die "Missing Master for index=$index\n" if (! $master);
        my $path=$self->aip."/".$master;
        $canvases[$index]->{'master'}={
            'path' => $path,
            'mime' => $div->{'master.mimetype'},
            'size' => $self->filemetadata->{$master}->{'bytes'}
        };
        $canvases[$index]->{'source'}={
            'from' => 'cihm',
            'path' => $path
        };
        my $path=uri_escape_utf8($path)."/info.json";
        my $res=$self->cantaloupe->get($path,{},{deserializer => 'application/json'});
        # TODO: the 403 is a bit odd!
	    if ($res->code != 200 && $res->code != 403) {
	        die "Cantaloupe call to `$path` returned: ".$res->code."\n";
	    }
	    if (defined $res->data->{height}) {
	        $canvases[$index]->{'master'}->{'height'}=$res->data->{height};
        }
        if (defined $res->data->{width}) {
	        $canvases[$index]->{'master'}->{'width'}=$res->data->{width};
	    }
        if (defined $div->{'distribution.flocat'}) {
            warn "Distribution file not PDF for $index\n" if ($div->{'distribution.mimetype'} ne 'application/pdf');
	        $canvases[$index]->{'ocrPdf'}= {
                'path' => $self->aip."/".$div->{'distribution.flocat'},
                'size' => $self->filemetadata->{$div->{'distribution.flocat'}}->{'bytes'}
            }
        }
    }
    $self->manifest->{'canvases'}=\@mancanvases;
    $self->{'canvases'}=\@canvases;
}


sub loadFileMeta {
    my $self=shift;

    $self->{filemetadata}={};

    my $prefix=$self->aip.'/';
    # List of objects with AIP as prefix
    my %containeropt = (
        "prefix" => $prefix
    );

    # Need to loop possibly multiple times as Swift has a maximum of
    # 10,000 names.
    my $more=1;
    while ($more) {
        my $bagdataresp = $self->swift->container_get($self->preservation_files,\%containeropt);
        if ($bagdataresp->code != 200) {
	        die "container_get(".$self->container.") for $prefix returned ". $bagdataresp->code . " - " . $bagdataresp->message. "\n";
        };
        $more=scalar(@{$bagdataresp->content});
        if ($more) {
	        $containeropt{'marker'}=$bagdataresp->content->[$more-1]->{name};
            foreach my $object (@{$bagdataresp->content}) {
	            my $file=substr $object->{name},(length $prefix);
	            $self->filemetadata->{$file}=$object;
	        }
        }
    }
}

sub assignNoids {
    my $self=shift;

    my @manifestnoids=@{$self->mintNoids(1,'manifest')};
    die "Couldn't allocate 1 manifest noid\n" if (scalar @manifestnoids != 1);
    $self->manifest->{'_id'}=$manifestnoids[0];

    my $canvascount=scalar @{$self->canvases};
    my @canvasnoids=@{$self->mintNoids($canvascount,'canvases')};
    die "Couldn't allocate $canvascount canvas noids\n" if (scalar @canvasnoids != $canvascount);
    for my $index (0..$canvascount-1) {
        $self->manifest->{'canvases'}->[$index]->{'id'}=$canvasnoids[$index];
        $self->canvases->[$index]->{'_id'}=$canvasnoids[$index];
    }
}

# See https://github.com/crkn-rcdr/noid for details
sub mintNoids {
    my ($self,$number,$type) = @_;

    return [] if (!$number);

    my $res = $self->noidsrv->post("/mint/$number/$type", {}, {deserializer => 'application/json'});
    if ($res->code != 200) {
        die "Fail communicating with noid server for /mint/$number/$type: " . $res->code . "\n";
    }
    return $res->data->{ids};
}


# TODO: For now a direct write to CouchDB, later through an Upholstery interface
sub writeDocuments {
    my ($self) = @_;

    my $res=$self->manifestdb->post("/".$self->manifestdb->database."/_bulk_docs", { docs => [$self->manifest]}, {deserializer => 'application/json'});
    if ($res->code != 201) {
        if (defined $res->response->content) {
            warn $res->response->content."\n";
        }
        die "dbupdate of 'manifest' return code: " . $res->code . "\n";
    }

    $res=$self->canvasdb->post("/".$self->canvasdb->database."/_bulk_docs", { docs => $self->canvases}, {deserializer => 'application/json'});
    if ($res->code != 201) {
        if (defined $res->response->content) {
            warn $res->response->content."\n";
        }
        die "dbupdate of 'canvas' return code: " . $res->code . "\n";
    }
}

1;
