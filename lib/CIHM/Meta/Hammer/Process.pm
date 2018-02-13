package CIHM::Meta::Hammer::Process;

use 5.014;
use strict;
use CIHM::TDR::TDRConfig;
use CIHM::TDR::Repository;
use CIHM::TDR::REST::filemeta;
use CIHM::TDR::REST::internalmeta;
use CIHM::METS::parse;
use XML::LibXML;
use Try::Tiny;
use JSON;
use Switch;
use URI::Escape;

=head1 NAME

CIHM::Meta::Hammer::Process - Handles the processing of individual AIPs for
CIHM::Meta::Process

=head1 SYNOPSIS

    my $t_repo = CIHM::TDR::Hammer::Process->new($args);
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

    if (!$self->config) {
        die "TDRConfig object parameter is mandatory\n";
    }
    if (!$self->cos) {
        die "cos object parameter is mandatory\n";
    }
    if (!$self->filemeta) {
        die "filemeta object parameter is mandatory\n";
    }
    if (!$self->internalmeta) {
        die "internalmeta object parameter is mandatory\n";
    }
    if (!$self->repo) {
        die "repo object parameter is mandatory\n";
    }
    if (!$self->aip) {
        die "Parameter 'aip' is mandatory\n";
    }
    $self->{updatedoc} = {};
    $self->{pageinfo} = {};
    $self->pageinfo->{count}=0;
    $self->pageinfo->{dimensions}=0;

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
sub metspath {
    my $self = shift;
    return $self->args->{metspath};
}
sub configpath {
    my $self = shift;
    return $self->args->{configpath};
}
sub config {
    my $self = shift;
    return $self->args->{config};
}
sub log {
    my $self = shift;
    return $self->config->logger;
}
sub cos {
    my $self = shift;
    return $self->args->{cos};
}
sub filemeta {
    my $self = shift;
    return $self->args->{filemeta};
}
sub filemetadata {
    my $self = shift;
    return $self->{filemetadata};
}
sub internalmeta {
    my $self = shift;
    return $self->args->{internalmeta};
}
sub repo {
    my $self = shift;
    return $self->args->{repo};
}
sub updatedoc {
    my $self = shift;
    return $self->{updatedoc};
}
sub pageinfo {
    my $self = shift;
    return $self->{pageinfo};
}
sub mets {
    my $self = shift;
    return $self->{mets};
}


sub process {
    my ($self) = @_;

    # Grab the METS record from COS, and parse into METS (LibXML) object
    $self->{mets} =
        CIHM::METS::parse->new({
            aip => $self->aip,
            metspath => $self->metspath,
            xmlfile => $self->get_metadata($self->metspath),
            metsaccess => $self
                               });

    # Fill in information about the TYPE="physical" structMap
    $self->mets->mets_walk_structMap("physical");
    my $metsdata = $self->mets->metsdata("physical");

    # Build and then parse the "cmr" data
    # TODO: incrementally replace with new CIHM::Meta::Hammer::METS functions
    my $idata = $self->mets->extract_idata();
    if (scalar(@{$idata}) != 1) {
        die "Need exactly 1 array element for item data\n";
    }
    $idata=$idata->[0];

    my $label;
    my $pubmin;
    my $seq;
    my $canonicalDownload="";
    my %hammerfields;

    # Loop through array of item+components
    for my $i (0 .. $#$metsdata) {

        if ($i == 0) {
            # Item processing
            # Copy fields from $idata , skipping some...
            foreach my $key (keys $idata) {
                switch ($key) {
                    case "label" {}
                    case "contributor" {}
                    else  {
                        $metsdata->[0]->{$key} = $idata->{$key};
                    }
                }
            }

            # If item download exists, set the canonicalDownload
            if (exists $metsdata->[0]->{'canonicalDownload'}) {
                $canonicalDownload=$metsdata->[0]->{'canonicalDownload'};
            }
            # Copy into separate variables (used for components, couch)
            $label = $metsdata->[0]->{'label'};
            $pubmin = $metsdata->[0]->{'pubmin'};
            $seq = $metsdata->[0]->{'seq'};
        } else {
            # Component processing

            # Grab OCR as text (no positional data for now)
            my $ocrtxt = $self->mets->getOCRtxt("physical",$i);
            if ($ocrtxt) {
                $metsdata->[$i]->{'tx'} = [ $ocrtxt ];
            }
        }

        # manipulate $self->updatedoc with data to be stored within
        # main couchdb document (Separate from attachment)
        $self->updateAIPdoc($metsdata->[$i]);

        if (exists $metsdata->[$i]->{'canonicalMaster'}) {
            my $filedata=$self->getFileData($metsdata->[$i]->{'canonicalMaster'},'physical');
            foreach my $key (keys $filedata) {
                $metsdata->[$i]->{"canonicalMaster${key}"}=$filedata->{$key};
                if ($key eq 'Width') {
                    $self->pageinfo->{dimensions}++;
                }
            }
        }
        if (exists $metsdata->[$i]->{'canonicalDownload'}) {
            my $filedata=$self->getFileData($metsdata->[$i]->{'canonicalDownload'},'physical');
            foreach my $key (keys $filedata) {
                $metsdata->[$i]->{"canonicalDownload${key}"}=$filedata->{$key};
                if ($key eq 'Width') {
                    $self->pageinfo->{dimensions}++;
                }
            }
        }

        # at end of loop, after field names possibly updated
        foreach my $field (keys $metsdata->[$i]) {
            $hammerfields{$field}=1;
        }
    }

    # Add field 'label' to couchdb document
    if (defined $label) {
        $self->updatedoc->{'label'}=$label;
    }
    # Add field 'pubmin' to couchdb document
    if (defined $pubmin) {
        $self->updatedoc->{'pubmin'}=$pubmin;
    }
    # Add field 'seq' to couchdb document
    if (defined $seq) {
        $self->updatedoc->{'seq'}=$seq;
    }
    
    # If there is pageinfo, make sure it gets added as well
    if ($self->pageinfo->{count} > 0) {
        $self->updatedoc->{'pageinfo'}= encode_json $self->pageinfo;
    }

    # This always defined
    $self->updatedoc->{'canonicalDownload'}= $canonicalDownload;

    # Set array of fields
    my @hammerfields= sort(keys %hammerfields);
    $self->updatedoc->{'hammerfields'}= encode_json \@hammerfields;

    # Create document if it doesn't already exist
    $self->internalmeta->update_basic($self->aip,{});

    my $return = $self->internalmeta->put_attachment($self->aip,{
        type => "application/json",
        content => encode_json $metsdata,
        filename => "hammer.json",
        updatedoc => $self->updatedoc
                                                      });

    if ($return != 201) {
        die "Return code $return for internalmeta->put_attachment(" 
            . $self->aip . ")\n";
    }

    $self->saveFileMeta();
}

sub get_metadata {
    my ($self,$file) = @_;

    # Will retry for a second time.
    my $count=2;

    my $cospath=$self->aip."/$file";
    while ($count--) {
        my $r = $self->cos->get('/'.$cospath);
        if ($r->code == 200) {
            return $r->response->content;
        } elsif ($r->code == 599) {
            warn("Accessing $cospath returned code: " . $r->code."\n");
        } else {
            die("Accessing $cospath returned code: " . $r->code."\n");
        }
    }
}

sub get_filemd5 {
    my ($self,$file) = @_;

    if (! exists $self->{filemd5}) {
        $self->{filemd5}={};
        my $md5txt=$self->get_metadata("manifest-md5.txt");
        foreach my $row (split(/\n/,$md5txt)) {
            my ($md5,$path) = split(/\s+/,$row);
            if ($md5 && $path) {
                $self->{filemd5}->{$path}=$md5;
            }
        }
    }
    return $self->{filemd5}->{$file};
}

sub loadFileMeta {
    my $self=shift;

    if (! $self->filemetadata) {
        $self->{filemetadata}={};

        $self->filemeta->type("application/json");
        my $res = $self->filemeta->get("/".$self->filemeta->database."/_all_docs",{
            include_docs => 'true',
            startkey => '"'.$self->aip.'/"',
            endkey => '"'.$self->aip.'/'.chr(0xfff0).'"'

                                       }, {deserializer => 'application/json'});
        if ($res->code == 200) {
            foreach my $row (@{$res->data->{rows}}) {
                $self->{filemetadata}->{$row->{key}}=$row->{doc};
            }
        }
        else {
            die "loadFileMeta return code: ".$res->code."\n"; 
        }
    }
}

sub saveFileMeta {
    my $self=shift;

    if ($self->filemetadata) {
        my @files=sort keys $self->filemetadata;
        my @update;

        foreach my $file (@files) {
            my $thisfile=$self->filemetadata->{$file};
            if ($thisfile->{changed}) {
                delete $thisfile->{changed};
                push @update, $thisfile;
            }
        }

        if (@update) {
            my $res = $self->filemeta->post("/".$self->filemeta->database."/_bulk_docs",
                                            { docs => \@update },
                                            {deserializer => 'application/json'});
            if ($res->code != 201) {
                die "saveFileMeta _bulk_docs returned: ".$res->code."\n";
            }
        }
    }
}

sub set_fmetadata {
    my ($self,$fmetadata,$key,$value) = @_;

    if (! exists $fmetadata->{$key} ||
        $fmetadata->{$key} ne $value) {
        $fmetadata->{$key}=$value;
        $fmetadata->{'changed'}=1;
    }
}

# Path within AIP and the structMap type ('physical' for now)
sub getFileData {
    my ($self,$pathname,$structtype) = @_;

    # Always return at least a blank
    my $filedata={};
    my $jhovexml;

    # Load if not already loaded
    $self->loadFileMeta();
    my $fmetadata=$self->filemetadata->{$pathname};

    # If record doesn't already exist, create one.
    if (! $fmetadata) {
        $fmetadata={
            '_id' => $pathname,
            changed => 1
        };
        $self->filemetadata->{$pathname}=$fmetadata;
    }

    my $divs=$self->mets->fileinfo("physical")->{'divs'};
    my $fileindex=$self->mets->fileinfo("physical")->{'fileindex'};

    # Strip off the AIP ID
    my $pathinaip=substr($pathname , index($pathname,'/')+1);

    if (! exists $fileindex->{$pathinaip}) {
        die "fileindex for $pathname doesn't exist\n";
    }
    my $div=$divs->[$fileindex->{$pathinaip}->{'index'}];
    my $use=$fileindex->{$pathinaip}->{'use'};

    if (exists $div->{$use.'.jhove'}) {
        my $jhovefile=$div->{$use.'.jhove'};
        my $md5=$self->get_filemd5($jhovefile);

        # Only load jhove file if needed.
        # (IE: We haven't already stored information in couch document)
        if (!$md5 || ! exists $fmetadata->{'jhovefilemd5'} ||
            $md5 ne $fmetadata->{'jhovefilemd5'}) {
            $self->set_fmetadata($fmetadata,'jhovefilename',$jhovefile);
            if ($md5) {
                $self->set_fmetadata($fmetadata,'jhovefilemd5',$md5);
            }
            # Load from COS
            $jhovexml=$self->get_metadata($jhovefile)
        }
    } else {
        # If attachment exists, load
        if (exists $fmetadata->{'_attachments'} &&
            exists $fmetadata->{'_attachments'}->{'jhove.xml'}) {
            $jhovexml=$self->filemeta->get_attachment(uri_escape($pathname),"jhove.xml");
        }
    }

    if ($jhovexml) {
        my $jhove = eval { XML::LibXML->new->parse_string($jhovexml) };
        if ($@) {
            warn "parse_string for $pathname: $@\n";
            return;
        }
        my $xpc = XML::LibXML::XPathContext->new($jhove);
        $xpc->registerNs('jhove', "http://hul.harvard.edu/ois/xml/ns/jhove");
        $xpc->registerNs('mix', "http://www.loc.gov/mix/v20");

        my $size=$xpc->findvalue("descendant::jhove:size",$jhove);
        if ($size > 0) {
            $self->set_fmetadata($fmetadata,'Size',$size);
        }

        my $md5=$xpc->findvalue('descendant::jhove:checksum[@type="MD5"]',$jhove);
        if ($md5) {
            $self->set_fmetadata($fmetadata,'MD5',$md5);
        }

        my $mimetype=$xpc->findvalue("descendant::jhove:mimeType",$jhove);
        if (index($mimetype,"image/")==0) {
            my @mix=$xpc->findnodes("descendant::mix:mix",$jhove);
            if (scalar(@mix)>0) {
                $self->set_fmetadata($fmetadata,'Width',$xpc->findvalue("descendant::mix:imageWidth",$mix[0]));
                $self->set_fmetadata($fmetadata,'Height',$xpc->findvalue("descendant::mix:imageHeight",$mix[0]));
            }
        }

        # Updating Filemeta database with potentially new information
        my $format=$xpc->findvalue("descendant::jhove:format",$jhove);
        if ($format) {
            $self->set_fmetadata($fmetadata,'format',$format);
        }
        my $version=$xpc->findvalue("descendant::jhove:version",$jhove);
        if ($version) {
            $self->set_fmetadata($fmetadata,'version',$version);
        }
        my $status=$xpc->findvalue("descendant::jhove:status",$jhove);
        if ($status) {
            $self->set_fmetadata($fmetadata,'status',$status);
        }
        my $mimetype=$xpc->findvalue("descendant::jhove:mimeType",$jhove);
        if ($mimetype) {
            $self->set_fmetadata($fmetadata,'mimetype',$mimetype);
        }
        my @errormsgs;
        foreach my $errormsg ($xpc->findnodes('descendant::jhove:message[@severity="error"]',$jhove)) {
            my $txtmsg=$errormsg->to_literal;
            if (! ($txtmsg =~ /^\d+$/)) {
                push @errormsgs,$txtmsg;
            }
        }
        if (@errormsgs) {
            $self->set_fmetadata($fmetadata,'errormsg',join(',',@errormsgs));
        }
    }

    foreach my $field ('Size','MD5','Width','Height') {
        if (exists $fmetadata->{$field}) {
            $filedata->{$field}=$fmetadata->{$field};
        }
    }

    # TODO:  If we still don't have dimensions, talk to Image Server

    # TODO: Mark files which are referenced in METS, and remove from CouchDB those which are no longer in current revision.

    return $filedata;
}

# This looks at the flattened data and extracts specific fields to be
# posted as couchdb fields
sub updateAIPdoc {
    my($self, $doc) = @_;

    my $type = $doc->{type} // '';
    my $key = $doc->{key} // '';
    my $pkey = $doc->{pkey} // '';
    my $seq = $doc->{seq};

    if ($type ne 'page') {
        if ($key ne $self->aip) {
            $self->updatedoc->{'sub-type'}="cmr key mismatch";
        } else {
            $self->updatedoc->{'sub-type'}=$type;
        }
        if ($type eq 'document') {
            if ($pkey ne '') {
                $self->updatedoc->{'parent'}=$pkey;
            }
        } elsif ($type eq 'series') {
            if ($pkey ne '') {
                warn $self->aip." is series with parent $pkey\n";
            }
        }
    } else {
        if ($seq) {
            if (!$self->pageinfo->{max} || $self->pageinfo->{max} < $seq) {
                $self->pageinfo->{max} = $seq;
            }
            if (!$self->pageinfo->{min} || $self->pageinfo->{min} > $seq) {
                $self->pageinfo->{min} = $seq;
            }
            $self->pageinfo->{count}++;
        }
        if (!$self->updatedoc->{'sub-type'} || 
            $self->updatedoc->{'sub-type'} ne 'document') {
            warn "Page $key 's parent not 'document' for ".$self->aip."\n";
        }
        if ($pkey ne $self->aip) {
            warn "Page $key has mismatched parent key for ".$self->aip."\n";
        }
    }
}

1;
