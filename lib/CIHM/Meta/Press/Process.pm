package CIHM::Meta::Press::Process;

use 5.014;
use strict;
use Try::Tiny;
use JSON;
use DateTime;

=head1 NAME

CIHM::Meta::Press::Process - Handles the processing of individual AIPs for
CIHM::Meta::Press

=head1 SYNOPSIS

    CIHM::Meta::Press::Process->new($args);
      where $args is a hash of arguments.

=cut

sub new {
    my ( $class, $args ) = @_;
    my $self = bless {}, $class;

    if ( ref($args) ne "HASH" ) {
        die "Argument to CIHM::Meta::Press::Process->new() not a hash\n";
    }
    $self->{args} = $args;

    if ( !$self->log ) {
        die "Log::Log4perl object parameter is mandatory\n";
    }
    if ( !$self->internalmeta ) {
        die "internalmeta object parameter is mandatory\n";
    }
    if ( !$self->extrameta ) {
        die "extrameta object parameter is mandatory\n";
    }
    if ( !$self->cosearch ) {
        die "cosearch object parameter is mandatory\n";
    }
    if ( !$self->copresentation ) {
        die "copresentation object parameter is mandatory\n";
    }
    if ( !$self->aip ) {
        die "Parameter 'aip' is mandatory\n";
    }
    $self->{searchdoc}  = {};
    $self->{presentdoc} = {};

    # Flag for update status (false means problem with update)
    $self->{ustatus} = 1;

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

sub config {
    my $self = shift;
    return $self->args->{config};
}

sub log {
    my $self = shift;
    return $self->args->{log};
}

sub internalmeta {
    my $self = shift;
    return $self->args->{internalmeta};
}

sub extrameta {
    my $self = shift;
    return $self->args->{extrameta};
}

sub cosearch {
    my $self = shift;
    return $self->args->{cosearch};
}

sub copresentation {
    my $self = shift;
    return $self->args->{copresentation};
}

sub pressme {
    my $self = shift;
    return $self->args->{pressme};
}

sub searchdoc {
    my $self = shift;
    return $self->{searchdoc};
}

sub presentdoc {
    my $self = shift;
    return $self->{presentdoc};
}

sub process {
    my ($self) = @_;

    if ( $self->pressme ) {
        $self->adddocument();
    }
    else {
        $self->deletedocument();
    }
}

sub deletedocument {
    my ($self) = @_;

    $self->delete_couch( $self->cosearch );
    $self->delete_couch( $self->copresentation );
}

sub adddocument {
    my ($self) = @_;

    # Grab the data for the CouchDB document
    my $aipdata = $self->internalmeta->get_aip( $self->aip );

    my $extradata = {};

    # Get the Extrameta data, if it exists..
    $self->extrameta->type("application/json");
    my $res =
      $self->extrameta->get(
        "/" . $self->extrameta->{database} . "/" . $self->aip,
        {}, { deserializer => 'application/json' } );
    if ( $res->code == 200 ) {
        $extradata = $res->data;
    }

    # Every AIP in the queue must have an attachment from Hammer.
    # (Test is part of the queue map)
    $self->process_hammer();

    # Map also counts for a minimum of repos, so adding in current array
    # to presentation.
    $self->presentdoc->{ $self->aip }->{'repos'} = $aipdata->{'repos'}
      if defined $aipdata->{'repos'};

    # All Items should have a date
    $self->presentdoc->{ $self->aip }->{'updated'} =
      DateTime->now()->iso8601() . 'Z';

    # If collections field exists, set accordingly within the item
    # Note: Not stored within pages, so no need to loop through all keys
    if ( exists $aipdata->{'collections'} ) {

        # The 's' is not used in the schemas, so not using here.
        $self->presentdoc->{ $self->aip }->{'collection'} =
          $aipdata->{'collections'};
        $self->searchdoc->{ $self->aip }->{'collection'} =
          $aipdata->{'collections'};
    }

   # If a parl.json attachment exists, process it. (parl-terms.json is obsolete)
    if ( exists $extradata->{'_attachments'}->{'parl.json'} ) {
        $self->process_parl();
    }

    # Determine if series or issue/monograph
    if ( $aipdata->{'sub-type'} eq 'series' ) {

        # Process series

        if ( exists $aipdata->{'parent'} ) {
            die $self->aip . " is a series and has parent field\n";
        }
        if ( scalar( keys %{ $self->presentdoc } ) != 1 ) {
            die $self->aip
              . " is a series and has "
              . scalar( keys %{ $self->presentdoc } )
              . " records\n";
        }
        if ( $self->presentdoc->{ $self->aip }->{'type'} ne 'series' ) {
            die $self->aip . " is a series, but record type not series\n";
        }
        $self->process_series();
    }
    else {
        # Process issue or monograph

        # If 'parent' field exists, process as issue of series
        if ( exists $aipdata->{'parent'} ) {
            $self->process_issue( $aipdata->{'parent'} );
        }

        # For the 'collection' field to be complete, processing components
        # needs to happen after process_issue().
        $self->process_components();
    }

    # If an externalmetaHP.json attachment exists, process it.
    # - Needs to be processed after process_components() as
    #   process_externalmetaHP() sets a flag within component field.
    if ( exists $extradata->{'_attachments'}->{'externalmetaHP.json'} ) {
        $self->process_externalmetaHP();
    }

    if (
        scalar( keys %{ $self->searchdoc } ) !=
        scalar( keys %{ $self->presentdoc } ) )
    {
        warn $self->aip . " had "
          . scalar( keys %{ $self->searchdoc } )
          . " searchdoc and "
          . scalar( keys %{ $self->presentdoc } )
          . " presentdoc\n";
        print $self->aip . " had doc count discrepancy\n";
    }

    $self->update_couch( $self->cosearch,       $self->searchdoc );
    $self->update_couch( $self->copresentation, $self->presentdoc );

    if ( $self->{ustatus} == 0 ) {
        die "One or more updates were not successful\n";
    }
}

sub update_couch {
    my ( $self, $dbo, $docs ) = @_;

    # Get revision of the parent document
    $dbo->type("application/json");
    my $res = $dbo->get(
        "/" . $dbo->{database} . "/_all_docs?key=\"" . $self->aip . "\"",
        {}, { deserializer => 'application/json' } );
    if ( $res->code == 200 ) {
        if ( $res->data->{rows} && $res->data->{rows}[0]->{key} ) {
            my $thisdoc = $res->data->{rows}[0];

            # Put the previous revision into doc
            my $docid = $thisdoc->{key};
            if ( exists $docs->{$docid} ) {
                $docs->{$docid}->{"_rev"} =
                  $thisdoc->{value}->{rev};
            }
            else {
                warn "$docid has revision but not doc("
                  . $dbo->{database} . ")\n";
            }
        }
    }
    else {
        die "update_couch ("
          . $dbo->{database}
          . ") GET _all_docs return code: "
          . $res->code . "\n";
    }

    # Get revisions for all the child docs, using known pattern for IDs
    $dbo->type("application/json");
    my $res = $dbo->get(
        "/"
          . $dbo->{database}
          . "/_all_docs?startkey=\""
          . $self->aip
          . ".\"&endkey=\""
          . $self->aip
          . ".\ufff0\"",
        {},
        { deserializer => 'application/json' }
    );
    if ( $res->code == 200 ) {
        if ( exists $res->data->{rows} ) {
            foreach my $thisdoc ( @{ $res->data->{rows} } ) {

                # Put the previous revision into doc
                my $docid = $thisdoc->{key};
                if ( exists $docs->{$docid} ) {
                    $docs->{$docid}->{"_rev"} =
                      $thisdoc->{value}->{rev};
                }
                else {
     # previous revision of AIP had more components, so delete the extra ones...
                    my %deletedoc = (
                        "_id", $docid, "_rev", $thisdoc->{value}->{rev},
                        "_deleted", JSON::true
                    );
                    $docs->{$docid} = \%deletedoc;
                }
            }
        }
    }
    else {
        die "update_couch ("
          . $dbo->{database}
          . ") GET _all_docs return code: "
          . $res->code . "\n";
    }

    # Initialize structure to be used for bulk update
    my $postdoc = { docs => [] };

    # Post the updated documents...
    # TODO: Ensure $docid within $self->aip , and any IDs no longer
    # referenced are removed (IE: fewer pages in update).
    foreach my $docid ( keys %{$docs} ) {
        $docs->{$docid}->{"_id"} = $docid;
        push @{ $postdoc->{docs} }, $docs->{$docid};
    }

    $dbo->type("application/json");
    my $res = $dbo->post( "/" . $dbo->{database} . "/_bulk_docs",
        $postdoc, { deserializer => 'application/json' } );

    if ( $res->code == 201 ) {
        my @data = @{ $res->data };
        if ( exists $data[0]->{id} ) {
            foreach my $thisdoc (@data) {

                # Check if any ID's failed
                if ( !$thisdoc->{ok} ) {
                    warn $thisdoc->{id}
                      . " was not indicated OK update_couch ("
                      . $dbo->{database} . ")\n";
                    $self->{ustatus} = 0;
                }
            }
        }
    }
    else {
        die "update_couch ("
          . $dbo->{database}
          . ") POST return code: "
          . $res->code . "\n";
    }
}

sub delete_couch {
    my ( $self, $dbo ) = @_;

    # Initialize structure to be used for bulk update
    my $postdoc = { docs => [] };

    # Get revision of the parent document
    $dbo->type("application/json");
    my $res = $dbo->get(
        "/" . $dbo->{database} . "/_all_docs?key=\"" . $self->aip . "\"",
        {}, { deserializer => 'application/json' } );
    if ( $res->code == 200 ) {
        if ( $res->data->{rows} && $res->data->{rows}[0]->{key} ) {
            my $thisdoc = $res->data->{rows}[0];
            my %thisdoc = (
                "_id", $thisdoc->{key}, "_rev", $thisdoc->{value}->{rev},
                "_deleted", JSON::true
            );
            push @{ $postdoc->{docs} }, \%thisdoc;
        }
    }
    else {
        die "update_couch ("
          . $dbo->{database}
          . ") GET _all_docs return code: "
          . $res->code . "\n";
    }

    # Get revisions for all the child docs, using known pattern for IDs
    $dbo->type("application/json");
    my $res = $dbo->get(
        "/"
          . $dbo->{database}
          . "/_all_docs?startkey=\""
          . $self->aip
          . ".\"&endkey=\""
          . $self->aip
          . ".\ufff0\"",
        {},
        { deserializer => 'application/json' }
    );
    if ( $res->code == 200 ) {
        if ( exists $res->data->{rows} ) {
            foreach my $thisdoc ( @{ $res->data->{rows} } ) {
                my %thisdoc = (
                    "_id", $thisdoc->{key}, "_rev", $thisdoc->{value}->{rev},
                    "_deleted", JSON::true
                );
                push @{ $postdoc->{docs} }, \%thisdoc;
            }
        }
    }
    else {
        die "update_couch ("
          . $dbo->{database}
          . ") GET _all_docs return code: "
          . $res->code . "\n";
    }

    $dbo->type("application/json");
    my $res = $dbo->post( "/" . $dbo->{database} . "/_bulk_docs",
        $postdoc, { deserializer => 'application/json' } );

    if ( $res->code == 201 ) {
        my @data = @{ $res->data };
        if ( exists $data[0]->{id} ) {
            foreach my $thisdoc (@data) {

                # Check if any ID's failed
                if ( !$thisdoc->{ok} ) {
                    warn $thisdoc->{id}
                      . " was not indicated OK update_couch ("
                      . $dbo->{database} . ")\n";
                    $self->{ustatus} = 0;
                }
            }
        }
    }
    else {
        die "update_couch ("
          . $dbo->{database}
          . ") POST return code: "
          . $res->code . "\n";
    }
}

sub process_hammer {
    my ($self) = @_;

    # Grab the data from the attachment (will be hammer.json soon)
    my $hammerdata =
      $self->internalmeta->get_aip( $self->aip . "/hammer.json" );

    # Hammer data is an ordered array with element [0] being item, and other
    # elements being components

    # First loop to generate the item 'tx' field if it doesn't already exist
    if ( !exists $hammerdata->[0]->{'tx'} ) {
        my @tx;
        for my $i ( 1 .. $#$hammerdata ) {
            my $doc = $hammerdata->[$i];
            if ( exists $doc->{'tx'} ) {
                foreach my $t ( @{ $doc->{'tx'} } ) {
                    push @tx, $t;
                }
            }
        }
        if (@tx) {
            $hammerdata->[0]->{'tx'} = \@tx;
        }
    }

    # If there is now an item 'tx' field, handle its count
    if ( exists $hammerdata->[0]->{'tx'} ) {
        my $count = scalar( @{ $hammerdata->[0]->{'tx'} } );
        if ($count) {
            $hammerdata->[0]->{'component_count_fulltext'} = $count;
        }
    }

    # These fields copied from item into each component.
    my $pubmin = $hammerdata->[0]->{'pubmin'};
    my $pubmax = $hammerdata->[0]->{'pubmax'};
    my $lang   = $hammerdata->[0]->{'lang'};

    # Loop through and copy into cosearch/copresentation
    for my $i ( 0 .. $#$hammerdata ) {
        my $doc = $hammerdata->[$i];
        my $key = $doc->{'key'}
          || die "Key missing from document in Hammer.json";

        # Copy fields into components
        if ($i) {
            if ($pubmin) {
                $doc->{'pubmin'} = $pubmin;
            }
            if ($pubmax) {
                $doc->{'pubmax'} = $pubmax;
            }
            if ($lang) {
                $doc->{'lang'} = $lang;
            }
        }

        # Hash of all fields that are set
        my %docfields = map { $_ => 1 } keys %{$doc};

        $self->searchdoc->{$key} = {};

        # Copy the fields for cosearch
        foreach my $cf (
            "key",                      "type",
            "depositor",                "label",
            "pkey",                     "seq",
            "pubmin",                   "pubmax",
            "lang",                     "identifier",
            "pg_label",                 "ti",
            "au",                       "pu",
            "su",                       "no",
            "ab",                       "tx",
            "no_rights",                "no_source",
            "component_count_fulltext", "noid"
          )
        {
            $self->searchdoc->{$key}->{$cf} = $doc->{$cf} if exists $doc->{$cf};
            delete $docfields{$cf};
        }

        $self->presentdoc->{$key} = {};

        # Copy the fields for copresentation
        foreach my $cf (
            "key",                   "type",
            "label",                 "pkey",
            "seq",                   "lang",
            "media",                 "identifier",
            "canonicalUri",          "canonicalMaster",
            "canonicalMasterMime",   "canonicalMasterSize",
            "canonicalMasterMD5",    "canonicalMasterWidth",
            "canonicalMasterHeight", "canonicalDownload",
            "canonicalDownloadMime", "canonicalDownloadSize",
            "canonicalDownloadMD5",  "ti",
            "au",                    "pu",
            "su",                    "no",
            "ab",                    "no_source",
            "no_rights",             "component_count_fulltext",
            "noid"
          )
        {
            $self->presentdoc->{$key}->{$cf} = $doc->{$cf}
              if exists $doc->{$cf};
            delete $docfields{$cf};
        }

        if ( keys %docfields ) {
            warn "Unused Hammer fields in $key: "
              . join( ",", keys %docfields ) . "\n";
            print "Unused Hammer fields in $key: "
              . join( ",", keys %docfields ) . "\n";
        }
    }
}

sub process_parl {
    my ($self) = @_;

    $self->extrameta->type("application/json");
    my $res = $self->extrameta->get(
        "/" . $self->extrameta->{database} . "/" . $self->aip . "/parl.json",
        {}, { deserializer => 'application/json' } );
    if ( $res->code != 200 ) {
        die "get of parl.json return code: " . $res->code . "\n";
    }
    my $parl = $res->data;

    my %term_map = (
        language       => "lang",
        label          => "parlLabel",
        chamber        => "parlChamber",
        session        => "parlSession",
        type           => "parlType",
        node           => "parlNode",
        reportTitle    => "parlReportTitle",
        callNumber     => "parlCallNumber",
        primeMinisters => "parlPrimeMinisters",
        pubmin         => "pubmin",
        pubmax         => "pubmax"
    );

    my @search_terms =
      qw/language label chamber session type reportTitle callNumber primeMinisters pubmin pubmax/;
    foreach my $st (@search_terms) {
        $self->searchdoc->{ $self->aip }->{ $term_map{$st} } = $parl->{$st}
          if exists $parl->{$st};
    }

    foreach my $pt ( keys %term_map ) {
        $self->presentdoc->{ $self->aip }->{ $term_map{$pt} } = $parl->{$pt}
          if exists $parl->{$pt};
    }
}

# Merging multi-value fields
sub mergemulti {
    my ( $doc, $field, $value ) = @_;

    if ( !defined $doc->{$field} ) {
        $doc->{$field} = $value;
    }
    else {
        # Ensure values being pushed are unique.
        foreach my $mval ( @{$value} ) {
            my $found = 0;
            foreach my $tval ( @{ $doc->{$field} } ) {
                if ( $mval eq $tval ) {
                    $found = 1;
                    last;
                }
            }
            if ( !$found ) {
                push @{ $doc->{$field} }, $mval;
            }
        }
    }
}

sub process_externalmetaHP {
    my ($self) = @_;

    # Grab the data for the CouchDB document

    $self->extrameta->type("application/json");
    my $res = $self->extrameta->get(
        "/"
          . $self->extrameta->{database} . "/"
          . $self->aip
          . "/externalmetaHP.json",
        {},
        { deserializer => 'application/json' }
    );
    if ( $res->code != 200 ) {
        die "get of externalmetaHP.json eturn code: " . $res->code . "\n";
    }
    my $emHP = $res->data;

    foreach my $seq ( keys %{$emHP} ) {
        my $pageid = $self->aip . "." . $seq;
        my $tags   = $emHP->{$seq};
        if ( defined $self->searchdoc->{$pageid} ) {
            my %tagfields = map { $_ => 1 } keys %{$tags};

            # Copy the fields for cosearch && copresentation
            # In parent as well..
            foreach my $cf (
                "tag",     "tagPerson",
                "tagName", "tagPlace",
                "tagDate", "tagNotebook",
                "tagDescription"
              )
            {
                if ( exists $tags->{$cf} ) {
                    if ( ref( $tags->{$cf} ne "ARRAY" ) ) {
                        die
                          "externalmetaHP tag $cf for page $pageid not array\n";
                    }

                    mergemulti( $self->searchdoc->{$pageid}, $cf,
                        $tags->{$cf} );
                    mergemulti( $self->presentdoc->{$pageid},
                        $cf, $tags->{$cf} );
                    mergemulti( $self->searchdoc->{ $self->aip },
                        $cf, $tags->{$cf} );
                    mergemulti( $self->presentdoc->{ $self->aip },
                        $cf, $tags->{$cf} );
                }
                delete $tagfields{$cf};
            }

            # Set flag in item to indicate this component has tags
            $self->presentdoc->{ $self->aip }->{'components'}->{$pageid}
              ->{'hasTags'} = JSON::true;

            # Set flag in item to indicate some component has tags
            $self->presentdoc->{ $self->aip }->{'hasTags'} = JSON::true;

            if ( keys %tagfields ) {
                warn "Unused externalmetaHP fields in $pageid: "
                  . join( ",", keys %tagfields ) . "\n";
            }
        }
        else {
            die "externalmetaHP sequence $seq doesn't exist in"
              . $self->aip . "\n";
        }
    }
}

sub process_issue {
    my ( $self, $parent ) = @_;

    # Force parent to be processed (likely again) later, and grab label
    my $res = $self->internalmeta->post(
        "/"
          . $self->internalmeta->{database}
          . "/_design/tdr/_update/parent/$parent",
        {},
        { deserializer => 'application/json' }
    );
    if ( $res->code != 201 && $res->code != 200 ) {
        die "_update/parent/$parent POST return code: " . $res->code . "\n";
    }
    if ( $res->data->{return} ne 'updated' ) {
        die "_update/parent/$parent POST function returned: "
          . $res->data->{return} . "\n";
    }
    $self->presentdoc->{ $self->aip }->{'plabel'} = $res->data->{label};
    $self->searchdoc->{ $self->aip }->{'plabel'}  = $res->data->{label};

    # Merge collection information
    if ( exists $res->data->{collection} ) {
        my %collections;
        foreach my $a ( @{ $res->data->{'collection'} } ) {
            $collections{$a} = 1;
        }
        foreach my $a ( @{ $self->presentdoc->{ $self->aip }->{'collection'} } )
        {
            $collections{$a} = 1;
        }

        my @collections = sort keys %collections;

        $self->presentdoc->{ $self->aip }->{'collection'} = \@collections;

        $self->searchdoc->{ $self->aip }->{'collection'} =
          $self->presentdoc->{ $self->aip }->{'collection'};
    }
}

sub process_series {
    my ($self) = @_;

    my @order;
    my $items = {};

    # Look up issues for this series
    $self->internalmeta->type("application/json");
    my $res = $self->internalmeta->get(
        "/"
          . $self->internalmeta->{database}
          . "/_design/tdr/_view/issues?reduce=false&startkey=[\""
          . $self->aip
          . "\"]&endkey=[\""
          . $self->aip
          . "\",{}]",
        {},
        { deserializer => 'application/json' }
    );
    if ( $res->code != 200 ) {
        die "_view/issues for "
          . $self->aip
          . " return code: "
          . $res->code . "\n";
    }
    foreach my $issue ( @{ $res->data->{rows} } ) {

        # Only add issues which have been approved
        if ( $issue->{value}->{approved} ) {
            delete $issue->{value}->{approved};
            push( @order, $issue->{id} );

            # All the other values from the map are currently used
            # for the items field
            $items->{ $issue->{id} } = $issue->{value};
        }
    }
    $self->presentdoc->{ $self->aip }->{'order'}     = \@order;
    $self->presentdoc->{ $self->aip }->{'items'}     = $items;
    $self->searchdoc->{ $self->aip }->{'item_count'} = scalar(@order);
}

=head1 $self->process_components()

Process component AIPs to build the 'components' and 'order' fields.
Currently order is numeric order by sequence, but later may be built
into metadata.xml

=cut

sub process_components {
    my ($self) = @_;

    my $components = {};
    my %seq;
    my @order;

    foreach my $thisdoc ( keys %{ $self->presentdoc } ) {
        next if ( $self->presentdoc->{$thisdoc}->{'type'} ne 'page' );
        $seq{ $self->presentdoc->{$thisdoc}->{'seq'} + 0 } =
          $self->presentdoc->{$thisdoc}->{'key'};
        $components->{ $self->presentdoc->{$thisdoc}->{'key'} }->{'label'} =
          $self->presentdoc->{$thisdoc}->{'label'};
        if ( exists $self->presentdoc->{$thisdoc}->{'canonicalMaster'} ) {
            $components->{ $self->presentdoc->{$thisdoc}->{'key'} }
              ->{'canonicalMaster'} =
              $self->presentdoc->{$thisdoc}->{'canonicalMaster'};
        }
        if ( exists $self->presentdoc->{$thisdoc}->{'noid'} ) {
            $components->{ $self->presentdoc->{$thisdoc}->{'key'} }->{'noid'} =
              $self->presentdoc->{$thisdoc}->{'noid'};
        }
        if ( exists $self->presentdoc->{$thisdoc}->{'canonicalMasterWidth'} ) {
            $components->{ $self->presentdoc->{$thisdoc}->{'key'} }
              ->{'canonicalMasterWidth'} =
              $self->presentdoc->{$thisdoc}->{'canonicalMasterWidth'};
        }
        if ( exists $self->presentdoc->{$thisdoc}->{'canonicalMasterHeight'} ) {
            $components->{ $self->presentdoc->{$thisdoc}->{'key'} }
              ->{'canonicalMasterHeight'} =
              $self->presentdoc->{$thisdoc}->{'canonicalMasterHeight'};
        }
        if ( exists $self->presentdoc->{$thisdoc}->{'canonicalDownload'} ) {
            $components->{ $self->presentdoc->{$thisdoc}->{'key'} }
              ->{'canonicalDownload'} =
              $self->presentdoc->{$thisdoc}->{'canonicalDownload'};
        }
        if ( defined $self->presentdoc->{ $self->aip }->{'collection'} ) {
            $self->presentdoc->{$thisdoc}->{'collection'} =
              $self->presentdoc->{ $self->aip }->{'collection'};
            $self->searchdoc->{$thisdoc}->{'collection'} =
              $self->presentdoc->{ $self->aip }->{'collection'};
        }
    }
    foreach my $page ( sort { $a <=> $b } keys %seq ) {
        push @order, $seq{$page};
    }

    $self->{presentdoc}->{ $self->aip }->{'order'}          = \@order;
    $self->{presentdoc}->{ $self->aip }->{'components'}     = $components;
    $self->{searchdoc}->{ $self->aip }->{'component_count'} = scalar(@order);
}

1;
