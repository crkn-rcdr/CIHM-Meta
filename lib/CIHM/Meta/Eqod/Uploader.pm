package CIHM::Meta::Eqod::Uploader;

use strict;
use Carp;
use CIHM::TDR::TDRConfig;
use CIHM::TDR::REST::externalmeta;
use JSON;
use Text::CSV;
use List::Util qw(first);
use File::Path qw(make_path);
use Data::Dumper;
use feature qw(say);

=head1 NAME

CIHM::Meta::Eqod::Uploader - Upload EQOD data from csv to Externalmeta DB

=head1 SYNOPSIS

      $args->{configpath} is as defined in CIHM::TDR::TDRConfig

=cut

sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

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
sub reel {
	my $self = shift;
	return $self->{reel}
}
sub since {
    my $self = shift;
    return $self->{args}->{since};
}
sub file {
    my $self = shift;
    return $self->{args}->{file};
}

sub uploader {
    my ($self) = @_;
	
	my $file = $self->{args}->{file};
	#say $file;

	
   	my $csv = Text::CSV->new({binary=>1});
	open my $fh, "<:encoding(iso-8859-1)", $file or die "Cannot read $file: $!";
	my $header =  ($csv->getline ($fh));


	#extract url column location
	my $url_column = get_url($header);
	
	# Get reel information and add corresponding pages to each reel
	my %csv_data = ();
	while (my $row = $csv->getline($fh)) {
		my $url = $row->[$url_column];
		my $reel = get_reel($url);
		next unless ($reel);		
		
		
		#add rows for each reel
		unless ($csv_data {$reel}){	
			$csv_data {$reel} = [];
		}
		push ($csv_data {$reel}, $row);	 
	}

	# Create json document for each reel, containing corresponding pages and tags
	my %page_data = ();
	foreach my $reel (keys(%csv_data)){	
		my $response;
		say $reel;
		

		
		#get pages		
		%page_data = get_page($url_column, $csv_data{$reel});
		
		# process each page			
		foreach my $page (sort({$a <=> $b} keys(%page_data))){
		
			my $properties = [];
			my %types = ();
			my %cells =	get_properties ($properties, $page_data{$page}, $header);
			
			#combine properties under the same tag
			foreach my $prop(keys(%cells)){	
				my $value = $cells{$prop};
				my $type = lc($prop);
			}
			
			#create json structure for each page
			my $doc = {reel => $reel, page =>$page, eqodProperties => \%cells};
			my $json = JSON->new->utf8(1)->pretty(1)->encode({document => {object => $doc}});
			my $page_id = $reel.".".$page;
			my $filename = $page_id.'_HP.json';
	
	 		# Create document if it doesn't already exist
	 		#warn $reel;
   			$self->externalmeta->update_basic($reel,{});
		
			my $return = $self->externalmeta->put_attachment($reel, {
				type => "application/json",
				content => $json,
				filename => $filename,
		       # updatedoc => $self->updatedoc		
				});
			if ($return != 201){
				 die "Return code $return for externalmeta->put_attachment(" 
	            . $self->reel . ")\n";
			}	
		}
	}
}
sub get_url{
	my($header) = @_;
	
	#determine index location of URL column
	my $url_column = first {@$header[$_] eq 'URL' || @$header[$_] eq 'URLs' || @$header[$_] eq 'URL for where document starts' || @$header[$_] eq 'Reel Location'}0..@$header;
	return $url_column;
}
sub get_reel{
	my($url) = @_;
	
	# Reel information can only be extracted from the url column
	foreach ($url){
		#if the value matches a url sequence then extract the reel number
		#say $url;
		if ($url =~ m{(oocihm.*/)}m){
			#warn "contains oocihm";
			my $reel = $1;
			
			#remove trailingin slashes from reel numbers
			if ($reel =~ m{(.*/)}m){
				$reel =~ s/\///;
			}
			#say $reel;
			return $reel;
			
		} else{
			#say "invalid reel number";
		}
	}	
}
sub get_page{
	my ($url_column, $pages) = @_;
	my %page_data = ();
	
	# Get page number and corresponding rows
	foreach my $page (@{$pages}){
		my ($url, $page_id) = @$page[$url_column] =~ m{(.*/)([^?]*)}m; #page number is acquired from url column
		next unless ($page_id);
		unless ($page_data {$page_id}){
			$page_data {$page_id} = [];
		}		
		push ($page_data {$page_id}, $page);
	}
	return %page_data;
}
sub get_properties{
	my($properties, $pages, $header) = @_;

	#foreach header that matches an eqod property grab corresponding value for each page
	my %cells = ();
	foreach my $property(@$header){	
			my $lc_prop = lc($property); #convert fields to lowercase
			my $value;
			foreach my $page(@$pages){
				$value = shift(@$page);			
			
				next unless ($value);
				unless ($cells {$lc_prop}){
					$cells {$lc_prop} = [];
				}		
				push ($cells{$lc_prop}, $value);
			}
	}

	return %cells;
}
1;
