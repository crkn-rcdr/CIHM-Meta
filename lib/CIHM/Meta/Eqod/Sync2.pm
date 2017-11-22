package CIHM::Meta::Eqod::Sync2;

use strict;
use Carp;
use CIHM::TDR::TDRConfig;
use CIHM::TDR::REST::externalmeta;
use CIHM::TDR::REST::internalmeta;
use Archive::BagIt::Fast;
use Try::Tiny;
use JSON;
use CIHM::CMR;
use DateTime;
use Data::Dumper;

=head1 NAME

CIHM::Meta::Eqod::Sync - Process eqod data from
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
sub file {
    my $self = shift;
    return $self->{args}->{file};
}



sub eqodsync {
    my ($self) = @_;
   
    #Read list of eqod aips to process from file
	my $file = $self->{args}->{file};
    open (FH, "< $file") or die "Can't open $file for read: $!";
	my @reels;
	while (<FH>) {
		#warn $_;
		chomp;
    	push (@reels, $_);
	}
	close FH or die "Cannot close $file: $!";

    # Loop through all the page attachments, grab data, normalize, then update "internalmeta"    
    foreach my $reel (@reels) {
    	my $page_doc;
    	
    	#get aip couch document
    	my $eqod_data = $self->externalmeta->get_aip($reel);
    	my $eqod_pages = {};
		
		if (!$eqod_data){
    			warn "$reel does not exist";
    	}
		
    	#get list of attachments - this is at the reel level
    	foreach my $eqod_page($eqod_data->{_attachments}){
    		
    		#process each json file - this is at the page level
    		foreach my $e_filename (keys($eqod_page)){

    			#get attachment
    			my $eqod_data = $self->externalmeta->get_attachment($reel, $e_filename);
			    if (!$eqod_data) {
			        die "No metadata attachment\n";
			    }
			    
    			process_attachment($eqod_pages, $reel, $eqod_data);	
    		}
    	}
    	print Dumper($eqod_pages);
    	die;
	   	#create json structure at the reel (aip) level
	   	my $json = JSON->new->utf8(1)->pretty(1)->encode($eqod_pages);

		#upload json to internalmeta db
		my $response = upload_internalmeta($self, $reel, $json);
		if ($response != 201){
				print STDERR "Return code $response for $reel\n";
		} else{
				#print STDERR "Return code $response for $reel\n";
		}	
    }
}
sub process_attachment{
	my ($eqod_pages, $reel, $eqod_data) = @_;
		
				#process eqod fields
				my $properties = [];
				my %types = ();
							
			    my $eqod_json = decode_json $eqod_data;
			    my $page = $eqod_json->{document}->{object}->{page};

			    my $e_fields = $eqod_json->{document}->{object}->{eqodProperties};
			    process_eqod($properties, $e_fields);
				
				#combine properties under the same tag		    
			    foreach my $prop(@$properties){	
					my $value = $prop->{'value'};
					my $type = $prop->{'type'};
					next unless ($value);
					unless ($types{$type}){
						$types{$type} = [];
					}
					push ($types{$type}, $value);
				}
				
				#remove duplicate values
				foreach my $tag (keys(%types)){
					foreach my $value ($types{$tag}){
						my $values = remove_duplicates_array($value);
						$types{$tag} = [keys(%$values)];					
					}
				}
				next unless (%types);
				$eqod_pages->{$page} = \%types;
}
sub process_eqod{
	my ($properties, $e_fields) = @_;

	#Eqod fields to cosearch tags - undef means the field is not being mapped to CO tags
	my %field2tag = (
		  'author' => 'tagPerson',
          'recipient' => 'tagPerson',
          'name' => 'tagPerson', 
          'names' => 'tagPerson',
          'title' => 'tagName', 
          'place', => 'tagPlace',
          'place - province' => 'tagPlace',
          'place - city, county, etc.' => 'tagPlace',
          'state name' => 'tagPlace',
          'date' => undef,
          'dates' => undef,
          'years' => undef,
          'year' => undef,
          'month' => undef,
          'day' => undef,
          'year1', => undef,
          'day1', => undef,
          'month1', => undef,
          'year2', => undef,
          'day2', => undef,
          'month2', => undef,
          'notebook', => 'tagNotebook',
          'land book number' => 'tagNotebook',
          'keywords' => 'tag',
          'document type' => 'tag',
          'document type (songs, photo, illustration, map, book, pamphlet)' => 'tag',
		  'description' => 'tagDescription', 	       
          'content/comment', => 'tagDescription',
          'content' => 'tagDescription',
          'url' => undef,	
          'urls', => undef,
          'volumes' => undef,
          'finding aid page #' => undef,
          'page start' => undef,
          'page end' => undef,
          'reel #s' => undef,
          'folder' => undef,
	);

	my $url = $e_fields->{'URLs'}; #for debugging
	
	#if eqod field matches a tag property then add it to properties
	foreach my $eqod_field (keys($e_fields)){
		my $values = $e_fields->{$eqod_field};
		next unless ($values);
		my $eqod_field = lc($eqod_field);
				
		if ($eqod_field eq 'year1' || $eqod_field eq 'year2'){
			#do nothing - process dates later
		}
		
		elsif ($field2tag{$eqod_field}){
			foreach my $value(@$values){
				my @fields = split('; ', $value); #split on delimiters
				foreach my $field_value(@fields){
					push (@$properties, add_eqod_property($field2tag{$eqod_field}, $field_value));
				}		
			} 	
		}
		elsif (exists($field2tag{$eqod_field})){
			#do nothing - undefined properties
			#warn "Found existing key $element";
		}
		else{
			#properties not accounted for
			#warn "Found unknown property: $eqod_field";
		}
	}
	
	my $date1;
	my $date2;
	my $date_normal1; 
	my $date_normal2;
	my $dateRange;
	my $years = $e_fields->{years};
	my $year = $e_fields->{year};
	my $year1 = $e_fields->{year1};
	my $year2 = $e_fields->{year2};
	my $date_normal;
	
	#process specific csv files for the Homestead Grant Register
	my $adates = $e_fields->{'date of application'};
	my $gdates = $e_fields->{'date of grant'};
	my $hgr_dates = [];
	foreach my $adate (@$adates){
		next unless ($adate);
		my $dMin;
		my $dMax;
		if ($adate =~ m{(\d\d\d\d;)}m){
			my @fields = split('; ', $adate); #split on delimiters
			my $size_fields = @fields - 1;
			foreach my $fd(@fields){
				$dMin = CIHM::CMR::iso8601($fd,0);
				$dateRange = $dMin;
				push(@$hgr_dates, $dateRange);		
			}
		}
	}
	foreach my $gdate (@$gdates){
		next unless ($gdate);
		my $dMin;
		my $dMax;
		if ($gdate =~ m{(\d\d\d\d;)}m){
			my @fields = split('; ', $gdate); #split on delimiters
			my $size_fields = @fields - 1;
			foreach my $fd(@fields){
				$dMin = CIHM::CMR::iso8601($fd,0);
				$dateRange = $dMin;
				if ($dateRange){
					push(@$hgr_dates, $dateRange);		
										
				}
			}
		}
	}
	foreach my $hgr_date (@$hgr_dates){
		push(@$properties, add_eqod_property('tagDate', $hgr_date));	
	}
	
	#Process range of dates
	foreach my $yearRange (@$years){
		next unless ($yearRange);
		next unless ($yearRange ne "n.d." || $yearRange ne "nd");
		my $dMin;
		my $dMax;
		if ($yearRange =~ m{(\d\d\d\d)(\s-\s)(\d\d\d\d)}m){
			warn $yearRange;
			$dMin = CIHM::CMR::iso8601($1,0);
			$dMax = CIHM::CMR::iso8601($2,1);
		}
		if ($yearRange =~ m{(\d\d)(\d\d)(-)(\d\d)}m){
			my $min = $1.$2;
			my $max = $1.$4;
			$dMin = CIHM::CMR::iso8601($min,0);
			$dMax = CIHM::CMR::iso8601($max,1);
		}
		if ($dMin && $dMax){
				$dateRange = sprintf('[%s TO %s]', (sort($dMin, $dMax)));							
		}elsif ($dMin) {
				$dateRange = $dMin;
		}
	}
	
	my $date = $e_fields->{date};	
	foreach my $d (@$date){
		#warn $d;
		next unless ($d);
		next unless ($d ne "n.d." || $d ne "nd");
		my $dMin;
		my $dMax;
		if ($d =~ m{(\d\d\d\d;)}m){
			#warn "delimiter: $d";
			my @fields = split('; ', $d); #split on delimiters
			foreach my $fd(@fields){
				next if ($fd =~ m{(0000)}m);
				if 	($fd =~ m{(.*)(\s-\s)(.*)}m){
					$dMin = CIHM::CMR::iso8601($1,0);	
					$dMax = CIHM::CMR::iso8601($3,1);
				}
				$dMin = CIHM::CMR::iso8601($fd,0);
				if ($dMin && $dMax){
					$dateRange = sprintf('[%s TO %s]', (sort($dMin, $dMax)));							
				}elsif ($dMin) {
					$dateRange = $dMin;
				}
				push(@$properties, add_eqod_property('tagDate', $dateRange));	
			}
		}elsif ($d =~ m{(.*)(\s-\s)(.*)}m){
			#warn "range included: $d";
			$dMin = CIHM::CMR::iso8601($1,0);	
			$dMax = CIHM::CMR::iso8601($3,1);
		}elsif ($d =~ m{(\d\d\d\d)}m){
			#warn "year only: $d";
			$dMin = CIHM::CMR::iso8601($d,0);
		}else{
			#warn $d;
			#warn "invalid date";	
			next;	
		}
		if ($dMin && $dMax){
				$dateRange = sprintf('[%s TO %s]', (sort($dMin, $dMax)));							
		}elsif ($dMin) {
				$dateRange = $dMin;
		}
	}
	
	my $dates = $e_fields->{dates};	
	foreach my $d (@$dates){
	#	warn $d;
		next unless ($d);
		next unless ($d ne "n.d." || $d ne "nd");
		my $dMin;
		my $dMax;
		if ($d =~ m{(^\d\d\d\d-\d\d-\d\d;)}m){
			#warn "delimiter: $d";
			my @fields = split('; ', $d); #split on delimiters
			foreach my $fd(@fields){
				next if ($fd =~ m{(0000)}m);
				if 	($fd =~ m{(.*)(\s-\s)(.*)}m){
					$dMin = CIHM::CMR::iso8601($1,0);	
					$dMax = CIHM::CMR::iso8601($3,1);
				}
				$dMin = CIHM::CMR::iso8601($fd,0);

				if ($dMin && $dMax){
					$dateRange = sprintf('[%s TO %s]', (sort($dMin, $dMax)));							
				}elsif ($dMin) {
					$dateRange = $dMin;
				}
				push(@$properties, add_eqod_property('tagDate', $dateRange));	
			}
		}
		elsif ($d =~ m{(.*)(\s-\s)(.*)}m){
			#warn "range included: $d";
			$dMin = CIHM::CMR::iso8601($1,0);	
			$dMax = CIHM::CMR::iso8601($3,1);
			die;
		}elsif ($d =~ m{(^\d\d\d\d-\d\d-\d\d$)}m){
			#warn "year only: $d";
			$dMin = CIHM::CMR::iso8601($d,0);
			push(@$properties, add_eqod_property('tagDate', $dMin));
			
		}else{
			#warn $d;
			#warn "invalid date";	
		}

	}

	foreach my $y (@$year){
		my $months = $e_fields->{month};
		my $days = $e_fields->{day};
		next unless ($y ne "n.d." || $y ne "nd");
		my $date = $y || "";
		if ($months){
			$date = get_date($y, $months, $days);
		}
		$date_normal = CIHM::CMR::iso8601($date,0);		
		push(@$properties, add_eqod_property('tagDate', $date_normal));
	}
	foreach my $y1 (@$year1){
		next if ($y1 =~ m{(0000)}m);
		my $months = $e_fields->{month1};
		my $days = $e_fields->{day1};
		$date1 = $y1 || "";	
		if ($months){
			$date1 = get_date($y1, $months, $days);
		}
		$date_normal1 = CIHM::CMR::iso8601($date1,0);				
	}
	foreach my $y2 (@$year2){
		my $months = $e_fields->{month2};
		my $days = $e_fields->{day2};
		$date2 = $y2 || "";		
		if ($months){
			$date2 = get_date($y2, $months, $days);
		}
		$date_normal2 = CIHM::CMR::iso8601($date2,1); 	
	}
	
	#create date range field
	if ($date_normal1 && $date_normal2){
		$dateRange = sprintf('[%s TO %s]', (sort($date_normal1, $date_normal2)));
		push(@$properties, add_eqod_property('tagDate', $dateRange));		
	}elsif ($date_normal1){
		$dateRange = $date_normal1;
		push(@$properties, add_eqod_property('tagDate', $dateRange));			
	}
}
sub get_date{
	my($y, $months, $days) = @_;
	my $date = "";
	
	next if ($y =~ /\D/);
	
	foreach my $m(@$months){
			if ($m =~ m{\d}m){
				$m = sprintf("%02d", $m);
			}
			#if the month value is an abbreviated month term translate to numeric form
			if ($m =~ m{\D}m){ 
				$m = get_month($m);
			}
			next unless($m);
			$date = sprintf("%04d-%02d-00", $y, $m);
			if ($days){
				foreach my $d(@$days){
				$date = sprintf("%04d-%02d-%02d", $y, $m, $d);			
				}
			}
	}
	return $date;
	die;
}
sub get_month{
	my($month) = @_;
	
	#convert month to number
	my %mon2num = qw(
	jan 1  feb 2  mar 3  ap 4 apr 4  may 5  jun 6 
	jul 7  aug 8  sep 9  oct 10 nov 11 dec 12
	);	
	my $m = $mon2num{lc substr($month, 0, 3)};	
	return $m;
}
sub add_eqod_property{
	my($type, $value) = @_;

	my %property;
	next unless ($value);	
	%property = (
	        type => $type,
	        value => $value
    );

	return \%property;
}
sub remove_duplicates_array{
	my($value) = @_;
	my %values = map {$_ => 1} @$value;
	return \%values;
}
sub upload_internalmeta{
	my ($self, $reel, $json) = @_;

	my $response = $self->internalmeta->put_attachment($reel, {
				type => "application/json",
				content => $json,
				filename => 'externalmetaHP.json'
		       # updatedoc => $self->updatedoc		
				});
	return $response;	
}
1;
