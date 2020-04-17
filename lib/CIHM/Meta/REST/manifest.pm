package CIHM::Meta::REST::manifest;

use strict;
use Carp;
use Data::Dumper;
use DateTime;
use JSON;

use Moo;
with 'Role::REST::Client';
use Types::Standard qw(HashRef Str Int Enum HasMethods);


=head1 NAME

CIHM::Meta::REST::manifest - Subclass of Role::REST::Client used to
interact with "manifest" CouchDB database

=head1 SYNOPSIS

    my $t_repo = CIHM::Meta::REST::manifest->new($args);
      where $args is a hash of arguments.  In addition to arguments
      processed by Role::REST::Client we have the following 

      $args->{database} is the Couch database name.

=cut

sub BUILD {
    my $self = shift;
    my $args = shift;

    $self->{LocalTZ} = DateTime::TimeZone->new( name => 'local' );
    $self->{database} = $args->{database};
    $self->set_persistent_header('Accept' => 'application/json');
}

# Simple accessors for now -- Do I want to Moo?
sub database {
    my $self = shift;
    return $self->{database};
}

1;
