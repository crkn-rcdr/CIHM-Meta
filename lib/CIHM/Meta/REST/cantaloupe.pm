package CIHM::Meta::REST::cantaloupe;

use DateTime;
use Crypt::JWT;

use Moose;
with 'Role::REST::Client';
use Types::Standard qw(HashRef Str Int Enum HasMethods);

# Build our own user agent, which will add the header.
sub _build_user_agent {
	my $self = shift;
	require CIHM::Meta::REST::UserAgent;
	return CIHM::Meta::REST::UserAgent->new(%{$self->clientattrs});
}

sub BUILD {
    my $self = shift;
    my $args = shift;

    $self->{LocalTZ} = DateTime::TimeZone->new( name => 'local' );

    $self->server($args->{url});
    $self->{clientattrs}->{c7a_id}=$args->{key};
    $self->{clientattrs}->{jwt_secret}=$args->{password};

    if (! $self->server) {
        die "You need to supply Content Server URL (in config file or command line)";
    }

    # JWT specific
    if (defined $args->{jwt_algorithm}) {
        $self->{clientattrs}->{jwt_algorithm}=$args->{jwt_algorithm};
    }
    if (defined $args->{jwt_payload}) {
        $self->{clientattrs}->{jwt_payload}=$args->{jwt_payload};
    }
}

sub get_clientattrs {
    my ($self) = shift;

    return $self->{clientattrs};
}

1;
