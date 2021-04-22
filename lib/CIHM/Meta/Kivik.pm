
package CIHM::Meta::Kivik;

use strict;
use utf8;
use warnings;
use File::chdir;
use JSON;
use File::Temp ();

=head1 NAME

CIHM::Meta::Kivik - Common place for Kivik validation


=head1 SYNOPSIS

    my $error = CIHM::Meta::Kivik::validateRecord(database, record);

    where:
      datbase is a database within https://github.com/crkn-rcdr/Access-Platform/tree/main/couchdb
        IE: "access" or "canvas"

      record is a hash representing the record to validate

    Returns null when validated, or an error string as returned from Kivik when invalid.


=cut


# Validation, temporarily using a temporary file and calling kivik from the command line.
sub validateRecord {
    my ( $database, $record ) = @_;

    my $tmp = File::Temp->new( SUFFIX => '.json' );
    print $tmp encode_json($record);
    close($tmp);

    local $CWD = "/home/tdr/Access-Platform/couchdb";

    my $results;
    open( FH, "pnpx kivik validate $database ".$tmp->filename." |" )
      or die $!;
    {
        local $/;
        $results = <FH>;
    }
    close(FH);
    chomp($results);

    # Currently looks for specific string rather than return code.
    # There will eventually be a REST call, so this is all temporary.
    if ( !( $results =~ / is valid./ ) ) {
        return $results;
    }
}

1;
