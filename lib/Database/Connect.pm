package Database::Connect;
use Moose;
use autodie; use re 'taint'; use 5.010;
our $VERSION = 1.0529;# Created: 2010-03-16
use Config::Tiny;
use Path::Class;

=pod

=head1 NAME

Database::Connect - Connect to your databases

=head1 SYNOPSIS

 use strict;
 use Database::Connect;

 my $dbc = Database::Connect->new;

 # information gathering
 say $dbc->dsn("mydb");

 # Use with DBI
 my $dbh  = $dbc->dbh("mydb");
 my $dbh2 = DBI->connect( $dbc->dbi_args("mydb") );
 $dbc->on_connect("mydb")->($dbh2);

 # DBIx::Simple
 my $dbs = $dbc->dbix_simple("mydb");

 # DBIx::Class
 my $schema  = $dbc->dbic_schema_connect("mydb");
 my $schema1 = $dbc->dbic_schema_connect("mydb", undef, 'My::Other::Schema');
 my $schema2 = MySchema->connect(
   $dbc->dbi_args("mydb"),
   { AutoCommit => 1, RaiseError => 1 },
   { on_connect_do => [ $dbc->on_connect_sql("mydb") ] },
 );

 # Catalyst::Model::DBIC::Schema
 my $mydb = "mydb";
 __PACKAGE__->config(
   schema_class => $dbc->dbic_schema($mydb),
   connect_info => [ $dbc->dbi_args($mydb),
                     { AutoCommit => 1, RaiseError => 1 },
                     { on_connect_do => [ $dbc->on_connect_sql($mydb) ] },
                   ],
 );


=head1 DESCRIPTION

Reads and processes Database connection information from:

 /etc/databases/conf.d/*
 $ENV{HOME}/.databases/*

A configuration file contains one or more sections of the form:

 [mydb]
 dbd = Pg
 schema_search_path = foo, public
 dbic_schema = My::Schema::Class
 dbname = my_test_db
 host = 127.0.0.1
 username = guest
 password = 12345

Access to these files should be controlled by standard operating system
file access permissions; If the runner of the script can read the file,
they will be granted access.

It is the goal of this module to (as much as is reasonable) relieve the
burden of managing database connection information. Including, but not
limited to:

=over 4

=item *

Switching database names, drivers, and/or hosts without requiring
modification of source code (assuming code uses an ORM or compatible SQL).

=item *

Support Pg schemas as painlessly as possible.

=back

=head1 USAGE

=cut

has search_paths =>
  traits    => ['Array'],
  is        => 'ro',
  isa       => 'ArrayRef[Str]',
  handles   => {
      push_path    => 'push',
      unshift_path => 'unshift',
      remove_path  => 'delete',
      set_paths    => 'set',
    },
  lazy_build => 1,
;

has sources =>
  traits    => ['Hash'],
  isa       => 'HashRef[HashRef[Str]]',
  default   => sub { {} },
  handles   => {
      _set_source    => 'set',
      _get_source    => 'get',
      delete_source  => 'delete',
      has_source     => 'exists',
      forget_sources => 'clear',
      sources        => 'keys',
    },
;

has loaded =>
  traits    => ['Bool'],
  is        => 'ro',
  isa       => 'Bool',
  default   => 0,
  handles   => {
    _set_loaded    => 'set',
    reload_sources => 'unset',
  },
;

our %DBD_PARAMS;
$DBD_PARAMS{pg} ||= [ qw/ host hostaddr port options service sslmode /, [qw/ database dbname db /] ];
$DBD_PARAMS{mysql} ||= [ qw/ host port mysql_client_found_rows mysql_compression
  mysql_connect_timeout mysql_read_default_file mysql_read_default_group
  mysql_socket mysql_ssl mysql_ssl_client_key mysql_ssl_client_cert mysql_ssl_ca_file
  mysql_ssl_ca_path mysql_ssl_cipher mysql_local_infile mysql_multi_statements
  mysql_server_prepare mysql_embedded_options mysql_embedded_groups
  /, [qw/ database dbname db /] ];
$DBD_PARAMS{sqlite} ||= [ [qw/ dbname database db /] ];
$DBD_PARAMS{csv} ||= [qw/ f_dir csv_eol csv_sep_char csv_quote_char csv_escape_char csv_class /];
$DBD_PARAMS{dbm} ||= [qw/ f_dir ext mldbm lockfile store_metadata cols /, [qw/ type dbm_type /]];



before qw/ has_source sources _get_source /, sub {
  my $self = shift;
  $self->_load_sources unless $self->loaded;
};

sub _build_search_paths {
  [ "/etc/databases/conf.d",
    ($ENV{HOME} ? "$ENV{HOME}/.databases" : ()),
  ]
}



=head2 Getting Information About a Source

=head3 dsn(Str|HashRef $source)

=head3 dbd(Str|HashRef $source)

=head3 username(Str|HashRef $source)

=head3 password(Str|HashRef $source)

=head3 schema_search_path(Str|HashRef $source)

=head3 dbic_schema(Str|HashRef $source)

Each of the above return a string containing the requested information.

=head3 db_params(Str|HashRef $source)

The parameter portion of the DSN (example: "host=127.0.0.1;database=test_db")

=head3 dbi_args(Str|HashRef $source)

A list of three items, the dsn, username, and password. Useful when you
want to create your own DBI connection.

=head3 dbh(Str|HashRef $source, HashRef $dbi_opts?)

Open a new DBI connection to the database and execute any C<on_connect> commands.

=head3 dbic_schema_connect(Str|HashRef $source, HashRef $dbi_opts?, Str $dbic_schema?)

Use the DBIx::Schema class given to connect to the given data source.

=cut

# $source is Auto-extended via method modifier
sub dbh {
  my ($self, $source, $dbi_opts) = @_;
  require DBI;
  my $dbh = DBI->connect($self->dbi_args($source), $dbi_opts || { AutoCommit => 1, RaiseError => 1 });
  $self->on_connect($source)->($dbh);
  return $dbh;
}

sub dbix_simple {
  my $self = shift;
  require DBIx::Simple;
  DBIx::Simple->connect($self->dbh(@_));
}

# $source is Auto-extended via method modifier
sub dbic_schema_connect {
  my ($self, $source, $dbi_opts, $schema_class) = @_;
  $schema_class ||= $self->dbic_schema($source);
  eval "require $schema_class; 1" or die $@;
  $schema_class->connect(
    $self->dbi_args($source),
    $dbi_opts || { AutoCommit => 1, RaiseError => 1 },
    { on_connect_do => [ $self->on_connect_sql($source) ] }
  );
}

# $source is Auto-extended via method modifier
sub dsn {
  my ($self, $source) = @_;
  sprintf "dbi:%s:%s", $self->dbd($source), $self->db_params($source);
}

# $source is Auto-extended via method modifier
sub dbi_args {
  my ($self, $source) = @_;
  ($self->dsn($source), $self->username($source)//undef, $self->password($source)//undef);
}

# $source is Auto-extended via method modifier
sub on_connect_sql {
  my ($self, $source) = @_;
  my @sql;
  if ($self->dbd($source) eq 'Pg' and $self->schema_search_path($source)) {
    push @sql, sprintf "SET search_path = '%s'", $self->schema_search_path($source);
  }
  return @sql;
}

# $source is Auto-extended via method modifier
sub on_connect {
  my ($self, $source) = @_;
  sub {
    my $dbh = shift;
    $dbh->do($_) for $self->on_connect_sql($source);
  }
}

for (qw/ dbd username password schema_search_path dbic_schema /) {
  my $prop = $_;
  no strict 'refs';
  *{$prop} = sub {
    my ($self, $source) = @_;
    $self->_get_field($source, $prop);
  }
}

# $source is Auto-extended via method modifier
sub db_params {
  my ($self, $source) = @_;
  join ";",
    map "$$_[0]=$$_[1]",
    grep 1 < @$_,
    map [('ARRAY' eq ref($_)?$$_[0]:$_), $self->_get_field($source, $_)],
    $self->_db_param_fields( $source );
}


=head2 Data Loading

Connection information is searched for in all files contained in
directories in the search path. Standard unix permissions should be used to
restrict access to passwords. Files at the beginning of the search path
take priority over files at the end if more than one happen to define the
same source.

=head3 search_paths()

ArrayRef of paths to look in to find database connection information.

=head3 push_path(Str $new_path)

=head3 unshift_path(Str $new_path)

=head3 remove_path(Str $old_path)

=head3 set_paths


=head2 Source Management

=head3 delete_source(Str $source)

=head3 has_source(Str $source)

=head3 forget_sources()

=head3 sources()

=head3 reload_sources()

=cut


=head1 CONFIG FILE FORMAT

The configuration files use the INI format as parsed by
L<Config::Tiny|Config::Tiny>.

=over 4

=item *

Each section represents one source

=item *

Multiple sections may appear in each file

=back

=head2 Source Parameters

=over 4

=item dbd [REQUIRED]

Database driver as would be used in the second component of a DSN (see L<DBI|DBI>).

=item username

Database user

=item password

Database password for user given above

=item schema_search_path

Search path for use with PostgreSQL. Only used when C<dbd> is "Pg". Should
be a comma-separated list of schemas to use as the search path. Identifiers
should be quoted as appropriate since this module will not do that for you.

=back

In addition to the above parameters, any DBD-specific connection parameters
may also be present. Some of these parameters are treated specially:

=over 4

=item dbname | database | db  [TYPICALLY REQUIRED]

The database to connect to.

Note: Some drivers accept only a subset of the above; You do not need to
worry about that as this module will always use whichever name is
appropriate for the driver.

=back



=head1 PRIVATE METHODS

=head3 _get_field(HashRef $source, Str|ArrayRef $key)

Extract value associated with key from C<$source>. Returns empty list if key
does not exist. If C<$key> is an ArrayRef, tries all keys in the array
until one exists.

=cut

# $source is Auto-extended via method modifier
sub _get_field {
  my ($self, $source, $key) = @_;
  if ('ARRAY' eq ref($key)) {
    for (@$key) {
      my @res = $self->_get_field($source, $_);
      return $res[0] if @res;
    }
  } elsif (exists $$source{$key}) {
    return $$source{$key};
  }
  return;
}

=head3 _db_param_fields(HashRef $source)

Returns list of parameters that are accepted by the DBD associated with
C<$source>.

=cut

sub _db_param_fields {
  my ($self, $source) = @_;
  my $dbd = $self->dbd($source);
  die "Unrecognized DBD '$dbd'" unless exists $DBD_PARAMS{lc $dbd};
  @{$DBD_PARAMS{lc $dbd}};
}

=head3 _load_sources()

Loops through search path and loads all readable config files therein. Does
not check that sources were already loaded and does not clear existing sources.

=cut

sub _load_sources {
  my $self = shift;
  for (reverse @{$self->search_paths}) {
    next unless -d $_;
    for my $f (grep !$_->is_dir, dir($_)->children) {
      next unless $f =~ m#/[\w.+-]+$# and -r $f;
      my $config = Config::Tiny->read( $f );
      $self->_set_source( $_, { %{$$config{$_}} } ) for keys %$config;
    }
  }
  $self->_set_loaded;
}


around qw/ _get_field dbh dbi_args dsn db_params on_connect_sql on_connect dbic_schema_connect /, sub {
  my ($code, $self, $arg, @rest) = @_;
  $arg = $self->_get_source($arg) || return unless 'HASH' eq ref($arg);
  $code->($self, $arg, @rest);
};


no Moose;
__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 AUTHOR

 Dean Serenevy
 dean@serenevy.net
 http://dean.serenevy.net/

=head1 COPYRIGHT

This software is hereby placed into the public domain. If you use this
code, a simple comment in your code giving credit and an email letting
me know that you find it useful would be courteous but is not required.

The software is provided "as is" without warranty of any kind, either
expressed or implied including, but not limited to, the implied warranties
of merchantability and fitness for a particular purpose. In no event shall
the authors or copyright holders be liable for any claim, damages or other
liability, whether in an action of contract, tort or otherwise, arising
from, out of or in connection with the software or the use or other
dealings in the software.

=head1 SEE ALSO

perl(1)
