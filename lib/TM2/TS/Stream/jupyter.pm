package TM2::TS::Stream::jupyter::factory;

use Moose;
extends 'TM2::TS::Stream::factory';

use Data::Dumper;


has 'loop' => (
    is        => 'rw',
    isa       => 'IO::Async::Loop',
    );

has 'url' => (
    is => 'ro',
    isa => 'Str'
    );

around 'BUILDARGS' => sub {
    my $orig = shift;
    my $class = shift;
#warn "JUPYTER factory new $_[0]";
#warn "params $class $orig ".Dumper \@_;

    if (scalar @_ % 2 == 0) {
        return $class->$orig (@_);
    } else {
        my $url = shift;
        $url = $url->[0] if $url->isa ('TM2::Literal');
        return $class->$orig ({ url => $url, @_ });
    }
};

sub prime {
    my $elf = shift;
    my $out = [];
    tie @$out, 'TM2::TS::Stream::jupyter', $elf->loop, $elf->url, @_;
    return $out;
}

1;

package TM2::TS::Stream::jupyter;

use strict;
use warnings;

use Data::Dumper;

use vars qw( @ISA );

use TM2::TS::Stream;
@ISA = ('TM2::TS::Stream');

#--

use ZMQ::FFI qw(ZMQ_REQ ZMQ_PUB ZMQ_SUB ZMQ_FD ZMQ_REP ZMQ_ROUTER ZMQ_DEALER);
our $ZMQ = ZMQ::FFI->new(); # shared in this process

our $CONNECTION = {
          'kernel_name' => 'templescript',
          'key' => 'xxx',
          'control_port' => 45765,
          'ip' => '127.0.0.1',
          'signature_scheme' => 'hmac-sha256',
          'stdin_port' => 52463,
          'iopub_port' => 58281,
          'hb_port' => 56703,
          'transport' => 'tcp',
          'shell_port' => 33797
        };

use TM2::jupyter;

#--
my $iopub_ctr = 0;
our $shell_ctr = 0;
my $execution_ctr = 0;
my $control_ctr = 0;


use TM2::jupyter;

sub new_sockets {
    my ($loop, $connection, $tail, $stopper) = @_;

    $connection->{ip} =~ s/localhost/127.0.0.1/; # cleanup

    my $server = $connection->{transport} .'://'. $connection->{ip};

#-- iopub
    my $iopub = $ZMQ->socket(ZMQ_PUB);
    $iopub->bind( $server .':'. $connection->{iopub_port} );

#-- shell
    use Net::Async::0MQ::Socket;
    my $shell = Net::Async::0MQ::Socket->new(
	endpoint => $server .':'. $connection->{shell_port},
	type     => ZMQ_ROUTER,
	context  => $ZMQ,
	on_recv  => sub {
	    my $s = shift;
	    my ($sender, $delimiter, undef, @c) = $s->recv_multipart();
warn "shell -> ".Dumper [$sender, $delimiter, @c ];
	    my $req  = TM2::jupyter::decode_jupyter (@c);
#-- kernel info request --------------
	    if ($req->{header}->{msg_type} eq 'kernel_info_request') {
		TM2::jupyter::reply($s, $sender, $delimiter,
		       TM2::jupyter::mk_kernel_info( $req, $TM2::jupyter::shell_ctr++ ),
		       "shell" );

#-- execute request --------------
	    } elsif ($req->{header}->{msg_type} eq 'execute_request') {
		TM2::jupyter::reply( $iopub, 'status', $delimiter,
				     TM2::jupyter::jupyter_status ($req, 'busy', $iopub_ctr++),  # signal to the clients that we are busy
				     'iopub' );
		push @$tail, [ $sender, $delimiter, $req, $s, $iopub ];                          # response here, but also then the idle status

	    } else {
		$TM2::log->warn( "unhandled message '".$req->{header}->{msg_type}."'" );
	    }
	}
	);
    $loop->add( $shell );

#--
    my $hb = Net::Async::0MQ::Socket->new(
	endpoint => $server .':'. $connection->{hb_port},
	type     => ZMQ_REP,
	context  => $ZMQ,
	on_recv => sub {
	    my $s = shift;
	    my @c = $s->recv_multipart();
# warn "hb received ".Dumper \@c;
	    $s->send_multipart(\@c);
	}
	);
    $loop->add( $hb );

#--
    my $control = Net::Async::0MQ::Socket->new(
	endpoint => $server .':'. $connection->{control_port},
	type     => ZMQ_ROUTER,
	context  => $ZMQ,
	on_recv  => sub {
	    my $s = shift;
	    my ($sender, $delimiter, undef, @c) = $s->recv_multipart();
warn "control received ".Dumper \@c;
	    my $req  = TM2::jupyter::decode_jupyter (@c);
#warn Dumper $req;
	    if ($req->{header}->{msg_type} eq 'shutdown_request') {
		if ($req->{content}->{restart} eq "true") {
		    warn "cannot restart";
		}

		TM2::jupyter::reply($s, $sender, $delimiter,
		       {
			   header        => TM2::jupyter::_mk_header ($req, 'shutdown_reply', $control_ctr++),
			   parent_header => $req->{header},
			   metadata      => {},
			   content       => $req->{content},
		       },
		       "control" );
warn "stopping jupyter";
		$stopper->done;

	    } else {
		$TM2::log->warn( "unhandled control message '".$req->{header}->{msg_type}."'" );
	    }
	}
	);
    $loop->add( $control );

    return {
	control => $control,
	shell   => $shell,
	hb      => $hb,
	iopub   => $iopub,
    }, $connection; # and the connection configuration
}

sub remove_sockets {
    my $loop    = shift;
    my $sockets = shift;

    $loop->remove( $sockets->{control} );
    $loop->remove( $sockets->{hb} );
    $loop->remove( $sockets->{shell} );
    $sockets->{iopub}->close;
}


#== ARRAY interface ==========================================================

sub TIEARRAY {
    my $class = shift;
    my ($loop, $url, $tail) = @_;

#warn "TIE $loop, $url, $tail";

    use URI;
    my $uri   = URI->new( $url );

    return bless {
        creator   => $$,          # we can only be killed by our creator (not some fork()ed process)
        uri       => $uri,        # just for reference
	sockets   => undef,       # yet undefined
	config    => undef,       # yet undefined
        stopper   => $loop->new_future,
        loop      => $loop,
        tail      => $tail,
    }, $class;
}

sub DESTROY {
    my $elf = shift;
    return unless $elf->{creator} == $$;
#warn "DESTROY web server? waiting for stopper";
    $elf->{loop}->await( $elf->{stopper} ); # we do not give up so easily
    remove_sockets ($elf->{loop}, $elf->{sockets});
#warn "really DESTROYing";
}

sub FETCH {
    return undef;
}

sub PUSH {
    my $elf = shift;
warn "jupyter PUSH ".Dumper \@_;

    foreach (@_) {
        if ( ref($_) eq 'ts:kickoff') {
#warn "jupyter 0mq ports starting ";
	    $elf->{uri} =~ qr{^jupyter:(?<ip>[^;]+)(;.+)?};
	    my %config  = %+;
	    my %options = map { split /=/, $_ } split /;/, ($2//'');
	    my %connection = ( %$CONNECTION,
			       %config,       # basically ip information
			       %options,      # "quality" inside the URI: ports, banner ...
		             );
	    ($elf->{sockets}, $elf->{config}) = new_sockets( $elf->{loop}, \%connection, $elf->{tail}, $elf->{stopper},  );
            $TM2::log->info ("started jupyter at '".$elf->{uri}."'");

        } elsif ( ref ($_) eq 'ts:collapse') {

            if (tied ( @{ $elf->{tail} })) { # if this is something which honors the collapse
                push @{ $elf->{tail} }, $_;  # propagate it
            }
            $elf->{stopper}->done;

        } else { # some data
	    my %connection; # agenda

	    if ( ref( $_->[0] ) eq 'HASH' ) {
#warn "non kick HASH ".Dumper $_->[0];
		%connection = ( %$CONNECTION,   # default, shallow copy
				%{ $_->[0] } ), # HASH merged in

		$elf->{uri} =~ qr{^jupyter:(?<ip>[^;]+)(;.+)?};
		my %config  = %+;
		my %options = map { split /=/, $_ } split /;/, ($2//'');
		%connection = ( # merge
				%connection,
				%config,       # basically ip information
				%options,      # "quality" inside the URI: ports, banner ...
		              );

	    } elsif ( ref( $_->[0] ) eq 'TM2::Literal' 
 		        && $_->[0]->[0] =~ /^jupyter:/ ) {   # but we take the first value if it looks pertinent

		%connection = ( %$CONNECTION ),  # default, shallow copy

		# merge in incoming config
		$_->[0]->[0] =~ qr{^jupyter:(?<ip>[^;]+)(;.+)?};
		my %config  = %+;
		my %options = map { split /=/, $_ } split /;/, ($2//'');
		%connection = ( # merge
				%connection,
				%config,       # basically ip information
				%options,      # "quality" inside the URI: ports, banner ...
		              );
		# merge in existing config
		$elf->{uri} =~ qr{^jupyter:(?<ip>[^;]+)(;.+)?};
		%config  = %+;
		%options = map { split /=/, $_ } split /;/, ($2//'');
		%connection = ( # merge
				%connection,
				%config,       # basically ip information
				%options,      # "quality" inside the URI: ports, banner ...
		              );

	    } else {
		$TM2::log->logdie ("jupyter suspicious data at non-kickoff: ".Dumper $_->[0]);
	    }

	    ($elf->{sockets}, $elf->{config}) = new_sockets( $elf->{loop}, \%connection, $elf->{tail}, $elf->{stopper} );
            $TM2::log->info ("started jupyter (non-kickoff)");
        }
    }
}




sub FETCHSIZE {
    my $elf = shift;
    return 0;
}

sub CLEAR {
    my $elf = shift;
    $elf->{size} = 0;
}

our $VERSION = 0.02;

1;

__END__

# 		$iopub->send_multipart([
# map { (warn "iopub <- ".Dumper $_) && $_ }
# 		    'status', DELIMITER,
# 		    sign_msg(
# 			encode_jupyter( TM2::jupyter::jupyter_status ($req, 'busy', $iopub_ctr++) ) )
# 				       ]);

		# # signal to the clients the data we have
		# # prepare message to be returned on shell channel
		# _reply($s, $sender, $delimiter,
		#        TM2::jupyter::mk_execute_reply( $req, $TM2::jupyter::shell_ctr++ )		    );

