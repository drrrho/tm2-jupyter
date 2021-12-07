package TM2::jupyter;

use strict;
use warnings;

use JSON;
use Data::Dumper;

=head1 NAME

TM2::jupyter - TempleScript support for the Jupyter protocol

=cut

our $connection = {
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

# invoke the notebook:
# $ jupyter-notebook --debug --NotebookApp.log_level=DEBUG --Session.key="b''" --KernelManager.ip=127.0.0.1 --KernelManager.shell_port=33797 --KernelManager.stdin_port=52463 --KernelManager.iopub_port=58281 --KernelManager.hb_port=56703 --KernelManager.control_port=45725

use ZMQ::FFI qw(ZMQ_PUB ZMQ_SUB ZMQ_FD ZMQ_REP ZMQ_ROUTER);
#our $ctx = ZMQ::FFI->new();

#--
my $iopub_ctr = 0;
my $shell_ctr = 0;
my $execution_ctr = 0;
my $control_ctr = 0;



sub reply {
    my ($s, $sender, $delimiter, $resp, $channel) = @_;

    $s->send_multipart([
map { (warn "$channel <- ".Dumper $_) && $_ }
	$sender, $delimiter,
	sign_msg(
	    encode_jupyter( $resp ) )
		       ]);
}


sub decode_jupyter {
    my ($header, $parent, $metadata, $content) = @_;
    my $msg = {
	header   => from_json( $header ),
	parent   => from_json( $parent ),
	metadata => from_json( $metadata ),
	content  => from_json( $content ),
    };
    $msg->{msg_id}   = $msg->{header}->{msg_id};
    $msg->{msg_type} = $msg->{header}->{msg_type};
    return $msg;
}

sub encode_jupyter {
    my $msg    = shift;

    return 
        (map { to_json( $_ ) }
         map { $msg->{$_}    }  qw(header parent_header metadata content) )
	;
}

sub _mk_header {
    my ($req, $type, $msg_id) = @_;

    use POSIX qw(strftime);
    use Time::HiRes;
    my $now = Time::HiRes::time();

    return {
	'msg_id'   => ( sprintf "%010d", $msg_id ),
	'session'  => $req->{header}->{session},
	'msg_type' => $type,
	'version'  => '5.0',
	'date'     => ( strftime('%Y-%m-%dT%H:%M:%sZ', gmtime($now)) ),
    };
}

sub mk_kernel_info {
    my ($req, $shell_ctr) = @_;
    return {
	header        => _mk_header ($req, 'kernel_info_reply', $TM2::jupyter::shell_ctr++),
	parent_header => $req->{header},
	metadata      => {},
	content       => {
	    'status' => 'ok',
	    'protocol_version' => '5.0',
	    'implementation' => 'itemplescript',
	    'implementation_version' => '0.0.1',
	    
	    'language_info' => {
		'name' => 'templescript',
		'version' =>  '1.0',
		'mimetype' => 'text/templescript',
		'file_extension' => '.ts',
	    },
	    'banner' => 'TempleScript',
	    'help_links' => [
		{'text' => "TempleScript.org", 'url' => 'http://templescript.org/'},
		],
	},
    };
}

sub mk_execute_reply {
    my ($req, $shell_ctr) = @_;
    return {
	header        => _mk_header ($req, 'execute_reply', $shell_ctr++),
	parent_header => $req->{header},
	metadata      => {},
	content => {
	    'status' => 'ok',
	    'execution_count' => $execution_ctr++,
	    metadata        => {},
#		    data            => { 'text/plain' => 'ZZZZZZZZZZZZZZZ', 'text/html' => 'HHHHHHHHHHHHHHH' },
		# 'status' => 'error',
		# 'ename'  =>  'XXXXXXXXXXXXXXXXXX',
		# 'evalue' =>  'foo',
		# 'traceback' => 'YYYYYYYYYYYYYYY',
	},
    };
}

sub sign_msg {
    unless ($connection->{key}) {
	return ('', @_);
	
    } else {
	use Digest::SHA qw(hmac_sha256_hex);
	my $data = join ('', @_);
	my $sig  = hmac_sha256_hex( $data, $connection->{key} );
	return ( $sig, @_ );
    }
}

sub jupyter_status {
    my $req    = shift;
    my $status = shift;
    my $msg_id = shift;

    return {
	header        => _mk_header ($req, 'status', $msg_id),
	parent_header => $req->{header},
	metadata => {},
	content => {
	    execution_state => $status,
	},
    };
}

sub jupyter_response {
    my $req    = shift;
    my $msg_id = shift;
    my $iopub  = shift;

    if ($req->{header}->{msg_type} eq 'kernel_info_request') {
	return {
	    header        => _mk_header ($req, 'kernel_info_reply', $msg_id),
	    parent_header => $req->{header},
	    metadata      => {},
	    content       => {
		'status' => 'ok',
		'protocol_version' => '5.0',
		'implementation' => 'itemplescript',
		'implementation_version' => '0.0.1',
		
		'language_info' => {
		    'name' => 'templescript',
		    'version' =>  '1.0',
		    'mimetype' => 'text/templescript',
		    'file_extension' => '.ts',
		},
		'banner' => 'TempleScript',
		'help_links' => [
		    {'text' => "TempleScript.org", 'url' => 'http://templescript.org/'},
		    ],
	    },
	};

    } elsif ($req->{header}->{msg_type} eq 'execute_request') {
	use constant DELIMITER => '<IDS|MSG>';

	# 1) signal to the clients that we are busy
	$iopub->send_multipart([
map { (warn "iopub <- ".Dumper $_) && $_ }
	    'status', DELIMITER,
	    sign_msg(
	    encode_jupyter( jupyter_status ($req, 'busy', $iopub_ctr++) ) )
			   ]);
	# 2) signal to the clients the data we have
	$iopub->send_multipart([
map { (warn "iopub <- ".Dumper $_) && $_ }
	    'execute_result', DELIMITER,
	    sign_msg(
	    encode_jupyter( {
		header        => _mk_header ($req, 'execute_result', $iopub_ctr++),
		parent_header => $req->{header},
		metadata      => {},
		content       => {
		    metadata        => {},
		    data            => { 'text/plain' => 'ZZZZZZZZZZZZZZZ', 'text/html' => 'HHHHHHHHHHHHHHHXXXXXX' },
		    execution_count => $execution_ctr,
		},
			    } ) )
					 ]);
	# 3) signal to the clients that we are idle again
	$iopub->send_multipart([
map { (warn "iopub <- ".Dumper $_) && $_ }
	    'status', DELIMITER,
	    sign_msg(
	    encode_jupyter( jupyter_status ($req, 'idle', $iopub_ctr++) ) )
					 ]);
	# 4) prepare message to be returned on shell channel
	return {
	    header        => _mk_header ($req, 'execute_reply', $shell_ctr++),
	    parent_header => $req->{header},
	    metadata      => {},
	    content => {
		'status' => 'ok',
		'execution_count' => $execution_ctr++,
		    metadata        => {},
#		    data            => { 'text/plain' => 'ZZZZZZZZZZZZZZZ', 'text/html' => 'HHHHHHHHHHHHHHH' },
		# 'status' => 'error',
		# 'ename'  =>  'XXXXXXXXXXXXXXXXXX',
		# 'evalue' =>  'foo',
		# 'traceback' => 'YYYYYYYYYYYYYYY',
	    },
	};

    } elsif ($req->{header}->{msg_type} eq 'shutdown_request') {
	if ($req->{content}->{restart} eq "true") {
	    warn "cannot restart";
	}
	return {
	    header        => _mk_header ($req, 'shutdown_reply', $shell_ctr++),
	    parent_header => $req->{header},
	    metadata      => {},
	    content       => $req->{content},
	};
    }
}


sub setup_responders {
    my $ctx  = shift;
    my $loop = shift;
    my $connection = shift;
    my $iopub      = shift;

    my $server = $connection->{transport} .'://'. $connection->{ip};

#--
    use Net::Async::0MQ::Socket;
    my $shell = Net::Async::0MQ::Socket->new(
	endpoint => $server .':'. $connection->{shell_port},
	type     => ZMQ_ROUTER,
	context  => $ctx,
	on_recv  => sub {
	    my $s = shift;
	    my ($sender, $delimiter, undef, @c) = $s->recv_multipart();
warn "shell -> ".Dumper [$sender, $delimiter, @c ];
	    my $req  = decode_jupyter (@c);
	    $s->send_multipart([
map { (warn "shell <- ".Dumper $_) && $_ }
		$sender, $delimiter,
		sign_msg(
		    encode_jupyter( jupyter_response ($req, $shell_ctr++, $iopub) ) )
			       ]);
	}
	);
    $loop->add( $shell );

#--
    my $hb = Net::Async::0MQ::Socket->new(
	endpoint => $server .':'. $connection->{hb_port},
	type     => ZMQ_REP,
	context  => $ctx,
	on_recv => sub {
	    my $s = shift;
	    my @c = $s->recv_multipart();
warn "hb received ".Dumper \@c;
	    $s->send_multipart(\@c);
	}
	);
    $loop->add( $hb );

# my $hb = $ctx->socket( ZMQ_ROUTER );
# $ctx->proxy( $hb, $hb );


#--
    my $control = Net::Async::0MQ::Socket->new(
	endpoint => $server .':'. $connection->{control_port},
	type     => ZMQ_ROUTER,
	context  => $ctx,
	on_recv => sub {
	    my $s = shift;
	    my ($sender, $delimiter, undef, @c) = $s->recv_multipart();
warn "control received ".Dumper \@c;
	    my $req  = decode_jupyter (@c);
warn Dumper $req;
	    $s->send_multipart([
		$sender, $delimiter,
map { (warn "control back: ".Dumper $_) && $_ }
		sign_msg(
		    encode_jupyter( jupyter_response ($req, $control_ctr++, $iopub) ) )
			       ]);
	    $loop->stop;
	}
	);
    $loop->add( $control );
}

=pod

=head1 AUTHOR

Robert Barta, C<< <rho at devc.at> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2020 Robert Barta.

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

Any use, modification, and distribution of the Standard or Modified
Versions is governed by this Artistic License. By using, modifying or
distributing the Package, you accept this license. Do not use, modify,
or distribute the Package, if you do not accept this license.

If your Modified Version has been derived from a Modified Version made
by someone other than you, you are nevertheless required to ensure that
your Modified Version complies with the requirements of this license.

This license does not grant you the right to use any trademark, service
mark, tradename, or logo of the Copyright Holder.

This license includes the non-exclusive, worldwide, free-of-charge
patent license to make, have made, use, offer to sell, sell, import and
otherwise transfer the Package with respect to any patent claims
licensable by the Copyright Holder that are necessarily infringed by the
Package. If you institute patent litigation (including a cross-claim or
counterclaim) against any party alleging that the Package constitutes
direct or contributory patent infringement, then this Artistic License
to you shall terminate on the date that such litigation is filed.

Disclaimer of Warranty: THE PACKAGE IS PROVIDED BY THE COPYRIGHT HOLDER
AND CONTRIBUTORS "AS IS' AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE, OR NON-INFRINGEMENT ARE DISCLAIMED TO THE EXTENT PERMITTED BY
YOUR LOCAL LAW. UNLESS REQUIRED BY LAW, NO COPYRIGHT HOLDER OR
CONTRIBUTOR WILL BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES ARISING IN ANY WAY OUT OF THE USE OF THE PACKAGE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


=cut

our $VERSION = '0.01';

1;
