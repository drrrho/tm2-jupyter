%include file:jupyter.atm


jupyter:reply isa ts:function
              isa ts:side-effect
return """
   my ($sender, $delimiter, $req, $resp, $s, $iopub) = @_;
# warn "jsend ".$sender, $delimiter, $s, $iopub;

use Data::Dumper;
warn "jsend".Dumper $resp;

   use TM2::jupyter;
   TM2::jupyter::reply( $iopub, $resp->{header}->{msg_type},
                                                   $delimiter, $resp, "iopub");
   TM2::jupyter::reply( $iopub, "status",          $delimiter,
			                                       TM2::jupyter::jupyter_status ($req, "idle", $TM2::jupyter::iopub_ctr++),
			                                              "iopub" );
   TM2::jupyter::reply( $s,     $sender,           $delimiter, $resp, "shell" );

   return TM2::Literal->new( "jupyter execution (".$resp->{header}->{msg_type}.") of >>>".$req->{content}->{code}."<<<" );
""" ^^ lang:perl !

jupyter:response isa ts:function
return """
   my ($sender, $delimiter, $req, $s, $iopub) = @_;

   use TM2::jupyter;
# use Data::Dumper;
# warn ">>>>". $req->{content}->{code};

   my $cs = [ { "\$_" => $tm } ];

   my ($tss, $err, $resp);
   eval {
       use TM2::TempleScript;
       $tss = TM2::TempleScript::return( $cs, $req->{content}->{code} );
   }; if ($@) {
       $resp = {
		    header        => TM2::jupyter::_mk_header ($req, "error", $TM2::jupyter::shell_ctr++),
		    parent_header => $req->{header},
		    metadata      => {},
		    content       => {
                       "status"    => "error",
                       "ename"     => "compile_error",
                       "evalue"    => $@,
                       "traceback" => [ $@ ],
                    },
		};
   } else {
       use TM2::TS::TS::IO;
       $resp = {
		    header        => TM2::jupyter::_mk_header ($req, "execute_result", $TM2::jupyter::iopub_ctr++),
		    parent_header => $req->{header},
		    metadata      => {},
		    content       => {
			metadata        => {},
			data            => { "text/html" => TM2::TS::TS::IO::as_html_table( $tss, $tm->baseuri ) },
			execution_count => $TM2::jupyter::execution_ctr++,
		    },
		};
    }
   return [ $sender, $delimiter, $req, $resp, $s, $iopub ];
""" ^^ lang:perl !



%cancel

xxx isa ts:function
return """
   my $loop = TM2::TempleScript::PE::lookup_var ($cs, '$loop');
   use TM2::TS::Stream::zeromq;
   my $zmq  = $TM2::TS::Stream::zeromq::CTX;
   warn "XXXXX $loop $zmq";

   use TM2::jupyter;
   my $connection = $TM2::jupyter::connection;
   my $server = $connection->{transport} .'://'. $connection->{ip};

   use ZMQ::FFI qw(ZMQ_PUB ZMQ_SUB ZMQ_FD ZMQ_REP ZMQ_ROUTER);
   my $iopub = $zmq->socket(ZMQ_PUB);
   $iopub->bind( $server .':'. $connection->{iopub_port} );

#  $cs->[0]->{'$jupyter:iopub'} = $iopub;
   TM2::jupyter::setup_responders ($zmq, $loop, $connection, $iopub);
""" ^^ lang:perl !


§ isa ts:stream
return
     ->> xxx
   | <+ every 3 secs | ( "jupyter" ) |->> io:write2stderr

