use strict;
use warnings;
use utf8;

use Test::More;
use Test::Exception;
use Test::Moose;
use TM2::TS::Test;

use Data::Dumper;
$Data::Dumper::Indent = 1;

use TM2::TempleScript;
use TM2::TempleScript::Test;

my $warn = shift @ARGV;
unless ($warn) {
    close STDERR;
    open (STDERR, ">/dev/null");
    select (STDERR); $| = 1;
}

use TM2; # to see the $log
use Log::Log4perl::Level;
$TM2::log->level($warn ? $DEBUG : $ERROR); # one of DEBUG, INFO, WARN, ERROR, FATAL

use constant DONE => 0;

use TM2::jupyterAble; # MUST BE before any TM2::TempleScript* stuff

sub _parse {
    my $t = shift;
    use TM2::Materialized::TempleScript;
    my $tm = TM2::Materialized::TempleScript->new (baseuri => 'tm:')
	->extend ('TM2::ObjectAble')
#            ->establish_storage ('templescript/*' => {})
            ->establish_storage ('*/*' => {})
        ->extend ('TM2::Executable')
             ;
    
    $tm->deserialize ($t);
    return $tm;
}

sub _mk_ctx {
    my $stm = shift;
    return [ { '$_'  => $stm, '$__' => $stm } ];
}

#== TESTS =====================================================================

unshift  @TM2::TempleScript::Parser::TS_PATHS, './ontologies/';

my $UR_PATH = $warn ? '../templescript/ontologies/' : '/usr/share/templescript/ontologies/';
$TM2::TempleScript::Parser::UR_PATH = $UR_PATH;

use TM2::Materialized::TempleScript;
my $env = TM2::Materialized::TempleScript->new (
    file => $TM2::TempleScript::Parser::UR_PATH . 'env.ts',                # then the processing map
    baseuri => 'ts:')
	->extend ('TM2::ObjectAble')
	->extend ('TM2::ImplementAble')
    ->sync_in;


# require_ok( 'TM2::jupyter' );


if (DONE) {
    my $AGENDA = q{jupyter control via connection: };

    my $hb_port = 56705;

    foreach my $ccc ( q| ( $JSON ~[application/perl]~ ) |,                    # via JSON and then HASH
		     qq| ( "jupyter:localhost;hb_port=$hb_port" ^^ xsd:uri )| # via jupyter uri
	            ) {
	my $tm = _parse ('

%include file:jupyter.ts

juppy isa ts:stream
return
     '.$ccc.'
    | <~ jupyter:localhost ~>
    |->> jupyter:response
    |->> jupyter:reply |->> io:write2log

alive isa ts:stream
return
    <++ every 5 secs
  | ( counter ~~> + 1 ==> <~~ counter )
  | ( "counter: {$0}" ) |->> io:write2log

counter holds "0" ^^ xsd:integer

    ');
	my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
			   ));

	use IO::Async::Loop;
	my $loop = IO::Async::Loop->new;

	use TM2::Materialized::TempleScript;
	$ctx = [ { '$loop' => $loop,
		   '$JSON' => TM2::Literal->new( qq|{ "hb_port" : "$hb_port" }|, 'urn:x-mime:application/json' ),
		 }, @$ctx ];

	use TM2::TS::Stream::jupyter;
	my $server = $TM2::TS::Stream::jupyter::CONNECTION->{transport} .'://'. $TM2::TS::Stream::jupyter::CONNECTION->{ip};

	use ZMQ::FFI qw(ZMQ_REQ ZMQ_PUB ZMQ_SUB ZMQ_FD ZMQ_REP ZMQ_ROUTER ZMQ_DEALER);
	my $hb = Net::Async::0MQ::Socket->new(
	    endpoint => $server .':'. $hb_port,
	    type     => ZMQ_REQ,
	    context  => $TM2::TS::Stream::jupyter::ZMQ,
	    on_recv => sub {
		my $s = shift;
		my @c = $s->recv_multipart();
		is_deeply (\@c, ['xxx', 'yyy'], $AGENDA.'heartbeat');
		#warn "hb back ".Dumper \@c;
	    }
	    );
	$loop->add( $hb );

	use IO::Async::Timer::Periodic;
	my $timer = IO::Async::Timer::Periodic->new(
	    interval => 10,
	    on_tick => sub {
		$hb->{socket}->send_multipart(['xxx', 'yyy']);
	    },
	);
	$timer->start;
	$loop->add( $timer );

	my $ts;
	{
	    (my $ss, $ts) = $tm->execute ($ctx);
	    $loop->watch_time( after => 16, code => sub { diag "stopping timeout " if $warn; push @$ss, bless [], 'ts:collapse'; } );
	}

	$loop->watch_time( after => 17, code => sub { diag "stopping loop " if $warn; $loop->stop; } );
	$loop->run;
    }
#warn Dumper $a;
}

if (1||DONE) {
    my $AGENDA = q{jupyter interfacing: };

    my $tm = _parse ('

%include file:jupyter.ts

juppy isa ts:stream
return
    <~ jupyter:localhost ~>
  |->> jupyter:response
  |->> jupyter:reply |->> io:write2log

alive isa ts:stream
return
    <++ every 5 secs
  | ( counter ~~> + 1 ==> <~~ counter )
  | ( "counter: {$0}" ) |->> io:write2log

counter holds "0" ^^ xsd:integer

    ');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

    my $a = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ { '$a' => $a,
	       '$loop' => $loop,
	     }, @$ctx ];

    use TM2::TS::Stream::jupyter;
    my $server = $TM2::TS::Stream::jupyter::CONNECTION->{transport} .'://'. $TM2::TS::Stream::jupyter::CONNECTION->{ip};

    use ZMQ::FFI qw(ZMQ_REQ ZMQ_PUB ZMQ_SUB ZMQ_FD ZMQ_REP ZMQ_ROUTER ZMQ_DEALER);
    my $hb = Net::Async::0MQ::Socket->new(
	endpoint => $server .':'. $TM2::TS::Stream::jupyter::CONNECTION->{hb_port},
	type     => ZMQ_REQ,
	context  => $TM2::TS::Stream::jupyter::ZMQ,
	on_recv => sub {
	    my $s = shift;
	    my @c = $s->recv_multipart();
	    is_deeply (\@c, ['xxx', 'yyy'], $AGENDA.'heartbeat');
#warn "hb back ".Dumper \@c;
	}
	);
    $loop->add( $hb );

    use IO::Async::Timer::Periodic;
    my $timer = IO::Async::Timer::Periodic->new(
	interval => 10,
	on_tick => sub {
	    $hb->{socket}->send_multipart(['xxx', 'yyy']);
	},
	);
    $timer->start;
    $loop->add( $timer );

    my $ts;
    {
        (my $ss, $ts) = $tm->execute ($ctx);
        $loop->watch_time( after => 160, code => sub { diag "stopping timeout " if $warn; push @$ss, bless [], 'ts:collapse'; } );
    }

    $loop->watch_time( after => 161, code => sub { diag "stopping loop " if $warn; $loop->stop; } );
    $loop->run;
warn Dumper $a;
}

done_testing;

__END__

if (0&&DONE) {
    my $AGENDA = q{embedding jupyter.ts: };
    
    my $tm = _parse ('

%include file:jupyter.ts

s1 isa ts:stream
return
   <+ every 5 secs |->> io:write2stdout

    ');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

    my $tsx = [];

    use TM2::Materialized::TempleScript;
    $ctx = [ @$ctx, {  '$a' => $tsx, '$loop' => $loop } ];

    my $ts;
    {
        (my $ss, $ts) = $tm->execute ($ctx);
        $loop->watch_time( after => 150, code => sub { diag "stopping timeout " if $warn; push @$ss, bless [], 'ts:collapse'; } );
    }
    $loop->watch_time( after => 151, code => sub { diag "stopping loop " if $warn; $loop->stop; } );
    $loop->run;

#warn Dumper $ts;
}

if (DONE) {
    my $AGENDA = q{submap sync: };
    diag $AGENDA if $warn;

    my $tm = _parse ('

aaa isa person
! AAA

submap isa topicmap
holds {
   bbb isa person
   ! BBB

   ccc isa person
   ! CCC

   names isa ts:function
   return
       ( $0 / name )

   ( person >> instances ) |->> names

}

');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));
    my $tss;

    if (1) {
	$tss = TM2::TempleScript::return ($ctx, q{ submap });
	is_singleton ($tss, 'tm:submap', $AGENDA.'rampup topic');
    }
    if (1) {
	$tss = TM2::TempleScript::return ($ctx, q{ submap ~~> });
	is_singleton ($tss, undef, $AGENDA.'rampup topicmap');
	ok ($tss->[0]->[0]->isa ('TM2'), $AGENDA.'map');
    }
    if (1) {
	$tss = TM2::TempleScript::return ($ctx, q{ submap ~~> ! });
	is_singleton ($tss, TM2::Literal->new ("AAA"), $AGENDA.'map execute, no data change');
    }
    if (1) {
	$tss = TM2::TempleScript::return ($ctx, q{ submap ~~> !! });
	ok (eq_set ([ "BBB", "CCC" ],
		    [ map { $_->[0]->[0] } @$tss ]), $AGENDA.'map execute, only inner data');
    }
    if (1) {
	$tss = TM2::TempleScript::return ($ctx, q{ submap ~~> !!! });
	ok (eq_set ([ "AAA", "BBB", "CCC" ],
		    [ map { $_->[0]->[0] } @$tss ]), $AGENDA.'map execute, inner and outer data');
    }
#warn Dumper $tss;
}

if (DONE) {
    my $AGENDA = q{maps as function bodies (async): };

    my $tm = _parse (q{

m1 isa ts:function
return {

   § isa ts:stream
     isa ts:side-effect
   return
     <<- 10 sec | <+ every 3 secs |->> io:write2log

}

s1 isa ts:stream
return
   (2) |->> m1 |->> io:write2log

s2 isa ts:stream
   isa ts:side-effect
return
   (3) |->> m1 |->> io:write2log

});

    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));
    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;
    $ctx = [ @$ctx, { '$loop' => $loop } ];

    my $tss;
    {
        (my $ss, $tss) = $tm->execute ($ctx);
        $loop->watch_time( after => 12, code => sub { diag "stopping timeout " if $warn; push @$ss, bless [], 'ts:collapse'; } );
    }
    $loop->watch_time( after => 13, code => sub { diag "stopping loop " if $warn; $loop->stop; } );
    $loop->run;

    is (ref ($tss->[-1]), 'ts:disable', $AGENDA.'disable last');
    pop @$tss; # forget about disable
    ok (eq_set ([ map { $_->[0]->[0] } @$tss ],
		[ 3, 3, 3 ]), $AGENDA.'3 strikes');

#warn Dumper $tss;
}

if (DONE) {
    my $AGENDA = q{maps as function bodies: };
    diag $AGENDA if $warn;

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

    foreach (1..1) {
	my $tm = _parse (q{

h isa ts:function
return
  ( $0 + 1 )


map isa ts:function
return {

   g isa ts:function
   return
     ( $0 ) |->> f |->> h

   f isa ts:function
   return
     ( $0 + 1 )

   § isa ts:stream
     isa ts:side-effect
   return
     ( $0 + 1 ) |->> f |->> g

}

s isa ts:stream
  isa ts:side-effect
return
    (3, 4, 5) 
  | zigzag
  |->> map

});
 	my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                           TM2::TempleScript::Stacked->new (orig => $env)
			   ));
	$ctx = [ @$ctx, { '$loop' => $loop } ];

	if (1) { # rampup, not the real thing
	    # get map tm
	    my (undef, $vv) = %{ $tm->object ('templescript/map', 'tm:map') } ;
	    my $tm  = $vv->{'templescript/map'};
	    my $ctm = TM2::TempleScript::PE::lookup_var ($ctx, '$__');                       # current control map
	    my $ipe = $tm->main_stream( [ { '$__'   => $ctm->unshift( $tm ) }, @$ctx ] );
#warn "original ".$pe->toString;
#warn "final resolved".$ipe->toString;
#	    is ((scalar ( grep { $_ =~ /PEcomp/ } keys %$ss )), 5, $AGENDA.'all PEcomp');
	    isa_ok( $ipe->last, 'PEide', $AGENDA.'last' );

#	    my $ope = $ipe->optimize( [ { '$__'   => $ctm->unshift( $tm ) }, @$ctx ] );
#warn $ope->toString;
	    my $tss = TM2::TempleScript::return( [ { '@_' => [ TM2::Literal->new( 2 ) ] }, @$ctx ], $ipe );
	    is_singleton ($tss, TM2::Literal->new( 6 ), $AGENDA.'optimized correct result');
	}

	if (1) { # rampup
	    my (undef, $vv) = %{ $tm->object ('templescript/query', 'tm:s') } ;
	    my $pe  = $vv->{'templescript/query'};
#warn "original ".$pe->toString;
#	    my $ss = {};
#	    my $ipe = $pe->resolve( $ss, $ctx );
#warn "resolve ".$ipe->toString;
#	    is ((scalar ( grep { $_ =~ /PEcomp/ } keys %$ss )), 8, $AGENDA.'all PEcomp');

#	    my $ope = $ipe->optimize( $ctx );
#warn $ope->toString;
	    my $tss = TM2::TempleScript::return( $ctx, $pe );
#warn Dumper $tss;
	    ok (eq_set ([map { $_->[0]->[0] } @$tss], [ 7, 8, 9 ]), $AGENDA.'result');
	}

	if (1) { # real thing
	    my (undef, $tss) = $tm->execute ($ctx);
	    ok (eq_set ([map { $_->[0]->[0] } @$tss], [ 7, 8, 9 ]), $AGENDA.'result, full featured');
#warn Dumper $tss;
	}
    }
}

if (DONE) {
    my $AGENDA = q{maps as function bodies for forks (sync,async and backgrounded): };
    diag $AGENDA if $warn;

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

#-- sync background
    foreach (1..3) {
	my $tm = _parse (q{

map isa ts:function
return {

   f isa ts:function
   return
     ( $0 + 1 )

   § isa ts:stream
     isa ts:side-effect
   return
     ( $0 + 1 ) |->> f

}

s isa ts:stream
  isa ts:side-effect
return
    (3, 4, 5) 
  | zigzag
  |->> map &

});
 	my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                           TM2::TempleScript::Stacked->new (orig => $env)
			   ));
	$ctx = [ @$ctx, { '$loop' => $loop } ];

	my $tss;
	{
	    (my $ss, $tss) = $tm->execute ($ctx);
	    $loop->watch_time( after => 10, code => sub { diag "stopping timeout " if $warn; push @$ss, bless [], 'ts:collapse'; } );
	}
	$loop->watch_time( after => 11, code => sub { diag "stopping loop " if $warn; $loop->stop; } );
	$loop->run;

	ok (eq_set ([map { $_->[0]->[0] } @$tss], [ 5, 6, 7 ]), $AGENDA.'local function, 1 process, 1 block, sync background, killed immediately, shortcut');
#warn Dumper $tss;
    }

#--
    foreach (1..3) {
	my $tm = _parse (q{

map isa ts:function
return {

   f isa ts:function
   return
     ( $0 + 1 )

   § isa ts:stream
     isa ts:side-effect
   return
     ( $0 + 1 ) |->> f

}

s isa ts:stream
  isa ts:side-effect
return
    (3, 4, 5) 
  | zigzag
  | -{ count | (1,  map ~[templescript/map]~> ) |->> ts:fusion (ts:forks) => $bg    # sync + ts:forks => still terminating
        |><|
        <<- 5 sec | @ $bg # |->> io:write2log
     }-

});
 	my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                           TM2::TempleScript::Stacked->new (orig => $env)
			   ));
	$ctx = [ @$ctx, { '$loop' => $loop } ];

	my $tss;
	{
	    (my $ss, $tss) = $tm->execute ($ctx);
	    $loop->watch_time( after => 10, code => sub { diag "stopping timeout " if $warn; push @$ss, bless [], 'ts:collapse'; } );
	}
	$loop->watch_time( after => 11, code => sub { diag "stopping loop " if $warn; $loop->stop; } );
	$loop->run;

	ok (eq_set ([map { $_->[0]->[0] } @$tss], [ 5, 6, 7 ]), $AGENDA.'local function, 1 process, 1 block, sync background, killed after 5 sec');
#warn Dumper $tss;
    }
#--
    foreach (1..3) {
	my $tm = _parse (q{

map isa ts:function
return {

   f isa ts:function
   return
     ( $0 + 1 )

   § isa ts:stream
     isa ts:side-effect
   return
     <<- 7 sec | <+ every 2 secs | ( 2 ) |->> f

}

s isa ts:stream
  isa ts:side-effect
return
    -{ count | (1,  map ~[templescript/map]~> ) |->> ts:fusion (ts:forks-joins) => $bg
       |><|
       <<- 10 sec | @ $bg |->> io:write2log
     }-

});

	my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                           TM2::TempleScript::Stacked->new (orig => $env)
			   ));
	$ctx = [ @$ctx, { '$loop' => $loop } ];
	my $tss;
	{
	    (my $ss, $tss) = $tm->execute ($ctx);
	    $loop->watch_time( after => 12, code => sub { diag "stopping timeout " if $warn; push @$ss, bless [], 'ts:collapse'; } );
	}
	$loop->watch_time( after => 13, code => sub { diag "stopping loop " if $warn; $loop->stop; } );
	$loop->run;

	ok (eq_set ([map { $_->[0]->[0] } @$tss], [ 3, 3, 3 ]), $AGENDA.'1 process, 1 block, async background, stopped after 7 sec, killed after 10 sec');
    }
#-- multiple inputs
    foreach (1..3) {
	my $tm = _parse (q{

map isa ts:function
return {

   f isa ts:function
   return
     ( $0 + 1 )

   § isa ts:stream
     isa ts:side-effect
   return
     <<- 7 sec | <+ every 2 secs |->> f

}

s isa ts:stream
  isa ts:side-effect
return
     ( 1, 2, 3 )
   | zigzag
   |-{ count | (1,  map ~[templescript/map]~> ) |->> ts:fusion (ts:forks-joins) => $bg
       |><|
       <<- 30 sec |_1_| @ $bg |->> io:write2log
     }-


});

	my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                           TM2::TempleScript::Stacked->new (orig => $env)
			   ));
	$ctx = [ @$ctx, { '$loop' => $loop } ];
	my $tss;
	{
	    (my $ss, $tss) = $tm->execute ($ctx);
	    $loop->watch_time( after => 10, code => sub { diag "stopping timeout " if $warn; push @$ss, bless [], 'ts:collapse'; } );
	}
	$loop->watch_time( after => 12, code => sub { diag "stopping loop " if $warn; $loop->stop; } );
	$loop->run;

	ok (eq_set ([map { $_->[0]->[0] } @$tss], [ (2)x3, (3)x3, (4)x3 ]), $AGENDA.'1 process, 1 block with 3 tuples, async background, stopped after 7 sec, killed after 30 sec');
#warn Dumper $tss; exit;
    }
#--
    foreach my $N (qw(2 3 4 5)) {
	next;
	my $KILLTIME = ($N * 2 + 1);

	my $tm = _parse (q[

map isa ts:function
return {

   f isa ts:function
   return
     ( $0 + 1 )

   § isa ts:stream
     isa ts:side-effect
   return
     <+ every 2 secs | ( 2 ) |->> f    # it NEVER collapses

}

s isa ts:stream
  isa ts:side-effect
return
    -{ count | (1,  map ~[templescript/map]~> ) |->> ts:fusion (ts:forks) => $bg
       |><|
       <<- ] . $KILLTIME . q[ sec | @ $bg |->> io:write2log
     }-

]);

	my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                           TM2::TempleScript::Stacked->new (orig => $env)
			   ));
	$ctx = [ @$ctx, { '$loop' => $loop } ];
	my $tss;
	{
	    (my $ss, $tss) = $tm->execute ($ctx);
	    $loop->watch_time( after => $KILLTIME + 3, code => sub { diag "stopping timeout " if $warn; push @$ss, bless [], 'ts:collapse'; } );
	}
	$loop->watch_time( after => $KILLTIME + 4, code => sub { diag "stopping loop " if $warn; $loop->stop; } );
	$loop->run;

	ok (eq_set ([map { $_->[0]->[0] } @$tss], [ (3) x $N ]), $AGENDA.'1 process, 1 block, async background, not stopped, killed after '.$KILLTIME.' sec');
#warn Dumper $tss;
    }
}


if (DONE) {
    my $AGENDA = q{maps as function bodies, no side effect: };
    diag $AGENDA if $warn;

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

#-- sync background, but no side effect, local
    foreach (1..1) {
	my $tm = _parse (q{

map isa ts:function
return {

   § isa ts:stream
   return
     ( $0 + 1 )

}

s isa ts:stream
  isa ts:side-effect
return
    (3) |->> map

});
 	my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                           TM2::TempleScript::Stacked->new (orig => $env)
			   ));
	$ctx = [ @$ctx, { '$loop' => $loop } ];

	my $tss;
	{
	    (my $ss, $tss) = $tm->execute ($ctx);
	    $loop->watch_time( after => 10, code => sub { diag "stopping timeout " if $warn; push @$ss, bless [], 'ts:collapse'; } );
	}
	$loop->watch_time( after => 11, code => sub { diag "stopping loop " if $warn; $loop->stop; } );
	$loop->run;

	is ((scalar @$tss), 0, $AGENDA.'local execution');
#warn Dumper $tss;
    }
#-- sync background, but side effect, fork
    foreach (1..1) {
	my $tm = _parse (q{

map isa ts:function
return {

   § isa ts:stream
     isa ts:side-effect
   return
     ( $0 + 1 )

}

s isa ts:stream
  isa ts:side-effect
return
    (3) |->> map &

});
 	my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                           TM2::TempleScript::Stacked->new (orig => $env)
			   ));
	$ctx = [ @$ctx, { '$loop' => $loop } ];

	my $tss;
	{
	    (my $ss, $tss) = $tm->execute ($ctx);
	    $loop->watch_time( after => 10, code => sub { diag "stopping timeout " if $warn; push @$ss, bless [], 'ts:collapse'; } );
	}
	$loop->watch_time( after => 11, code => sub { diag "stopping loop " if $warn; $loop->stop; } );
	$loop->run;

	is_singleton ($tss, TM2::Literal->new( 4 ), $AGENDA.'forked execution, side effect');
#warn Dumper $tss;
    }
#-- sync background, but no side effect, fork
    foreach (1..1) {
	my $tm = _parse (q{

map isa ts:function
return {

   § isa ts:stream # no ts:side-effect
   return
     ( $0 + 1 )

}

s isa ts:stream
  isa ts:side-effect
return
    (3) |->> map &

});
 	my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                           TM2::TempleScript::Stacked->new (orig => $env)
			   ));
	$ctx = [ @$ctx, { '$loop' => $loop } ];

	my $tss;
	{
	    (my $ss, $tss) = $tm->execute ($ctx);
	    $loop->watch_time( after => 10, code => sub { diag "stopping timeout " if $warn; push @$ss, bless [], 'ts:collapse'; } );
	}
	$loop->watch_time( after => 11, code => sub { diag "stopping loop " if $warn; $loop->stop; } );
	$loop->run;

	is ((scalar @$tss), 0, $AGENDA.'forked execution');
#warn Dumper $tss;
    }
}

if (DONE) {
    my $AGENDA = q{adhoc map as query: };
    diag $AGENDA if $warn;

    my $tm = _parse ('

');
    my $ctx = _mk_ctx (TM2::TempleScript::Stacked->new (orig => $tm, upstream =>
                       TM2::TempleScript::Stacked->new (orig => $env)
                       ));

    use IO::Async::Loop;
    my $loop = IO::Async::Loop->new;

    $ctx = [ @$ctx, { '$loop' => $loop } ];
    my $ts = TM2::TempleScript::return ($ctx, q{ ( 42 ) | { ( $0 ) } ! });

#warn Dumper $ts;
      is_singleton ($ts, TM2::Literal->new (42), $AGENDA.'result');
}

done_testing;

__END__

if (0&&DONE) {
    my $AGENDA = q{stream compilation with extension: };

    my $tm = _parse ('

s1 isa ts:stream
return
   (1)

s2 isa ts:stream
return
   (2)

xxx isa ts:slurper

yyy                isa ts:burper

zzz isa ts:slurper isa ts:burper

aaa isa restdaemon

bbb isa restdaemon
= http://xxx.com/

ccc isa restdaemon
= http://yyy.com/

    ');
    {
        package Slurpy;
        use Moose::Role;

        around 'main_stream' => sub {
            my $orig = shift;
            my $self = shift;
            my $ms = $self->$orig (@_);

            my %iol = map { $_ => [ 1, 0 ] }
                      $self->instances ('ts:slurper');
            map { $iol{$_}->[0] //= 0 , $iol{$_}->[1] = 1 }
                      $self->instances ('ts:burper');
            if (%iol) {
                $ms = PEio->new (hta => $ms, # the original
                                 iol => [ map { [ $_, @{$iol{$_}} ] } keys %iol ]); # the sync info
            }
            return $ms;
        };
        1;
    }

    my $cpr = $tm->main_stream ($CTX);
    isa_ok ($cpr, 'PEpp', $AGENDA);
#--
    use Moose::Util qw( apply_all_roles );
    apply_all_roles( $tm, 'Slurpy' );

    $cpr = $tm->main_stream ($CTX);
    isa_ok ($cpr, 'PEio', $AGENDA);
#    warn $cpr->toString;
#    exit;

    my @iol = sort { $a->[0] cmp $b->[0] } @{ $cpr->iol };
    ok (eq_array (\@iol, [  [ 'tm:xxx', 1, 0 ],
                            [ 'tm:yyy', 0, 1 ],
                            [ 'tm:zzz', 1, 1 ] ],
                 ), $AGENDA.'iol');
#--
    {
        package RESTless;
        use Moose::Role;

        around 'main_stream' => sub {
            my $orig = shift;
            my $self = shift;

            $self->internalize ('fake');
            use Test::More;
            diag "faking rest:daemons in map" if $warn;
            return $self->$orig (@_);
        };
        1;
    }
    apply_all_roles( $tm, 'RESTless' );
    $cpr = $tm->main_stream ($CTX);
    isa_ok ($cpr, 'PEio', $AGENDA);
    is ($tm->tids ('fake'), 'tm:fake', $AGENDA.'testing for fakes');
}

if (0&&DONE) { # structural test
    my $AGENDA = q{structural test, parsing: };
    # explicit topic first
    my $tm = _parse ('
aaa isa person

tx1 isa ts:modification
   after ts:initiate
modifies
   42 ==> << zoomer */* aaa

');

    ok (eq_set ([ $tm->instances  ('ts:modification') ], ['tm:tx1']  ),          $AGENDA.'explicit: only modifications');

    my $init = $tm->tids ( \ 'http://templescript.org/ns/events/initiate');
    ok ($init,                                                                 $AGENDA.'explicit: found load event transaction');
    my $term = $tm->tids ( \ 'http://templescript.org/ns/events/terminate');
    ok ($term,                                                                 $AGENDA.'explicit: found unload event transaction');

    ok (eq_set ([ $tm->instancesT ('ts:transition') ],    ['tm:tx1', $init, $term]  ), $AGENDA.'explicit: all transitions');
    ok (eq_set ([ $tm->instancesT ('ts:transaction') ],   [          $init, $term]  ), $AGENDA.'explicit: all transactions');
    ok (eq_set ([ $tm->instancesT ('ts:modification') ],  ['tm:tx1']  ),               $AGENDA.'explicit: all modifications');

    my @followeds = 
	map  { $_->[TM2->PLAYERS]->[0] }
        $tm->match (TM2->FORALL, type => 'ts:following',   iplayer => 'tm:tx1');
    is ((scalar @followeds), 1,                                               $AGENDA.'only one tx followed initiate');
    is ($followeds[0], $init,                                                 $AGENDA.'only followed is initiate');

    # SUPER HACK
    $TM2::toplet_ctr = 0;  # reset that to have maps more comparable

    # implicit now
    my $tm2 = _parse ('

aaa isa person

42 ==> << zoomer */* aaa

');

    my $diff = TM2::diff ($tm, $tm2);
    ok (eq_set ([ keys %{ $diff->{plus}  } ],  [ 'tm:tx1' ]  ),             $AGENDA.'implicit: same as explicit, but without some topics');
    ok (eq_set ([ keys %{ $diff->{minus} } ],  [ 'tm:uuid-0000000000' ]  ), $AGENDA.'implicit: same as explicit, but with some topics');

    is (
	(scalar keys %{ $diff->{modified}->{'tm:followed'}->{plus} }),
	(scalar keys %{ $diff->{modified}->{'tm:followed'}->{minus} }),
	$AGENDA.'implicit: same nr followed as explicit');
}

if (0&&DONE) {
    my $AGENDA = q{structural after build: };
    
    my $tm = _parse ('
aaa

tx1 isa ts:modification
   after ts:initiate
modifies
   42 ==> << zoomer */* aaa

tx2 isa ts:transition
   after tx1
transacts """
   return TM2::Literal->new (43);
"""^^lang:perl !

');
    my $pn = TM2::Executable::PetriNet->new ([ { '$__' => $tm } ]);
#    warn Dumper $pn;
    isa_ok ($pn, 'TM2::Executable::PetriNet', $AGENDA.'petrinet');

#--
    my ($i) = $pn->things ('ts:initiate');
    isa_ok ($i, 'Transition', $AGENDA.'initiate a transition');
    is ($i->map, $tm, $AGENDA.'initiate map');
    my $t0 = TM2::ContiguousTime::time();
    is (1, $i->ignite ([ { '$__' => $tm, '$_' => $tm },
			 { '$__' => $TM2::TempleScript::Parser::ur } ]), $AGENDA.'initiate invoked');
    ok ($t0 < $i->last_ignition, $AGENDA.'rippled');

    my ($t) = $pn->things ('ts:terminate');
    isa_ok ($t, 'Transition', $AGENDA.'initiate a transition');
    is ($t->map, $tm, $AGENDA.'initiate map');
    $t0 = TM2::ContiguousTime::time();
    is (1, $t->ignite ([ { '$__' => $tm, '$_' => $tm },
			 { '$__' => $TM2::TempleScript::Parser::ur } ]), $AGENDA.'terminate invoked');
    ok ($t0 < $t->last_ignition, $AGENDA.'rippled');
#--
    my ($tx1) = $pn->things ('tm:tx1');
    isa_ok ($tx1, 'Transition', $AGENDA.'tx1 a transition');
    isa_ok ($tx1->code, 'PE', $AGENDA.'code PEcpr');

    my $res = TM2::TempleScript::PE::eval ([ { '$__' => $tm, '$_' => $tm } ], $tx1->code);
    is_singleton ($res, new TM2::Literal (42), $AGENDA.'code execution ');
    my (undef, $vv) = %{ $tm->object ('templescript/literal', 'tm:aaa') } ;
    is_deeply ($vv->{'templescript/literal;numeric=decimal'}, new TM2::Literal (42), $AGENDA.'assignment');

    $t0 = TM2::ContiguousTime::time();
    is_singleton ($tx1->ignite ([ { '$__' => $tm, '$_' => $tm },
				  {  } ]),
		  TM2::Literal->new (42), $AGENDA.'tx1 invoked');
    ok ($t0 < $tx1->last_ignition, $AGENDA.'rippled');
#--
    my ($tx2) = $pn->things ('tm:tx2');
    isa_ok ($tx2, 'Transition', $AGENDA.'tx2 a transition');
    isa_ok ($tx1->code, 'PE', $AGENDA.'code PEcpr');
    $res = TM2::TempleScript::PE::eval ([ { '$__' => $tm, '$_' => $tm } ], $tx2->code);
    is_singleton ($res, new TM2::Literal (43), $AGENDA.'code execution ');

    $t0 = TM2::ContiguousTime::time();
    is_singleton ($tx2->ignite ([ { '$__' => $tm, '$_' => $tm },
				  { } ]),
		  TM2::Literal->new (43), $AGENDA.'tx2 invoked');
    ok ($t0 < $tx2->last_ignition, $AGENDA.'rippled');
}

if (0&&DONE) {
    my $AGENDA = q{structural acquisition after build: };
    my $tm = _parse ('
aaa

bbb

ccc
  depends-on aaa
  depends-on bbb


');
    throws_ok {
	TM2::Executable::PetriNet->new ([ { '$__' => $tm } ]);
    } qr{no.+code}i, $AGENDA.'transition code missing';


#--
    $tm = _parse ('
aaa

bbb

ccc isa ts:function
  depends-on aaa
  depends-on bbb
returns ( 42 )

');

    my $pn = TM2::Executable::PetriNet->new ([ { '$__' => $tm } ]);
    my ($ccc) = $pn->things ('tm:ccc');
    isa_ok ($ccc, 'Place', $AGENDA.'ccc a place');

    my ($ccc2) = $pn->things ('tm:aaa,tm:bbb -> tm:ccc');
    isa_ok ($ccc2, 'Transition', $AGENDA.'...-> ccc a transition');
    isa_ok ($ccc2->code, 'PE', $AGENDA.'code PE');
#    warn Dumper $ccc2->code; exit;

    $tm->objectify (undef, 'tm:aaa', TM2::Literal->new (44));
    $tm->objectify (undef, 'tm:bbb', TM2::Literal->new (45));

    my $res = TM2::TempleScript::PE::eval ([ { '$__' => $tm, '$_' => $tm } ], $ccc2->code);
    is_singleton ($res, new TM2::Literal (42), $AGENDA.'code execution ');
}

if (0&&DONE) {
    my $AGENDA = q{structural mime converter after build: };
    
    my $tm = _parse ('
aaa

bbb
  depends-on aaa
~ urn:x-mime:something/bbb

');
    throws_ok {
	TM2::Executable::PetriNet->new ([ { '$__' => $tm } ]);
    } qr{no.*mime}i, $AGENDA.'mime type missing';

#--
    $tm = _parse ('
aaa
isa ~ urn:x-mime:something/aaa

bbb
  depends-on aaa
isa ~ urn:x-mime:something/bbb

* isa ts:converter
   ts:mimetype @ ts:input  : templescript/literal
   ts:mimetype @ ts:output : something/bbb
return ( 44 )

');
    my $pn = TM2::Executable::PetriNet->new ([ { '$__' => $tm } ]);

    my ($aaa) = $pn->things ('tm:aaa');
    isa_ok ($aaa, 'Place', $AGENDA.'aaa isa place');

    my ($bbb) = $pn->things ('tm:bbb');
    isa_ok ($bbb, 'Place', $AGENDA.'bbb isa place');

    my ($aaabbb) = $pn->things ('tm:aaa -> tm:bbb');
    isa_ok ($aaabbb, 'Transition', $AGENDA.'aaa->bbb isa transition');
    isa_ok ($aaabbb->code, 'PE', $AGENDA.'aaa->bbb code PE');

    $tm->objectify ('something/aaa', 'tm:aaa', TM2::Literal->new (42));
    my $res = TM2::TempleScript::PE::eval_cpr ([ { '$__' => $tm, '$_' => $tm } ], $aaabbb->code);
    is_singleton ($res, new TM2::Literal (42), $AGENDA.'converter execution ');
    is_object ($tm->object (undef, 'tm:bbb'), 'something/bbb', TM2::Literal->new (44));
#    warn Dumper $pn;
}

if (0&&DONE) { # structural only
    my $WHAT = q{transitions with technologies: };
    my $tm = _parse (q{

tx1 isa ts:transition
  isa ts:synchronous
  isa ts:non-forking
transacts
   42

tx2 isa ts:transition
  isa ts:asynchronous
  isa ts:forking
transacts
   42

tx3 isa ts:transition
  isa ts:asynchronous
transacts
   42

tx4 isa ts:transition
  isa non-forking
transacts
   42

tx5 isa ts:transition
transacts
   42

});

    my $pn = TM2::Executable::PetriNet->new ([ { '$__' => $tm } ]);
    my ($tx1, $tx2, $tx3, $tx4, $tx5) = $pn->things (qw(tm:tx1 tm:tx2 tm:tx3 tm:tx4 tm:tx5));
    does_ok ($tx1, 'Synchronous',  "transition is synchronous");
    does_ok ($tx1, 'InProcess',    "transition is non-forking");

    does_ok ($tx2, 'Asynchronous', "transition is asynchronous");
    does_ok ($tx2, 'Forking',      "transition is forking");

    does_ok ($tx3, 'Asynchronous', "transition is asynchronous");
    does_ok ($tx3, 'InProcess',    "transition default non-forking");

    does_ok ($tx4, 'Synchronous',  "transition default synchronous");
    does_ok ($tx4, 'InProcess',    "transition is non-forking");

    does_ok ($tx5, 'Synchronous',  "transition default synchronous");
    does_ok ($tx5, 'InProcess',    "transition default non-forking");
}


