package TM2::jupyterAble;

use strict;
use warnings;

use Moose::Role;

use Data::Dumper;

use TM2::TempleScript::Parser;
BEGIN {
    $TM2::TempleScript::Parser::grammar .= q{
    listener_factory          : jupyter_listener

    jupyter_listener          : uri                                   {
                                                                        if ($item[1] =~ /^jupyter:/) {
                                                                            use TM2::TS::Stream::jupyter;
                                                                            $return = TM2::TS::Stream::jupyter::factory->new (url => $item[1]); # loop can only be determined later
                                                                        } else {
                                                                            $return = undef;
                                                                        }
                                                                      }

   uri                        : /(jupyter:[^ ~\>]+)/              { $return = $item[1]; }

};
}


our $VERSION = "0.01";

1;

__END__
