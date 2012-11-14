%% API
-module(riak_repl2_rtframe).
-export([encode/2, decode/1]).

-define(MSG_OBJECTS, 16#10). %% List of objects to write
-define(MSG_ACK,     16#20). %% Ack

%% Build an IOlist suitable for sending over a socket
encode(Type, Payload) ->
    IOL = encode_payload(Type, Payload),
    Size = iolist_size(IOL),
    [<<Size:32/unsigned-big-integer>> | IOL].

encode_payload(objects, {Seq, BinObjs}) when is_binary(BinObjs) ->
    [?MSG_OBJECTS,
     <<Seq:64/unsigned-big-integer>>,
     BinObjs];
encode_payload(ack, Seq) ->
    [?MSG_ACK,
     <<Seq:64/unsigned-big-integer>>].
    
decode(<<Size:32/unsigned-big-integer, 
         Msg:Size/binary, % MsgCode is included in size calc
         Rest/binary>>) ->
    <<MsgCode:8/unsigned, Payload/binary>> = Msg,
    {ok, decode_payload(MsgCode, Payload), Rest};
decode(<<Rest/binary>>) ->
    {ok, undefined, Rest}.

decode_payload(?MSG_OBJECTS, <<Seq:64/unsigned-big-integer, BinObjs/binary>>) ->
    {objects, {Seq, BinObjs}};
decode_payload(?MSG_ACK, <<Seq:64/unsigned-big-integer>>) ->
    {ack, Seq}.
