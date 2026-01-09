%%--------------------------------------------------------------------
%% Copyright (c) 2015-2017 Feng Lee <feng@emqtt.io>.
%%
%% Modified by Ramez Hanna <rhanna@iotblue.net>
%% 
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_plugin_kafka).

% -include("emqx_plugin_kafka.hrl").

% -include_lib("emqx/include/emqx.hrl").

-include("emqx.hrl").
-include_lib("kernel/include/logger.hrl").

-export([load/1, unload/0]).
-import(lists, [map/2]).

%% Client Lifecircle Hooks
-export([on_client_connect/3
  , on_client_connack/4
  , on_client_connected/3
  , on_client_disconnected/4
  , on_client_authenticate/3
  , on_client_check_acl/5
  , on_client_subscribe/4
  , on_client_unsubscribe/4
]).

%% Session Lifecircle Hooks
-export([on_session_created/3
  , on_session_subscribed/4
  , on_session_unsubscribed/4
  , on_session_resumed/3
  , on_session_discarded/3
  , on_session_takeovered/3
  , on_session_terminated/4
]).

%% Message Pubsub Hooks
-export([on_message_publish/2
  , on_message_delivered/3
  , on_message_acked/3
  , on_message_dropped/4
]).


%% Called when the plugin application start
load(Env) ->
  kafka_init([Env]),
  emqx:hook('client.connect', {?MODULE, on_client_connect, [Env]}),
  emqx:hook('client.connack', {?MODULE, on_client_connack, [Env]}),
  emqx:hook('client.connected', {?MODULE, on_client_connected, [Env]}),
  emqx:hook('client.disconnected', {?MODULE, on_client_disconnected, [Env]}),
  emqx:hook('client.authenticate', {?MODULE, on_client_authenticate, [Env]}),
  emqx:hook('client.check_acl', {?MODULE, on_client_check_acl, [Env]}),
  emqx:hook('client.subscribe', {?MODULE, on_client_subscribe, [Env]}),
  emqx:hook('client.unsubscribe', {?MODULE, on_client_unsubscribe, [Env]}),
  emqx:hook('session.created', {?MODULE, on_session_created, [Env]}),
  emqx:hook('session.subscribed', {?MODULE, on_session_subscribed, [Env]}),
  emqx:hook('session.unsubscribed', {?MODULE, on_session_unsubscribed, [Env]}),
  emqx:hook('session.resumed', {?MODULE, on_session_resumed, [Env]}),
  emqx:hook('session.discarded', {?MODULE, on_session_discarded, [Env]}),
  emqx:hook('session.takeovered', {?MODULE, on_session_takeovered, [Env]}),
  emqx:hook('session.terminated', {?MODULE, on_session_terminated, [Env]}),
  emqx:hook('message.publish', {?MODULE, on_message_publish, [Env]}),
  emqx:hook('message.delivered', {?MODULE, on_message_delivered, [Env]}),
  emqx:hook('message.acked', {?MODULE, on_message_acked, [Env]}),
  emqx:hook('message.dropped', {?MODULE, on_message_dropped, [Env]}).

  % ekaf_init(_Env) ->
  %   io:format("Init emqx plugin kafka....."),
  %   {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
  %   ?LOG_INFO("[KAFKA PLUGIN]BrokerValues = ~p~n", [BrokerValues]),
  %   KafkaHost = proplists:get_value(host, BrokerValues),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaHost = ~s~n", [KafkaHost]),
  %   KafkaPort = proplists:get_value(port, BrokerValues),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaPort = ~s~n", [KafkaPort]),
  %   KafkaPartitionStrategy = proplists:get_value(partitionstrategy, BrokerValues),
  %   KafkaPartitionWorkers = proplists:get_value(partitionworkers, BrokerValues),
  %   KafkaTopic = proplists:get_value(payloadtopic, BrokerValues),
  %   KafkaSettingsTopic = proplists:get_value(settings, BrokerValues),
  %   KafkaEventsTopic = proplists:get_value(events, BrokerValues),
  %   KafkaMetricsTopic = proplists:get_value(metrics, BrokerValues),
  %   KafkaConnectedTopic = proplists:get_value(connected, BrokerValues),
  %   KafkaDisconnectedTopic = proplists:get_value(disconnected, BrokerValues),
  %   KafkaOtherTopic = proplists:get_value(other, BrokerValues),
  %   KafkaSightmindEventMetadata = proplists:get_value(sightmind_event_metadata,BrokerValues),
  %   KafkaSightmindEventEvents = proplists:get_value(sightmind_event_events,BrokerValues),
  %   KafkaSightmindEventCommand = proplists:get_value(sightmind_event_command,BrokerValues),
  %   KafkaDmproEventMetadata = proplists:get_value(dmpro_event_metadata,BrokerValues),
  %   KafkaDmproEventEvents = proplists:get_value(dmpro_event_events,BrokerValues),
  %   KafkaDmproEventCommand = proplists:get_value(dmpro_event_command,BrokerValues),
  %   KafkaChannelConnectedTopic = proplists:get_value(channel_connected,BrokerValues),
  %   KafkaChannelDisconnectedTopic = proplists:get_value(channel_disconnected,BrokerValues),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaTopic = ~s~n", [KafkaTopic]),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaSettingsTopic = ~s~n", [KafkaSettingsTopic]),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaEventsTopic = ~s~n", [KafkaEventsTopic]),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaMetricsTopic = ~s~n", [KafkaMetricsTopic]),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaConnectedTopic = ~s~n", [KafkaConnectedTopic]),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaDisconnectedTopic = ~s~n", [KafkaDisconnectedTopic]),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaOtherTopic = ~s~n", [KafkaOtherTopic]),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaSightmindEventMetadata = ~s~n", [KafkaSightmindEventMetadata]),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaSightmindEventEvents = ~s~n", [KafkaSightmindEventEvents]),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaSightmindEventCommand = ~s~n", [KafkaSightmindEventCommand]),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaDmproEventMetadata = ~s~n", [KafkaDmproEventMetadata]),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaDmproEventEvents = ~s~n", [KafkaDmproEventEvents]),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaDmproEventCommand = ~s~n", [KafkaDmproEventCommand]),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaChannelConnectedTopic = ~s~n", [KafkaChannelConnectedTopic]),
  %   ?LOG_INFO("[KAFKA PLUGIN]KafkaChannelDisconnectedTopic = ~s~n", [KafkaChannelDisconnectedTopic]),
  %   application:set_env(emqx_plugin_kafka, settings_topic, list_to_binary(KafkaSettingsTopic)),
  %   application:set_env(ekaf, ekaf_bootstrap_broker, {KafkaHost, list_to_integer(KafkaPort)}),
  %   application:set_env(ekaf, ekaf_partition_strategy, list_to_atom(KafkaPartitionStrategy)),
  %   application:set_env(ekaf, ekaf_per_partition_workers, KafkaPartitionWorkers),
  % % We want lazy loading approach, so not going to support ekaf_bootstrap_topics  
  % %  application:set_env(ekaf, ekaf_bootstrap_topics, list_to_binary(KafkaTopic)),
  %   application:set_env(ekaf, ekaf_buffer_ttl, 10),
  %   application:set_env(ekaf, ekaf_max_downtime_buffer_size, 5),
  %   % {ok, _} = application:ensure_all_started(kafkamocker),
  %   {ok, _} = application:ensure_all_started(gproc),
  %   % {ok, _} = application:ensure_all_started(ranch),
  %   {ok, _} = application:ensure_all_started(ekaf).

% get_settings_topic() ->
% %  ?LOG_INFO("[KAFKA PLUGIN]>> get_settings_topic"),
%   {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
%   Topic = proplists:get_value(settings, BrokerValues),
%   Topic.
% %  test = application:get_env(emqx_plugin_kafka, settings_topic),
% %  {ok, Topic} =  application:get_env(emqx_plugin_kafka, settings),
% %  ?LOG_INFO("[KAFKA PLUGIN] settings_topic = ~s~n", [test]),
% %  Topic = <<ek_emqx_settings>>,
% %  Topic.

% get_events_topic() ->
%   {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
%   Topic = proplists:get_value(events, BrokerValues),
%   Topic.
% %  {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
% %  {ok, Topic} = proplists:get_value(<<"events">>, BrokerValues),
% %  Topic.

% get_metrics_topic() ->
%   {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
%   Topic = proplists:get_value(metrics, BrokerValues),
%   Topic.

% get_client_connected_topic() ->
% %  {ok, Topic} =  application:get_env(emqx_plugin_kafka, connected),
% %  Topic.
%   {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
%   Topic = proplists:get_value(connected, BrokerValues),
% %  Topic = <<ek_emqx_connected>>,
%   Topic.

% %  {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
% %  {ok, Topic} = proplists:get_value(<<"connected">>, BrokerValues),
% %  Topic.

% get_client_disconnected_topic() ->
% %  {ok, Topic} =  application:get_env(emqx_plugin_kafka, disconnected),
% %  Topic.
% %  Topic = <<ek_emqx_disconnected>>,
%   {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
%   Topic = proplists:get_value(disconnected, BrokerValues),
%   Topic.
% %
% %  {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
% %  {ok, Topic} = proplists:get_value(<<"disconnected">>, BrokerValues),
% %  Topic.

% get_other_messages_topic() ->
% %  {ok, Topic} =  application:get_env(emqx_plugin_kafka, other),
% %  Topic.
% %  Topic = <<ek_emqx_msg>>,
%   {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
%   Topic = proplists:get_value(other, BrokerValues),
%   Topic.
% %
% %  {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
% %  {ok, Topic} = proplists:get_value(<<"other">>, BrokerValues),
% %  Topic.

% get_sightmind_event_metadata_topic() ->
%   {ok,BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
%   Topic = proplists:get_value(sightmind_event_metadata,BrokerValues),
%   Topic.

% get_sightmind_event_event_topic() ->
%     {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
%     Topic = proplists:get_value(sightmind_event_events,BrokerValues),
%     Topic.

% get_sightmind_event_command_topic() ->
%   {ok,BrokerValues} = application:get_env(emqx_plugin_kafka,broker),
%   Topic = proplists:get_value(sightmind_event_command,BrokerValues),
%   Topic.

% get_dmpro_event_metadata_topic() ->
%   {ok, BrokerValues} = application:get_env(emqx_plugin_kafka,broker),
%   Topic = proplists:get_value(dmpro_event_metadata,BrokerValues),
%   Topic.

% get_dmpro_event_event_topic() ->
%   {ok, BrokerValues} = application:get_env(emqx_plugin_kafka,broker),
%   Topic = proplists:get_value(dmpro_event_events,BrokerValues),
%   Topic.

% get_dempro_event_command_topic() ->
%   {ok, BrokerValues} = application:get_env(emqx_plugin_kafka,broker),
%   Topic = proplists:get_value(dmpro_event_command,BrokerValues),
%   Topic.

% get_channel_conn_topic() ->
%   {ok, BrokerValues} = application:get_env(emqx_plugin_kafka,broker),
%   Topic = proplists:get_value(channel_connected,BrokerValues),
%   Topic.

% get_channel_disconn_topic() ->
%   {ok, BrokerValues} = application:get_env(emqx_plugin_kafka,broker),
%   Topic = proplists:get_value(channel_disconnected,BrokerValues),
%   Topic.

on_client_connect(ConnInfo = #{clientid := ClientId}, Props, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) connect, ConnInfo: ~p, Props: ~p~n",
    [ClientId, ConnInfo, Props]),
  ok.

on_client_connack(ConnInfo = #{clientid := ClientId}, Rc, Props, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) connack, ConnInfo: ~p, Rc: ~p, Props: ~p~n",
    [ClientId, ConnInfo, Rc, Props]),
  ok.

on_client_connected(ClientInfo = #{clientid := ClientId}, ConnInfo, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) connected, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
    [ClientId, ClientInfo, ConnInfo]),
  {IpAddr, _Port} = maps:get(peername, ConnInfo),
  Action = <<"connected">>,
  Now = now_mill_secs(os:timestamp()),
  Online = 1,
  Payload = [
    {action, Action},
    {device_id, ClientId},
    {username, maps:get(username, ClientInfo)},
    {keepalive, maps:get(keepalive, ConnInfo)},
    {ipaddress, iolist_to_binary(ntoa(IpAddr))},
    {proto_name, maps:get(proto_name, ConnInfo)},
    {proto_ver, maps:get(proto_ver, ConnInfo)},
    {ts, Now},
    {online, Online}
  ],
  {ok, KafkaTopics} =  application:get_env(emqx_plugin_kafka, connect_kafka_topics),
  KafkaConnectedTopic = proplists:get_value("connected", KafkaTopics),
  %?LOG_INFO("[KAFKA PLUGIN]KafkaConnectedTopic = ~s", [KafkaConnectedTopic]),
  produce_kafka_payload(ClientId, KafkaConnectedTopic, Payload),
  ok.

on_client_disconnected(ClientInfo = #{clientid := ClientId}, ReasonCode, ConnInfo, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) disconnected due to ~p, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
    [ClientId, ReasonCode, ClientInfo, ConnInfo]),
  Action = <<"disconnected">>,
  Now = now_mill_secs(os:timestamp()),
  Online = 0,
  Payload = [
    {action, Action},
    {device_id, ClientId},
    {username, maps:get(username, ClientInfo)},
    {reason, ReasonCode},
    {ts, Now},
    {online, Online}
  ],
  {ok, KafkaTopics} =  application:get_env(emqx_plugin_kafka, connect_kafka_topics),
  KafkaDisconnectedTopic = proplists:get_value("disconnected", KafkaTopics),
  produce_kafka_payload(ClientId, KafkaDisconnectedTopic, Payload),
  ok.

on_client_authenticate(_ClientInfo = #{clientid := ClientId}, Result, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) authenticate, Result:~n~p~n", [ClientId, Result]),
  ok.

on_client_check_acl(_ClientInfo = #{clientid := ClientId}, Topic, PubSub, Result, _Env) ->
  ?LOG_DEBUG("[KAFKA PLUGIN]Client(~s) check_acl, PubSub:~p, Topic:~p, Result:~p~n",
    [ClientId, PubSub, Topic, Result]),
  ok.

%%---------------------------client subscribe start--------------------------%%
on_client_subscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
  ?LOG_DEBUG("[KAFKA PLUGIN]Client(~s) will subscribe: ~p~n", [ClientId, TopicFilters]),
  Topic = erlang:element(1, erlang:hd(TopicFilters)),
  Qos = erlang:element(2, lists:last(TopicFilters)),
  Action = <<"subscribe">>,
  Now = now_mill_secs(os:timestamp()),
  Payload = [
    {device_id, ClientId},
    {action, Action},
    {topic, Topic},
    {qos, maps:get(qos, Qos)},
    {ts, Now}
  ],
  %produce_kafka_payload(ClientId, Payload),
  ok.
%%---------------------client subscribe stop----------------------%%
on_client_unsubscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
  ?LOG_DEBUG("[KAFKA PLUGIN]Client(~s) will unsubscribe ~p~n", [ClientId, TopicFilters]),
  Topic = erlang:element(1, erlang:hd(TopicFilters)),
  Action = <<"unsubscribe">>,
  Now = now_mill_secs(os:timestamp()),
  Payload = [
    {device_id, ClientId},
    {action, Action},
    {topic, Topic},
    {ts, Now}
  ],
  %produce_kafka_payload(ClientId, Payload),
  ok.

on_message_dropped(#message{topic = <<"$SYS/", _/binary>>}, _By, _Reason, _Env) ->
  ok;
on_message_dropped(Message, _By = #{node := Node}, Reason, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Message dropped by node ~s due to ~s: ~s~n",
    [Node, Reason, emqx_message:format(Message)]),
  %%Topic = Message#message.topic,
  %%Payload = Message#message.payload,  
  %% We're interested in settings, events as well to be published to the right Kafka topic,
  %% so use get_kafka_topic_produce to identify correct route.
  %% get_kafka_topic_produce(Topic, Payload),
  ok.


%%---------------------------message publish start--------------------------%%
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
  ok;
on_message_publish(Message, _Env) ->
  Topic = Message#message.topic,
  {ok, ClientId, Payload} = format_payload(Message),
  From = Message#message.from,  
  ?LOG_INFO("[KAFKA PLUGIN]Message published to client(~s): ~s~n",
    [From, emqx_message:format(Message)]),
  produce_kafka_payload(ClientId, Topic, From, Payload),
  ok.
%%---------------------message publish stop----------------------%%

  % Topic = Message#message.topic,
  % Payload = Message#message.payload,
  % Qos = Message#message.qos,
  % From = Message#message.from,
  % Timestamp = Message#message.timestamp,
  % %%Content = [
  % %%  {action, <<"message_published">>},
  % %%  {from, From},
  % %%  {to, ClientId},
  % %%  {topic, Topic},
  % %%  {payload, Payload},
  % %%  {qos, Qos},
  % %%  {cluster_node, node()},
  % %%  {ts, Timestamp}
  % %%],
  % ?LOG_INFO("[KAFKA PLUGIN]Message published to client(~s): ~s~n",
  %   [From, emqx_message:format(Message)]),
  % get_kafka_topic_produce(Topic, Payload),  
  % ok.
%%---------------------message publish stop----------------------%%

on_message_delivered(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
  ?LOG_DEBUG("[KAFKA PLUGIN]Message delivered to client(~s): ~s~n",
    [ClientId, emqx_message:format(Message)]),
  Topic = Message#message.topic,
  Payload = transform_payload(Message#message.payload),
  Qos = Message#message.qos,
  From = Message#message.from,
  Timestamp = Message#message.timestamp,
  Content = [
    {action, <<"message_delivered">>},
    {from, From},
    {to, ClientId},
    {topic, Topic},
    {payload, Payload},
    {qos, Qos},
    {cluster_node, node()},
    {ts, Timestamp}
  ],
  %produce_kafka_payload(ClientId, Content),
  ok.

on_message_acked(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
  ?LOG_DEBUG("[KAFKA PLUGIN]Message acked by client(~s): ~s~n",
    [ClientId, emqx_message:format(Message)]),
  Topic = Message#message.topic,
  Payload = transform_payload(Message#message.payload),
  Qos = Message#message.qos,
  From = Message#message.from,
  Timestamp = Message#message.timestamp,
  Content = [
    {action, <<"message_acked">>},
    {from, From},
    {to, ClientId},
    {topic, Topic},
    {payload, Payload},
    {qos, Qos},
    {cluster_node, node()},
    {ts, Timestamp}
  ],
  %produce_kafka_payload(ClientId, Content),
  ok.

%%--------------------------------------------------------------------
%% Session Lifecircle Hooks
%%--------------------------------------------------------------------

on_session_created(#{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_DEBUG("[KAFKA PLUGIN]Session(~s) created, Session Info:~n~p~n", [ClientId, SessInfo]),
  ok.


on_session_subscribed(#{clientid := ClientId}, Topic, SubOpts, _Env) ->
  ?LOG_DEBUG("[KAFKA PLUGIN]Session(~s) subscribed ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]),
  ok.

on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, _Env) ->
  ?LOG_DEBUG("[KAFKA PLUGIN]Session(~s) unsubscribed ~s with opts: ~p~n", [ClientId, Topic, Opts]),
  ok.

on_session_resumed(#{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_DEBUG("[KAFKA PLUGIN]Session(~s) resumed, Session Info:~n~p~n", [ClientId, SessInfo]),
  ok.

on_session_discarded(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_DEBUG("[KAFKA PLUGIN]Session(~s) is discarded. Session Info: ~p~n", [ClientId, SessInfo]),
  ok.

on_session_takeovered(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_DEBUG("[KAFKA PLUGIN]Session(~s) is takeovered. Session Info: ~p~n", [ClientId, SessInfo]),
  ok.

on_session_terminated(_ClientInfo = #{clientid := ClientId}, Reason, SessInfo, _Env) ->
  ?LOG_DEBUG("[KAFKA PLUGIN]Session(~s) is terminated due to ~p~nSession Info: ~p~n",
    [ClientId, Reason, SessInfo]),
  ok.


 
do_connect_topic(Topic) ->
  ?LOG_INFO("[KAFKA PLUGIN]Start kafka producer ~p~n", [Topic]),
  ok = brod:start_producer(emqx_repost_worker, Topic, []),
  ok.


kafka_init(_Env) ->
  ?LOG_INFO("LET'S F'N GO!!"),
  ?LOG_INFO("Start to init emqx plugin kafka..... ~n"),
  {ok, AddressList} = application:get_env(emqx_plugin_kafka, kafka_address_list),
  ?LOG_INFO("[KAFKA PLUGIN]KafkaAddressList = ~p~n", [AddressList]),
  {ok, KafkaConfig} = application:get_env(emqx_plugin_kafka, kafka_config),
  ?LOG_INFO("[KAFKA PLUGIN]KafkaConfig = ~p~n", [KafkaConfig]),
  {ok, KafkaTopics} =  application:get_env(emqx_plugin_kafka, connect_kafka_topics),
  ?LOG_INFO("[KAFKA PLUGIN]KafkaTopics = ~p~n", [KafkaTopics]),
  {ok, KafkaTopic} = application:get_env(emqx_plugin_kafka, topic),
  ?LOG_INFO("[KAFKA PLUGIN]KafkaTopic = ~s~n", [KafkaTopic]),

  {ok, _} = application:ensure_all_started(brod),
  ok = brod:start_client(AddressList, emqx_repost_worker, KafkaConfig),
  ok = brod:start_producer(emqx_repost_worker, KafkaTopic, []),
   % Iterate through our topics and dynamically subscribe instead of hardcoded
  lists:foreach(fun(X) -> 
                  ?LOG_INFO("[KAFKA PLUGIN]List Item : ~p~n", [X]),
                  ok = do_connect_topic(element(2, X))
                  end,
                  KafkaTopics), 
  ?LOG_INFO("Init emqx plugin kafka successfully.....~n"),
  ok.

get_kafka_topic() ->
  {ok, Topic} = application:get_env(emqx_plugin_kafka, topic),
  Topic.

need_base64() ->
  {ok, NeedBase64} = application:get_env(emqx_plugin_kafka, publish_base64),
  NeedBase64.

transform_payload(Payload) ->
  NeedBase64 = need_base64(),
  if
    NeedBase64 == true ->
      Content = list_to_binary(base64:encode_to_string(Payload));
    NeedBase64 == false ->
      Content = Payload
  end,
  Content.


format_payload(Message) ->
  Username = emqx_message:get_header(username, Message),
  Topic = Message#message.topic,
  % Tail = string:right(binary_to_list(Topic), 4),
  % RawType = string:equal(Tail, <<"_raw">>),
  % ?LOG_INFO("[KAFKA PLUGIN]Tail= ~s , RawType= ~s~n",[Tail,RawType]),
  ClientId = Message#message.from,
  Content = transform_payload(Message#message.payload),
  % Not needed since backend is only expecting the message sent by the device
  % Payload = [{action, message_publish},
  %   {device_id, ClientId},
  %   {username, Username},
  %   {topic, Topic},
  %   {payload, Content},
  %   {ts, Message#message.timestamp}],

  {ok, ClientId, Content}.


%% Called when the plugin application stop
unload() ->
  emqx:unhook('client.connect', {?MODULE, on_client_connect}),
  emqx:unhook('client.connack', {?MODULE, on_client_connack}),
  emqx:unhook('client.connected', {?MODULE, on_client_connected}),
  emqx:unhook('client.disconnected', {?MODULE, on_client_disconnected}),
  emqx:unhook('client.authenticate', {?MODULE, on_client_authenticate}),
  emqx:unhook('client.check_acl', {?MODULE, on_client_check_acl}),
  emqx:unhook('client.subscribe', {?MODULE, on_client_subscribe}),
  emqx:unhook('client.unsubscribe', {?MODULE, on_client_unsubscribe}),
  emqx:unhook('session.created', {?MODULE, on_session_created}),
  emqx:unhook('session.subscribed', {?MODULE, on_session_subscribed}),
  emqx:unhook('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
  emqx:unhook('session.resumed', {?MODULE, on_session_resumed}),
  emqx:unhook('session.discarded', {?MODULE, on_session_discarded}),
  emqx:unhook('session.takeovered', {?MODULE, on_session_takeovered}),
  emqx:unhook('session.terminated', {?MODULE, on_session_terminated}),
  emqx:unhook('message.publish', {?MODULE, on_message_publish}),
  emqx:unhook('message.delivered', {?MODULE, on_message_delivered}),
  emqx:unhook('message.acked', {?MODULE, on_message_acked}),
  emqx:unhook('message.dropped', {?MODULE, on_message_dropped}).


% get_kafka_topic_produce(Topic, Message) ->
%   ?LOG_INFO("[KAFKA PLUGIN]Kafka topic name = ~s~n", [Topic]),
%   TopicPrefix = string:left(binary_to_list(Topic),2),
%   TlinkFlag = string:equal(TopicPrefix, <<"d/">>),
%   ?LOG_INFO("[KAFKA PLUGIN]TopicPrefix = ~s~n", [TopicPrefix]),
%   if
%     TlinkFlag == true ->
%       TopicStr = binary_to_list(Topic),
%       SettingsIndex = string:str(TopicStr,"d/settings"),
%       EventsIndex = string:str(TopicStr,"d/events"),
%       MetricsIndex = string:str(TopicStr,"d/metrics"),
%       SightMindMetadataIndex = string:str(TopicStr,"d/sm-metadata"),
%       SightMindEventIndex = string:str(TopicStr,"d/sm-event"),
%       SightMindCommandIndex = string:str(TopicStr,"d/sm-command"),
%       DmproMetadataIndex = string:str(TopicStr,"d/dm-metadata"),
%       DmproEventIndex = string:str(TopicStr,"d/dm-event"),
%       DmproCommandIndex = string:str(TopicStr,"d/dm-command"),
%       ChannelConnIndex = string:str(TopicStr,"d/ch/conn"),
%       ChannelDisconnIndex = string:str(TopicStr,"d/ch/disconn"),
%       if
%         SettingsIndex /= 0 ->
%           TopicKafka = get_settings_topic();
%         MetricsIndex /= 0 ->
%           TopicKafka = get_metrics_topic();
%         EventsIndex /= 0 ->
%           TopicKafka = get_events_topic();
%         SightMindMetadataIndex /= 0 ->
%           TopicKafka = get_sightmind_event_metadata_topic();
%         SightMindEventIndex /= 0 ->
%           TopicKafka = get_sightmind_event_event_topic();
%         SightMindCommandIndex /= 0 ->
%           TopicKafka = get_sightmind_event_command_topic();
%         DmproMetadataIndex /= 0 ->
%           TopicKafka = get_dmpro_event_metadata_topic();
%         DmproEventIndex /= 0 ->
%           TopicKafka = get_dmpro_event_event_topic();
%         DmproCommandIndex /= 0 ->
%           TopicKafka = get_dempro_event_command_topic();
%         ChannelConnIndex /= 0 ->
%           TopicKafka = get_channel_conn_topic();
%         ChannelDisconnIndex /= 0 ->
%           TopicKafka = get_channel_disconn_topic();
%         SettingsIndex + EventsIndex + MetricsIndex == 0 ->
%           TopicKafka = get_other_messages_topic()
%       end,
%       produce_kafka_payload(TopicKafka, Message);
%     TlinkFlag == false ->
%       ?LOG_INFO("[KAFKA PLUGIN]MQTT topic prefix is not tlink = ~s~n",[Topic])
%   end,
%   ok.

get_key_value(MqttTopic,TopicPrefix,KafkaTopics) ->
  case string:str(MqttTopic,TopicPrefix) of
    0 -> 
      TopicKafka = proplists:get_value(TopicPrefix, KafkaTopics)
  end,
  TopicKafka.

do_get_topic(MqttTopic,ListTopics) ->


    ok.

get_duclo_kafka_topic(Topic) ->

  % Let's get the prefix so we only send kafka messages from devices
  TopicPrefix = string:left(binary_to_list(Topic),2),
  TlinkFlag = string:equal(TopicPrefix, <<"d/">>),
  if
    TlinkFlag == true ->
      TopicStr = binary_to_list(Topic),
      {ok, KafkaTopics} =  application:get_env(emqx_plugin_kafka, connect_kafka_topics),
      % {PubTopic, Start, Length} = regexp:match("^d[^/]*/", TopicStr),
      % ?LOG_INFO("[PLUGIN KAFKA]Regex result = ~s", [PubTopic]),

      SettingsStr = "d/settings",
      SettingsIndex = string:str(TopicStr,SettingsStr),
      EventsStr = "d/events",
      EventsIndex = string:str(TopicStr,EventsStr),
      MetricsStr = "d/metrics",
      MetrixIndex = string:str(TopicStr,MetricsStr),
      ChConnectedStr = "d/ch/conn",
      ChConnectedIndex = string:str(TopicStr,ChConnectedStr),
      ChDisconnectedStr = "d/ch/disconn",
      ChDisconnectedIndex = string:str(TopicStr,ChDisconnectedStr),
      if
        SettingsIndex /= 0->
          TopicKafka = proplists:get_value(SettingsStr, KafkaTopics);
        EventsIndex /= 0->
          TopicKafka = proplists:get_value(EventsStr, KafkaTopics);
        MetrixIndex /= 0->
          TopicKafka = proplists:get_value(MetricsStr, KafkaTopics);
        ChConnectedIndex /= 0->
          TopicKafka = proplists:get_value(ChConnectedStr, KafkaTopics);
        ChDisconnectedIndex /= 0->
          % Y= proplists:get_value("d/ch/disconn", KafkaTopics),
          % ?LOG_INFO("[KAFKA PLUGIN]d/ch/disconn = ~s", [Y]),
          % Z= proplists:get_value(ChDisconnectedStr, KafkaTopics),
          % ?LOG_INFO("[KAFKA PLUGIN]ChDisconnectedStr = ~s", [Z]),
          % ?LOG_INFO("[KAFKA PLUGIN]ChDisconnectedStr = ~s", [ChDisconnectedStr]),
          TopicKafka = proplists:get_value(ChDisconnectedStr, KafkaTopics),
          ?LOG_INFO("[KAFKA PLUGIN]ChDisconnectedStr: ~s = ~s", [ChDisconnectedStr,TopicKafka]);
        true ->
          TopicKafka = undefined                                         

      % case regexp:match("^d[^/]*/", TopicStr) of
      %   {PubTopic,Start,Length} ->
      %     TopicKafka = proplists:get_value(PubTopic, KafkaTopics)
      % end;
      end;
    TlinkFlag == false ->
      ?LOG_INFO("[KAFKA PLUGIN]MQTT topic prefix is not tlink = ~p~n",[Topic]),
      TopicKafka = undefined
  end,
  TopicKafka.

produce_kafka_payload(Key, Topic, From, Message) ->
  TopicKafka = get_duclo_kafka_topic(Topic),
  case TopicKafka of
      undefined -> ?LOG_INFO("[KAFKA PLUGIN]Not handling topic. = ~p~n",[Topic]);
      _ -> 
        ?LOG_INFO("[KAFKA PLUGIN]Handling Topic = ~s~n", [TopicKafka]),
        produce_kafka_payload(Key, TopicKafka, Message)
  end,
  ok.

produce_kafka_payload(Key, Topic, Message) ->
  %Topic = get_kafka_topic(),
  %?LOG_INFO("[KAFKA PLUGIN]TopicKafka topic = ~s~n", [Topic]),
  {ok, MessageBody} = emqx_json:safe_encode(Message),
  ?LOG_INFO("[KAFKA PLUGIN]Publishing Message = [[~s]] to Kafka Topic = ~s~n",[MessageBody,Topic]),
  Payload = iolist_to_binary(MessageBody),
  brod:produce_cb(emqx_repost_worker, Topic, hash, Key, Payload, fun(_,_) -> ok end),
  ok.


% produce_kafka_payload(TopicKafka,Topic,Message) ->
%   %%Topic = ekaf_get_topic(),
%   ?LOG_INFO("TopicKafka topic = ~s~n", [TopicKafka]),
%   {ok, MessageBody} = emqx_json:safe_encode(Message),
%   ?LOG_INFO("[KAFKA PLUGIN]Message = ~s~n",[MessageBody]),
%   Payload = iolist_to_binary(MessageBody),
%   ekaf:o(TopicKafka, Payload).

% produce_kafka_payload(Message) ->
%   {ok, MessageBody} = emqx_json:safe_encode(Message),
%   % ?LOG_INFO("[KAFKA PLUGIN]Message = ~s~n",[MessageBody]),
%   Payload = iolist_to_binary(MessageBody),
%   brod:produce_cb(emqx_repost_worker, Topic, hash, Key, Payload, fun(_,_) -> ok end),
%   ok.

ntoa({0, 0, 0, 0, 0, 16#ffff, AB, CD}) ->
  inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(IP) ->
  inet_parse:ntoa(IP).
now_mill_secs({MegaSecs, Secs, _MicroSecs}) ->
  MegaSecs * 1000000000 + Secs * 1000 + _MicroSecs.
