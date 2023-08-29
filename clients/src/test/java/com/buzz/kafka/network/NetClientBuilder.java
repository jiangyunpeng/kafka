package com.buzz.kafka.network;

import com.buzz.kafka.test.Env;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;


public class NetClientBuilder {


    NetworkClient netClient;
    ConsumerNetworkClient client;
    Metadata metadata;

    public static NetClientBuilder builder() {
        return new NetClientBuilder();
    }

    public NetClientBuilder build(String groupId, Env env) {
        Properties props = new Properties();
        props.setProperty("group.id", groupId);
        props.put("bootstrap.servers", env.getServers());
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", ByteArrayDeserializer.class);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", ByteArraySerializer.class);
        init(props, groupId);
        return this;
    }

    private void init(Properties properties, String groupId) {
        String clientId = "test";
        ConsumerConfig config = new ConsumerConfig(properties);
        Metrics metrics = new Metrics();
        Time time = new SystemTime();
        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time);
        LogContext logContext = new LogContext("[Consumer clientId=" + clientId + ", groupId=" + groupId + "] ");
        Sensor throttleTimeSensor = null;
        int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

        metadata = new Metadata(retryBackoffMs,
                config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG),
                true,
                false,
                new ClusterResourceListeners());

        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG));
        //初始化node
        metadata.bootstrap(addresses, time.milliseconds());


        netClient = new NetworkClient(
                new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, "kafka", channelBuilder, logContext),
                metadata,
                clientId,
                100, // a fixed large enough value will suffice for max in-flight requests
                config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG),
                config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
                config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                ClientDnsLookup.forConfig(config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG)),
                time,
                true,
                new ApiVersions(),
                throttleTimeSensor,
                logContext);
        this.client = new ConsumerNetworkClient(
                logContext,
                netClient,
                metadata,
                time,
                retryBackoffMs,
                config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                heartbeatIntervalMs); //Will avoid blocking an extended period of time to prevent heartbeat thread starvation

    }

    public NetworkClient getNetClient() {
        return netClient;
    }

    public ConsumerNetworkClient getClient() {
        return client;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public static void main(String[] args) {
        NetClientBuilder builder = NetClientBuilder.builder().build("bairen.test.1", Env.TEST);
        builder.getClient();
        builder.getNetClient();
        System.out.println("ok");
    }
}

