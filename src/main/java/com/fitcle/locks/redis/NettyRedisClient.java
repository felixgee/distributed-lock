/*
 * Copyright 2024 Felix Luo.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fitcle.locks.redis;

import com.fitcle.locks.DistributedLockException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.redis.*;
import io.netty.util.CharsetUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * Light implementation of redis client based on netty framework.
 *
 * @since 17
 * @version 1.0
 * @author felix.luo.hwong@gmail.com
 */
@Slf4j
public class NettyRedisClient {

    /**
     * holds the redis clients so that same redis client instantiated can be avoided.
     */
    private static final ConcurrentMap<String, NettyRedisClient> clients = new ConcurrentHashMap<>();

    /**
     * redis hostname or ip
     */
    private final String host;

    /**
     * redis server's port
     */
    private final int port;

    /**
     * flag to indicate whether a redis client has connected to redis server.
     */
    private volatile boolean connected;

    /**
     * the instance to send redis commands excepts subscribe/unsubscribe
     */
    private InternalClient internalClient;

    /**
     * the instance to send redis subscribe/unsubscribe command only
     */
    private InternalClient pubSubClient;

    private NettyRedisClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Creates the redis client instance and put the instance into client container.
     *
     * @param host redis server's hostname
     * @param port redis server's port
     * @return the instance of NettyRedisClient
     */
    public static NettyRedisClient create(String host, int port) {
        String clientKey = String.join(":", host, String.valueOf(port));
        return clients.computeIfAbsent(clientKey, k -> new NettyRedisClient(host, port));
    }

    /**
     * Returns the connection without pub/sub operations
     * @return the redis connection
     */
    public RedisConnection connect() {
        return connect(false);
    }

    /**
     * Returns the connection with pub/sub operations
     *
     * @param withPubSub true to enable the pub/sub channel, false to disable
     * @return the redis connection
     */
    public RedisConnection connect(boolean withPubSub) {
        if (!connected) {
            synchronized (this) {
                if (!connected) {
                    this.internalClient = new InternalClient(host, port, null);
                    if (withPubSub) {
                        this.pubSubClient = new InternalClient(host, port, new RedisUnlockStateListener());
                    }
                    this.connected = true;
                }
            }
        }
        NioSocketChannel channel = Objects.nonNull(this.pubSubClient) ? this.pubSubClient.getChannel() : null;
        return new InternalConnection(this.internalClient.getChannel(), channel);
    }

    /**
     * close the NettyRedisClient
     */
    public void close() {
        clients.remove(String.join(":", host, String.valueOf(port)));
        this.internalClient.close();
        if (Objects.nonNull(this.pubSubClient)) {
            this.pubSubClient.close();
        }
    }

    /**
     * Internal redis client
     */
    private static class InternalClient {
        private final EventLoopGroup eventGroup;
        private final NioSocketChannel channel;

        InternalClient(String host, int port, NettyRedisMessageListener listener) {
            this.eventGroup = new NioEventLoopGroup(1);
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(this.eventGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel sc) {
                            ChannelPipeline pipeline = sc.pipeline();
                            pipeline.addLast(new RedisDecoder());
                            pipeline.addLast(new RedisBulkStringAggregator());
                            pipeline.addLast(new RedisArrayAggregator());
                            pipeline.addLast(new RedisEncoder());
                            pipeline.addLast(new NettyRedisDuplexHandler(listener));
                        }
                    });

            try {
                this.channel = (NioSocketChannel) bootstrap.connect(host, port).sync().channel();
            } catch (InterruptedException e) {
                throw new DistributedLockException(e);
            }
        }

        NioSocketChannel getChannel() {
            return this.channel;
        }

        void close() {
            this.channel.close();
            this.eventGroup.shutdownGracefully();
        }
    }

    /**
     * holds the redis command and response
     */
    private static class NettyRedisCommand implements Serializable {
        /**
         * use this synchronizer to notify the client thread once netty receives the response of redis server
         */
        private final Semaphore semaphore = new Semaphore(0);
        private final Object cmd;
        private volatile Object res;

        public NettyRedisCommand(Object cmd) {
            this.cmd = cmd;
        }

        public Object get() {
            return cmd;
        }

        public Object await() {
            semaphore.acquireUninterruptibly(1);
            return res;
        }

        public void signal(Object res) {
            this.res = res;
            semaphore.release();
        }
    }

    /**
     * To handle the redis client commands and redis server response
     */
    private static class NettyRedisDuplexHandler extends ChannelDuplexHandler {
        /**
         * All client commands should be put into this queue, so that once netty receives
         * redis server's response, we know which client command to be answered.
         */
        private final LinkedBlockingQueue<NettyRedisCommand> messageQueue = new LinkedBlockingQueue<>();

        /**
         * Mainly used to handle the redis pub/sub response
         */
        private final NettyRedisMessageListener listener;

        NettyRedisDuplexHandler(NettyRedisMessageListener listener) {
            this.listener = listener;
        }

        public void channelActive(ChannelHandlerContext ctx) {
            log.debug("Channel active");
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object obj, ChannelPromise promise) {
            if (obj instanceof NettyRedisCommand msg) {
                if (msg.get() instanceof String s) { // handles the normal redis command, eg: 'get test', 'set test hello px 6000', etc.
                    String[] commands = s.split("\\s+");
                    List<RedisMessage> children = new ArrayList<>(commands.length);
                    for (String cmd : commands) {
                        children.add(new FullBulkStringRedisMessage(ByteBufUtil.writeUtf8(ctx.alloc(), cmd)));
                    }
                    RedisMessage request = new ArrayRedisMessage(children);
                    if (messageQueue.offer(msg)) {
                        ctx.write(request, promise);
                    }
                } else if (msg.get() instanceof List<?> list) { // handles the eval command
                    List<RedisMessage> children = new ArrayList<>(list.size());
                    for (Object cmd : list) {
                        children.add(new FullBulkStringRedisMessage(ByteBufUtil.writeUtf8(ctx.alloc(), cmd.toString())));
                    }
                    RedisMessage request = new ArrayRedisMessage(children);
                    if (messageQueue.offer(msg)) {
                        ctx.write(request, promise);
                    }
                } else {
                    log.error("Unsupported redis command: {}", obj);
                }
            }
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object res) {
            NettyRedisCommand req = messageQueue.poll();
            if (Objects.isNull(req)) {
                return;
            }
            RedisMessage msg = (RedisMessage) res;
            List<String> list = parseMessage(msg);
            handleResponse(req, list);
        }

        private List<String> parseMessage(RedisMessage res) {
            List<String> list = new ArrayList<>();

            if (res instanceof SimpleStringRedisMessage msg) {
                list.add(msg.content());
            } else if (res instanceof ErrorRedisMessage msg) {
                list.add(msg.content());
            } else if (res instanceof IntegerRedisMessage msg) {
                list.add(String.valueOf(msg.value()));
            } else if (res instanceof FullBulkStringRedisMessage msg) {
                if (msg.isNull()) {
                    list.add(null);
                } else {
                    list.add(msg.content().toString(CharsetUtil.UTF_8));
                }
            } else if (res instanceof ArrayRedisMessage msg) {
                for (RedisMessage child : msg.children()) {
                    list.addAll(parseMessage(child));
                }
            } else {
                throw new CodecException("unknown message type: " + res);
            }

            return list;
        }

        /**
         * handles the redis response and notify clients
         * @param req client command object
         * @param cmd response from redis server
         */
        private void handleResponse(NettyRedisCommand req, List<String> cmd) {
            log.debug("cmd: {}, res: {}", req.get(), String.join(" ", cmd));
            if (cmd.size() == 1) {
                req.signal(cmd.get(0));
            } else if (cmd.size() == 3 && ("subscribe".equals(cmd.get(0)) || "unsubscribe".equals(cmd.get(0)))) {
                req.signal(cmd.get(2));
            } else if (cmd.size() == 3 && "message".equals(cmd.get(0))) {
                if (Objects.nonNull(listener)) {
                    listener.handleMessage(cmd.get(1), cmd.get(2));
                }
            } else {
                log.error("unknown response: {}", String.join(" ", cmd));
                req.signal(null);
            }
        }
    }

    /**
     * This implementation is only used in NettyRedisClient
     */
    static class InternalConnection implements RedisConnection {
        private final NioSocketChannel channel;
        private final NioSocketChannel pubSubChannel;

        InternalConnection(NioSocketChannel channel, NioSocketChannel pubSubChannel) {
            this.channel = channel;
            this.pubSubChannel = pubSubChannel;
        }

        @Override
        public String get(String key) {
            NettyRedisCommand msg = new NettyRedisCommand(String.format("get %s", key));
            channel.writeAndFlush(msg);
            return (String) msg.await();
        }

        @Override
        public Object set(String key, String value, long ttl) {
            NettyRedisCommand msg = new NettyRedisCommand(String.format("set %s %s px %d", key, value, ttl));
            channel.writeAndFlush(msg);
            return msg.await();
        }

        @Override
        public Object setnx(String key, String value, long ttl) {
            NettyRedisCommand msg = new NettyRedisCommand(String.format("set %s %s nx px %d", key, value, ttl));
            channel.writeAndFlush(msg);
            return msg.await();
        }

        @Override
        public Object del(String key) {
            NettyRedisCommand msg = new NettyRedisCommand(String.format("del %s", key));
            channel.writeAndFlush(msg);
            return msg.await();
        }

        @Override
        public Object eval(String script, List<String> keys, List<String> args) {
            List<String> cmd = new ArrayList<>();
            cmd.add("eval");
            cmd.add(script);
            cmd.add(String.valueOf(keys.size()));
            cmd.addAll(keys);
            cmd.addAll(args);
            NettyRedisCommand msg = new NettyRedisCommand(cmd);
            channel.writeAndFlush(msg);
            return msg.await();
        }

        @Override
        public Object subscribe(String listenChannel) {
            NettyRedisCommand msg = new NettyRedisCommand(String.format("subscribe %s", listenChannel));
            pubSubChannel.writeAndFlush(msg);
            return msg.await();
        }

        @Override
        public Object unsubscribe(String listenChannel) {
            NettyRedisCommand msg = new NettyRedisCommand(String.format("unsubscribe %s", listenChannel));
            pubSubChannel.writeAndFlush(msg);
            return msg.await();
        }
    }
}
