package com.server.net;

import static io.netty.handler.codec.http.HttpHeaders.getHost;
import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.protobuf.ByteString;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;

public class Final {
    private final int port = 8080;
    Map<String, List<String>> queryParams = new HashMap<>();
   

    public void run() throws Exception {
        // Configure the server.
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .childHandler(new ChannelInitializer<SocketChannel>(){

				@Override
				protected void initChannel(SocketChannel ch) throws Exception {
			        ChannelPipeline p = ch.pipeline();


			        p.addLast("decoder", new HttpRequestDecoder());
			        p.addLast("encoder", new HttpResponseEncoder());
			        
			        p.addLast("handler", new SimpleChannelInboundHandler<Object>(){

			            private HttpRequest request;
			            /** Buffer that stores the response content */
			            private final StringBuilder buf = new StringBuilder();

			            @Override
			            protected void channelRead0(ChannelHandlerContext ctx, Object msg)
			                    throws Exception {
			                if (msg instanceof HttpRequest) {
			                    HttpRequest request = this.request = (HttpRequest) msg;
			                    buf.setLength(0);
			                    // hostname
			                    buf.append("HOSTNAME:").append(getHost(request, "unknown"));
			                    // url
			                    buf.append("REQUEST_URI:").append(request.getUri());
			                    // parm
			                    QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
			                    Map<String, List<String>> params = queryStringDecoder.parameters();
			                    queryParams =params;
			                    if (!params.isEmpty()) {
			                        for (Entry<String, List<String>> p : params.entrySet()) {
			                            String key = p.getKey();
			                            List<String> vals = p.getValue();
			                            for (String val : vals) {
			                                buf.append("PARAM:").append(key).append("=")
			                                        .append(val);
			                            }
			                        }
			                    }
			                }
			                if (msg instanceof HttpContent) {
			                    if (msg instanceof LastHttpContent) {
			                        LastHttpContent trailer = (LastHttpContent) msg;
			                        writeResponse(trailer, ctx);
			                    }
			                }
			            }

			            @Override
			            public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
			                ctx.flush();
			            }

			            private boolean writeResponse(HttpObject currentObj,ChannelHandlerContext ctx) {
			                boolean keepAlive = isKeepAlive(request);
			                FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
			                        currentObj.getDecoderResult().isSuccess() ? OK : BAD_REQUEST,
			                        Unpooled.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
			                response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
			                if (keepAlive) {
			                    response.headers().set(CONTENT_LENGTH,
			                            response.content().readableBytes());
			                    response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
			                }
			                ctx.write(response);
			                return keepAlive;
			            }

});
			    }
            	 
             });

            Channel ch = b.bind(port).sync().channel();
            ch.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        new Final().run();
        new Final().addInKafkaQueue();
    }

	private void addInKafkaQueue() {
    Properties configProperties = new Properties();
    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:8080");
    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.connect.json.JsonSerializer");
  Producer producer = new KafkaProducer(configProperties);
  ProducerRecord<String, ByteString> rec = new ProducerRecord<String, ByteString>("kafka",protoBuffConversion());
  producer.send(rec);
  producer.close();

    }

	private  ByteString protoBuffConversion() {
        java.lang.Object ref = queryParams.toString();
          return (com.google.protobuf.ByteString) ref;
      
}
}
