// Copyright 2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package io.liwei.fullstack.examples.nats.streaming;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.Duration;

import io.nats.client.AuthHandler;
import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.streaming.NatsStreaming;

import javax.net.ssl.*;

public class ExampleUtils {

    private static String KEYSTORE_PATH;
    private static String TRUSTSTORE_PATH;
    private static String STORE_PASSWORD;
    private static String KEY_PASSWORD;
    private static String ALGORITHM = "SunX509";

    /**
     * @param urls Nats streaming 单机/集群URL
     *             单机:字符串 例如:tls://server:4222
     *             集群: 多个地址以英文半角逗号隔开 例如: tls://server1:4222,tls://server2:4222,tls://server3:4222
     * @throws Exception 创建Nats客户端发生异常时抛出
     * @author veily
     */
    public static io.nats.streaming.Options buildOpts(String urls) throws Exception {
        io.nats.streaming.Options opts = NatsStreaming.defaultOptions();
        Connection nc = null;
        if (urls != null) {
            if (urls.startsWith("tls://")) {
                nc = Nats.connect(createClusterTLSExampleOptions(urls, true));
            } else {
                nc = Nats.connect(createExampleOptions(urls,
                        true, true, false));
            }
        }

        if (nc != null) {
            opts = new io.nats.streaming.Options.Builder().natsConn(nc).build();
        }
        return opts;
    }

    /**
     * @param keyStorePath   keystore file path
     * @param keyStorePwd    keystore password
     * @param trustStorePath truststore file path
     * @param trustStorePwd  truststore password
     */

    public static void initKeyStore(String keyStorePath,
                                    String keyStorePwd,
                                    String trustStorePath,
                                    String trustStorePwd) {
        KEYSTORE_PATH = keyStorePath;
        TRUSTSTORE_PATH = trustStorePath;
        KEY_PASSWORD = keyStorePwd;
        STORE_PASSWORD = trustStorePwd;
    }

    private static Options createClusterTLSExampleOptions(
            String serverUrls, boolean allowReconnect) throws Exception {
        return createExampleOptions(serverUrls, true, allowReconnect, true);
    }

    /**
     * @param urls      服务器地址:
     *                  单机:字符串 例如:tls://server:4222
     *                  集群: 多个地址以英文半角逗号隔开 例如: tls://server1:4222,tls://server2:4222,tls://server3:4222
     * @param isCluster nats streaming server是集群模式时 传入 true
     * @param isTLS     nats streaming server开启TLS时 传入true
     * @return nats streaming options
     * @throws Exception 创建ssl上下文发生异常时抛出
     * @author veily
     */
    private static Options createExampleOptions(String urls,
                                                boolean isCluster,
                                                boolean allowReconnect,
                                                boolean isTLS) throws Exception {
        Options.Builder builder = new Options.Builder();
        if (isCluster) {
            builder.servers(urls.split(","));
        } else {
            builder.server(urls);
        }
        if (isTLS) {
            //使用自定义SSL Context
            builder.sslContext(createContext());
        }
        builder.
                connectionTimeout(Duration.ofSeconds(5)).
                pingInterval(Duration.ofSeconds(10)).
                reconnectWait(Duration.ofSeconds(1)).
                errorListener(new ErrorListener() {
                    public void exceptionOccurred(Connection conn, Exception exp) {
                        System.out.println("Exception " + exp.getMessage());
                    }

                    public void errorOccurred(Connection conn, String type) {
                        System.out.println("Error " + type);
                    }

                    public void slowConsumerDetected(Connection conn, Consumer consumer) {
                        System.out.println("Slow consumer");
                    }
                }).
                connectionListener(new ConnectionListener() {
                    public void connectionEvent(Connection conn, Events type) {
                        System.out.println("Status change " + type);
                    }
                });

        if (!allowReconnect) {
            builder = builder.noReconnect();
        } else {
            builder = builder.maxReconnects(-1);
        }

        if (System.getenv("NATS_NKEY") != null && System.getenv("NATS_NKEY") != "") {
            AuthHandler handler = new ExampleAuthHandler(System.getenv("NATS_NKEY"));
            builder.authHandler(handler);
        } else if (System.getenv("NATS_CREDS") != null && System.getenv("NATS_CREDS") != "") {
            builder.authHandler(Nats.credentials(System.getenv("NATS_CREDS")));
        }

        return builder.build();
    }

    private static SSLContext createContext() throws Exception {
        SSLContext ctx = SSLContext.getInstance(io.nats.client.Options.DEFAULT_SSL_PROTOCOL);
        ctx.init(createPrivateKeyManagers(), createTrustManagers(), new SecureRandom());
        return ctx;
    }

    private static KeyStore loadKeystore(String path) throws Exception {
        KeyStore store = KeyStore.getInstance("JKS");
        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(path));
            store.load(in, STORE_PASSWORD.toCharArray());
        } finally {
            if (in != null) {
                in.close();
            }
        }
        return store;
    }

    private static KeyManager[] createPrivateKeyManagers() throws Exception {
        KeyStore store = loadKeystore(KEYSTORE_PATH);
        KeyManagerFactory factory = KeyManagerFactory.getInstance(ALGORITHM);
        factory.init(store, KEY_PASSWORD.toCharArray());
        return factory.getKeyManagers();
    }

    private static TrustManager[] createTrustManagers() throws Exception {
        KeyStore store = loadKeystore(TRUSTSTORE_PATH);
        TrustManagerFactory factory = TrustManagerFactory.getInstance(ALGORITHM);
        factory.init(store);
        return factory.getTrustManagers();
    }
}
