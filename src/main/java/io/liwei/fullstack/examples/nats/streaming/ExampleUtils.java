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

import javax.net.ssl.*;

public class ExampleUtils {

    private static String KEYSTORE_PATH;
    private static String TRUSTSTORE_PATH;
    private static String STORE_PASSWORD;
    private static String KEY_PASSWORD;
    private static String ALGORITHM = "SunX509";

    public static void initKeyStore(String keyPath, String keyPwd, String trustStorePath, String trustPwd) {
        KEYSTORE_PATH = keyPath;
        TRUSTSTORE_PATH = trustStorePath;
        KEY_PASSWORD = keyPwd;
        STORE_PASSWORD = trustPwd;
    }

    public static Options createClusterTLSExampleOptions(
            String serverUrls, boolean allowReconnect) throws Exception {
        return createExampleOptions(serverUrls, true, allowReconnect, true);
    }

    public static Options createExampleOptions(String servers,
                                               boolean isCluster,
                                               boolean allowReconnect,
                                               boolean isTLS) throws Exception {
        Options.Builder builder = new Options.Builder();
        if (isCluster) {
            builder.servers(servers.split(","));
        } else {
            builder.server(servers);
        }
        if (isTLS) {
//            SSL
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
