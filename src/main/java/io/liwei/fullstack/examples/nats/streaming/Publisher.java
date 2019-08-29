package io.liwei.fullstack.examples.nats.streaming;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.streaming.AckHandler;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Publisher {

    private String keystorePath;
    private String truststorePath;
    private String storePassword;
    private String keyPassword;
    private String serverUrls;
    private String clusterId = "matrixcluster";
    private String subject = "foo";
    private String payloadString = "bar";

    private String clientId = "testJavaPub";
    private boolean async;


    private static final String usageString =
            "\nUsage: java Publisher [options] <subject> <message>\n\nOptions:\n"
                    + "    -s, --server   <urls>           NATS Streaming server URL(s)\n"
                    + "    -c, --cluster  <cluster name>   NATS Streaming cluster name\n"
                    + "    -id,--clientid <client ID>      NATS Streaming client ID\n"
                    + "    -a, --async                     Asynchronous publish mode";

    public Publisher(String[] args) {
        parseArgs(args);
        ExampleUtils.initKeyStore(keystorePath, keyPassword, truststorePath, storePassword);

    }

    private static void usage() {
        System.err.println(usageString);
    }

    public void run() throws Exception {

        Options opts = NatsStreaming.defaultOptions();

        Connection nc;
        if (serverUrls.startsWith("tls://")) {
            nc = Nats.connect(ExampleUtils.createClusterTLSExampleOptions(serverUrls, true));
        } else {
            nc = Nats.connect(ExampleUtils.createExampleOptions(serverUrls,
                    true, true, false));
        }

        if (nc != null) {
            opts = new Options.Builder().natsConn(nc).build();
        }

        System.out.println(serverUrls);
        try (StreamingConnection sc = NatsStreaming.connect(clusterId, clientId, opts)) {
            final CountDownLatch latch = new CountDownLatch(1);
            final String[] guid = new String[1];
            byte[] payload = payloadString.getBytes();

            AckHandler acb = new AckHandler() {
                @Override
                public void onAck(String nuid, Exception ex) {
                    System.out.printf("Received ACK for guid %s\n", nuid);
                    if (ex != null) {
                        System.err.printf("Error in server ack for guid %s: %s", nuid,
                                ex.getMessage());
                    }
                    if (!guid[0].equals(nuid)) {
                        System.err.printf(
                                "Expected a matching guid in ack callback, got %s vs %s\n", nuid,
                                guid[0]);
                    }
                    System.out.flush();
                    latch.countDown();
                }
            };

            if (!async) {
                try {
                    //noinspection ConstantConditions
                    sc.publish(subject, payload);
                } catch (Exception e) {
                    System.err.printf("Error during publish: %s\n", e.getMessage());
                    throw (e);
                }
                System.out.printf("Published [%s] : '%s'\n", subject, payloadString);
            } else {
                try {
                    //noinspection ConstantConditions
                    guid[0] = sc.publish(subject, payload, acb);
                    latch.await();
                } catch (IOException e) {
                    System.err.printf("Error during async publish: %s\n", e.getMessage());
                    throw (e);
                }

                if (guid[0].isEmpty()) {
                    String msg = "Expected non-empty guid to be returned.";
                    System.err.println(msg);
                    throw new IOException(msg);
                }
                System.out.printf("Published [%s] : '%s' [guid: %s]\n", subject, payloadString,
                        guid[0]);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void parseArgs(String[] args) {
        if (args == null || args.length < 2) {
            throw new IllegalArgumentException("must supply at least subject and msg");
        }

        List<String> argList = new ArrayList<String>(Arrays.asList(args));

        // The last two args should be subject and payloadString
        // get the payloadString and remove it from args
        payloadString = argList.remove(argList.size() - 1);

        // get the subject and remove it from args
        subject = argList.remove(argList.size() - 1);

        // Anything left is flags + args
        Iterator<String> it = argList.iterator();
        while (it.hasNext()) {
            String arg = it.next();
            switch (arg) {
                case "-s":
                case "--server":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    serverUrls = it.next();
                    it.remove();
                    continue;
                case "-ks":
                case "--keystore":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    keystorePath = it.next();
                    it.remove();
                    continue;
                case "-ts":
                case "--truststore":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    truststorePath = it.next();
                    it.remove();
                    continue;
                case "-ks_pwd":
                case "--keystore_password":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    keyPassword = it.next();
                    it.remove();
                    continue;
                case "-ts_pwd":
                case "--truststore_password":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    storePassword = it.next();
                    it.remove();
                    continue;
                case "-c":
                case "--cluster":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    clusterId = it.next();
                    it.remove();
                    continue;
                case "-id":
                case "--clientid":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    clientId = it.next();
                    it.remove();
                    continue;
                case "-a":
                case "--async":
                    async = true;
                    it.remove();
                    continue;
                default:
                    throw new IllegalArgumentException(
                            String.format("Unexpected token: '%s'", arg));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 2) {
            throw new IllegalArgumentException("must supply at least subject and msg");
        }

        Publisher publisher = new Publisher(args);
        try {
            publisher.run();
        } catch (IllegalArgumentException e) {
            System.out.flush();
            System.err.println(e.getMessage());
            Publisher.usage();
            System.err.flush();
            throw e;
        }

    }


}
