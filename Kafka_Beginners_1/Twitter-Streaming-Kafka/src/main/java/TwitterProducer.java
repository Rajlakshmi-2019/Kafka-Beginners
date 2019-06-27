import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String consumerKey = "JE5moN5FOKU3bvXEDDjhP8oWZ";
    String consumerSecret = "kc8CDAQ8uKS6x5rqBzu6Hgg8PNuGA3hnKjeeWUGbgBiIiYZ7EE";
    String token = "2860587519-87zk8rQaxOd5TjBMoDt2lq4n0V97EVNBHrXLla8";
    String secret = "oOwGaNEYkmGPXGVMTpb6udVwgdPDZtsuOFMVuWadfryX4";

    public TwitterProducer() {

    }

    public static void main(String[] args) {

        new TwitterProducer().run();

    }

    public void run() {

        logger.info("Setup");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        //Twitter Client
        Client client = createTwitterClient(msgQueue);
        client.connect();


        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(10, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
            }
        }

        logger.info("End of Application ");

    }


        public Client createTwitterClient (BlockingQueue < String > msgQueue) {


/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
            Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
            StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

            // Optional: set up some followings and track terms
//            List<Long> followings = Lists.newArrayList(1234L, 566788L);
            List<String> terms = Lists.newArrayList("Modi","Bitcoin","Kafka");
            //  hosebirdEndpoint.followings(followings);
            hosebirdEndpoint.trackTerms(terms);

            logger.info("Establishing Connection !!");
// These secrets should be read from a config file
            Authentication hosebirdAuth = new OAuth1("consumerKey", "consumerSecret", "token", "secret");
            logger.info("Establised Connection !!");

            ClientBuilder builder = new ClientBuilder()
                    .name("Hosebird-Client-01")                              // optional: mainly for the logs
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(msgQueue));

            Client hosebirdClient = builder.build();
            return hosebirdClient;

        }
        // Create kafka producer

        //loop tweets to Kafka


    }

