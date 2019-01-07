package tweetfactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;



import kafka.producer.KeyedMessage;

import kafka.producer.ProducerConfig;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterKafkaProducer {

    private static final String topic = "test";

    private static Gson gson=new Gson() ;

    public static void run(String consumerKey, String consumerSecret,
                           String token, String secret) throws InterruptedException {

        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("client.id","camus");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
                producerConfig);

        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(1000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // add some track terms
        endpoint.trackTerms(Lists.newArrayList("trump"));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
                secret);
        // Authentication auth = new BasicAuth(username, password);

        // Create a new BasicClient. By default gzip is enabled.
        Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();

        // Establish a connection
        client.connect();

        // Do whatever needs to be done with messages
        // Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < 1000; msgRead++) {



            Tweet tweet=gson.fromJson(queue.take(), Tweet.class);
           // Tweet tweet1=



            KeyedMessage<String, String> message = null;


            message = new KeyedMessage<String, String>(topic,queue.take());
            producer.send(message);
        }

        producer.close();
        Thread.sleep(500);
        client.stop();


    }


    public static void main(String[] args) {
        try {
            TwitterKafkaProducer.run("hl665yOewbr9EaXvDUNb422BS",  "74h4gUcd89uoxtta61WjC7U6oqEAdNHLeIDXZBkrPppoim0pse", "3186964358-it93aOprDJu6aZrxRyfrjk9Fff7Vgc5l4iwH5BE","WW3zxcYFQsf8u7Lr4qrbOgoVXDY3lj6oO77VaeK7dJOEz");
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }






    private static String removeUrl(String commentstr)
    {
        String urlPattern = "((https?|ftp|gopher|telnet|file|t.co|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
        Pattern p = Pattern.compile(urlPattern,Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(commentstr);
        int i = 0;
        while (m.find()) {
            commentstr = commentstr.replaceAll(m.group(i),"").trim();
            i++;
        }

        //  commentstr.replaceFirst("RT","");
        //System.out.println(commentstr);
        return commentstr;
    }
}
