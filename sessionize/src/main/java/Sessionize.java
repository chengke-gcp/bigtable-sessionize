import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.transforms.*;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.KV;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;

import org.json.JSONObject;
import org.joda.time.Duration;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class Sessionize {

    /**
     * Class to hold info about message
     */
    @DefaultCoder(AvroCoder.class)
    static class Message{
        Integer message_num;
        Integer value;
        Integer session_id;

        public Message() {}

        public Message(Integer session_id, Integer message_num, Integer value) {
            this.session_id = session_id;
            this.message_num = message_num;
            this.value = value;
        }


        public Integer getMessage_num() {
            return this.message_num;
        }
        public Integer getValue() {
            return this.value;
        }
        public Integer getSession_id() {
            return this.session_id;
        }
    }




    static class ParseMessages extends DoFn<String, Message> {

        // Log and count parse errors.
        private static final Logger LOG = LoggerFactory.getLogger(ParseMessages.class);
        private final Counter numParseErrors = Metrics.counter("main", "ParseErrors");

        @ProcessElement
        public void processElement(ProcessContext c) {
            JSONObject json = new JSONObject(c.element());
            try {
                Integer session_id = json.getInt("session_id");
                Integer message_num = json.getInt("message_num");
                Integer value = json.getInt("value");
                Message message = new Message(session_id, message_num, value);
                c.output(message);
            } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
                numParseErrors.inc();
                LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
            }
        }
    }

    static final DoFn<Message, Mutation> FormatAsHBASEMutation = new DoFn<Message, Mutation>() {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(DoFn<Message, Mutation>.ProcessContext c) throws Exception {

            Message message = c.element();

            Integer session_id = message.getSession_id();
            Integer message_num = message.getMessage_num();
            Integer value = message.getValue();

            Put p = new Put((session_id.toString() + "#" + String.format("%05d", message_num)).getBytes());

            p.addColumn(("data").getBytes(), ("value").getBytes(), (value.toString().getBytes()));
            p.addColumn(("identifiers").getBytes(), ("session_id").getBytes(), session_id.toString().getBytes());
            p.addColumn(("identifiers").getBytes(), ("message_num").getBytes(), message_num.toString().getBytes());

            c.output(p);

        }
    };

    static class BuildMessageKV extends DoFn<Message, KV<Integer, Message>> {
        private static final Logger LOG = LoggerFactory.getLogger(BuildMessageKV.class);
        @ProcessElement
        public void processElement(ProcessContext c) {
            Message input = c.element();
            LOG.info("Built KV for session_id: " + input.getSession_id() + " message_num: " + input.getMessage_num());
            c.output(KV.of(input.getSession_id(), input));
        }
    }

    static class BuildPubsubMessage extends DoFn<Integer, PubsubMessage> {
        private static final Logger LOG = LoggerFactory.getLogger(BuildPubsubMessage.class);
        @ProcessElement
        public void processElement(ProcessContext c) {
            Integer session_id = c.element();
            String message = session_id.toString();
            PubsubMessage pubsubMessage = new PubsubMessage(
                message.getBytes(StandardCharsets.UTF_8),
                Collections.emptyMap());
            c.output(pubsubMessage);
        }
    }


    interface Options extends StreamingOptions {
        @Description("Pub/Sub topic to read from")
        @Default.String("projects/bigtable-sessionize/subscriptions/messages-dataflow")
        String getMessagesSubscription();
        void setMessagesSubscription(String value);

        @Description("Pub/Sub topic to write to")
        @Default.String("projects/bigtable-sessionize/topics/completed-sessions")
        String getSessionTopic();
        void setSessionTopic(String value);

        @Description("Bigtable Project Id")
        @Default.String("bigtable-sessionize")
        String getBigtableProjectId();
        void setBigtableProjectId(String value);

        @Description("Bigtable Cluster Id")
        @Default.String("messages")
        String getBigtableInstanceId();
        void setBigtableInstanceId(String value);

        @Description("Bigtable Table")
        @Default.String("messages")
        String getBigtableTableId();
        void setBigtableTableId(String value);
    }


    public static void main(String[] args) {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        // Enforce that this pipeline is always run in streaming mode.
        options.setStreaming(true);

        CloudBigtableTableConfiguration bigtableConfig =
            new CloudBigtableTableConfiguration.Builder()
                .withProjectId(options.getBigtableProjectId())
                .withInstanceId(options.getBigtableInstanceId())
                .withTableId(options.getBigtableTableId())
                .build();

        // Create the Pipeline object with the options we defined above.
        Pipeline pipeline = Pipeline.create(options);


        // Apply the pipeline's transforms.

        PCollection<Message> messages = pipeline
            .apply("Read from PubSub", PubsubIO.readStrings()
                .withTimestampAttribute("timestamp_ms")
                .withIdAttribute("message_id")
                .fromSubscription(options.getMessagesSubscription()))
            .apply("Parse Messages", ParDo.of(new ParseMessages()));

        // Write Messages to Cloud Bigtable
        messages.apply("Format as HBASE Mutations", ParDo.of(FormatAsHBASEMutation))
            .apply("Write to Bigtable", CloudBigtableIO.writeToTable(bigtableConfig));

        // Window Messages into Sessions, emit Session ID once Session completes
        PCollection<Integer> completed_sessions = messages
            .apply("Build Message KV", ParDo.of(new BuildMessageKV()))
            .apply("Window into Sessions", Window.<KV<Integer, Message>>into(
                //FixedWindows.of(Duration.standardSeconds(10)))
                Sessions.withGapDuration(Duration.standardMinutes(1)))
                .withAllowedLateness(Duration.standardMinutes(60))
                .triggering(
                    AfterWatermark.pastEndOfWindow())
                //    AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1)))
                .discardingFiredPanes()
            )
            .apply("Group by Session ID", GroupByKey.<Integer, Message>create())
            .apply("Extract Session ID", Keys.create());

        // Publish completed sessions to Pub/Sub
        completed_sessions
            .apply("Build PubSub Message", ParDo.of(new BuildPubsubMessage()))
            .apply("Write Messages to PubSub", PubsubIO.writeMessages().to(options.getSessionTopic()));

        // Run the pipeline.
        pipeline.run().waitUntilFinish();
    }
}
