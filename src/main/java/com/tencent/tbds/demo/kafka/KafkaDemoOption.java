package com.tencent.tbds.demo.kafka;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.IOException;

public class KafkaDemoOption {

    private final OptionParser optionParser;
    private final OptionSet optionSet;
    private final OptionSpec<String> authId;
    private final OptionSpec<String> authKey;
    private final OptionSpec<String> kafkaBrokers;
    private final OptionSpec<String> topic;
    private final OptionSpec<String> offsetReset;

    public KafkaDemoOption(String[] args) {
        this.optionParser = new OptionParser();

        authId = optionParser.accepts("auth-id")
                .withRequiredArg()
                .describedAs("authentication secure id");

        authKey = optionParser.accepts("auth-key")
                .withRequiredArg()
                .describedAs("authentication secure key");

        kafkaBrokers = optionParser.accepts("kafka-brokers")
                .withRequiredArg()
                .required()
                .describedAs("comma separated list of kafka brokers, in the form host1:port1,host2:port2...");

        topic = optionParser.accepts("topic")
                .withRequiredArg()
                .required()
                .describedAs("kafka topic to read or write data");

        offsetReset = optionParser.accepts("offset-reset")
                .withRequiredArg()
                .defaultsTo("latest")
                .describedAs("what to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server, could be set to earliest or latest");

        optionParser.accepts("help").forHelp();
        this.optionSet = optionParser.parse(args);
    }

    public boolean hasHelp() {
        return optionSet.has("help");
    }

    public void printHelp() throws IOException {
        optionParser.printHelpOn(System.out);
    }

    public String getAuthId() {
        return optionSet.valueOf(authId);
    }

    public String getAuthKey() {
        return optionSet.valueOf(authKey);
    }

    public String getKafkaBrokers() {
        return optionSet.valueOf(kafkaBrokers);
    }

    public String getTopic() {
        return optionSet.valueOf(topic);
    }

    public String getOffsetReset() {
        return optionSet.valueOf(offsetReset);
    }
}
