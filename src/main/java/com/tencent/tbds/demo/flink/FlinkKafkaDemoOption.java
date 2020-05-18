package com.tencent.tbds.demo.flink;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.IOException;

public class FlinkKafkaDemoOption {

    private final OptionParser optionParser;
    private final OptionSet optionSet;
    private final OptionSpec<String> kafkaBrokers;
    private final OptionSpec<String> groupId;
    private final OptionSpec<String> authId;
    private final OptionSpec<String> authKey;
    private final OptionSpec<String> topic;

    public FlinkKafkaDemoOption(String[] args) {
        this.optionParser = new OptionParser();

        kafkaBrokers = optionParser.accepts("kafka-brokers")
                .withRequiredArg()
                .required()
                .describedAs("kafka broker list, in the form of host1:port1,host2:port2...");

        groupId = optionParser.accepts("group-id")
                .withRequiredArg()
                .required()
                .describedAs("kafka group id to consume data");

        authId = optionParser.accepts("auth-id")
                .withRequiredArg()
                .required()
                .describedAs("authentication secure id");

        authKey = optionParser.accepts("auth-key")
                .withRequiredArg()
                .required()
                .describedAs("authentication secure key");

        topic = optionParser.accepts("topic")
                .withRequiredArg()
                .required()
                .describedAs("kafka topic");

        optionParser.accepts("help").forHelp();
        this.optionSet = optionParser.parse(args);
    }

    public boolean hasHelp() {
        return optionSet.has("help");
    }

    public void printHelp() throws IOException {
        optionParser.printHelpOn(System.out);
    }

    public String getKafkaBrokers() {
        return optionSet.valueOf(kafkaBrokers);
    }

    public String getAuthId() {
        return optionSet.valueOf(authId);
    }

    public String getAuthKey() {
        return optionSet.valueOf(authKey);
    }

    public String getTopic() {
        return optionSet.valueOf(topic);
    }

    public String getGroupId() {
        return optionSet.valueOf(groupId);
    }
}
