package com.tencent.tbds.demo.spark;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.IOException;

public class SparkHBaseDemoOption {

    private final OptionParser optionParser;
    private final OptionSet optionSet;
    private final OptionSpec<String> authId;
    private final OptionSpec<String> authKey;
    private final OptionSpec<String> authUser;
    private final OptionSpec<String> zkHost;

    public SparkHBaseDemoOption(String[] args) {
        this.optionParser = new OptionParser();

        authId = optionParser.accepts("auth-id")
                .withRequiredArg()
                .required()
                .describedAs("authentication secure id");

        authKey = optionParser.accepts("auth-key")
                .withRequiredArg()
                .required()
                .describedAs("authentication secure key");

        authUser = optionParser.accepts("auth-user")
                .withRequiredArg()
                .describedAs("authentication user name");

        zkHost = optionParser.accepts("zk-host")
                .withRequiredArg()
                .required()
                .describedAs("comma separated list of zookeeper servers, in the form host1,host2...");

        optionParser.accepts("help").forHelp();
        this.optionSet = optionParser.parse(args);
    }

    public boolean hasHelp() {
        return optionSet.has("help");
    }

    public void printHelp() throws IOException {
        optionParser.printHelpOn(System.out);
    }

    public OptionParser getOptionParser() {
        return optionParser;
    }

    public OptionSet getOptionSet() {
        return optionSet;
    }

    public String getAuthId() {
        return optionSet.valueOf(authId);
    }

    public String getAuthKey() {
        return optionSet.valueOf(authKey);
    }

    public String getAuthUser() {
        return optionSet.valueOf(authUser);
    }

    public String getZkHost() {
        return optionSet.valueOf(zkHost);
    }
}
