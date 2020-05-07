package com.tencent.tbds.demo.mapreduce;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.IOException;

public class MapReduceDemoOption {

    private final OptionParser optionParser;
    private final OptionSet optionSet;
    private final OptionSpec<String> authUser;
    private final OptionSpec<String> authId;
    private final OptionSpec<String> authKey;
    private final OptionSpec<String> input;
    private final OptionSpec<String> output;

    public MapReduceDemoOption(String[] args) {
        this.optionParser = new OptionParser();

        authUser = optionParser.accepts("auth-user")
                .withRequiredArg()
                .required()
                .describedAs("authentication user name");

        authId = optionParser.accepts("auth-id")
                .withRequiredArg()
                .required()
                .describedAs("authentication secure id");

        authKey = optionParser.accepts("auth-key")
                .withRequiredArg()
                .required()
                .describedAs("authentication secure key");

        input = optionParser.accepts("input")
                .withRequiredArg()
                .required()
                .describedAs("input path to read data");

        output = optionParser.accepts("output")
                .withRequiredArg()
                .required()
                .describedAs("output path to write data");

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

    public String getAuthUser() {
        return optionSet.valueOf(authUser);
    }

    public String getAuthId() {
        return optionSet.valueOf(authId);
    }

    public String getAuthKey() {
        return optionSet.valueOf(authKey);
    }

    public String getInput() {
        return optionSet.valueOf(input);
    }

    public String getOutput() {
        return optionSet.valueOf(output);
    }
}
