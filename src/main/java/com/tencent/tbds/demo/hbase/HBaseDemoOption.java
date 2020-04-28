package com.tencent.tbds.demo.hbase;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.IOException;

public class HBaseDemoOption {
    private final OptionParser optionParser;
    private final OptionSet optionSet;
    private final OptionSpec<String> authId;
    private final OptionSpec<String> authKey;
    private final OptionSpec<String> zkList;
    private final OptionSpec<String> tableName;

    public HBaseDemoOption(String[] args) {
        this.optionParser = new OptionParser();

        authId = optionParser.accepts("auth-id")
                .withRequiredArg()
                .required()
                .describedAs("authentication secure id");

        authKey = optionParser.accepts("auth-key")
                .withRequiredArg()
                .required()
                .describedAs("authentication secure key");

        zkList = optionParser.accepts("zk-list")
                .withRequiredArg()
                .required()
                .describedAs("comma separated list of zookeeper servers, in the form host1,host2...");

        tableName = optionParser.accepts("table-name")
                .withRequiredArg()
                .required()
                .describedAs("hbase table which read data from");

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

    public String getZkList() {
        return optionSet.valueOf(zkList);
    }

    public String getTableName() {
        return optionSet.valueOf(tableName);
    }
}
