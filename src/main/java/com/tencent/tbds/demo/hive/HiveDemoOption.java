package com.tencent.tbds.demo.hive;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.IOException;

public class HiveDemoOption {

    private final OptionParser optionParser;

    private final OptionSet optionSet;

    private final OptionSpec<String> zkList;

    private final OptionSpec<String> user;

    private final OptionSpec<String> password;

    public HiveDemoOption(String[] args) {
        this.optionParser = new OptionParser();

        user = optionParser.accepts("user")
                .withRequiredArg()
                .required()
                .describedAs("the user name for hive connection");

        password = optionParser.accepts("password")
                .withRequiredArg()
                .required()
                .describedAs("the password for hive connection");

        zkList = optionParser.accepts("zk-list")
                .withRequiredArg()
                .required()
                .describedAs("the zookeeper connection in the form host1:port1,host2:port2");

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

    public String getZkList() {
        return optionSet.valueOf(zkList);
    }

    public String getUser() {
        return optionSet.valueOf(user);
    }

    public String getPassword() {
        return optionSet.valueOf(password);
    }
}
