package com.tencent.tbds.demo.spark;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.IOException;

public class ReadHiveTableDemoOption {
    private final OptionParser optionParser;
    private final OptionSet optionSet;
    private final OptionSpec<String> hiveMetastoreUris;
    private final OptionSpec<String> defaultFS;
    private final OptionSpec<String> hiveDb;
    private final OptionSpec<String> hiveTable;

    public ReadHiveTableDemoOption(String[] args) {
        this.optionParser = new OptionParser(false);

        // hive metastore address, which is required
        hiveMetastoreUris = optionParser.accepts("hive-metastore-uris")
                .withRequiredArg()
                .required()
                .describedAs("a list of uris to connect to hive meta store, e.g. thrift://host1:9083,thrift://host2:9083");

        // file system address, default is hdfs://hdfsCluster
        defaultFS = optionParser.accepts("default-fs")
                .withRequiredArg()
                .defaultsTo("hdfs://hdfsCluster")
                .describedAs("file system url, default is hdfs://hdfsCluster");

        // hive database to read data, which is required
        hiveDb = optionParser.accepts("hive-db")
                .withRequiredArg()
                .required()
                .describedAs("hive database");

        // hive table to read data, which is required
        hiveTable = optionParser.accepts("hive-table")
                .withRequiredArg()
                .required()
                .describedAs("hive table");

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

    public String getHiveMetastoreUris() {
        return optionSet.valueOf(hiveMetastoreUris);
    }

    public String getDefaultFS() {
        return optionSet.valueOf(defaultFS);
    }

    public String getHiveDb() {
        return optionSet.valueOf(hiveDb);
    }

    public String getHiveTable() {
        return optionSet.valueOf(hiveTable);
    }
}
