package de.tf.uni.freiburg.sparkrdf.run;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import de.tf.uni.freiburg.sparkrdf.constants.Const;

/**
 * Parses the command line arguments
 * 
 * @author Thorsten Berberich
 * 
 */
public final class ArgumentParser {

    @SuppressWarnings("static-access")
    public static void parseInput(String[] args) {
	// DEFINITION STAGE
	Options options = new Options();
	Option help = new Option("h", "help", false, "print this message");
	options.addOption(help);

	Option input = OptionBuilder.withArgName("file").hasArg()
		.withDescription("HDFS-Path to the RDF-Graph in N3-format")
		.withLongOpt("input").isRequired(true).create("i");
	options.addOption(input);

	Option query = OptionBuilder.withArgName("List of files").hasArg()
		.withDescription("Files that contain a SPARQL query")
		.withLongOpt("query").isRequired(false).create("q");
	options.addOption(query);

	Option output = OptionBuilder.withArgName("file").hasArg()
		.withDescription("HDFS-Path to store SPARQL query result file")
		.withLongOpt("output").isRequired(false).create("o");
	options.addOption(output);

	Option jobName = OptionBuilder.withArgName("String").hasArg()
		.withDescription("Name of this job").withLongOpt("jobName")
		.isRequired(false).create("jn");
	options.addOption(jobName);

	Option executorMem = OptionBuilder
		.withArgName("JVM memory string")
		.hasArg()
		.withDescription(
			"The memory that every executor will use given as JVM memory string (e.g. 512m or 24g).\n"
				+ "This sets spark.executor.memory")
		.withLongOpt("executorMem").isRequired(false).create("mem");
	options.addOption(executorMem);

	Option timeFile = OptionBuilder
		.withArgName("<file>")
		.hasArg()
		.withDescription(
			"Path to the file where the execution duration and the result count"
				+ " of this query should be appended on")
		.withLongOpt("time").isRequired(false).create("t");
	options.addOption(timeFile);

	Option saveLoadedGraph = OptionBuilder
		.withDescription(
			"Save the output of the loading as a object file."
				+ "This file will be loaded the next time you use this graph")
		.withLongOpt("saveOutput").isRequired(false).create("so");
	options.addOption(saveLoadedGraph);

	Option memoryFraction = OptionBuilder
		.withArgName("Float")
		.hasArg()
		.withDescription(
			"Memory to use to cache Spark data. Default: 0.6.\n"
				+ "This sets spark.storage.memoryFraction")
		.withLongOpt("memFraction").isRequired(false).create("fr");
	options.addOption(memoryFraction);

	Option defaultParallelism = OptionBuilder.withArgName("Int").hasArg()
		.withDescription("Sets spark.default.parallelism")
		.withLongOpt("parallel").isRequired(false).create("dp");
	options.addOption(defaultParallelism);

	Option printToConsole = OptionBuilder
		.withDescription(
			"Print the result to the console. May not work for big results.")
		.withLongOpt("print").isRequired(false).create("p");
	options.addOption(printToConsole);

	Option watDiv = OptionBuilder
		.withDescription(
			"Replaces the dc: and foaf: prefix to work with WatDiv"
				+ "Must be used if a WatDiv graph is loaded and if queries on a WatDiv graph are executed.")
		.withLongOpt("watDiv").isRequired(false).create("wd");
	options.addOption(watDiv);

	Option locale = OptionBuilder
		.withDescription(
			"Runs the application in local mode. The Spark master will be set to \"local\"")
		.withLongOpt("locale").isRequired(false).create("l");
	options.addOption(locale);

	Option countBased = OptionBuilder
		.withDescription(
			"Load the graph without hashing. Assign the node ids with count based method instead")
		.withLongOpt("countBasedGraphLoading").isRequired(false)
		.create("countBased");
	options.addOption(countBased);

	Option variablePredicate = OptionBuilder
		.withDescription(
			"Set this flag if you are planning to use queries where the predicate could be a variable field"
				+ "Must be set while the first loading of a graph.")
		.withLongOpt("variablePredicate").isRequired(false)
		.create("vp");
	options.addOption(variablePredicate);

	// PARSING STAGE
	CommandLineParser parser = new PosixParser();
	CommandLine cmd = null;
	try {
	    // parse the command line arguments
	    cmd = parser.parse(options, args);
	} catch (ParseException exp) {
	    // error when parsing commandline arguments
	    // automatically generate the help statement
	    HelpFormatter formatter = new HelpFormatter();
	    formatter.printHelp("PigSPARQL", options, true);
	    Logger.getLogger(QueryExecutor.class).fatal(exp.getMessage(), exp);
	    System.exit(-1);
	}

	if (cmd.hasOption("input")) {
	    Const.inputFile_$eq(cmd.getOptionValue("input"));
	}

	if (cmd.hasOption("query")) {
	    Const.query_$eq(cmd.getOptionValue("query"));
	}

	if (cmd.hasOption("output")) {
	    Const.outputFilePath_$eq(cmd.getOptionValue("output"));
	}

	if (cmd.hasOption("saveOutput")) {
	    Const.saveLoadOutput_$eq(cmd.hasOption("saveOutput"));
	}

	if (cmd.hasOption("executorMem")) {
	    Const.executorMem_$eq(cmd.getOptionValue("executorMem"));
	}

	if (cmd.hasOption("memFraction")) {
	    Const.memoryFraction_$eq(cmd.getOptionValue("memFraction"));
	}

	if (cmd.hasOption("locale")) {
	    Const.locale_$eq(cmd.hasOption("locale"));
	}

	if (cmd.hasOption("countBasedGraphLoading")) {
	    Const.countBasedLoading_$eq(cmd.hasOption("countBasedGraphLoading"));
	}

	if (cmd.hasOption("parallel")) {
	    Const.parallelism_$eq(cmd.getOptionValue("parallel"));
	}

	if (cmd.hasOption("jobName")) {
	    Const.jobName_$eq(cmd.getOptionValue("jobName"));
	}

	if (cmd.hasOption("time")) {
	    Const.timeFilePath_$eq(cmd.getOptionValue("time"));
	}

	if (cmd.hasOption("print")) {
	    Const.printToConsole_$eq(cmd.hasOption("print"));
	}

	if (cmd.hasOption("watDiv")) {
	    Const.watDiv_$eq(cmd.hasOption("watDiv"));
	}

	if (cmd.hasOption("variablePredicate")) {
	    Const.varPred_$eq(cmd.hasOption("variablePredicate"));
	}
    }

}
