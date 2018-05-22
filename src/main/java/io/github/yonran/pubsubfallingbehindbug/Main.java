package io.github.yonran.pubsubfallingbehindbug;

import org.slf4j.bridge.SLF4JBridgeHandler;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "PubSubber", mixinStandardHelpOptions = true, version = "v0.0.0",
        subcommands = {CloudPubSub.class})
public class Main implements Runnable {
	@Override
	public void run() {
	System.out.println("Please choose a subcommand (cloud)");
	}

	public static void main(String[] args) {
		// Add SLF4J handler to receive messages from java.util.logging
		// (which is used by com.google.cloud)
		SLF4JBridgeHandler.install();
		CommandLine.run(new Main(), System.out, args);
	}
}
