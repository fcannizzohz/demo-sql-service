package com.hazelcast.fcannizzohz;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

@Command(name = "payment-data-producer", mixinStandardHelpOptions = true, description = "Produces random payment data into Hazelcast.")
public class PaymentFlowToHazelcastCmd
        implements Runnable {

    @Option(names = {"-c", "--cluster"}, defaultValue = "dev", description = "Hazelcast cluster name (default: ${DEFAULT-VALUE})")
    String cluster;

    @Option(names = {"-m", "--member"}, defaultValue = "127.0.0.1:5701", description = "Hazelcast member address host:port, (default: ${DEFAULT-VALUE})")
    String member;

    @Option(names = {"-h", "--hours-past"}, defaultValue = "0", paramLabel = "<hours>", description = "Start the simulated timeline N hours in the past (default: ${DEFAULT-VALUE})")
    int hoursPast;

    @Option(names = {"-t", "--min-tx-count"}, defaultValue = "0", paramLabel = "<minTxCount>", description = "Min number of tx per payment file (default: ${DEFAULT-VALUE})")
    int minTxCount;

    @Option(names = {"-g", "--gen-poll-sec"}, defaultValue = "60", paramLabel = "<genPollSec>", description = "Generation interval - generate a file every N sec (default: ${DEFAULT-VALUE})")
    int genPollSec;

    @Option(names = {"-r", "--rate"}, defaultValue = "1.0", paramLabel = "<rate>", description = "Time scale factor (>0). 3600 => 1 real second = 1 simulated hour (default: ${DEFAULT-VALUE})")
    double rate;

    @Option(names = {"--switch-to-real-when-caught-up"}, defaultValue = "true", description = "Switch to system clock when simulated time reaches real now (default: ${DEFAULT-VALUE})")
    boolean switchToRealWhenCaughtUp;

    public static void main(String[] args) {
        int code = new CommandLine(new PaymentFlowToHazelcastCmd()).execute(args);
        System.exit(code);
    }

    @Override
    public void run() {
        if (hoursPast < 0) {
            throw new IllegalArgumentException("--hours-past must be >= 0");
        }
        if (!(rate > 0.0) || Double.isInfinite(rate)) {
            throw new IllegalArgumentException("--rate must be > 0 and finite");
        }

        final Clock clock;
        if (hoursPast == 0 && rate == 1.0) {
            clock = Clock.systemUTC(); // no scaling
        } else {
            Instant simStart = Instant.now().minus(Duration.ofHours(hoursPast)); // keeps current time-of-day
            clock = ScaledClock.startingAtPast(ZoneOffset.UTC, simStart, rate);
        }

        System.out.println("Connecting to cluster: " + cluster + " with member: " + member + " | hoursPast=" + hoursPast + " rate=" + rate + " switchToRealWhenCaughtUp=" + switchToRealWhenCaughtUp);

        try {
            // Preferred: pass the clock through
            PaymentFlowToHazelcast.run(cluster, member, minTxCount, genPollSec, clock, rate, switchToRealWhenCaughtUp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
