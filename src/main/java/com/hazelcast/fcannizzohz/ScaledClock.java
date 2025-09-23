package com.hazelcast.fcannizzohz;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Maps real time to a simulated timeline:
 * sim(t) = simStart + (realNow - realStart) * rate
 * where rate>1 speeds up time (e.g., 3600 => 1 real second = 1 simulated hour).
 */
public final class ScaledClock
        extends Clock {
    private final ZoneId zone;
    private final Instant realStart;
    private final Instant simStart;
    private final double rate; // e.g., 3600.0

    public ScaledClock(ZoneId zone, Instant simStart, Instant realStart, double rate) {
        if (rate <= 0.0 || Double.isNaN(rate) || Double.isInfinite(rate)) {
            throw new IllegalArgumentException("rate must be > 0 and finite");
        }
        this.zone = zone == null ? ZoneOffset.UTC : zone;
        this.simStart = simStart;
        this.realStart = realStart;
        this.rate = rate;
    }

    public static ScaledClock startingAtPast(ZoneId zone, Instant simStart, double rate) {
        return new ScaledClock(zone, simStart, Instant.now(), rate);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new ScaledClock(zone, simStart, realStart, rate);
    }

    @Override
    public Instant instant() {
        Instant realNow = Instant.now();
        long realNanos = Duration.between(realStart, realNow).toNanos();
        long simNanos = (long) Math.floor(realNanos * rate);
        return simStart.plusNanos(simNanos);
    }
}
