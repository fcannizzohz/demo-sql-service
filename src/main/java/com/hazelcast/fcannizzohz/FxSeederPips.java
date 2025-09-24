package com.hazelcast.fcannizzohz;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlService;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * FX seeder that reads fixed/reference data from {@link Data}.
 * Generates a single-day random FX snapshot (pip-based) for the requested date.
 */
public final class FxSeederPips {

    private FxSeederPips() {}

    /**
     * Generate one FX snapshot for the given date.
     *
     * @param hz        Hazelcast instance
     * @param asOf      date of the snapshot
     * @param rngSeed   RNG seed (null for non-deterministic)
     */
    public static void seedFxRatesForDate(HazelcastInstance hz,
                                          LocalDate asOf,
                                          Long rngSeed) {
        if (asOf == null) {
            asOf = LocalDate.now();
        }

        final SqlService sql = hz.getSql();
        final Random rnd = (rngSeed == null) ? new Random() : new Random(rngSeed);

        // Copy base rates from Data
        Map<String, BigDecimal> baseMap = new LinkedHashMap<>(Data.FX_RATE_TO_USD);

        for (Map.Entry<String, BigDecimal> e : baseMap.entrySet()) {
            final String ccy = e.getKey().toUpperCase();
            final BigDecimal base = e.getValue();

            BigDecimal rate;
            if (Data.FX_BASE_CURRENCY.equalsIgnoreCase(ccy)) {
                // Base currency (GBP) is fixed
                rate = BigDecimal.ONE.setScale(6, RoundingMode.HALF_UP);
            } else {
                // random step in pips: k ∈ {-4..-1, +1..+4}
                int k = rnd.nextInt(8) + 1;        // 1..8
                if (rnd.nextBoolean()) k = -k;     // ±
                BigDecimal pip = pipSizeFor(ccy, base);
                BigDecimal delta = pip.multiply(BigDecimal.valueOf(k));
                rate = base.add(delta).setScale(6, RoundingMode.HALF_UP);
            }

            // Insert single snapshot row
            sql.execute("INSERT INTO fx_rates (__key, Currency, AsOf, RateToUSD) VALUES (?, ?, ?, ?)",
                    Data.fxKey(ccy, asOf), ccy, asOf, rate);
        }
    }

    private static BigDecimal pipSizeFor(String ccy, BigDecimal base) {
        if (base.compareTo(new BigDecimal("0.010000")) < 0) {
            return new BigDecimal("0.000001"); // tiny quotes (JPY, INR)
        }
        return new BigDecimal("0.000100");     // default pip size
    }
}
