package com.hazelcast.fcannizzohz;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static com.hazelcast.fcannizzohz.Data.normCountry;

public final class Bank {
    public final String name;          // human-readable bank name
    public final String bic8;          // canonical BIC8 (uppercase, 8 chars)
    public final String country;       // ISO2 country
    public final List<String> ibanPrefixes; // country-specific IBAN "fingerprints" (prefixes of BBAN/IBAN)
    public final double weight;        // selection weight when picking a random bank in this country

    public Bank(String name, String bic, String country, List<String> ibanPrefixes, double weight) {
        this.name = Objects.requireNonNull(name).trim();
        String b = Objects.requireNonNull(bic).trim().toUpperCase(Locale.ROOT);
        this.bic8 = (b.length() >= 8) ? b.substring(0, 8) : b; // normalise to BIC8
        this.country = normCountry(country);
        this.ibanPrefixes = List.copyOf(ibanPrefixes == null ? List.of() : ibanPrefixes);
        this.weight = weight;
    }

    @Override public String toString() { return name + " (" + bic8 + "," + country + ")"; }
}