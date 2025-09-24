package com.hazelcast.fcannizzohz;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Centralised reference data for PAIN/PACS/CAMT generators & converters.
 * - Country lists
 * - Country→Currency bindings
 * - IBAN lengths (subset)
 * - Realistic fixed debtor names per country
 */
public final class Data {

    /**
     * JAXB package for pain.001.001.03
     */
    public static final String P001_PACKAGE = "com.hz.demo.pmt.pain001_03";
    /**
     * Default country pool (ISO 3166-1 alpha-2) used by generators.
     */
    public static final List<String> DEFAULT_COUNTRIES = List.of("DE", "FR", "ES", "IT", "NL", "BE", "GB", "IE", "PT", "US", "CA", "BR", "IN", "CN", "KR", "JP", "AU", "ZA", "AE", "SG");

    // -------------------------
    // Country & currency setup
    // -------------------------
    /**
     * Country → currency (ISO 4217). One canonical currency per country.
     */
    public static final Map<String, String> COUNTRY_TO_CURRENCY = Map.ofEntries(
            // Euro area & friends
            Map.entry("DE", "EUR"), Map.entry("FR", "EUR"), Map.entry("ES", "EUR"), Map.entry("IT", "EUR"), Map.entry("NL", "EUR"), Map.entry("BE", "EUR"), Map.entry("IE", "EUR"), Map.entry("PT", "EUR"),
            // Non-EUR
            Map.entry("GB", "GBP"), Map.entry("US", "USD"), Map.entry("CA", "CAD"), Map.entry("BR", "BRL"), Map.entry("IN", "INR"), Map.entry("CN", "CNY"), Map.entry("KR", "KRW"), Map.entry("JP", "JPY"), Map.entry("AU", "AUD"), Map.entry("ZA", "ZAR"), Map.entry("AE", "AED"),
            Map.entry("SG", "SGD"));
    /**
     * IBAN country lengths (subset). Non-IBAN countries are intentionally omitted.
     * Use {@link #isIbanCountry(String)} to test membership.
     */
    public static final Map<String, Integer> IBAN_LENGTHS = Map.ofEntries(Map.entry("DE", 22), Map.entry("FR", 27), Map.entry("ES", 24), Map.entry("IT", 27), Map.entry("NL", 18), Map.entry("BE", 16), Map.entry("GB", 22), Map.entry("IE", 22), Map.entry("PT", 25),
            Map.entry("AE", 23) // UAE uses IBAN
    );
    /**
     * Fixed (deterministic) debtor company names per country.
     * These are plausible/legalistic names matching each jurisdiction’s common styles.
     */
    public static final Map<String, String> COUNTRY_TO_DEBTOR_NAME = Map.ofEntries(Map.entry("DE", "Müller Industrie GmbH"), Map.entry("FR", "Dupont Services SAS"), Map.entry("ES", "Iberia Servicios SL"), Map.entry("IT", "Rossi Tecnologie S.r.l."),
            Map.entry("NL", "Van Dijk Solutions B.V."), Map.entry("BE", "BelTech NV"), Map.entry("GB", "Thames Valley Systems Ltd"), Map.entry("IE", "Emerald Systems Ltd"), Map.entry("PT", "Lusitania Tecnologia Lda"), Map.entry("US", "Acme Corporation"),
            Map.entry("CA", "Maple Leaf Industries Inc."), Map.entry("BR", "BrasilTech LTDA"), Map.entry("IN", "Sharma Infotech Pvt Ltd"), Map.entry("CN", "Huaxin Trading Co., Ltd."), Map.entry("KR", "Hanil Electronics Co., Ltd."), Map.entry("JP", "Sakura Trading KK"),
            Map.entry("AU", "Southern Cross Pty Ltd"), Map.entry("ZA", "Table Mountain (Pty) Ltd"), Map.entry("AE", "Desert Gate FZE"), Map.entry("SG", "Lion City Pte. Ltd."));

    // -------------------------
    // Realistic debtor names
    // -------------------------
    /**
     * Fallback debtor name if a country isn’t present in the map above.
     */
    public static final String DEFAULT_DEBTOR_NAME = "Global Demo Company Ltd";

    /**
     * Base currency for exposure normalisation.
     */
    public static final String FX_BASE_CURRENCY = "USD";

    /**
     * Currency -> rate to USD (DECIMAL scale 12), for demo/testing.
     * Source ballparks as of 2025-09-24. These move — don’t hardcode for prod.
     */
    public static final Map<String, java.math.BigDecimal> FX_RATE_TO_USD = Map.ofEntries(
            Map.entry("USD", new java.math.BigDecimal("1.000000000000")),
            Map.entry("GBP", new java.math.BigDecimal("1.350000000000")), // ~1 GBP = 1.35 USD :contentReference[oaicite:0]{index=0}
            Map.entry("EUR", new java.math.BigDecimal("1.175000000000")), // ~1 EUR = 1.17–1.18 USD :contentReference[oaicite:1]{index=1}
            Map.entry("CAD", new java.math.BigDecimal("0.725000000000")), // ~0.72–0.73 USD per CAD :contentReference[oaicite:2]{index=2}
            Map.entry("BRL", new java.math.BigDecimal("0.186000000000")), // ~0.186–0.189 USD per BRL :contentReference[oaicite:3]{index=3}
            Map.entry("INR", new java.math.BigDecimal("0.011360000000")), // ~₹88 per USD ⇒ 1/88 ≈ 0.01136 USD per INR :contentReference[oaicite:4]{index=4}
            Map.entry("CNY", new java.math.BigDecimal("0.140500000000")), // ~0.1405 USD per CNY :contentReference[oaicite:5]{index=5}
            Map.entry("KRW", new java.math.BigDecimal("0.000720000000")), // ~0.000719–0.000725 USD per KRW :contentReference[oaicite:6]{index=6}
            Map.entry("JPY", new java.math.BigDecimal("0.006750000000")), // ~0.00674–0.00685 USD per JPY :contentReference[oaicite:7]{index=7}
            Map.entry("AUD", new java.math.BigDecimal("0.659000000000")), // ~0.655–0.66 USD per AUD :contentReference[oaicite:8]{index=8}
            Map.entry("ZAR", new java.math.BigDecimal("0.057500000000")), // ~0.055–0.058 USD per ZAR :contentReference[oaicite:9]{index=9}
            Map.entry("AED", new java.math.BigDecimal("0.272294000000")), // peg: 1 USD = 3.6725 AED ⇒ 1 AED = 0.272294 USD :contentReference[oaicite:10]{index=10}
            Map.entry("SGD", new java.math.BigDecimal("0.780000000000"))  // ~0.775–0.786 USD per SGD :contentReference[oaicite:11]{index=11}
    );

    private Data() {
    }

    /**
     * Build a stable key for the fx_rates map (e.g., "USD@2025-09-22").
     */
    public static String fxKey(String currency, java.time.LocalDate asOf) {
        return (currency == null ? "" : currency.trim().toUpperCase(java.util.Locale.ROOT)) + "@" + (asOf == null ? java.time.LocalDate.now() : asOf);
    }

    // -------------------------
    // Convenience helpers
    // -------------------------

    /**
     * Normalise a country code to uppercase ISO2. Null-safe.
     */
    public static String normCountry(String cc) {
        return cc == null ? "" : cc.trim().toUpperCase(Locale.ROOT);
    }

    /**
     * Return true if the country uses IBAN per this subset.
     */
    public static boolean isIbanCountry(String countryIso2) {
        return IBAN_LENGTHS.containsKey(normCountry(countryIso2));
    }

    /**
     * Return Optional IBAN length for the given country code.
     */
    public static OptionalInt ibanLength(String countryIso2) {
        Integer len = IBAN_LENGTHS.get(normCountry(countryIso2));
        return (len == null) ? OptionalInt.empty() : OptionalInt.of(len);
    }

    /**
     * Get the country’s canonical currency; defaults to EUR if unknown.
     */
    public static String currencyForCountry(String countryIso2) {
        return COUNTRY_TO_CURRENCY.getOrDefault(normCountry(countryIso2), "EUR");
    }

    /**
     * Get a deterministic, realistic debtor name for the country (with sensible fallback).
     */
    public static String debtorNameForCountry(String countryIso2) {
        return COUNTRY_TO_DEBTOR_NAME.getOrDefault(normCountry(countryIso2), DEFAULT_DEBTOR_NAME);
    }

    /**
     * Filter/validate a provided country list against the default supported set.
     */
    public static List<String> sanitiseCountryPool(Collection<String> countries) {
        if (countries == null || countries.isEmpty()) {
            return DEFAULT_COUNTRIES;
        }
        Set<String> allowed = new HashSet<>(DEFAULT_COUNTRIES);
        List<String> out = countries.stream().filter(Objects::nonNull).map(Data::normCountry).filter(allowed::contains).collect(Collectors.toList());
        return out.isEmpty() ? DEFAULT_COUNTRIES : Collections.unmodifiableList(out);
    }
}
