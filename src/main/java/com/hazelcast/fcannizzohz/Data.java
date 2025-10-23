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

    // extend BANKS with remaining countries: US, CA, BR, IN, CN, KR, JP, AU, ZA, AE, SG
    private static final List<Bank> BANKS = List.of(
            // --- existing entries you already had (GB, DE, FR, ES, IT, NL, BE, IE, PT) ---
            new Bank("Barclays Bank PLC",            "BARCGB22", "GB", List.of("GB20","GB21"), 0.28),
            new Bank("HSBC UK Bank plc",             "MIDLGB22", "GB", List.of("GB40","GB41"), 0.27),
            new Bank("Lloyds Bank PLC",              "LOYDGB2L", "GB", List.of("GB30","GB31"), 0.23),
            new Bank("NatWest Bank",                 "NWBKGB2L", "GB", List.of("GB60","GB61"), 0.22),

            new Bank("Deutsche Bank AG",             "DEUTDEFF", "DE", List.of("DE37040044","DE50070010"), 0.35),
            new Bank("Commerzbank AG",               "COBADEFF", "DE", List.of("DE50040000","DE20040000"), 0.25),
            new Bank("UniCredit Bank AG (HypoVereinsbank)","HYVEDEMM","DE", List.of("DE70020270"), 0.20),
            new Bank("DZ BANK AG",                   "GENODEFF", "DE", List.of("DE50060400"), 0.20),

            new Bank("BNP Paribas",                  "BNPAFRPP", "FR", List.of("FR30004","FR30007"), 0.35),
            new Bank("Société Générale",             "SOGEFRPP", "FR", List.of("FR30003","FR30078"), 0.30),
            new Bank("Crédit Agricole",              "AGRIFRPP", "FR", List.of("FR30006"), 0.35),

            new Bank("BBVA",                         "BBVAESMM", "ES", List.of("ES0182"), 0.45),
            new Bank("CaixaBank",                    "CAIXESBB", "ES", List.of("ES2100"), 0.35),
            new Bank("Banco Sabadell",               "BSABESBB", "ES", List.of("ES0081","ES0082"), 0.20),

            new Bank("UniCredit S.p.A.",             "UNCRITMM", "IT", List.of("IT02008","IT03002"), 0.45),
            new Bank("Intesa Sanpaolo S.p.A.",       "BCITITMM", "IT", List.of("IT03069"), 0.40),
            new Bank("Banco BPM",                    "BPMOIT22", "IT", List.of("IT05034"), 0.15),

            new Bank("ABN AMRO",                     "ABNANL2A", "NL", List.of("NL04ABNA","NL13ABNA"), 0.40),
            new Bank("ING Bank N.V.",                "INGBNL2A", "NL", List.of("NL55INGB","NL66INGB"), 0.35),
            new Bank("Rabobank",                     "RABONL2U", "NL", List.of("NL19RABO"), 0.25),

            new Bank("KBC Bank",                     "KREDBEBB", "BE", List.of("BE730"), 0.45),
            new Bank("Belfius Bank",                 "GKCCBEBB", "BE", List.of("BE680"), 0.30),
            new Bank("BNP Paribas Fortis",           "GEBABEBB", "BE", List.of("BE260"), 0.25),

            new Bank("AIB",                          "AIBKIE2D", "IE", List.of("IE12AIBK"), 0.55),
            new Bank("Bank of Ireland",              "BOFIIE2D", "IE", List.of("IE29BOFI"), 0.45),

            new Bank("Millennium BCP",               "BCOMPTPL", "PT", List.of("PT500035"), 0.45),
            new Bank("Caixa Geral de Depósitos",     "CGDIPTPL", "PT", List.of("PT500035","PT500003"), 0.35),
            new Bank("Novo Banco",                   "BESCPTPL", "PT", List.of("PT500007"), 0.20),

            // --- US (no IBAN; prefixes denote ABA/rtn samples) ---
            new Bank("JPMorgan Chase Bank, N.A.",    "CHASUS33", "US", List.of("US-ABA-021000021"), 0.30),
            new Bank("Bank of America, N.A.",        "BOFAUS3N", "US", List.of("US-ABA-026009593"), 0.30),
            new Bank("Citibank, N.A.",               "CITIUS33", "US", List.of("US-ABA-021000089"), 0.20),
            new Bank("Wells Fargo Bank, N.A.",       "WFBIUS6S", "US", List.of("US-ABA-121000248"), 0.20),

            // --- CA (no IBAN; transit numbers) ---
            new Bank("Royal Bank of Canada (RBC)",   "ROYCCAT2", "CA", List.of("CA-TR-00010"), 0.30),
            new Bank("TD Canada Trust",              "TDOMCATT", "CA", List.of("CA-TR-00004"), 0.30),
            new Bank("Scotiabank",                   "NOSCCATT", "CA", List.of("CA-TR-00002"), 0.20),
            new Bank("Bank of Montreal (BMO)",       "BOFMCAM2", "CA", List.of("CA-TR-00001"), 0.20),

            // --- BR (IBAN exists; use BR… illustrative prefixes) ---
            new Bank("Banco do Brasil S.A.",         "BRASBRRJ", "BR", List.of("BR-00000000"), 0.35),
            new Bank("Itaú Unibanco S.A.",           "ITAUBRSP", "BR", List.of("BR-00360305"), 0.40),
            new Bank("Banco Bradesco S.A.",          "BBDEBRSP", "BR", List.of("BR-02374642"), 0.25),

            // --- IN (no IBAN; IFSC-style prefixes) ---
            new Bank("State Bank of India",          "SBININBB", "IN", List.of("IN-IFSC-SBIN000"), 0.40),
            new Bank("HDFC Bank",                    "HDFCINBB", "IN", List.of("IN-IFSC-HDFC000"), 0.35),
            new Bank("ICICI Bank",                   "ICICINBB", "IN", List.of("IN-IFSC-ICIC000"), 0.25),

            // --- CN (no IBAN) ---
            new Bank("Industrial & Commercial Bank of China (ICBC)","ICBKCNBJ","CN", List.of("CN-ICBC-01"), 0.40),
            new Bank("Bank of China",                "BKCHCNBJ", "CN", List.of("CN-BOC-01"), 0.35),
            new Bank("China Construction Bank",      "PCBCCNBJ", "CN", List.of("CN-CCB-01"), 0.25),

            // --- KR (no IBAN) ---
            new Bank("Shinhan Bank",                 "SHBKKRSE", "KR", List.of("KR-SHB-01"), 0.35),
            new Bank("KEB Hana Bank",                "HNBNKRSE", "KR", List.of("KR-HANA-01"), 0.35),
            new Bank("Industrial Bank of Korea",     "IBKOKRSE", "KR", List.of("KR-IBK-01"), 0.30),

            // --- JP (no IBAN) ---
            new Bank("MUFG Bank, Ltd.",              "BOTKJPJT", "JP", List.of("JP-MUFG-01"), 0.40),
            new Bank("Sumitomo Mitsui Banking Corp.", "SMBCJPJT","JP", List.of("JP-SMBC-01"), 0.35),
            new Bank("Mizuho Bank, Ltd.",            "MHCBJPJT", "JP", List.of("JP-MIZUHO-01"), 0.25),

            // --- AU (no IBAN; BSB-style prefixes) ---
            new Bank("Commonwealth Bank of Australia","CTBAAU2S","AU", List.of("AU-BSB-062"), 0.35),
            new Bank("Westpac Banking Corporation",  "WPACAU2S", "AU", List.of("AU-BSB-032"), 0.30),
            new Bank("Australia & New Zealand Bank (ANZ)","ANZBAU3M","AU", List.of("AU-BSB-013"), 0.20),
            new Bank("National Australia Bank (NAB)","NATAAU33","AU", List.of("AU-BSB-083"), 0.15),

            // --- ZA (no IBAN) ---
            new Bank("Standard Bank of South Africa","SBZAZAJJ","ZA", List.of("ZA-SB-01"), 0.45),
            new Bank("First National Bank (FNB)",    "FIRNZAJJ", "ZA", List.of("ZA-FNB-01"), 0.30),
            new Bank("Absa Bank",                    "ABSAZAJJ", "ZA", List.of("ZA-ABSA-01"), 0.25),

            // --- AE (IBAN) ---
            new Bank("First Abu Dhabi Bank (FAB)",   "NBADAEAA", "AE", List.of("AE07"), 0.45),
            new Bank("Emirates NBD",                 "EBILAEAD", "AE", List.of("AE07","AE08"), 0.35),
            new Bank("Dubai Islamic Bank",           "DUIBAEAD", "AE", List.of("AE07"), 0.20),

            // --- SG (no IBAN) ---
            new Bank("DBS Bank Ltd",                 "DBSSSGSG", "SG", List.of("SG-DBS-01"), 0.45),
            new Bank("Oversea-Chinese Banking Corp.", "OCBCSGSG","SG", List.of("SG-OCBC-01"), 0.30),
            new Bank("United Overseas Bank (UOB)",   "UOVBSGSG", "SG", List.of("SG-UOB-01"), 0.25)
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

    // Indexes for fast lookup
    public static final Map<String, Bank> BANK_BY_BIC8 =
            Collections.unmodifiableMap(BANKS.stream().collect(Collectors.toMap(b -> b.bic8, b -> b, (a,b) -> a)));

    public static final Map<String, List<Bank>> BANKS_BY_COUNTRY =
            Collections.unmodifiableMap(BANKS.stream().collect(Collectors.groupingBy(b -> b.country)));

    public static final Map<String, Bank> BANK_BY_IBAN_PREFIX = buildIbanPrefixIndex(BANKS);

    private static Map<String, Bank> buildIbanPrefixIndex(List<Bank> banks) {
        Map<String, Bank> m = new java.util.HashMap<>();
        for (Bank b : banks) {
            for (String p : b.ibanPrefixes) {
                m.put(p.toUpperCase(Locale.ROOT), b);
            }
        }
        return Collections.unmodifiableMap(m);
    }

    /** Generate a syntactically valid IBAN for a bank. Uses first configured prefix if present. */
    public static String generateIbanForBank(Bank bank, java.util.concurrent.ThreadLocalRandom rnd) {
        if (bank == null) return null;
        String cc = bank.country;
        int len = IBAN_LENGTHS.getOrDefault(cc, 22); // default fall-back
        String country = cc.toUpperCase(Locale.ROOT);

        // Base prefix to anchor the bank "fingerprint"
        String base = bank.ibanPrefixes.isEmpty() ? (country) : bank.ibanPrefixes.get(0);
        base = base.toUpperCase(Locale.ROOT).replaceAll("\\s+","");

        // If base already starts with country letters, it may include check digits. Strip them; we will recompute.
        if (base.startsWith(country)) {
            base = base.substring(2); // drop country letters; keep rest as BBAN-preamble
            if (base.length() >= 2 && Character.isDigit(base.charAt(0)) && Character.isDigit(base.charAt(1))) {
                base = base.substring(2); // drop any baked check digits in the sample prefix
            }
        }

        // Build a BBAN core of digits/upper letters until reaching target length minus 4 (for CC + check)
        StringBuilder bban = new StringBuilder(base);
        while (country.length() + 2 + bban.length() < len) {
            // Alnum mix; many IBANs are digits-only in BBAN, but demo can tolerate letters
            int k = rnd.nextInt(0, 36);
            char c = (k < 10) ? (char)('0' + k) : (char)('A' + (k-10));
            bban.append(c);
        }

        // Compute check digits
        String ibanNoCheck = country + "00" + bban;
        String check = ibanMod97CheckDigits(ibanNoCheck);
        return country + check + bban;
    }

    /** ISO 13616 mod-97 check digit calculation (two-letter country + "00" + BBAN given). */
    private static String ibanMod97CheckDigits(String ibanWith00) {
        // Move first 4 chars to end and convert letters A=10..Z=35
        String re = (ibanWith00.substring(4) + ibanWith00.substring(0, 4)).toUpperCase(Locale.ROOT);
        StringBuilder digits = new StringBuilder(re.length() * 2);
        for (int i = 0; i < re.length(); i++) {
            char c = re.charAt(i);
            if (c >= 'A' && c <= 'Z') digits.append(10 + (c - 'A'));
            else digits.append(c);
        }
        // mod 97 in streaming fashion
        int mod = 0;
        for (int i = 0; i < digits.length(); i++) {
            char c = digits.charAt(i);
            mod = (mod * 10 + (c - '0')) % 97;
        }
        int check = 98 - mod;
        return (check < 10) ? ("0" + check) : Integer.toString(check);
    }

    /** Lookup a bank by BIC (8 or 11) */
    public static Bank bankForBic(String bic) {
        if (bic == null || bic.isEmpty()) return null;
        String b8 = bic.trim().toUpperCase(Locale.ROOT);
        if (b8.length() >= 8) b8 = b8.substring(0, 8);
        return BANK_BY_BIC8.get(b8);
    }

    /** Best-effort mapping of an IBAN to a Bank via registered prefixes. */
    public static Bank bankForIban(String iban) {
        if (iban == null) return null;
        String s = iban.replaceAll("\\s+", "").toUpperCase(Locale.ROOT);
        // try longest-first prefix match
        Bank best = null; int bestLen = -1;
        for (Map.Entry<String,Bank> e : BANK_BY_IBAN_PREFIX.entrySet()) {
            String p = e.getKey();
            if (s.startsWith(p) && p.length() > bestLen) {
                best = e.getValue(); bestLen = p.length();
            }
        }
        return best;
    }

    /** Weighted pick of a bank for a given ISO2 country. */
    public static Bank pickBankForCountry(String countryIso2, java.util.concurrent.ThreadLocalRandom rnd) {
        List<Bank> list = BANKS_BY_COUNTRY.get(normCountry(countryIso2));
        if (list == null || list.isEmpty()) return null;
        double sum = 0.0; for (Bank b : list) sum += Math.max(0.0, b.weight);
        double r = rnd.nextDouble() * sum, acc = 0.0;
        for (Bank b : list) {
            acc += Math.max(0.0, b.weight);
            if (r <= acc) return b;
        }
        return list.get(list.size()-1);
    }

}
