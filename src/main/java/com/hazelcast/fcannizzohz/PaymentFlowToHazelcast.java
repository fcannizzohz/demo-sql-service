package com.hazelcast.fcannizzohz;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlService;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public final class PaymentFlowToHazelcast {

    public static final String MAPPINGS_SQL = "mappings.sql";
    // --- Map names (must match your JSON mappings) ---
    private static final String M_PAIN_GHDR = "pain001_group_header";
    private static final String M_PAIN_PMTINF = "pain001_payment_information";
    private static final String M_PAIN_TX = "pain001_credit_transfer_transaction";
    private static final String M_PACS_TX = "pacs008_credit_transfer_transaction";
    private static final String M_CAMT_NTF = "camt054_notification";
    private static final String M_CAMT_ENTRY = "camt054_entry";
    private static final DateTimeFormatter ISO_OFFSET = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private static final long ONE_SECOND_NANOS = 1_000_000_000L;

    private PaymentFlowToHazelcast() {
    }

    public static void run(String clusterName, String memberAddress, int minTxCount, int genPollSec, Clock clock, double rate, boolean switchToRealWhenCaughtUp) {
        // 1) Start member (or connect a client instead if you have a remote cluster)
        ClientConfig cfg = new ClientConfig();
        cfg.setClusterName(clusterName);                  // default for hazelcast/hazelcast:latest
        ClientNetworkConfig net = cfg.getNetworkConfig();
        net.addAddress(memberAddress);           // point at your Docker container

        HazelcastInstance hz = HazelcastClient.newHazelcastClient(cfg);

        // 2) Create JSON mappings (idempotent)
        runSqlScriptFromClasspath(hz.getSql(), MAPPINGS_SQL);

        startRealtimeGenerationRandomCountry(hz, minTxCount, genPollSec, clock, rate, switchToRealWhenCaughtUp);
    }

    private static void startRealtimeGenerationRandomCountry(HazelcastInstance hz,
                                                             int minTxCount,
                                                             int genPollSec,
                                                             Clock initialClock,
                                                             double initialRate,
                                                             boolean switchToRealWhenCaughtUp) {
        if (!(initialRate > 0.0) || Double.isInfinite(initialRate)) {
            throw new IllegalArgumentException("rate must be > 0 and finite");
        }
        java.time.LocalDate lastSeededDate = null;

        final ThreadLocalRandom rnd = ThreadLocalRandom.current();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { try { System.out.println("\nStopping generator…"); hz.shutdown(); } catch (Throwable ignore) {} }));

        // ---- scheduling state (may change if we switch clocks) ----
        Clock clock = initialClock;
        double rate = initialRate;
        boolean usingSystemClock = isSystemClock(clock);

        final Instant initialSim = clock.instant();
        final Instant initialReal = Instant.now();
        final boolean startedAhead  = initialSim.isAfter(initialReal);    // sim > real
        final boolean startedBehind = !startedAhead;                      // sim <= real

        long periodNanos;
        long nextDeadline;
        double simRate = initialRate;

        // IMMEDIATE SWITCH if start is in the future
        if (switchToRealWhenCaughtUp && startedAhead && !usingSystemClock) {
            clock = Clock.systemUTC();
            usingSystemClock = true;
            rate = 1.0;
            long realPeriodNanos = genPollSec * ONE_SECOND_NANOS;
            periodNanos = realPeriodNanos;
            nextDeadline = System.nanoTime() + computeInitialDelayNanos(clock, rate);
            System.out.println("[info] Simulated start is in the future; switched to system clock immediately.");
        } else {
            periodNanos = Math.max(1L, Math.round(ONE_SECOND_NANOS / simRate));              // 1 sim sec per tick
            nextDeadline = System.nanoTime() + computeInitialDelayNanos(clock, rate);     // align to whole-second boundary
        }

        long tick = 0L;
        while (true) {
            try {
                // derive "today" from the active clock
                java.time.LocalDate today = java.time.Instant.now(clock)
                                                             .atZone(java.time.ZoneOffset.UTC) // or ZoneId.systemDefault()
                                                             .toLocalDate();

                // seed only if the day rolled over
                if (!today.equals(lastSeededDate)) {
                    FxSeederPips.seedFxRatesForDate(hz, today, 42L);
                    lastSeededDate = today;
                    System.out.println("[info] Seeded fx_rates for " + today);
                }

                // --- generate one dataset using 'clock' (may be system after switch) ---
                String debtorCountry = Data.DEFAULT_COUNTRIES.get(rnd.nextInt(Data.DEFAULT_COUNTRIES.size()));
                int txCount = minTxCount + rnd.nextInt(1, minTxCount);
                long minCents   = rnd.nextLong(50_000, 1_000_001 + 1);
                long extraCents = rnd.nextLong(500_000, 50_000_001 + 1);
                BigDecimal min  = BigDecimal.valueOf(minCents, 2);
                BigDecimal max  = BigDecimal.valueOf(minCents + extraCents, 2);

                String painXml = Pain001ComprehensiveGenerator.generateFixedDebtor(
                        txCount, min, max, Data.DEFAULT_COUNTRIES, debtorCountry, null, clock);

                try (InputStream painIn = new ByteArrayInputStream(painXml.getBytes(StandardCharsets.UTF_8))) {
                    var pacsList = Pain001ToPacs008Converter.convertPain001_03_to_Pacs008_03_PerTx(painIn, "DEUTDEFFXXX", "BNPAFRPPXXX");
                    writePain(hz, painXml);
                    for (String pacsXml : pacsList) {
                        try (InputStream pacsIn = new ByteArrayInputStream(pacsXml.getBytes(StandardCharsets.UTF_8))) {
                            String camtXml = Pacs008ToCamt054Converter.pacs008_to_camt054_debtor(pacsIn);
                            writePacs(hz, pacsXml);
                            writeCamt(hz, camtXml);
                        }
                    }
                }

                if (++tick % 10 == 0) {
                    String q = """
                    SELECT 'pain_tx' AS metric, CAST(COUNT(*) AS BIGINT) AS cnt FROM pain001_credit_transfer_transaction
                    UNION ALL
                    SELECT 'pacs_tx' AS metric, CAST(COUNT(*) AS BIGINT) AS cnt FROM pacs008_credit_transfer_transaction
                    UNION ALL
                    SELECT 'camt_entries' AS metric, CAST(COUNT(*) AS BIGINT) AS cnt FROM camt054_entry
                """;
                    Map<String, Long> m = new LinkedHashMap<>();
                    hz.getSql().execute(q).forEach(row -> m.put((String) row.getObject(0), ((Number) row.getObject(1)).longValue()));
                    System.out.printf("[%s] pain_tx=%d, pacs_tx=%d, camt_entries=%d%n",
                            Instant.now(clock).toString(),
                            m.getOrDefault("pain_tx", 0L),
                            m.getOrDefault("pacs_tx", 0L),
                            m.getOrDefault("camt_entries", 0L));
                }
            } catch (Throwable e) {
                e.printStackTrace(System.err);
            }

            // Catch-up switch only for "started in the past" runs
            if (switchToRealWhenCaughtUp && startedBehind && !usingSystemClock) {
                Instant sim = clock.instant();
                Instant real = Instant.now();
                if (!sim.isBefore(real.minusSeconds(1))) { // within 1s
                    clock = Clock.systemUTC();
                    usingSystemClock = true;
                    rate = 1.0;
                    periodNanos = genPollSec * ONE_SECOND_NANOS;
                    nextDeadline = System.nanoTime() + computeInitialDelayNanos(clock, rate);
                    System.out.println("[info] Switched to system clock (caught up).");
                }
            }

            // fixed-rate scheduling (1 simulated second per tick)
            nextDeadline += periodNanos;
            long sleep = nextDeadline - System.nanoTime();
            if (sleep > 0) {
                java.util.concurrent.locks.LockSupport.parkNanos(sleep);
            } else {
                long missed = (-sleep) / periodNanos + 1;
                nextDeadline += missed * periodNanos;
            }
        }
    }

    // ------------------------------------------------------------------------
    // WRITE HELPERS (PAIN / PACS / CAMT)
    // ------------------------------------------------------------------------
    private static void writePain(HazelcastInstance hz, String painXml)
            throws Exception {
        Document doc = parseXml(painXml);
        XPath xp = XPathFactory.newInstance().newXPath();

        String base = "/*[" + "local-name()='Document' or local-name()='document']/" + ln("CstmrCdtTrfInitn");

        // Group Header
        String msgId = eval(xp, doc, base + "/" + ln("GrpHdr") + "/" + ln("MsgId"));
        String cre = eval(xp, doc, base + "/" + ln("GrpHdr") + "/" + ln("CreDtTm"));
        String nbTxs = eval(xp, doc, base + "/" + ln("GrpHdr") + "/" + ln("NbOfTxs"));
        String ctrl = eval(xp, doc, base + "/" + ln("GrpHdr") + "/" + ln("CtrlSum"));
        String initNm = eval(xp, doc, base + "/" + ln("GrpHdr") + "/" + ln("InitgPty") + "/" + ln("Nm"));

        if (isBlank(msgId)) {
            throw new IllegalArgumentException("PAIN MsgId not found (namespace/case?).");
        }

        Map<String, Object> gh = new LinkedHashMap<>();
        gh.put("MessageId", msgId);
        putIfPresent(gh, "CreationDateTime", toIso(cre));
        putIfPresent(gh, "NumberOfTransactions", toInteger(nbTxs));
        putIfPresent(gh, "ControlSum", toDecimal(ctrl));
        putIfPresent(gh, "InitiatingPartyName", initNm);
        putJson(hz.getMap(M_PAIN_GHDR), msgId, gh);

        // PaymentInformation nodes
        NodeList pmtInfNodes = (NodeList) xp.evaluate(base + "/" + ln("PmtInf"), doc, XPathConstants.NODESET);

        for (int i = 0; i < pmtInfNodes.getLength(); i++) {
            Node n = pmtInfNodes.item(i);

            String pmtInfId = eval(xp, n, ln("PmtInfId"));
            if (isBlank(pmtInfId)) {
                throw new IllegalArgumentException("PAIN PmtInfId missing.");
            }

            String reqdDate = eval(xp, n, ln("ReqdExctnDt")); // sample has datetime
            String dbtrNm = eval(xp, n, ln("Dbtr") + "/" + ln("Nm"));
            String dbtrIban = eval(xp, n, ln("DbtrAcct") + "/" + ln("Id") + "/" + ln("IBAN"));
            String dbtrOthr = eval(xp, n, ln("DbtrAcct") + "/" + ln("Id") + "/" + ln("Othr") + "/" + ln("Id"));
            String dbtrAgtBic = eval(xp, n, ln("DbtrAgt") + "/" + ln("FinInstnId") + "/" + "*[local-name()='BIC' or local-name()='BICFI']"); // PAIN uses BIC

            String btchBookg = eval(xp, n, ln("BtchBookg"));
            String svcLvlCd = eval(xp, n, ln("PmtTpInf") + "/" + ln("SvcLvl") + "/" + ln("Cd"));
            String lclInstrm = eval(xp, n, ln("PmtTpInf") + "/" + ln("LclInstrm") + "/" + ln("Cd"));
            String ctgyPurp = eval(xp, n, ln("PmtTpInf") + "/" + ln("CtgyPurp") + "/" + ln("Cd"));
            String chrgBr = eval(xp, n, ln("ChrgBr"));

            Map<String, Object> pi = new LinkedHashMap<>();
            pi.put("PaymentInformationId", pmtInfId);
            pi.put("MessageId", msgId);
            putIfPresent(pi, "RequestedExecutionDate", toDateOnly(reqdDate));
            putIfPresent(pi, "DebtorName", dbtrNm);
            putIfPresent(pi, "DebtorAccountIdentificationIBAN", dbtrIban);
            putIfPresent(pi, "DebtorAccountIdentificationOtherId", dbtrOthr);
            putIfPresent(pi, "DebtorAgentBICFI", dbtrAgtBic);
            putIfPresent(pi, "BatchBooking", toBoolean(btchBookg));
            putIfPresent(pi, "ServiceLevelCode", svcLvlCd);
            putIfPresent(pi, "LocalInstrumentCode", lclInstrm);
            putIfPresent(pi, "CategoryPurposeCode", ctgyPurp);
            putIfPresent(pi, "ChargeBearer", chrgBr);
            putJson(hz.getMap(M_PAIN_PMTINF), pmtInfId, pi);

            // Transactions
            NodeList txs = (NodeList) xp.evaluate(ln("CdtTrfTxInf"), n, XPathConstants.NODESET);
            for (int t = 0; t < txs.getLength(); t++) {
                Node tx = txs.item(t);

                String e2e = eval(xp, tx, ln("PmtId") + "/" + ln("EndToEndId"));
                String instdAmt = eval(xp, tx, ln("Amt") + "/" + ln("InstdAmt") + "/text()");
                String instdCcy = eval(xp, tx, ln("Amt") + "/" + ln("InstdAmt") + "/@Ccy");
                String cdtrNm = eval(xp, tx, ln("Cdtr") + "/" + ln("Nm"));
                String cdtrIban = eval(xp, tx, ln("CdtrAcct") + "/" + ln("Id") + "/" + ln("IBAN"));
                String cdtrOthr = eval(xp, tx, ln("CdtrAcct") + "/" + ln("Id") + "/" + ln("Othr") + "/" + ln("Id"));
                String cdtrAgtBic = eval(xp, tx, ln("CdtrAgt") + "/" + ln("FinInstnId") + "/" + "*[local-name()='BIC' or local-name()='BICFI']"); // PAIN: BIC

                String ustrd = eval(xp, tx, ln("RmtInf") + "/" + ln("Ustrd"));
                String purpCd = eval(xp, tx, ln("Purp") + "/" + ln("Cd"));
                String chrgBrTx = eval(xp, tx, ln("ChrgBr"));

                Map<String, Object> row = new LinkedHashMap<>();
                row.put("PaymentInformationId", pmtInfId);
                putIfPresent(row, "EndToEndIdentification", e2e);
                putIfPresent(row, "InstructedAmount", toDecimal(instdAmt));
                putIfPresent(row, "InstructedAmountCurrency", instdCcy);
                putIfPresent(row, "CreditorName", cdtrNm);
                putIfPresent(row, "CreditorAccountIdentificationIBAN", cdtrIban);
                putIfPresent(row, "CreditorAccountIdentificationOtherId", cdtrOthr);
                putIfPresent(row, "CreditorAgentBICFI", cdtrAgtBic);
                putIfPresent(row, "RemittanceInformationUnstructured", ustrd);
                putIfPresent(row, "PurposeCode", purpCd);
                putIfPresent(row, "ChargeBearer", chrgBrTx);

                String txKey = isBlank(e2e) ? UUID.randomUUID().toString() : e2e;
                putJson(hz.getMap(M_PAIN_TX), txKey, row);
            }
        }
    }

    private static void writePacs(HazelcastInstance hz, String pacsXml)
            throws Exception {
        Document doc = parseXml(pacsXml);
        XPath xp = XPathFactory.newInstance().newXPath();

        String base = "/*[" + "local-name()='Document' or local-name()='document']/" + ln("FIToFICstmrCdtTrf");

        String msgId = eval(xp, doc, base + "/" + ln("GrpHdr") + "/" + ln("MsgId"));
        String cre = eval(xp, doc, base + "/" + ln("GrpHdr") + "/" + ln("CreDtTm"));

        NodeList txs = (NodeList) xp.evaluate(base + "/" + ln("CdtTrfTxInf"), doc, XPathConstants.NODESET);
        for (int i = 0; i < txs.getLength(); i++) {
            Node tx = txs.item(i);

            String e2e = eval(xp, tx, ln("PmtId") + "/" + ln("EndToEndId"));
            String txId = eval(xp, tx, ln("PmtId") + "/" + ln("TxId"));
            String amt = eval(xp, tx, ln("IntrBkSttlmAmt") + "/text()");
            String ccy = eval(xp, tx, ln("IntrBkSttlmAmt") + "/@Ccy");
            String dt = eval(xp, tx, ln("IntrBkSttlmDt"));

            String dbtrNm = eval(xp, tx, ln("Dbtr") + "/" + ln("Nm"));
            String dbtrIban = eval(xp, tx, ln("DbtrAcct") + "/" + ln("Id") + "/" + ln("IBAN"));
            String dbtrOthr = eval(xp, tx, ln("DbtrAcct") + "/" + ln("Id") + "/" + ln("Othr") + "/" + ln("Id"));
            String dbtrBic = eval(xp, tx, ln("DbtrAgt") + "/" + ln("FinInstnId") + "/" + "*[local-name()='BIC' or local-name()='BICFI']");

            String cdtrNm = eval(xp, tx, ln("Cdtr") + "/" + ln("Nm"));
            String cdtrIban = eval(xp, tx, ln("CdtrAcct") + "/" + ln("Id") + "/" + ln("IBAN"));
            String cdtrOthr = eval(xp, tx, ln("CdtrAcct") + "/" + ln("Id") + "/" + ln("Othr") + "/" + ln("Id"));
            String cdtrBic = eval(xp, tx, ln("CdtrAgt") + "/" + ln("FinInstnId") + "/" + "*[local-name()='BIC' or local-name()='BICFI']");

            String svcLvlCd = eval(xp, tx, ln("PmtTpInf") + "/" + ln("SvcLvl") + "/" + ln("Cd"));
            String chrgBr = eval(xp, tx, ln("ChrgBr"));

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("MessageId", msgId);
            putIfPresent(row, "CreationDateTime", toIso(cre));
            putIfPresent(row, "EndToEndIdentification", e2e);
            putIfPresent(row, "TransactionIdentification", txId);
            putIfPresent(row, "InterbankSettlementAmount", toDecimal(amt));
            putIfPresent(row, "InterbankSettlementCurrency", ccy);
            putIfPresent(row, "InterbankSettlementDate", dt);
            putIfPresent(row, "DebtorName", dbtrNm);
            putIfPresent(row, "DebtorAccountIdentificationIBAN", dbtrIban);
            putIfPresent(row, "DebtorAccountIdentificationOtherId", dbtrOthr);
            putIfPresent(row, "DebtorAgentBICFI", dbtrBic);
            putIfPresent(row, "CreditorName", cdtrNm);
            putIfPresent(row, "CreditorAccountIdentificationIBAN", cdtrIban);
            putIfPresent(row, "CreditorAccountIdentificationOtherId", cdtrOthr);
            putIfPresent(row, "CreditorAgentBICFI", cdtrBic);
            putIfPresent(row, "ServiceLevelCode", svcLvlCd);
            putIfPresent(row, "ChargeBearer", chrgBr);

            String key = !isBlank(txId) ? txId : (!isBlank(e2e) ? e2e : UUID.randomUUID().toString());
            putJson(hz.getMap(M_PACS_TX), key, row);
        }
    }

    private static void writeCamt(HazelcastInstance hz, String camtXml)
            throws Exception {
        Document doc = parseXml(camtXml);
        XPath xp = XPathFactory.newInstance().newXPath();

        String base = "/*[" + "local-name()='Document' or local-name()='document']/" + ln("BkToCstmrDbtCdtNtfctn");

        String grpMsgId = eval(xp, doc, base + "/" + ln("GrpHdr") + "/" + ln("MsgId"));
        String grpCre = eval(xp, doc, base + "/" + ln("GrpHdr") + "/" + ln("CreDtTm"));

        Node ntf = (Node) xp.evaluate(base + "/" + ln("Ntfctn"), doc, XPathConstants.NODE);
        if (ntf != null) {
            String ntfId = eval(xp, ntf, ln("Id"));
            String ntfCre = eval(xp, ntf, ln("CreDtTm"));
            String iban = eval(xp, ntf, ln("Acct") + "/" + ln("Id") + "/" + ln("IBAN"));
            String othr = eval(xp, ntf, ln("Acct") + "/" + ln("Id") + "/" + ln("Othr") + "/" + ln("Id"));

            Map<String, Object> row = new LinkedHashMap<>();
            putIfPresent(row, "GroupHeaderMessageId", grpMsgId);
            putIfPresent(row, "GroupHeaderCreationDateTime", toIso(grpCre));
            row.put("NotificationId", ntfId);
            putIfPresent(row, "NotificationCreationDateTime", toIso(ntfCre));
            putIfPresent(row, "AccountIdentificationIBAN", iban);
            putIfPresent(row, "AccountIdentificationOtherId", othr);

            if (isBlank(ntfId)) {
                throw new IllegalArgumentException("CAMT Notification Id not found.");
            }
            putJson(hz.getMap(M_CAMT_NTF), ntfId, row);

            NodeList entries = (NodeList) xp.evaluate(ln("Ntry"), ntf, XPathConstants.NODESET);
            for (int i = 0; i < entries.getLength(); i++) {
                Node en = entries.item(i);
                String ntryRef = eval(xp, en, ln("NtryRef"));
                String amt = eval(xp, en, ln("Amt") + "/text()");
                String ccy = eval(xp, en, ln("Amt") + "/@Ccy");
                String cdi = eval(xp, en, ln("CdtDbtInd"));
                String sts = eval(xp, en, ln("Sts"));

                String e2e = eval(xp, en, ln("NtryDtls") + "/" + ln("TxDtls") + "/" + ln("Refs") + "/" + ln("EndToEndId"));
                String dbtrNm = eval(xp, en, ln("NtryDtls") + "/" + ln("TxDtls") + "/" + ln("RltdPties") + "/" + ln("Dbtr") + "/" + ln("Nm"));
                String cdtrNm = eval(xp, en, ln("NtryDtls") + "/" + ln("TxDtls") + "/" + ln("RltdPties") + "/" + ln("Cdtr") + "/" + ln("Nm"));
                String dbtrBic = eval(xp, en, ln("NtryDtls") + "/" + ln("TxDtls") + "/" + ln("RltdAgts") + "/" + ln("DbtrAgt") + "/" + ln("FinInstnId") + "/" + "*[local-name()='BIC' or local-name()='BICFI']");
                String cdtrBic = eval(xp, en, ln("NtryDtls") + "/" + ln("TxDtls") + "/" + ln("RltdAgts") + "/" + ln("CdtrAgt") + "/" + ln("FinInstnId") + "/" + "*[local-name()='BIC' or local-name()='BICFI']");

                Map<String, Object> erow = new LinkedHashMap<>();
                erow.put("NotificationId", ntfId);
                putIfPresent(erow, "EntryReference", ntryRef);
                putIfPresent(erow, "Amount", toDecimal(amt));
                putIfPresent(erow, "Currency", ccy);
                putIfPresent(erow, "CreditDebitIndicator", cdi);
                putIfPresent(erow, "Status", sts);
                putIfPresent(erow, "EndToEndIdentification", e2e);
                putIfPresent(erow, "DebtorName", dbtrNm);
                putIfPresent(erow, "CreditorName", cdtrNm);
                putIfPresent(erow, "DebtorAgentBICFI", dbtrBic);
                putIfPresent(erow, "CreditorAgentBICFI", cdtrBic);

                String key = !isBlank(ntryRef) ? ntryRef : UUID.randomUUID().toString();
                putJson(hz.getMap(M_CAMT_ENTRY), key, erow);
            }
        }
    }

    // ------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------
    private static Document parseXml(String xml)
            throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true); // <== enable NS; we'll use local-name() in XPath
        return dbf.newDocumentBuilder().parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }

    private static String eval(XPath xp, Object ctx, String expr) {
        try {
            String s = (String) xp.evaluate(expr, ctx, XPathConstants.STRING);
            return (s != null && !s.isEmpty()) ? s : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static void putIfPresent(Map<String, Object> m, String k, Object v) {
        if (v != null) {
            m.put(k, v);
        }
    }

    private static String toIso(String v) {
        if (isBlank(v)) return null;
        try {
            return OffsetDateTime.parse(v).format(ISO_OFFSET);
        } catch (Exception ignored) {
            try { // try local date-time as UTC
                return java.time.LocalDateTime.parse(v).atOffset(java.time.ZoneOffset.UTC).format(ISO_OFFSET);
            } catch (Exception e) {
                return null; // or keep original v
            }
        }
    }

    private static BigDecimal toDecimal(String v) {
        if (isBlank(v)) {
            return null;
        }
        return new BigDecimal(v);
    }

    private static Integer toInteger(String v) {
        if (isBlank(v)) {
            return null;
        }
        return Integer.parseInt(v);
    }

    private static Boolean toBoolean(String v) {
        if (isBlank(v)) {
            return null;
        }
        return Boolean.parseBoolean(v);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isEmpty();
    }

    private static void putJson(IMap<String, HazelcastJsonValue> map, String key, Map<String, Object> payload) {
        String json = toJson(payload);
        System.out.println(json);
        map.set(key, new HazelcastJsonValue(json));
    }

    // Simple JSON builder to avoid extra deps; replace with Jackson if you prefer.
    private static String toJson(Map<String, Object> m) {
        StringBuilder sb = new StringBuilder(256);
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> e : m.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('"').append(escape(e.getKey())).append('"').append(':');
            Object v = e.getValue();
            if (v == null) {
                sb.append("null");
            } else if (v instanceof Number || v instanceof Boolean) {
                sb.append(v);
            } else {
                sb.append('"').append(escape(v.toString())).append('"');
            }
        }
        sb.append('}');
        return sb.toString();
    }

    private static String escape(String s) {
        StringBuilder out = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\': out.append("\\\\"); break;
                case '"':  out.append("\\\""); break;
                case '\b': out.append("\\b");  break;
                case '\f': out.append("\\f");  break;
                case '\n': out.append("\\n");  break;
                case '\r': out.append("\\r");  break;
                case '\t': out.append("\\t");  break;
                default:
                    if (c < 0x20) { // other control chars
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
            }
        }
        return out.toString();
    }

    private static void runSqlScriptFromClasspath(SqlService sql, String resourceName) {
        String script = readResourceUtf8(resourceName);
        for (String stmt : splitSqlStatements(script)) {
            if (!stmt.isBlank()) {
                System.out.println("RUNNING\n-----\n" + stmt + "\n-----\n");
                sql.execute(stmt);
            }
        }
    }

    /**
     * True when simulated time is at/after real time within a tolerance.
     */
    private static boolean caughtUpToNow(Clock clock, Duration tolerance) {
        Instant sim = clock.instant();
        Instant real = Instant.now(); // system clock
        return !sim.isBefore(real.minus(tolerance));
    }

    /**
     * Initial delay so the next tick lands on a whole-second boundary of the *simulated* (or real) clock.
     */
    private static long computeInitialDelayNanos(Clock clock, double rate) {
        long nanosIntoSecond = clock.instant().getNano();   // 0..999,999,999
        long deltaSimNanos = (nanosIntoSecond == 0) ? 0L : (ONE_SECOND_NANOS - nanosIntoSecond);
        // Convert simulated delta to real time
        return (long) Math.ceil(deltaSimNanos / rate);
    }

    private static boolean isSystemClock(Clock c) {
        // Detect JDK SystemClock implementations by class name (stable across JDKs)
        String cn = c.getClass().getName();
        return cn.endsWith("SystemClock");
    }

    private static String readResourceUtf8(String resourceName) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = PaymentFlowToHazelcast.class.getClassLoader();
        }
        try (InputStream in = cl.getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IllegalStateException("Resource not found on classpath: " + resourceName);
            }
            try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                StringBuilder sb = new StringBuilder(8192);
                String line;
                while ((line = r.readLine()) != null) {
                    sb.append(line).append('\n');
                }
                return sb.toString();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to read resource: " + resourceName, e);
        }
    }

    /**
     * Split on semicolons outside of quotes/comments.
     */
    private static List<String> splitSqlStatements(String sql) {
        List<String> out = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inSingle = false, inDouble = false, inLineComment = false, inBlockComment = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char n = (i + 1 < sql.length()) ? sql.charAt(i + 1) : '\0';

            if (inLineComment) {
                if (c == '\n') {
                    inLineComment = false;
                    sb.append(c);
                }
                continue;
            }
            if (inBlockComment) {
                if (c == '*' && n == '/') {
                    inBlockComment = false;
                    i++;
                }
                continue;
            }

            if (!inSingle && !inDouble) {
                if (c == '-' && n == '-') {
                    inLineComment = true;
                    i++;
                    continue;
                }
                if (c == '/' && n == '*') {
                    inBlockComment = true;
                    i++;
                    continue;
                }
            }

            if (c == '\'' && !inDouble) {
                inSingle = !inSingle;
                sb.append(c);
                continue;
            }
            if (c == '"' && !inSingle) {
                inDouble = !inDouble;
                sb.append(c);
                continue;
            }

            if (c == ';' && !inSingle && !inDouble) {
                String stmt = sb.toString().trim();
                if (!stmt.isEmpty()) {
                    out.add(stmt);
                }
                sb.setLength(0);
                continue;
            }
            sb.append(c);
        }
        String tail = sb.toString().trim();
        if (!tail.isEmpty()) {
            out.add(tail);
        }
        return out;
    }

    /**
     * Build an XPath step that matches any element with the given local-name, ignoring namespaces.
     */
    private static String ln(String name) {
        return "*[local-name()='" + name + "']";
    }

    private static String toDateOnly(String v) {
        if (isBlank(v)) {
            return null;
        }
        try { // most of your samples are OffsetDateTime-like
            return OffsetDateTime.parse(v).toLocalDate().toString(); // yyyy-MM-dd
        } catch (Exception ignored) {
            // try plain LocalDate
            try {
                return java.time.LocalDate.parse(v).toString();
            } catch (Exception e) {
                return null;
            }
        }
    }
}
