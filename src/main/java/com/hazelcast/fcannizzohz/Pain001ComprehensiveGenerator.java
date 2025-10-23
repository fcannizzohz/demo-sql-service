package com.hazelcast.fcannizzohz;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Comprehensive pain.001.001.03 generator using com.hazelcast.fcannizzohz.Data.
 * - No JAXB dependencies
 * - Debtor currency bound to debtor country (Data.COUNTRY_TO_CURRENCY)
 * - IBAN for IBAN countries (Data.IBAN_LENGTHS + MOD97), Othr/BBAN otherwise
 * - Multiple PmtInf blocks, optional fields, valid control sums
 */
public final class Pain001ComprehensiveGenerator {

    private static final String NS = "urn:iso:std:iso:20022:tech:xsd:pain.001.001.03";
    private static final DateTimeFormatter DT_FMT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private Pain001ComprehensiveGenerator() {
    }

    public static String generate(Long seed, List<String> countryPool, int pmtInfoCount, int minTxPerInfo, int maxTxPerInfo) {
        return generate(seed, countryPool, pmtInfoCount, minTxPerInfo, maxTxPerInfo, Clock.systemUTC());
    }

    /**
     * Generate a comprehensive pain.001.001.03 XML.
     *
     * @param seed         Optional seed (null => random)
     * @param countryPool  Optional list of ISO-2 countries; validated via Data.sanitiseCountryPool()
     * @param pmtInfoCount Number of PmtInf batches
     * @param minTxPerInfo Min tx per PmtInf
     * @param maxTxPerInfo Max tx per PmtInf (inclusive)
     */
    public static String generate(Long seed, List<String> countryPool, int pmtInfoCount, int minTxPerInfo, int maxTxPerInfo, java.time.Clock clock) {

        if (pmtInfoCount <= 0 || minTxPerInfo <= 0 || maxTxPerInfo < minTxPerInfo) {
            throw new IllegalArgumentException("Invalid counts: pmtInfoCount>0, minTxPerInfo>0, max>=min required");
        }

        final Random r = (seed == null) ? new Random() : new Random(seed);
        final List<String> countries = Data.sanitiseCountryPool(countryPool);

        final String msgId = "MSG-" + UUID.randomUUID();
        final OffsetDateTime now = OffsetDateTime.now(clock).withOffsetSameInstant(ZoneOffset.UTC);
        final String creDtTm = now.format(DT_FMT);

        StringBuilder xml = new StringBuilder(48_000);
        xml.append("<Document xmlns=\"").append(NS).append("\">\n");
        xml.append("  <CstmrCdtTrfInitn>\n");

        // Place-holder GrpHdr (we’ll replace with totals at the end)
        xml.append("    <GrpHdr>\n");
        xml.append("      <MsgId>").append(esc(msgId)).append("</MsgId>\n");
        xml.append("      <CreDtTm>").append(esc(creDtTm)).append("</CreDtTm>\n");
        xml.append("      <InitgPty><Nm>").append(esc("Demo Initiator Ltd")).append("</Nm></InitgPty>\n");
        xml.append("    </GrpHdr>\n");

        BigDecimal overallCtrlSum = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        int overallTxs = 0;

        for (int i = 0; i < pmtInfoCount; i++) {
            // Debtor country & attributes
            String dbtrCtry = pick(r, countries);
            String dbtrName = Data.debtorNameForCountry(dbtrCtry);
            String ccy = Data.currencyForCountry(dbtrCtry);
            Bank dbtrBank = Data.pickBankForCountry(dbtrCtry, ThreadLocalRandom.current());
            String dbtrBic = dbtrBank.bic8;

            int txCount = minTxPerInfo + r.nextInt(Math.max(1, (maxTxPerInfo - minTxPerInfo) + 1));
            overallTxs += txCount;

            String pmtInfId = "PMT-" + UUID.randomUUID();
            boolean batch = r.nextBoolean();

            // Payment type info knobs
            boolean includePmtTpInf = r.nextBoolean();
            String instrPrty = includePmtTpInf && r.nextBoolean() ? pick(r, List.of("HIGH", "NORM")) : null;
            String svcLvlCd = includePmtTpInf && r.nextBoolean() ? pick(r, List.of("SEPA", "URGP", "NURG")) : null;
            String lclInstrm = includePmtTpInf && r.nextBoolean() ? pick(r, List.of("INST", "CORE", "B2B")) : null;
            String ctgyPurp = includePmtTpInf && r.nextBoolean() ? pick(r, List.of("SALA", "SUPP", "DIVI")) : null;

            String reqdExctnDt = nextBusinessDate(now.toLocalDate(), r, 1, 3).toString();

            xml.append("    <PmtInf>\n");
            xml.append("      <PmtInfId>").append(esc(pmtInfId)).append("</PmtInfId>\n");
            xml.append("      <PmtMtd>TRF</PmtMtd>\n");
            xml.append("      <BtchBookg>").append(batch ? "true" : "false").append("</BtchBookg>\n");
            xml.append("      <ReqdExctnDt>").append(esc(reqdExctnDt)).append("</ReqdExctnDt>\n");

            if (includePmtTpInf) {
                xml.append("      <PmtTpInf>\n");
                if (instrPrty != null) {
                    xml.append("        <InstrPrty>").append(esc(instrPrty)).append("</InstrPrty>\n");
                }
                if (svcLvlCd != null) {
                    xml.append("        <SvcLvl><Cd>").append(esc(svcLvlCd)).append("</Cd></SvcLvl>\n");
                }
                if (lclInstrm != null) {
                    xml.append("        <LclInstrm><Cd>").append(esc(lclInstrm)).append("</Cd></LclInstrm>\n");
                }
                if (ctgyPurp != null) {
                    xml.append("        <CtgyPurp><Cd>").append(esc(ctgyPurp)).append("</Cd></CtgyPurp>\n");
                }
                xml.append("      </PmtTpInf>\n");
            }

            // Debtor party (+ occasional address)
            xml.append("      <Dbtr>\n");
            xml.append("        <Nm>").append(esc(dbtrName)).append("</Nm>\n");
            if (r.nextBoolean()) {
                xml.append("        <PstlAdr>\n").append("          <Ctry>").append(esc(Data.normCountry(dbtrCtry))).append("</Ctry>\n").append("          <AdrLine>").append(esc("1 Demo Street")).append("</AdrLine>\n").append("        </PstlAdr>\n");
            }
            xml.append("      </Dbtr>\n");

            // Debtor account
            xml.append("      <DbtrAcct><Id><IBAN>").append(esc(Data.generateIbanForBank(dbtrBank, ThreadLocalRandom.current()))).append("</IBAN></Id></DbtrAcct>\n");

            // Debtor agent (sometimes BIC vs BICFI)
            xml.append("      <DbtrAgt><FinInstnId>");
            if (r.nextBoolean()) {
                xml.append("<BIC>").append(esc(dbtrBic)).append("</BIC>");
            } else {
                xml.append("<BICFI>").append(esc(dbtrBic)).append("</BICFI>");
            }
            xml.append("</FinInstnId></DbtrAgt>\n");

            // Transactions
            BigDecimal pmtInfCtrlSum = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
            for (int t = 1; t <= txCount; t++) {
                String e2e = "E2E-" + String.format("%05d", t) + "-" + UUID.randomUUID();
                BigDecimal amount = randomAmount(r, BigDecimal.valueOf(1), BigDecimal.valueOf(20_000));

                pmtInfCtrlSum = pmtInfCtrlSum.add(amount);
                overallCtrlSum = overallCtrlSum.add(amount);

                String cdtrCtry = pick(r, countries);
                String cdtrName = Data.debtorNameForCountry(cdtrCtry) + " Recipient";
                String cdtrAcctXml;
                Bank cdtrBank = Data.pickBankForCountry(cdtrCtry, ThreadLocalRandom.current());
                cdtrAcctXml = "<Id><IBAN>" + esc(Data.generateIbanForBank(cdtrBank, ThreadLocalRandom.current())) + "</IBAN></Id>";
                String cdtrBic = cdtrBank.bic8;

                // RmtInf: mix of structured & unstructured
                StringBuilder rmt = new StringBuilder();
                if (r.nextInt(100) < 35) { // 35% structured
                    String ref = "RF" + randomAlnum(r, 10);
                    rmt.append("        <Strd>\n").append("          <CdtrRefInf><Tp><CdOrPrtry><Cd>SCOR</Cd></CdOrPrtry></Tp><Ref>").append(esc(ref)).append("</Ref></CdtrRefInf>\n").append("        </Strd>\n");
                }
                if (r.nextBoolean()) {
                    rmt.append("        <Ustrd>").append(esc("Invoice " + (1000 + r.nextInt(9000)))).append("</Ustrd>\n");
                }

                String chrgBr = pick(r, List.of("SLEV", "SHAR", "DEBT", "CRED"));
                String purpCd = (r.nextInt(100) < 20) ? pick(r, List.of("SALA", "SUPP", "DIVI")) : null;

                xml.append("      <CdtTrfTxInf>\n");
                xml.append("        <PmtId><EndToEndId>").append(esc(e2e)).append("</EndToEndId></PmtId>\n");
                xml.append("        <Amt><InstdAmt Ccy=\"").append(esc(ccy)).append("\">").append(amount.setScale(2, RoundingMode.HALF_UP).toPlainString()).append("</InstdAmt></Amt>\n");
                xml.append("        <Cdtr><Nm>").append(esc(cdtrName)).append("</Nm></Cdtr>\n");
                xml.append("        <CdtrAcct>").append(cdtrAcctXml).append("</CdtrAcct>\n");
                xml.append("        <CdtrAgt><FinInstnId><BIC>").append(esc(cdtrBic)).append("</BIC></FinInstnId></CdtrAgt>\n");

                if (rmt.length() > 0) {
                    xml.append("        <RmtInf>\n").append(rmt).append("        </RmtInf>\n");
                }
                xml.append("        <ChrgBr>").append(esc(chrgBr)).append("</ChrgBr>\n");
                if (purpCd != null) {
                    xml.append("        <Purp><Cd>").append(esc(purpCd)).append("</Cd></Purp>\n");
                }

                // Optional supplementary data, for realism
                if (r.nextInt(100) < 10) {
                    xml.append("        <SplmtryData><Envlp><Any>").append(esc("Meta-" + randomAlnum(r, 6))).append("</Any></Envlp></SplmtryData>\n");
                }

                xml.append("      </CdtTrfTxInf>\n");
            }

            xml.append("      <NbOfTxs>").append(txCount).append("</NbOfTxs>\n");
            xml.append("      <CtrlSum>").append(pmtInfCtrlSum.setScale(2, RoundingMode.HALF_UP).toPlainString()).append("</CtrlSum>\n");

            // Add SEPA service level at PmtInf if currency is EUR throughout this PmtInf
            if ("EUR".equalsIgnoreCase(ccy)) {
                xml.append("      <PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl></PmtTpInf>\n");
            }

            xml.append("    </PmtInf>\n");
        }

        xml.append("  </CstmrCdtTrfInitn>\n");
        xml.append("</Document>\n");

        // Rewrite GrpHdr with totals
        String body = xml.toString();
        String grpHdrReplacement =
                "    <GrpHdr>\n" + "      <MsgId>" + esc(msgId) + "</MsgId>\n" + "      <CreDtTm>" + esc(creDtTm) + "</CreDtTm>\n" + "      <NbOfTxs>" + overallTxs + "</NbOfTxs>\n" + "      <CtrlSum>" + overallCtrlSum.setScale(2, RoundingMode.HALF_UP).toPlainString()
                        + "</CtrlSum>\n" + "      <InitgPty><Nm>" + esc("Demo Initiator Ltd") + "</Nm></InitgPty>\n" + "    </GrpHdr>\n";
        body = body.replaceFirst("<GrpHdr>[\\s\\S]*?</GrpHdr>", grpHdrReplacement);

        try {
            return pretty(body);
        } catch (Exception ignore) {
            return body;
        }
    }

    public static String generateFixedDebtor(int txCount, BigDecimal minAmount, BigDecimal maxAmount, List<String> creditorCountries, String fixedDebtorCountry, String fixedDebtorName) {
        return generateFixedDebtor(txCount, minAmount, maxAmount, creditorCountries, fixedDebtorCountry, fixedDebtorName, null);
    }

    // -----------------------------------------------------------------------------
    // fixed debtor + amount band + exact txCount (single PmtInf)
    // -----------------------------------------------------------------------------
    public static String generateFixedDebtor(int txCount, BigDecimal minAmount, BigDecimal maxAmount, List<String> creditorCountries, String fixedDebtorCountry, String fixedDebtorName, java.time.Clock clock) {
        if (clock == null) {
            clock = Clock.systemUTC();
        }
        Objects.requireNonNull(minAmount, "minAmount");
        Objects.requireNonNull(maxAmount, "maxAmount");
        if (txCount <= 0 || minAmount.signum() <= 0 || maxAmount.compareTo(minAmount) < 0) {
            throw new IllegalArgumentException("Invalid args: txCount>0 and 0<min<=max required");
        }

        final Random r = new Random();
        final List<String> countries = Data.sanitiseCountryPool(creditorCountries);

        final String dbtrCtry = Data.normCountry((fixedDebtorCountry == null || fixedDebtorCountry.isBlank()) ? countries.get(r.nextInt(countries.size())) : fixedDebtorCountry);
        final String dbtrName = (fixedDebtorName == null || fixedDebtorName.isBlank()) ? Data.debtorNameForCountry(dbtrCtry) : fixedDebtorName;
        final String ccy = Data.currencyForCountry(dbtrCtry);
        Bank dbBank = Data.pickBankForCountry(dbtrCtry, ThreadLocalRandom.current());
        final String dbtrBic = dbBank.bic8;
        final String msgId = "MSG-" + UUID.randomUUID();
        final OffsetDateTime now = OffsetDateTime.now(clock).withOffsetSameInstant(ZoneOffset.UTC);
        final String creDtTm = now.format(DT_FMT);
        final String reqdExctnDt = nextBusinessDate(now.toLocalDate(), new Random(), 1, 3).toString();
        final String pmtInfId = "PMT-" + UUID.randomUUID();
        final boolean batch = true; // reasonable default for batches

        StringBuilder xml = new StringBuilder(32_000);
        xml.append("<Document xmlns=\"").append(NS).append("\">\n").append("  <CstmrCdtTrfInitn>\n").append("    <GrpHdr>\n").append("      <MsgId>").append(esc(msgId)).append("</MsgId>\n").append("      <CreDtTm>").append(esc(creDtTm)).append("</CreDtTm>\n")
           .append("      <InitgPty><Nm>").append(esc("Demo Initiator Ltd")).append("</Nm></InitgPty>\n").append("    </GrpHdr>\n").append("    <PmtInf>\n").append("      <PmtInfId>").append(esc(pmtInfId)).append("</PmtInfId>\n").append("      <PmtMtd>TRF</PmtMtd>\n")
           .append("      <BtchBookg>").append(batch).append("</BtchBookg>\n").append("      <ReqdExctnDt>").append(esc(reqdExctnDt)).append("</ReqdExctnDt>\n");

        // Dbtr
        xml.append("      <Dbtr><Nm>").append(esc(dbtrName)).append("</Nm></Dbtr>\n");

        // DbtrAcct
        Bank dbtrBank = Data.pickBankForCountry(dbtrCtry, ThreadLocalRandom.current());
        xml.append("      <DbtrAcct><Id><IBAN>").append(esc(Data.generateIbanForBank(dbtrBank, ThreadLocalRandom.current()))).append("</IBAN></Id></DbtrAcct>\n");

        // DbtrAgt (use BICFI in PACS-facing flows)
        xml.append("      <DbtrAgt><FinInstnId><BIC>").append(esc(dbtrBic)).append("</BIC></FinInstnId></DbtrAgt>\n");

        BigDecimal ctrl = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        for (int i = 1; i <= txCount; i++) {
            String e2e = "E2E-" + String.format("%05d", i) + "-" + UUID.randomUUID();
            BigDecimal amt = randomAmount(r, minAmount, maxAmount).setScale(2, RoundingMode.HALF_UP);
            ctrl = ctrl.add(amt);

            String cdtrCtry = countries.get(r.nextInt(countries.size()));
            String cdtrName = Data.debtorNameForCountry(cdtrCtry) + " Recipient";
            String cdtrAcctXml;
            Bank creditorBank = Data.pickBankForCountry(cdtrCtry, ThreadLocalRandom.current());
            cdtrAcctXml = "<Id><IBAN>" + esc(Data.generateIbanForBank(creditorBank, ThreadLocalRandom.current())) + "</IBAN></Id>";
            String cdtrBic = creditorBank.bic8;

            // Simple remittance (both types possible)
            StringBuilder rmt = new StringBuilder();
            if (r.nextBoolean()) {
                rmt.append("        <Ustrd>").append(esc("Invoice " + (1000 + r.nextInt(9000)))).append("</Ustrd>\n");
            }

            xml.append("      <CdtTrfTxInf>\n").append("        <PmtId><EndToEndId>").append(esc(e2e)).append("</EndToEndId></PmtId>\n").append("        <Amt><InstdAmt Ccy=\"").append(esc(ccy)).append("\">").append(amt.toPlainString()).append("</InstdAmt></Amt>\n")
               .append("        <Cdtr><Nm>").append(esc(cdtrName)).append("</Nm></Cdtr>\n").append("        <CdtrAcct>").append(cdtrAcctXml).append("</CdtrAcct>\n").append("        <CdtrAgt><FinInstnId><BIC>").append(esc(cdtrBic)).append("</BIC></FinInstnId></CdtrAgt>\n");
            if (rmt.length() > 0) {
                xml.append("        <RmtInf>\n").append(rmt).append("        </RmtInf>\n");
            }
            xml.append("      </CdtTrfTxInf>\n");
        }

        // totals + optional SEPA flag if EUR
        xml.append("      <NbOfTxs>").append(txCount).append("</NbOfTxs>\n").append("      <CtrlSum>").append(ctrl.toPlainString()).append("</CtrlSum>\n");
        if ("EUR".equalsIgnoreCase(ccy)) {
            xml.append("      <PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl></PmtTpInf>\n");
        }
        xml.append("    </PmtInf>\n").append("  </CstmrCdtTrfInitn>\n").append("</Document>\n");

        // patch GrpHdr with totals
        String replaced = xml.toString().replaceFirst("<GrpHdr>[\\s\\S]*?</GrpHdr>",
                "    <GrpHdr>\n" + "      <MsgId>" + esc(msgId) + "</MsgId>\n" + "      <CreDtTm>" + esc(creDtTm) + "</CreDtTm>\n" + "      <NbOfTxs>" + txCount + "</NbOfTxs>\n" + "      <CtrlSum>" + ctrl.toPlainString() + "</CtrlSum>\n" + "      <InitgPty><Nm>" + esc(
                        "Demo Initiator Ltd") + "</Nm></InitgPty>\n" + "    </GrpHdr>\n");
        try {
            return pretty(replaced);
        } catch (Exception ignore) {
            return replaced;
        }
    }

    // -------------------------
    // Helpers
    // -------------------------

    private static String pick(Random r, List<String> items) {
        return items.get(r.nextInt(items.size()));
    }

    private static BigDecimal randomAmount(Random r, BigDecimal min, BigDecimal max) {
        double lo = min.doubleValue();
        double hi = max.doubleValue();
        double v = lo + r.nextDouble() * (hi - lo);
        return BigDecimal.valueOf(v).setScale(2, RoundingMode.HALF_UP);
    }

    private static String randomAlnum(Random r, int n) {
        final String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append(chars.charAt(r.nextInt(chars.length())));
        }
        return sb.toString();
    }

    private static String esc(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&apos;");
    }

    private static String pretty(String xml)
            throws Exception {
        var tf = javax.xml.transform.TransformerFactory.newInstance();
        var t = tf.newTransformer();
        t.setOutputProperty(javax.xml.transform.OutputKeys.INDENT, "yes");
        t.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        var sw = new StringWriter();
        t.transform(new javax.xml.transform.sax.SAXSource(new org.xml.sax.InputSource(new java.io.StringReader(xml))), new javax.xml.transform.stream.StreamResult(sw));
        return sw.toString();
    }

    // Choose a ReqExctnDt between +minDays and +maxDays, skipping weekends.
    private static java.time.LocalDate nextBusinessDate(java.time.LocalDate base, Random r, int minDays, int maxDays) {
        int add = minDays + r.nextInt(Math.max(1, (maxDays - minDays + 1)));
        java.time.LocalDate d = base.plusDays(add);
        java.time.DayOfWeek w = d.getDayOfWeek();
        if (w == java.time.DayOfWeek.SATURDAY) {
            d = d.plusDays(2);
        } else if (w == java.time.DayOfWeek.SUNDAY) {
            d = d.plusDays(1);
        }
        return d;
    }

    // Optional: jitter creation timestamp within business hours for realism
    private static java.time.OffsetDateTime businessHourJitter(java.time.OffsetDateTime base, Random r, java.time.Clock clock) {
        // window 08:00–18:00 UTC; shift within +/- 90 mins
        int minutes = (r.nextInt(181) - 90);
        java.time.OffsetDateTime shifted = base.plusMinutes(minutes);
        int hour = shifted.getHour();
        if (hour < 8) {
            shifted = shifted.withHour(8).withMinute(r.nextInt(60));
        }
        if (hour > 18) {
            shifted = shifted.withHour(18).withMinute(r.nextInt(60));
        }
        return shifted.withOffsetSameInstant(ZoneOffset.UTC);
    }

    // -------------------------
    // Example main
    // -------------------------
    public static void main(String[] args) {
        // Example with your DEFAULT_COUNTRIES
        String xml = generate(1234L, Data.DEFAULT_COUNTRIES, 2, 2, 5);
        System.out.println(xml);
    }
}
