package com.hazelcast.fcannizzohz;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * pain.001.001.03 -> pacs.008.001.03 converter, compatible with the comprehensive PAIN generator.
 * - DOM + XPath only (no JAXB model classes).
 */
public final class Pain001ToPacs008Converter {

    private static final String PAIN_NS = "urn:iso:std:iso:20022:tech:xsd:pain.001.001.03";
    private static final String PACS_NS = "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.03";

    private Pain001ToPacs008Converter() {
    }

    /**
     * One PACS per PAIN transaction (keeps PAIN order).
     */
    public static List<String> convertPain001_03_to_Pacs008_03_PerTx(InputStream painXml, String defaultInstgAgtBic, String defaultInstdAgtBic) {
        try {
            ParsedPain p = parsePain(painXml);

            List<String> out = new ArrayList<>();
            for (PainPaymentInfo pi : p.paymentInfos) {
                for (PainTx tx : pi.txs) {
                    // Agents: prefer debtor/creditor agents, else defaults
                    String instgBic = firstNonBlank(pi.dbtrAgtBic, defaultInstgAgtBic);
                    String instdBic = firstNonBlank(tx.cdtrAgtBic, defaultInstdAgtBic);

                    PacsGrpHdr hdr = new PacsGrpHdr(
                            // compose per-tx MsgId from PAIN MsgId when present
                            (p.grpHdrMsgId != null ? p.grpHdrMsgId : "MSG-" + UUID.randomUUID()) + "-TX-" + UUID.randomUUID(), p.grpHdrCreDtTm, "1", tx.instdAmtValue,    // ctrl sum = single tx amount
                            instgBic, instdBic);

                    PacsTx pacsTx = toPacsTx(pi, tx);

                    String xml = buildPacsXml(hdr, Collections.singletonList(pacsTx));
                    out.add(pretty(xml));
                }
            }
            return out;
        } catch (Exception e) {
            throw new RuntimeException("Conversion (1-per-tx) failed", e);
        }
    }

    /**
     * Single PACS with all PAIN transactions.
     */
    public static String convertPain001_03_to_Pacs008_03(InputStream painXml, String defaultInstgAgtBic, String defaultInstdAgtBic) {
        try {
            ParsedPain p = parsePain(painXml);

            // Choose agents from first available; else defaults
            String firstDbtrAgt = null, firstCdtrAgt = null;
            for (PainPaymentInfo pi : p.paymentInfos) {
                if (firstDbtrAgt == null && notBlank(pi.dbtrAgtBic)) {
                    firstDbtrAgt = pi.dbtrAgtBic;
                }
                for (PainTx tx : pi.txs) {
                    if (firstCdtrAgt == null && notBlank(tx.cdtrAgtBic)) {
                        firstCdtrAgt = tx.cdtrAgtBic;
                    }
                }
            }
            String instgBic = firstNonBlank(firstDbtrAgt, defaultInstgAgtBic);
            String instdBic = firstNonBlank(firstCdtrAgt, defaultInstdAgtBic);

            // Flatten transactions
            List<PacsTx> txs = new ArrayList<>();
            for (PainPaymentInfo pi : p.paymentInfos) {
                for (PainTx tx : pi.txs) {
                    txs.add(toPacsTx(pi, tx));
                }
            }

            // Group header
            String nbOfTxs = String.valueOf(txs.size());
            String ctrlSum = sumAmounts(txs);
            PacsGrpHdr hdr = new PacsGrpHdr(firstNonBlank(p.grpHdrMsgId, "MSG-" + UUID.randomUUID()), p.grpHdrCreDtTm, nbOfTxs, ctrlSum, instgBic, instdBic);

            String xml = buildPacsXml(hdr, txs);
            return pretty(xml);
        } catch (Exception e) {
            throw new RuntimeException("Conversion pain.001.001.03 -> pacs.008.001.03 failed", e);
        }
    }

    // =====================================================================
    // Parsing PAIN (DOM + XPath), tolerant to generator variability
    // =====================================================================

    private static ParsedPain parsePain(InputStream in)
            throws Exception {
        var dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        // Harden basic parser options (avoid XXE)
        try {
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
            dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            dbf.setXIncludeAware(false);
            dbf.setExpandEntityReferences(false);
        } catch (Throwable ignore) {
        }
        Document doc = dbf.newDocumentBuilder().parse(in);

        XPath xp = XPathFactory.newInstance().newXPath();
        xp.setNamespaceContext(new IsoNs("p", PAIN_NS));

        // Group Header
        String msgId = str(xp, doc, "/p:Document/p:CstmrCdtTrfInitn/p:GrpHdr/p:MsgId");
        String creDtTm = str(xp, doc, "/p:Document/p:CstmrCdtTrfInitn/p:GrpHdr/p:CreDtTm");

        // Payment Infos
        NodeList pmtInfNodes = (NodeList) xp.evaluate("/p:Document/p:CstmrCdtTrfInitn/p:PmtInf", doc, XPathConstants.NODESET);
        List<PainPaymentInfo> infos = new ArrayList<>();

        for (int i = 0; i < pmtInfNodes.getLength(); i++) {
            Node pmt = pmtInfNodes.item(i);

            String reqdExctnDt = str(xp, pmt, "p:ReqdExctnDt");

            // Debtor
            String dbtrNm = str(xp, pmt, "p:Dbtr/p:Nm");

            // DbtrAcct (IBAN or Othr)
            AcctChoice dbtrAcct = readAcct(xp, pmt, "p:DbtrAcct/p:Id");

            // DbtrAgt BIC or BICFI
            String dbtrAgtBic = firstNonBlank(str(xp, pmt, "p:DbtrAgt/p:FinInstnId/p:BIC"), str(xp, pmt, "p:DbtrAgt/p:FinInstnId/p:BICFI"));

            // Merge ALL PmtTpInf occurrences (generator may emit more than one)
            NodeList pmtTpInfNodes = (NodeList) xp.evaluate("p:PmtTpInf", pmt, XPathConstants.NODESET);
            PmtType mergedPmtType = mergePmtType(xp, pmtTpInfNodes);

            // Transactions
            NodeList txNodes = (NodeList) xp.evaluate("p:CdtTrfTxInf", pmt, XPathConstants.NODESET);
            List<PainTx> txs = new ArrayList<>(txNodes.getLength());

            for (int t = 0; t < txNodes.getLength(); t++) {
                Node tx = txNodes.item(t);

                String e2e = str(xp, tx, "p:PmtId/p:EndToEndId");
                // Amount (string as-is, but we’ll normalise later for sums)
                String ccy = str(xp, tx, "p:Amt/p:InstdAmt/@Ccy");
                String val = str(xp, tx, "p:Amt/p:InstdAmt");

                String cdtrNm = str(xp, tx, "p:Cdtr/p:Nm");

                // CdtrAcct
                AcctChoice cdtrAcct = readAcct(xp, tx, "p:CdtrAcct/p:Id");

                // CdtrAgt BIC/BICFI
                String cdtrAgtBic = firstNonBlank(str(xp, tx, "p:CdtrAgt/p:FinInstnId/p:BIC"), str(xp, tx, "p:CdtrAgt/p:FinInstnId/p:BICFI"));

                // Remittance (Ustrd + common structured refs)
                List<String> ustrd = list(xp, tx, "p:RmtInf/p:Ustrd");
                List<String> structuredRefs = new ArrayList<>();
                structuredRefs.addAll(list(xp, tx, "p:RmtInf/p:Strd/p:CdtrRefInf/p:Ref"));
                structuredRefs.addAll(list(xp, tx, "p:RmtInf/p:Strd/p:RfrdDocInf/p:Nb"));

                // Charge bearer
                String chrgBr = str(xp, tx, "p:ChrgBr");

                // Purpose
                String purpCd = str(xp, tx, "p:Purp/p:Cd");
                String purpPr = str(xp, tx, "p:Purp/p:Prtry");

                txs.add(new PainTx(e2e, ccy, val, cdtrNm, cdtrAcct, cdtrAgtBic, ustrd, structuredRefs, chrgBr, purpCd, purpPr));
            }

            infos.add(new PainPaymentInfo(reqdExctnDt, dbtrNm, dbtrAcct, dbtrAgtBic, mergedPmtType, txs));
        }

        return new ParsedPain(msgId, creDtTm, infos);
    }

    private static AcctChoice readAcct(XPath xp, Object ctx, String base)
            throws Exception {
        String iban = str(xp, ctx, base + "/p:IBAN");
        String othrId = str(xp, ctx, base + "/p:Othr/p:Id");
        String othrPr = str(xp, ctx, base + "/p:Othr/p:SchmeNm/p:Prtry");
        return new AcctChoice(iban, othrId, othrPr);
    }

    private static PmtType mergePmtType(XPath xp, NodeList pmtTpInfNodes)
            throws Exception {
        String instrPrty = null, svcLvlCd = null, svcLvlPrtry = null, lclInstrmCd = null, lclInstrmPrtry = null, ctgyPurpCd = null, ctgyPurpPrtry = null;

        for (int i = 0; i < pmtTpInfNodes.getLength(); i++) {
            Node n = pmtTpInfNodes.item(i);
            instrPrty = firstNonBlank(instrPrty, str(xp, n, "p:InstrPrty"));
            svcLvlCd = firstNonBlank(svcLvlCd, str(xp, n, "p:SvcLvl/p:Cd"));
            svcLvlPrtry = firstNonBlank(svcLvlPrtry, str(xp, n, "p:SvcLvl/p:Prtry"));
            lclInstrmCd = firstNonBlank(lclInstrmCd, str(xp, n, "p:LclInstrm/p:Cd"));
            lclInstrmPrtry = firstNonBlank(lclInstrmPrtry, str(xp, n, "p:LclInstrm/p:Prtry"));
            ctgyPurpCd = firstNonBlank(ctgyPurpCd, str(xp, n, "p:CtgyPurp/p:Cd"));
            ctgyPurpPrtry = firstNonBlank(ctgyPurpPrtry, str(xp, n, "p:CtgyPurp/p:Prtry"));
        }

        return new PmtType(instrPrty, svcLvlCd, svcLvlPrtry, lclInstrmCd, lclInstrmPrtry, ctgyPurpCd, ctgyPurpPrtry);
    }

    // =====================================================================
    // PACS building
    // =====================================================================

    private static PacsTx toPacsTx(PainPaymentInfo pi, PainTx tx) {
        String intrBkSttlmDt = notBlank(pi.reqdExctnDt) ? pi.reqdExctnDt : LocalDate.now().toString();

        return new PacsTx("TX-" + UUID.randomUUID(), tx.endToEndId, tx.instdAmtCcy, normDec(tx.instdAmtValue), intrBkSttlmDt,
                // Debtor
                pi.dbtrNm, pi.dbtrAcct, pi.dbtrAgtBic,
                // Creditor
                tx.cdtrNm, tx.cdtrAcct, tx.cdtrAgtBic,
                // Payment type (merged)
                pi.pmtType,
                // Rmt (ustrd + structured)
                tx.ustrd, tx.structuredRefs,
                // Charges & purpose
                tx.chargeBearer, new Purpose(tx.purpCd, tx.purpPrtry));
    }

    private static String buildPacsXml(PacsGrpHdr hdr, List<PacsTx> txs) {
        StringBuilder sb = new StringBuilder(64_000);
        sb.append("<Document xmlns=\"").append(PACS_NS).append("\">");
        sb.append("<FIToFICstmrCdtTrf>");

        // Group Header
        sb.append("<GrpHdr>").append(tag("MsgId", hdr.msgId)).append(optTag("CreDtTm", hdr.creDtTm)).append(tag("NbOfTxs", firstNonBlank(hdr.nbOfTxs, Integer.toString(txs.size())))).append(optTag("CtrlSum", hdr.ctrlSum)).append("<InstgAgt><FinInstnId>")
          .append(optTag("BICFI", hdr.instgAgtBic)).append("</FinInstnId></InstgAgt>").append("<InstdAgt><FinInstnId>").append(optTag("BICFI", hdr.instdAgtBic)).append("</FinInstnId></InstdAgt>").append("</GrpHdr>");

        // Transactions
        for (PacsTx t : txs) {
            sb.append("<CdtTrfTxInf>");

            // PmtId
            sb.append("<PmtId>").append(optTag("EndToEndId", t.endToEndId)).append(tag("TxId", t.txId)).append("</PmtId>");

            // Amount + date
            sb.append("<IntrBkSttlmAmt Ccy=\"").append(esc(t.intrBkSttlmAmtCcy)).append("\">").append(esc(t.intrBkSttlmAmtValue)).append("</IntrBkSttlmAmt>");
            sb.append(optTag("IntrBkSttlmDt", t.intrBkSttlmDt));

            // Payment type
            if (t.pmtType != null && t.pmtType.hasAny()) {
                sb.append("<PmtTpInf>");
                if (notBlank(t.pmtType.instrPrty)) {
                    sb.append(tag("InstrPrty", t.pmtType.instrPrty));
                }
                if (t.pmtType.hasSvcLvl()) {
                    sb.append("<SvcLvl>").append(optTag("Cd", t.pmtType.svcLvlCd)).append(optTag("Prtry", t.pmtType.svcLvlPrtry)).append("</SvcLvl>");
                }
                if (t.pmtType.hasLclInstrm()) {
                    sb.append("<LclInstrm>").append(optTag("Cd", t.pmtType.lclInstrmCd)).append(optTag("Prtry", t.pmtType.lclInstrmPrtry)).append("</LclInstrm>");
                }
                if (t.pmtType.hasCtgyPurp()) {
                    sb.append("<CtgyPurp>").append(optTag("Cd", t.pmtType.ctgyPurpCd)).append(optTag("Prtry", t.pmtType.ctgyPurpPrtry)).append("</CtgyPurp>");
                }
                sb.append("</PmtTpInf>");
            }

            // Debtor
            if (notBlank(t.dbtrNm)) {
                sb.append("<Dbtr>").append(tag("Nm", t.dbtrNm)).append("</Dbtr>");
            }
            if (t.dbtrAcct != null && t.dbtrAcct.hasAny()) {
                sb.append("<DbtrAcct><Id>").append(toAcctIdXml(t.dbtrAcct)).append("</Id></DbtrAcct>");
            }
            if (notBlank(t.dbtrAgtBic)) {
                sb.append("<DbtrAgt><FinInstnId>").append(tag("BICFI", t.dbtrAgtBic)).append("</FinInstnId></DbtrAgt>");
            }

            // Creditor
            if (notBlank(t.cdtrAgtBic)) {
                sb.append("<CdtrAgt><FinInstnId>").append(tag("BICFI", t.cdtrAgtBic)).append("</FinInstnId></CdtrAgt>");
            }
            if (notBlank(t.cdtrNm)) {
                sb.append("<Cdtr>").append(tag("Nm", t.cdtrNm)).append("</Cdtr>");
            }
            if (t.cdtrAcct != null && t.cdtrAcct.hasAny()) {
                sb.append("<CdtrAcct><Id>").append(toAcctIdXml(t.cdtrAcct)).append("</Id></CdtrAcct>");
            }

            // Remittance
            if ((t.ustrd != null && !t.ustrd.isEmpty()) || (t.structuredRefs != null && !t.structuredRefs.isEmpty())) {
                sb.append("<RmtInf>");
                if (t.ustrd != null) {
                    for (String u : t.ustrd) {
                        sb.append(tag("Ustrd", u));
                    }
                }
                if (t.structuredRefs != null) {
                    for (String ref : t.structuredRefs) {
                        sb.append("<Strd><CdtrRefInf><Ref>").append(esc(ref)).append("</Ref></CdtrRefInf></Strd>");
                    }
                }
                sb.append("</RmtInf>");
            }

            // Charges & Purpose
            if (notBlank(t.chargeBearer)) {
                sb.append(tag("ChrgBr", t.chargeBearer));
            }
            if (t.purpose != null && t.purpose.hasAny()) {
                sb.append("<Purp>").append(optTag("Cd", t.purpose.cd)).append(optTag("Prtry", t.purpose.prtry)).append("</Purp>");
            }

            sb.append("</CdtTrfTxInf>");
        }

        sb.append("</FIToFICstmrCdtTrf></Document>");
        return sb.toString();
    }

    // =====================================================================
    // Small helpers
    // =====================================================================

    private static String sumAmounts(List<PacsTx> txs) {
        BigDecimal sum = txs.stream().map(t -> new BigDecimal(t.intrBkSttlmAmtValue)).reduce(BigDecimal.ZERO, BigDecimal::add);
        return sum.toPlainString();
    }

    private static String normDec(String s) {
        if (s == null) {
            return null;
        }
        return new BigDecimal(s.trim()).toPlainString();
    }

    private static String toAcctIdXml(AcctChoice a) {
        if (notBlank(a.iban)) {
            return "<IBAN>" + esc(a.iban) + "</IBAN>";
        }
        if (notBlank(a.othrId)) {
            String pr = notBlank(a.othrSchmePrtry) ? tag("Prtry", a.othrSchmePrtry) : "";
            return "<Othr><Id>" + esc(a.othrId) + "</Id><SchmeNm>" + pr + "</SchmeNm></Othr>";
        }
        return "";
    }

    private static String tag(String name, String val) {
        return "<" + name + ">" + esc(val) + "</" + name + ">";
    }

    private static String optTag(String name, String val) {
        return notBlank(val) ? tag(name, val) : "";
    }

    private static String esc(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&apos;");
    }

    private static String pretty(String xml)
            throws Exception {
        Transformer t = TransformerFactory.newInstance().newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        t.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        StringWriter out = new StringWriter();
        t.transform(new SAXSource(new org.xml.sax.InputSource(new StringReader(xml))), new StreamResult(out));
        return out.toString();
    }

    private static String str(XPath xp, Object ctx, String path)
            throws Exception {
        String s = (String) xp.evaluate(path, ctx instanceof Document d ? d : ctx, XPathConstants.STRING);
        return s != null && !s.isEmpty() ? s : null;
    }

    private static List<String> list(XPath xp, Object ctx, String path)
            throws Exception {
        NodeList nl = (NodeList) xp.evaluate(path, ctx instanceof Document d ? d : ctx, XPathConstants.NODESET);
        if (nl == null) {
            return Collections.emptyList();
        }
        List<String> out = new ArrayList<>(nl.getLength());
        for (int i = 0; i < nl.getLength(); i++) {
            String v = nl.item(i).getTextContent();
            if (v != null && !v.isBlank()) {
                out.add(v);
            }
        }
        return out;
    }

    private static boolean notBlank(String s) {
        return s != null && !s.isBlank();
    }

    private static String firstNonBlank(String a, String b) {
        return notBlank(a) ? a : b;
    }

    // =====================================================================
    // Data carriers
    // =====================================================================

    private record ParsedPain(String grpHdrMsgId, String grpHdrCreDtTm, List<PainPaymentInfo> paymentInfos) {
    }

    private record PainPaymentInfo(String reqdExctnDt, String dbtrNm, AcctChoice dbtrAcct, String dbtrAgtBic, PmtType pmtType, List<PainTx> txs) {
    }

    private record PainTx(String endToEndId, String instdAmtCcy, String instdAmtValue, String cdtrNm, AcctChoice cdtrAcct, String cdtrAgtBic, List<String> ustrd, List<String> structuredRefs, String chargeBearer, String purpCd, String purpPrtry) {
    }

    private record AcctChoice(String iban, String othrId, String othrSchmePrtry) {
        boolean hasAny() {
            return notBlank(iban) || notBlank(othrId);
        }
    }

    private record PmtType(String instrPrty, String svcLvlCd, String svcLvlPrtry, String lclInstrmCd, String lclInstrmPrtry, String ctgyPurpCd, String ctgyPurpPrtry) {
        boolean hasAny() {
            return hasSvcLvl() || hasLclInstrm() || hasCtgyPurp() || notBlank(instrPrty);
        }

        boolean hasSvcLvl() {
            return notBlank(svcLvlCd) || notBlank(svcLvlPrtry);
        }

        boolean hasLclInstrm() {
            return notBlank(lclInstrmCd) || notBlank(lclInstrmPrtry);
        }

        boolean hasCtgyPurp() {
            return notBlank(ctgyPurpCd) || notBlank(ctgyPurpPrtry);
        }
    }

    private record PacsGrpHdr(String msgId, String creDtTm, String nbOfTxs, String ctrlSum, String instgAgtBic, String instdAgtBic) {
    }

    private record PacsTx(String txId, String endToEndId, String intrBkSttlmAmtCcy, String intrBkSttlmAmtValue, String intrBkSttlmDt, String dbtrNm, AcctChoice dbtrAcct, String dbtrAgtBic, String cdtrNm, AcctChoice cdtrAcct, String cdtrAgtBic, PmtType pmtType, List<String> ustrd,
                          List<String> structuredRefs, String chargeBearer, Purpose purpose) {
    }

    private record Purpose(String cd, String prtry) {
        boolean hasAny() {
            return notBlank(cd) || notBlank(prtry);
        }
    }

    private static final class IsoNs
            implements NamespaceContext {
        private final String prefix, uri;

        IsoNs(String prefix, String uri) {
            this.prefix = prefix;
            this.uri = uri;
        }

        @Override
        public String getNamespaceURI(String p) {
            return prefix.equals(p) ? uri : null;
        }

        @Override
        public String getPrefix(String u) {
            return uri.equals(u) ? prefix : null;
        }

        @Override
        public Iterator<String> getPrefixes(String u) {
            return Collections.singleton(prefix).iterator();
        }
    }
}
