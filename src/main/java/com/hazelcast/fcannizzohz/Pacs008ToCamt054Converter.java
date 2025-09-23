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
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * Build a debtor-side camt.054.001.03 notification from a pacs.008.001.03.
 * - One <Ntry> per PACS <CdtTrfTxInf>
 * - Account = Debtor account (IBAN preferred, else Othr/Id + SchmeNm/Prtry)
 * - Amount = IntrBkSttlmAmt (normalised), NtryRef from EndToEndId (fallback TxId)
 * - Includes Related Parties & Agents and Remittance (Ustrd + common structured refs)
 * - No JAXB dependencies
 */
public final class Pacs008ToCamt054Converter {

    private static final String PACS_NS = "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.03";
    private static final String CAMT_NS = "urn:iso:std:iso:20022:tech:xsd:camt.054.001.03";
    private static final DateTimeFormatter ISO_OFFSET = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private Pacs008ToCamt054Converter() {
    }

    /**
     * Convert pacs.008.001.03 (any number of CdtTrfTxInf) to a debtor-side camt.054.001.03.
     * If no transactions are present or debtor account is missing, throws IllegalArgumentException.
     */
    public static String pacs008_to_camt054_debtor(InputStream pacsXml) {
        try {
            // ---- Parse PACS (secure, namespace-aware) ----
            Document doc = newSecureDocBuilder().parse(pacsXml);
            XPath xp = XPathFactory.newInstance().newXPath();
            xp.setNamespaceContext(new IsoNs("p", PACS_NS));

            NodeList txNodes = (NodeList) xp.evaluate("/p:Document/p:FIToFICstmrCdtTrf/p:CdtTrfTxInf", doc, XPathConstants.NODESET);
            if (txNodes == null || txNodes.getLength() == 0) {
                throw new IllegalArgumentException("pacs.008 has no CdtTrfTxInf");
            }

            // Debtor account (from first tx; required to build debtor-side CAMT)
            Node firstTx = txNodes.item(0);
            AcctChoice debtorAcct = readAcct(xp, firstTx, "p:DbtrAcct/p:Id");
            if (!debtorAcct.hasAny()) {
                throw new IllegalArgumentException("pacs.008 missing DbtrAcct/Id in first transaction");
            }

            // Optional parties/agents from first tx (for header-ish context if needed)
            String firstDbtrNm = str(xp, firstTx, "p:Dbtr/p:Nm");

            // ---- Build CAMT ----
            String now = OffsetDateTime.now(ZoneOffset.UTC).format(ISO_OFFSET);
            String msgId = "ACK-" + UUID.randomUUID();

            StringBuilder sb = new StringBuilder(64_000);
            sb.append("<Document xmlns=\"").append(CAMT_NS).append("\">").append("<BkToCstmrDbtCdtNtfctn>");

            // Group Header 58
            sb.append("<GrpHdr>").append(tag("MsgId", msgId)).append(tag("CreDtTm", now)).append("</GrpHdr>");

            // Single notification block; one entry per tx
            String ntfId = UUID.randomUUID().toString();
            sb.append("<Ntfctn>").append(tag("Id", ntfId)).append(tag("CreDtTm", now));

            // Account (CashAccount25) from Debtor account
            sb.append("<Acct><Id>").append(toAcctIdXml(debtorAcct)).append("</Id>");
            if (notBlank(firstDbtrNm)) {
                sb.append(tag("Nm", firstDbtrNm));
            }
            sb.append("</Acct>");

            // ---- Entries: one per PACS tx ----
            for (int i = 0; i < txNodes.getLength(); i++) {
                Node tx = txNodes.item(i);

                // Ids & amount
                String e2e = str(xp, tx, "p:PmtId/p:EndToEndId");
                String txId = str(xp, tx, "p:PmtId/p:TxId");
                String ntryRef = firstNonBlank(e2e, firstNonBlank(txId, "TX-" + UUID.randomUUID()));

                String ccy = str(xp, tx, "p:IntrBkSttlmAmt/@Ccy");
                String val = normDec(str(xp, tx, "p:IntrBkSttlmAmt"));

                // Parties / Agents
                String dbtrNm = str(xp, tx, "p:Dbtr/p:Nm");
                String cdtrNm = str(xp, tx, "p:Cdtr/p:Nm");
                String dbtrAgt = firstNonBlank(str(xp, tx, "p:DbtrAgt/p:FinInstnId/p:BICFI"), str(xp, tx, "p:DbtrAgt/p:FinInstnId/p:BIC"));
                String cdtrAgt = firstNonBlank(str(xp, tx, "p:CdtrAgt/p:FinInstnId/p:BICFI"), str(xp, tx, "p:CdtrAgt/p:FinInstnId/p:BIC"));

                // Remittance (unstructured + common structured)
                List<String> ustrd = list(xp, tx, "p:RmtInf/p:Ustrd");
                List<String> refs = new ArrayList<>();
                refs.addAll(list(xp, tx, "p:RmtInf/p:Strd/p:CdtrRefInf/p:Ref"));
                refs.addAll(list(xp, tx, "p:RmtInf/p:Strd/p:RfrdDocInf/p:Nb"));

                // ---- Ntry (ReportEntry3) ----
                sb.append("<Ntry>").append(tag("NtryRef", ntryRef)).append("<Amt Ccy=\"").append(esc(ccy)).append("\">").append(esc(val)).append("</Amt>").append(tag("CdtDbtInd", "DBIT")).append(tag("Sts", "BOOK"))
                  .append("<BkTxCd/>"); // keep simple; fill if you have domain/family/subfamily

                // Ntry details / Tx details
                sb.append("<NtryDtls><TxDtls>");

                // Refs (prefer EndToEndId)
                if (notBlank(e2e) || notBlank(txId)) {
                    sb.append("<Refs>");
                    if (notBlank(e2e)) {
                        sb.append(tag("EndToEndId", e2e));
                    }
                    if (notBlank(txId)) {
                        sb.append(tag("TxId", txId));
                    }
                    sb.append("</Refs>");
                }

                // Tx Amount
                sb.append("<Amt Ccy=\"").append(esc(ccy)).append("\">").append(esc(val)).append("</Amt>");

                // Related Parties
                if (notBlank(dbtrNm) || notBlank(cdtrNm)) {
                    sb.append("<RltdPties>");
                    if (notBlank(dbtrNm)) {
                        sb.append("<Dbtr>").append(tag("Nm", dbtrNm)).append("</Dbtr>");
                    }
                    if (notBlank(cdtrNm)) {
                        sb.append("<Cdtr>").append(tag("Nm", cdtrNm)).append("</Cdtr>");
                    }
                    sb.append("</RltdPties>");
                }

                // Related Agents
                if (notBlank(dbtrAgt) || notBlank(cdtrAgt)) {
                    sb.append("<RltdAgts>");
                    if (notBlank(dbtrAgt)) {
                        sb.append("<DbtrAgt><FinInstnId>").append(tag("BICFI", dbtrAgt)).append("</FinInstnId></DbtrAgt>");
                    }
                    if (notBlank(cdtrAgt)) {
                        sb.append("<CdtrAgt><FinInstnId>").append(tag("BICFI", cdtrAgt)).append("</FinInstnId></CdtrAgt>");
                    }
                    sb.append("</RltdAgts>");
                }

                // Remittance
                if ((ustrd != null && !ustrd.isEmpty()) || (refs != null && !refs.isEmpty())) {
                    sb.append("<RmtInf>");
                    if (ustrd != null) {
                        for (String u : ustrd) {
                            sb.append(tag("Ustrd", u));
                        }
                    }
                    if (refs != null) {
                        for (String ref : refs) {
                            sb.append("<Strd><CdtrRefInf><Ref>").append(esc(ref)).append("</Ref></CdtrRefInf></Strd>");
                        }
                    }
                    sb.append("</RmtInf>");
                }

                sb.append("</TxDtls></NtryDtls>").append("</Ntry>");
            }

            sb.append("</Ntfctn>").append("</BkToCstmrDbtCdtNtfctn>").append("</Document>");

            return pretty(sb.toString());

        } catch (Exception e) {
            throw new RuntimeException("pacs.008 -> camt.054 conversion failed", e);
        }
    }

    // ------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------

    private static javax.xml.parsers.DocumentBuilder newSecureDocBuilder()
            throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        // XXE hardening
        try {
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
            dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            dbf.setXIncludeAware(false);
            dbf.setExpandEntityReferences(false);
        } catch (Throwable ignore) {
        }
        return dbf.newDocumentBuilder();
    }

    private static AcctChoice readAcct(XPath xp, Object ctx, String base)
            throws Exception {
        String iban = str(xp, ctx, base + "/p:IBAN");
        String othrId = str(xp, ctx, base + "/p:Othr/p:Id");
        String othrPr = str(xp, ctx, base + "/p:Othr/p:SchmeNm/p:Prtry");
        return new AcctChoice(iban, othrId, othrPr);
    }

    private static String toAcctIdXml(AcctChoice a) {
        if (a == null) {
            return "";
        }
        if (notBlank(a.iban)) {
            return "<IBAN>" + esc(a.iban) + "</IBAN>";
        }
        if (notBlank(a.othrId)) {
            String pr = notBlank(a.othrSchmePrtry) ? tag("Prtry", a.othrSchmePrtry) : "";
            return "<Othr><Id>" + esc(a.othrId) + "</Id><SchmeNm>" + pr + "</SchmeNm></Othr>";
        }
        return "";
    }

    private static String normDec(String s) {
        if (s == null) {
            return "";
        }
        return new BigDecimal(s.trim()).toPlainString();
    }

    private static String tag(String name, String val) {
        return "<" + name + ">" + esc(val) + "</" + name + ">";
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
                out.add(v.trim());
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

    // ------------------------------------------------------------------------------------
    // Data carriers
    // ------------------------------------------------------------------------------------

    private record AcctChoice(String iban, String othrId, String othrSchmePrtry) {
        boolean hasAny() {
            return notBlank(iban) || notBlank(othrId);
        }
    }

    private static final class IsoNs
            implements NamespaceContext {
        private final String prefix;
        private final String uri;

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
