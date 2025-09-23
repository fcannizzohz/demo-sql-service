package com.hazelcast.fcannizzohz;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class Pain001QuickTest {

    public static void main(String[] args)
            throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        // how many PAIN batches to generate for this quick test
        int batches = 2;

        for (int b = 1; b <= batches; b++) {
            // pick a random debtor country from your supported pool
            String debtorCountry = Data.DEFAULT_COUNTRIES.get(rnd.nextInt(Data.DEFAULT_COUNTRIES.size()));

            // choose a small tx count to keep console readable
            int txCount = rnd.nextInt(1, 6); // 1..5

            // define an external amount band
            long minCents = rnd.nextLong(50_00, 10_000_00);    // 50.00 .. 10,000.00
            long extraCents = rnd.nextLong(100_00, 50_000_00);   // 100.00 .. 500,000.00
            BigDecimal min = BigDecimal.valueOf(minCents, 2);
            BigDecimal max = BigDecimal.valueOf(minCents + extraCents, 2);

            // --- PAIN: fixed debtor; creditors vary across DEFAULT_COUNTRIES ---
            String painXml = Pain001ComprehensiveGenerator.generateFixedDebtor(txCount, min, max, Data.DEFAULT_COUNTRIES,  // creditor country pool
                    debtorCountry,           // fixed debtor country
                    null                     // debtor name uses Data.debtorNameForCountry
            );

            System.out.printf("%n==================== BATCH %d :: PAIN (debtor=%s, tx=%d) ====================%n", b, debtorCountry, txCount);
            System.out.println(painXml);

            // --- PACS: 1 message per transaction ---
            try (InputStream painIn = new ByteArrayInputStream(painXml.getBytes(StandardCharsets.UTF_8))) {
                var pacsList = Pain001ToPacs008Converter.convertPain001_03_to_Pacs008_03_PerTx(painIn, "DEUTDEFFXXX",  // default InstgAgt BIC if PAIN DbtrAgt missing
                        "BNPAFRPPXXX"   // default InstdAgt BIC if PAIN CdtrAgt missing
                );

                int i = 0;
                for (String pacsXml : pacsList) {
                    i++;
                    System.out.printf("%n-------------------- BATCH %d :: PACS #%d --------------------%n", b, i);
                    System.out.println(pacsXml);

                    // --- CAMT: debtor-side notification for each PACS ---
                    try (InputStream pacsIn = new ByteArrayInputStream(pacsXml.getBytes(StandardCharsets.UTF_8))) {
                        String camtXml = Pacs008ToCamt054Converter.pacs008_to_camt054_debtor(pacsIn);
                        System.out.printf("%n~~~~~~~~~~~~~~~~~~~~ BATCH %d :: CAMT for PACS #%d ~~~~~~~~~~~~~~~~~~~~%n", b, i);
                        System.out.println(camtXml);
                    }
                }
            }
        }

        System.out.println("\nDone.");
    }
}
