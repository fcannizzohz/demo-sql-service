-- Dashboard layout (quick blueprint)
--
-- Row 1 (KPIs):
-- - Total Payments, Total Value, Avg Payment, #Currencies, #Files w/ Mismatch.
--
-- Row 2 (Data quality):
-- - Control-Sum Reconciliation table (query #1) with mismatch highlighting.
--
-- Row 3 (Distribution & Exposure):
-- - Histogram/boxplot of amounts (from #2, UI-calculated bins).
-- - Currency breakdown bar/treemap (from #3) + KPI: COUNT DISTINCT currency.
--
-- Row 4 (Counterparties):
-- - Top recipients by count (5A) and by value (5B) side-by-side.
--
-- Row 5 (Accounts & Banks):
-- - Debtor outflows by account (4A).
-- - Creditor inflows by agent BIC (4C).
--
-- Row 6 (Profile & Recs):
-- - Size bucket bars (6).
-- - Reconciliation tables (optional) filtered to mismatches (Optional sections).

-- ===========================================
-- 1) PAIN control-sum vs recomputed sum (per file)
-- What: Reconciles PAIN GrpHdr.ControlSum and NbOfTxs vs the actual sum/count of transactions.
-- Why: Early integrity check — quickly spots malformed files, duplicate loads, or partial ingests.
-- Dashboard: Table with conditional formatting (mismatch = red). Add KPIs: #Files, #Mismatched Files.
SELECT
    gh.MessageId,
    gh.NumberOfTransactions                  AS ReportedNbOfTxs,
    COUNT(*)                                 AS ActualNbOfTxs,
    gh.ControlSum                            AS ReportedControlSum,
    CAST(SUM(tx.InstructedAmount) AS DECIMAL) AS ComputedControlSum,
    (gh.ControlSum = CAST(SUM(tx.InstructedAmount) AS DECIMAL))        AS ControlSumMatches,
    (gh.NumberOfTransactions = COUNT(*))                                AS TxCountMatches
FROM pain001_group_header AS gh
         JOIN pain001_payment_information AS pi
              ON pi.MessageId = gh.MessageId
         JOIN pain001_credit_transfer_transaction AS tx
              ON tx.PaymentInformationId = pi.PaymentInformationId
GROUP BY gh.MessageId, gh.NumberOfTransactions, gh.ControlSum
ORDER BY gh.MessageId;

-- ===========================================
-- 2) Distribution of payment values (global)
-- What: Basic descriptive stats across all payments.
-- Why: Understand scale and skew; use alongside a histogram/box-plot to spot outliers.
-- Dashboard: KPI tiles for N/Min/Max/Avg, plus a histogram (client-side binning) or box-plot.
SELECT
    COUNT(*)               AS N,
    MIN(InstructedAmount)  AS MinAmount,
    MAX(InstructedAmount)  AS MaxAmount,
    AVG(InstructedAmount)  AS AvgAmount
FROM pain001_credit_transfer_transaction;

-- ===========================================
-- 3) Currency exposure (breakdown by currency)
-- What: Amount and count per payment currency.
-- Why: See multi-currency exposure and concentration; drive hedging/settlement prioritization.
-- Dashboard: Sorted bar/treemap by TotalAmount; small KPI: #Currencies (COUNT DISTINCT).
SELECT
    InstructedAmountCurrency AS Currency,
    COUNT(*)                  AS Payments,
    CAST(SUM(InstructedAmount) AS DECIMAL) AS TotalAmount,
    AVG(InstructedAmount)     AS AvgAmount
FROM pain001_credit_transfer_transaction
GROUP BY InstructedAmountCurrency
ORDER BY TotalAmount DESC;

-- ===========================================
-- 4A) Debtor outflows per debtor account
-- What: Total outflows aggregated by debtor account (IBAN or Other).
-- Why: Identify sending accounts with outsized activity; monitor limits and liquidity.
-- Dashboard: Horizontal bar chart sorted by TotalOutflow; add Min/Max columns in a table.
SELECT
    COALESCE(pi.DebtorAccountIdentificationIBAN, pi.DebtorAccountIdentificationOtherId) AS DebtorAccount,
    COUNT(*)                  AS Payments,
    CAST(SUM(tx.InstructedAmount) AS DECIMAL) AS TotalOutflow,
    MIN(tx.InstructedAmount)    AS MinOutflow,
    MAX(tx.InstructedAmount)    AS MaxOutflow
FROM pain001_payment_information AS pi
         JOIN pain001_credit_transfer_transaction AS tx
              ON tx.PaymentInformationId = pi.PaymentInformationId
GROUP BY COALESCE(pi.DebtorAccountIdentificationIBAN, pi.DebtorAccountIdentificationOtherId)
ORDER BY TotalOutflow DESC;

-- ===========================================
-- 4B) Creditor inflows per creditor account
-- What: Total inflows aggregated by creditor account (IBAN or Other).
-- Why: See where funds are landing; useful for AML thresholds and beneficiary analysis.
-- Dashboard: Pareto-style bar (by TotalInflow) with cumulative % overlay (client-side).
SELECT
    COALESCE(CreditorAccountIdentificationIBAN, CreditorAccountIdentificationOtherId) AS CreditorAccount,
    COUNT(*)                  AS Payments,
    CAST(SUM(InstructedAmount) AS DECIMAL) AS TotalInflow,
    MIN(InstructedAmount)     AS MinInflow,
    MAX(InstructedAmount)     AS MaxInflow
FROM pain001_credit_transfer_transaction
GROUP BY COALESCE(CreditorAccountIdentificationIBAN, CreditorAccountIdentificationOtherId)
ORDER BY TotalInflow DESC;

-- ===========================================
-- 4C) Creditor inflows per creditor agent (BIC)
-- What: Total inflows per receiving bank/agent.
-- Why: Bank concentration risk; informs correspondent choices and fee negotiations.
-- Dashboard: Bar chart by BIC; optionally annotate with bank names in the UI layer.
SELECT
    CreditorAgentBICFI        AS CreditorAgentBIC,
    COUNT(*)                  AS Payments,
    CAST(SUM(InstructedAmount) AS DECIMAL) AS TotalInflow
FROM pain001_credit_transfer_transaction
GROUP BY CreditorAgentBICFI
ORDER BY TotalInflow DESC;

-- ===========================================
-- 5A) Top recipients by number of payments
-- What: Most frequently paid counterparties.
-- Why: Operational hotspots; candidates for standing instructions or enhanced monitoring.
-- Dashboard: Leaderboard (top 20) as a horizontal bar by count; link to drill-through.
SELECT
    CreditorName,
    COUNT(*) AS Payments
FROM pain001_credit_transfer_transaction
GROUP BY CreditorName
ORDER BY Payments DESC
    LIMIT 20;

-- ===========================================
-- 5B) Top recipients by total value
-- What: Largest beneficiaries by value.
-- Why: Concentration of value; complements 5A (count vs value often differs).
-- Dashboard: Adjacent leaderboard to 5A; show both “Payments” and “TotalReceived”.
SELECT
    CreditorName,
    CAST(SUM(InstructedAmount) AS DECIMAL) AS TotalReceived
FROM pain001_credit_transfer_transaction
GROUP BY CreditorName
ORDER BY TotalReceived DESC
    LIMIT 20;

-- ===========================================
-- 6) Transaction size profile (bucketed)
-- What: Size buckets for transfers (tune thresholds to your business).
-- Why: Mix of micro/small/medium/large — great for spotting regime shifts and fee impacts.
-- Dashboard: Stacked bar (Payments vs TotalAmount) or two side-by-side bars by Bucket.
SELECT
    CASE
        WHEN InstructedAmount < 100      THEN 'Micro (<100)'
        WHEN InstructedAmount < 1000     THEN 'Small (100-1k)'
        WHEN InstructedAmount < 10000    THEN 'Medium (1k-10k)'
        ELSE                                   'Large (>=10k)'
        END AS Bucket,
    COUNT(*)                               AS Payments,
    CAST(SUM(InstructedAmount) AS DECIMAL) AS TotalAmount
FROM pain001_credit_transfer_transaction
GROUP BY
    CASE
        WHEN InstructedAmount < 100      THEN 'Micro (<100)'
        WHEN InstructedAmount < 1000     THEN 'Small (100-1k)'
        WHEN InstructedAmount < 10000    THEN 'Medium (1k-10k)'
        ELSE                                   'Large (>=10k)'
        END
ORDER BY Payments DESC;

-- ===========================================
-- Optional) PAIN vs PACS reconciliation (by EndToEndId)
-- What: Amount/currency alignment by E2E across PAIN and PACS.
-- Why: Ensures conversion preserved value/currency; flags mismatches early.
-- Dashboard: Table filtered to mismatches only; add a KPI “#Mismatched E2E”.
SELECT
    tx.EndToEndIdentification AS E2E,
    CAST(SUM(tx.InstructedAmount) AS DECIMAL)           AS PainAmount,
    MIN(tx.InstructedAmountCurrency)                    AS PainCcy,
    CAST(SUM(p8.InterbankSettlementAmount) AS DECIMAL)  AS PacsAmount,
    MIN(p8.InterbankSettlementCurrency)                 AS PacsCcy,
    (CAST(SUM(tx.InstructedAmount) AS DECIMAL) = CAST(SUM(p8.InterbankSettlementAmount) AS DECIMAL)) AS AmountMatches,
    (MIN(tx.InstructedAmountCurrency) = MIN(p8.InterbankSettlementCurrency))                         AS CurrencyMatches
FROM pain001_credit_transfer_transaction AS tx
         JOIN pacs008_credit_transfer_transaction AS p8
              ON p8.EndToEndIdentification = tx.EndToEndIdentification
GROUP BY tx.EndToEndIdentification
ORDER BY E2E;

-- ===========================================
-- Optional) PACS vs CAMT reconciliation (signed CAMT)
-- What: Compare PACS interbank amount vs. signed CAMT entry (DBIT negative, CRDT positive).
-- Why: Validates posting direction and amount on account statement vs interbank.
-- Dashboard: Table showing E2E, PacsAmount, CamtSignedAmount; highlight deltas.
SELECT
    p8.EndToEndIdentification AS E2E,
    CAST(SUM(p8.InterbankSettlementAmount) AS DECIMAL) AS PacsAmount,
    MIN(p8.InterbankSettlementCurrency)                AS PacsCcy,
    CAST(SUM(
            CASE
                WHEN e.CreditDebitIndicator = 'CRDT' THEN  e.Amount
                WHEN e.CreditDebitIndicator = 'DBIT' THEN -e.Amount
                ELSE 0
                END
         ) AS DECIMAL)                                       AS CamtSignedAmount,
    MIN(e.Currency)                                     AS CamtCcy
FROM pacs008_credit_transfer_transaction AS p8
         LEFT JOIN camt054_entry AS e
                   ON e.EndToEndIdentification = p8.EndToEndIdentification
GROUP BY p8.EndToEndIdentification
ORDER BY E2E;
