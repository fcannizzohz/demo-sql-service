-- PAIN.001 — Group Header (flat JSON)
CREATE OR REPLACE MAPPING pain001_group_header (
  __key VARCHAR,
  MessageId VARCHAR,
  CreationDateTime TIMESTAMP WITH TIME ZONE,
  NumberOfTransactions INT,
  ControlSum DECIMAL,
  InitiatingPartyName VARCHAR
)
TYPE IMap OPTIONS ('keyFormat'='varchar','valueFormat'='json-flat');

-- PAIN.001 — Payment Information (flat JSON)
CREATE OR REPLACE MAPPING pain001_payment_information (
  __key VARCHAR,
  PaymentInformationId VARCHAR,
  MessageId VARCHAR,
  RequestedExecutionDate DATE,
  DebtorName VARCHAR,
  DebtorAccountIdentificationIBAN VARCHAR,
  DebtorAccountIdentificationOtherId VARCHAR,
  DebtorAgentBICFI VARCHAR,
  BatchBooking BOOLEAN,
  ServiceLevelCode VARCHAR,
  LocalInstrumentCode VARCHAR,
  CategoryPurposeCode VARCHAR,
  ChargeBearer VARCHAR
)
TYPE IMap OPTIONS ('keyFormat'='varchar','valueFormat'='json-flat');

-- PAIN.001 — Credit Transfer Transaction (flat JSON)
CREATE OR REPLACE MAPPING pain001_credit_transfer_transaction (
  __key VARCHAR,
  PaymentInformationId VARCHAR,
  EndToEndIdentification VARCHAR,
  InstructedAmount DECIMAL,
  InstructedAmountCurrency VARCHAR,
  CreditorName VARCHAR,
  CreditorAccountIdentificationIBAN VARCHAR,
  CreditorAccountIdentificationOtherId VARCHAR,
  CreditorAgentBICFI VARCHAR,
  RemittanceInformationUnstructured VARCHAR,
  PurposeCode VARCHAR,
  ChargeBearer VARCHAR
)
TYPE IMap OPTIONS ('keyFormat'='varchar','valueFormat'='json-flat');

-- PACS.008 — Credit Transfer Transaction (flat JSON)
CREATE OR REPLACE MAPPING pacs008_credit_transfer_transaction (
  __key VARCHAR,
  MessageId VARCHAR,
  CreationDateTime TIMESTAMP WITH TIME ZONE,
  EndToEndIdentification VARCHAR,
  TransactionIdentification VARCHAR,
  InterbankSettlementAmount DECIMAL,
  InterbankSettlementCurrency VARCHAR,
  InterbankSettlementDate DATE,
  DebtorName VARCHAR,
  DebtorAccountIdentificationIBAN VARCHAR,
  DebtorAccountIdentificationOtherId VARCHAR,
  DebtorAgentBICFI VARCHAR,
  CreditorName VARCHAR,
  CreditorAccountIdentificationIBAN VARCHAR,
  CreditorAccountIdentificationOtherId VARCHAR,
  CreditorAgentBICFI VARCHAR,
  ServiceLevelCode VARCHAR,
  ChargeBearer VARCHAR
)
TYPE IMap OPTIONS ('keyFormat'='varchar','valueFormat'='json-flat');

-- CAMT.054 — Notification (flat JSON)
CREATE OR REPLACE MAPPING camt054_notification (
  __key VARCHAR,
  GroupHeaderMessageId VARCHAR,
  GroupHeaderCreationDateTime TIMESTAMP WITH TIME ZONE,
  NotificationId VARCHAR,
  NotificationCreationDateTime TIMESTAMP WITH TIME ZONE,
  AccountIdentificationIBAN VARCHAR,
  AccountIdentificationOtherId VARCHAR
)
TYPE IMap OPTIONS ('keyFormat'='varchar','valueFormat'='json-flat');

-- CAMT.054 — Entry (flat JSON)
CREATE OR REPLACE MAPPING camt054_entry (
  __key VARCHAR,
  NotificationId VARCHAR,
  EntryReference VARCHAR,
  Amount DECIMAL,
  Currency VARCHAR,
  CreditDebitIndicator VARCHAR,
  Status VARCHAR,
  EndToEndIdentification VARCHAR,
  DebtorName VARCHAR,
  CreditorName VARCHAR,
  DebtorAgentBICFI VARCHAR,
  CreditorAgentBICFI VARCHAR
)
TYPE IMap OPTIONS ('keyFormat'='varchar','valueFormat'='json-flat');

CREATE OR REPLACE MAPPING fx_rates (
  __key VARCHAR,                       -- e.g. 'USD@2025-09-22'
  Currency VARCHAR,                    -- ISO 4217
  AsOf DATE,                           -- valuation date (use CURRENT_DATE if static)
  RateToGBP DECIMAL                    -- multiply native amount * RateToGBP -> GBP
)
TYPE IMap
OPTIONS (
  'keyFormat'='varchar',
  'valueFormat'='json-flat'
);

-- ========= PAIN.001 =========

-- Equality join by MessageId (group header lookups from payment info)
CREATE INDEX IF NOT EXISTS idx_pain001_grp_msgid
    ON pain001_group_header (MessageId) TYPE HASH; -- equality joins: i.MessageId = g.MessageId

-- Equality join by MessageId (payment info → group header)
CREATE INDEX IF NOT EXISTS idx_pain001_pmtinf_msgid
    ON pain001_payment_information (MessageId) TYPE HASH; -- equality joins: i.MessageId = g.MessageId

-- Equality join by PaymentInformationId (tx → payment info)
CREATE INDEX IF NOT EXISTS idx_pain001_tx_pmtinf
    ON pain001_credit_transfer_transaction (PaymentInformationId) TYPE HASH; -- equality joins on PaymentInformationId

-- Equality joins and reconciliation by E2E (PAIN↔PACS)
CREATE INDEX IF NOT EXISTS idx_pain001_tx_e2e
    ON pain001_credit_transfer_transaction (EndToEndIdentification) TYPE HASH; -- equality joins on EndToEndIdentification

-- Filter/aggregate by currency category (kept HASH for wide client compatibility)
CREATE INDEX IF NOT EXISTS idx_pain001_tx_ccy
    ON pain001_credit_transfer_transaction (InstructedAmountCurrency) TYPE HASH; -- fast equality filters/rollups by currency

-- Range queries, order by, and top-N on amount
CREATE INDEX IF NOT EXISTS idx_pain001_tx_amt
    ON pain001_credit_transfer_transaction (InstructedAmount) TYPE SORTED; -- ranges and ORDER BY amount

-- ========= PACS.008 =========

-- Equality filter/group by MessageId when drilling into a PACS message
CREATE INDEX IF NOT EXISTS idx_pacs008_msgid
    ON pacs008_credit_transfer_transaction (MessageId) TYPE HASH; -- equality on MessageId

-- Primary reconciliation key (PACS↔PAIN, PACS↔CAMT) — equality lookups
CREATE INDEX IF NOT EXISTS idx_pacs008_e2e
    ON pacs008_credit_transfer_transaction (EndToEndIdentification) TYPE HASH; -- equality joins on E2E

-- Date windows, diurnal reporting, ORDER BY date
CREATE INDEX IF NOT EXISTS idx_pacs008_date
    ON pacs008_credit_transfer_transaction (InterbankSettlementDate) TYPE SORTED; -- ranges and ORDER BY date

-- Mixed predicate: E2E equality + date range (optional but very helpful for
-- queries like WHERE E2E=? AND date BETWEEN …; composite must be SORTED)
CREATE INDEX IF NOT EXISTS idx_pacs008_e2e_date
    ON pacs008_credit_transfer_transaction (EndToEndIdentification, InterbankSettlementDate) TYPE SORTED; -- eq on E2E + range on date

-- ========= CAMT.054 =========

-- Equality joins from entries → notification
CREATE INDEX IF NOT EXISTS idx_camt054_ntf_id
    ON camt054_notification (NotificationId) TYPE HASH; -- equality joins on NotificationId

-- Equality joins entries → notification
CREATE INDEX IF NOT EXISTS idx_camt054_entry_ntf
    ON camt054_entry (NotificationId) TYPE HASH; -- equality joins on NotificationId

-- Reconciliation by E2E (PACS↔CAMT)
CREATE INDEX IF NOT EXISTS idx_camt054_entry_e2e
    ON camt054_entry (EndToEndIdentification) TYPE HASH; -- equality joins on EndToEndIdentification

-- ========= FX rates =========
-- Replace two singles (Currency) + (AsOf) with a single composite:
-- equality on both columns and range scans on AsOf within a currency.

-- RECOMMENDED: drop single-column indexes if they exist
-- DROP INDEX IF EXISTS idx_fx_currency;
-- DROP INDEX IF EXISTS idx_fx_asof;

CREATE INDEX IF NOT EXISTS idx_fx_currency_asof
    ON fx_rates (Currency, AsOf) TYPE SORTED; -- eq on Currency + range/equality on AsOf (date-aligned lookups)
