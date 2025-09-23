# SQL Demo

The project demonstrates how to feed data into a Hazelcast cluster and plug a dashboard (Apache Superset) to do analytics connecting directly to the cluster.

## Steps

### Load cluster

Run docker compose to create the cluster

```shell
docker compose up
```

This command spins up:

- Two node cluster (`hazelcast1` and `hazelcast2`)
- Management Centre
- Apache superset

It won't spin up the data producer which is configured to run in the `producer` docker compose profile.

### Producer automation and testing

When running the producer application all the mappings are created automatically: `docker compose --profile producer up`.

### Start Management Center

Go to`http://localhost:8080`, load the cluster and navigate to "SQL Browser"

### Visualize data via SQL query

This project uses [Apache Superset](https://superset.apache.org/) to illustrate the integration with a modern Business Intelligence application, directly using SQL as the integration language. 

#### Build Superset with the SQL Alchemy driver for Hazelcast

Superset requires the SQL Alchemy driver for Hazelcast to be installed in the docker image.
To achieve this, run `docker compose build superset`, the build installs the driver in `./sql-alchemy-hz-dialect` in the image, allowing connection to the cluster.

#### Login to Superset

Hazelcast doesn't come with a SQLAlchemy integration with Superset so a basic version is available in `src/main/python` and installed automatically in Superset docker container used in this exercise.

The very first time Superset is started it requires a new admin user. Execute the following steps to create one.

 1. Get a shell in the running Superset container  `docker compose exec superset bash`
 2. Use the Fabric CLI to create (or overwrite) an admin user: `superset fab create-admin` (you can accept or change the defaults)
 3. (Re)initialize any missing metadata: `superset db upgrade`
 4. Exit and restart Superset: `docker-compose restart superset`

Superset is spawned as part of the docker compose. To login use `admin:admin` (or whatever other values you have chosen when creating the admin user)

#### Connect to Hazelcast

To connect to hazelcast, 

1. go to `Settings\Database Connection` 
2. Click on `+ DATABASE`
3. Select `Other` in `Supported Databases` 
4. In the `Basic` tab, set `Display Name` as Hazelcast the following connection string `hazelcast+python://hazelcast1:5701`
5. Hit `Test connection to validate.
6. In the Advanced tab make sure you have the following selected in `SQL Lab`: `Allow DDL and DML`, `Allow this database to be explored`.

Hit finish to save the settings.

#### Checking the schema

When correctly configured, go to `SQL\SQL Lab` and run the following query:

```sql
SELECT table_name FROM information_schema.mappings
WHERE table_schema = 'public'
```

### Integrating with PowerBI

The SQLAlchemy driver comes with an app that exposes an SQL interface over REST. This is used by teh Hazelcast.pq connector to connect powerbi to Hazelcast

## The dataset: ISO 20022 Messages: PAIN, PACS, and CAMT

ISO 20022 defines a family of standardized financial messages.  
Three of the most commonly encountered in **payments** are:

- **PAIN** (`Payments Initiation`)
- **PACS** (`Payments Clearing and Settlement`)
- **CAMT** (`Cash Management`)

### 1. PAIN (Customer-to-Bank Initiation)

- **Domain:** Customer → Bank
- **Example:** `pain.001.001.03` (Customer Credit Transfer Initiation)
- **Purpose:**
    - Used by corporates or retail customers to instruct their bank to execute credit transfers.
    - Can carry multiple payment instructions (one debtor, multiple creditors).
- **Key Elements:**
    - `GrpHdr`: Group header (message ID, creation date/time, number of transactions, control sum).
    - `PmtInf`: Payment instructions (debtor, account, execution date).
    - `CdtTrfTxInf`: Credit transfer transactions (amount, currency, creditor, creditor account, remittance info).

### 2. PACS (Interbank Clearing and Settlement)

- **Domain:** Bank ↔ Bank (interbank/clearing level)
- **Example:** `pacs.008.001.03` (FIToFICustomerCreditTransfer V03)
- **Purpose:**
    - Used by financial institutions to move funds between them in order to settle customer payments.
    - Usually derived from a PAIN message by the debtor’s bank.
- **Key Elements:**
    - `GrpHdr`: Interbank group header (message ID, agents, number of transactions, control sum).
    - `CdtTrfTxInf`: One or more interbank transactions carrying debtor/creditor, agents, amount, references.
    - `IntrBkSttlmAmt`: Interbank settlement amount.
    - `InstgAgt` / `InstdAgt`: Instructing and instructed agents (the sending and receiving banks).

### 3. CAMT (Bank-to-Customer Reporting)

- **Domain:** Bank → Customer (reporting)
- **Example:** `camt.054.001.03` (BankToCustomerDebitCreditNotification V03)
- **Purpose:**
    - Provides notifications and account statements to customers, showing credits/debits that have been booked.
    - Used for reconciliation and end-of-day or intra-day reporting.
- **Key Elements:**
    - `GrpHdr`: Group header (message ID, creation date/time).
    - `Ntfctn` (or `Stmt`): Notification or statement of account.
    - `Acct`: The account being reported (IBAN, BBAN, or other).
    - `Ntry`: Entries representing posted transactions (amount, debit/credit indicator, status, references, remittance info).

### 4. How They Relate in a Payment Flow

1. **Initiation (PAIN.001)**
    - A corporate (debtor) sends a payment initiation to their bank (debtor bank).
    - Example: “Pay 10,000 EUR to supplier’s account.”

2. **Interbank Settlement (PACS.008)**
    - The debtor bank transforms the customer’s request into an interbank payment message.
    - Funds are transferred between the debtor bank and the creditor bank via the clearing/settlement mechanism (e.g., SEPA,
      SWIFT, RTGS).

3. **Notification/Reporting (CAMT.054)**
    - Once booked, the bank notifies its customer (debtor or creditor) that the transaction has been debited/credited.
    - This enables reconciliation and confirmation that the payment has been executed.

### 5. Summary Table

| Aspect          | PAIN (pain.001)               | PACS (pacs.008)                       | CAMT (camt.054)                         |
|-----------------|-------------------------------|---------------------------------------|-----------------------------------------|
| **Domain**      | Customer → Bank               | Bank ↔ Bank                           | Bank → Customer                         |
| **Purpose**     | Initiate payment              | Execute/settle payment                | Report/notify posted entries            |
| **Perspective** | Instruction (what to pay)     | Interbank transfer (moving the money) | Notification (what happened on account) |
| **Key Players** | Debtor, Creditor, Debtor Bank | Debtor Bank, Creditor Bank            | Bank, Account Holder                    |
| **Main Amount** | Instructed Amount             | Interbank Settlement Amount           | Booked Amount (Debit or Credit)         |
| **Typical Msg** | `pain.001.001.03`             | `pacs.008.001.03`                     | `camt.054.001.03`                       |

### 6. Example End-to-End Flow

```text
Customer → Bank → Clearing → Receiving Bank → Customer
   |          |           |            |          |
   |          |           |            |          |
 pain.001  → pacs.008  → clearing   → pacs.008 → camt.054
 (initiate)   (send funds)          (receive funds) (notify)
