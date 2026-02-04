# CDC Pipeline vs. Dual Write: Reliability & Fault Tolerance Analysis

**Date:** February 3, 2026  
**Project:** PostgreSQL to Azure AI Search Pipeline (POC)  
**Architecture:** PostgreSQL (Wal2Json) → Python Consumer → Azure AI Search

## Executive Summary
This document details the fault tolerance experiments conducted on the Change Data Capture (CDC) pipeline. It compares the observed behavior against a traditional "Dual Write" architecture (where the application writes to the Database and Search Index simultaneously).

The tests confirm that the CDC architecture provides superior reliability, automatic recovery, and data integrity guarantees with significantly less application complexity.

---

## Scenario 1: Critical System Crash (Fault Tolerance)
**Test Script:** `tests/test_fault_tolerance.py`

### The Scenario
A user inserts a record into PostgreSQL. The pipeline receives the message and attempts to upload it to Azure, but the process crashes (e.g., OOM kill, power failure, network sever) **before** it can acknowledge the processing to the database.

### Handling Strategy
*   **CDC (Current System):** The pipeline relies on **At-Least-Once Delivery**. It tracks the Log Sequence Number (LSN) but only sends the "Ack" message to Postgres `flush_batch` completes successfully.
*   **Result (Observed):** When the pipeline restarted, PostgreSQL detected the unacknowledged LSN and **replayed** the exact same message. The data was successfully preserved and eventually uploaded.

### CDC vs. Dual Write
| Feature | CDC Architecture | Dual Write Architecture |
| :--- | :--- | :--- |
| **Failure Mode** | Crash during processing. | App writes to DB, crashes before Search write. |
| **Outcome** | **Zero Data Loss.** Automatic retry upon restart. | **Data Inconsistency.** DB has data, Search does not. |
| **Recovery** | Automatic (Postgres WAL logs). | Manual (Requires complex "Outbox Pattern" or manual reconciliation scripts). |

---

## Scenario 2: Transaction Rollbacks (Isolation)
**Test Script:** `tests/test_rollback_safety.py`

### The Scenario
An application starts a SQL transaction, inserts a "Ghost User" record, holds the transaction open for a few seconds, and then executes `ROLLBACK`.

### Handling Strategy
*   **CDC (Current System):** PostgreSQL's Logical Replication stream (`wal2json`) buffers changes in memory and only emits them to the stream **after a successful COMMIT**.
*   **Result (Observed):** The pipeline received **zero events**. Azure Search was never polluted with the uncommitted "Ghost User."

### CDC vs. Dual Write
| Feature | CDC Architecture | Dual Write Architecture |
| :--- | :--- | :--- |
| **Failure Mode** | SQL Rollback occurring after application logic. | App writes to Search, then attempts DB write and fails (rollback). |
| **Outcome** | **Clean Data.** Pipeline never sees the data. | **Dirty Data.** Search has "Ghost Data", DB does not. |
| **Correction** | None needed. | App must catch the error and issue a specific delete to Search (Complex & error-prone). |

---

## Scenario 3: Human Error / Bad Code Deployment
**Test Script:** `tests/test_bad_code_recovery.py`

### The Scenario
A bug is introduced into the pipeline code (e.g., a type mismatch or logic error) that causes the pipeline to crash immediately upon receiving a specific data packet.

### Handling Strategy
*   **CDC (Current System):** The buggy pipeline crashed without sending the LSN feedback. PostgreSQL kept the replication slot active and the message pending.
*   **Result (Observed):** We deployed a "Fixed" version of the pipeline. Upon startup, it immediately picked up the stuck message, processed it correctly, and cleared the backlog.

### CDC vs. Dual Write
| Feature | CDC Architecture | Dual Write Architecture |
| :--- | :--- | :--- |
| **Failure Mode** | Bug in transformation/upload logic. | Bug in search integration code within the main app. |
| **Outcome** | **Pipeline Stalls.** No data loss. User transaction succeeds (DB). | **User Error (500).** The user request fails entirely, or if async, the message is lost in a volatile queue (RAM). |
| **Recovery** | Fix code & Restart. | Replay "Dead Letter Queue" (if implemented) or ask user to retry. |

---

## Scenario 4: Compensating Transactions (Reversion)
**Test Script:** `tests/test_reversion.py`

### The Scenario
Valid but incorrect data is committed to the database (User error). The user then issues a `DELETE` or `UPDATE` to correct the mistake.

### Handling Strategy
*   **CDC (Current System):** The pipeline treats every change as an independent event. It logged the initial `INSERT` (uploading the wrong data) and then processed the `DELETE` (removing it from Azure).
*   **Result (Observed):** Azure Search was eventually consistent with the database state. No manual cleanup was required in Azure.

### CDC vs. Dual Write
| Feature | CDC Architecture | Dual Write Architecture |
| :--- | :--- | :--- |
| **Outcome** | **Consistent.** Azure reflects the final state of DB. | **Consistent**, assuming the app handles both writes successfully. |
| **Pros/Cons** | Decoupled. The pipeline doesn't care *why* data changed. | Tightly coupled. The app must know to update Search for every specific business action. |

---

## Scenario 5: Multi-Table Data Enrichment
**Test Script:** `tests/test_scenarios.py`

### The Scenario
Information for a single search document ("Customer Profile") comes from two different tables: `customers` (Name, Email) and `addresses` (Street, City).

### Handling Strategy
*   **CDC (Current System):** The pipeline listens to both tables independently. It maps them to the same Azure Document ID. Using Azure's `mergeOrUpload` logic, fragments from different tables are combined into a single Golden Record.
*   **Result (Observed):** We could insert customers and addresses in any order, and the final document in Azure contained both sets of data.

### CDC vs. Dual Write
| Feature | CDC Architecture | Dual Write Architecture |
| :--- | :--- | :--- |
| **Complexity** | **Low.** Unaware of other tables. Just maps ID -> Fragment. | **High.** When updating Address, App must read Customer data to build full document OR Use partial updates (complexity). |
| **Microservices** | **Enabler.** Auth Service updates Name; Shipping Service updates Address. | **Blocker.** Services must share Search logic or talk to each other to update the index. |

---

## Conclusion

The tests confirm that the **CDC Pipeline architecture offers robust "correct-by-construction" reliability**.

By shifting the responsibility of synchronization from the Application Layer (Dual Write) to the Infrastructure Layer (Postgres + CDC), we achieve:
1.  **Guaranteed Consistency:** If it's in the DB, it will get to Azure.
2.  **Crash Safety:** No need for complex retry queues in the application.
3.  **Simplicity:** The main application code is purely focused on SQL; search indexing is a background side-effect.
