### **VPN User Activity ETL Pipeline**

This project serves as a foundational framework, rather than a fully developed pipeline.

---

### **Use Case Overview**

Enterprises rely on VPN services to securely connect remote employees. These connections generate VPN session logs stored in Oracle. To ensure data consistency and support security auditing, this ETL pipeline compares Oracle VPN records with their replicas in Apache Phoenix (HBase), detects missing or outdated entries, applies transformations like timestamp normalization and IP masking, and writes the results back to Phoenix. This ensures clean, reliable VPN data for dashboards, compliance, and monitoring tools.

---

**How the Project Works**

1. **Extract** VPN session data from an **Oracle database** via JDBC
   - Fetches user **login/logout times, session IDs, IP addresses, and device types**.
   - Uses JDBC to ensure **direct, efficient querying**.
2. **Compare** it with **Phoenix (HBase) replica tables** to find
   - Detects **missing records** (sessions not yet copied to Phoenix).
   - Detects **updated records** (IP changes, extended sessions, etc.).
3. Perform **Transformations**
   - **Convert timestamps** to a standard format.
   - **Mask IP addresses** for privacy (e.g., partial IP masking).
4. **Load** the cleaned and enriched VPN activity data back into **Phoenix**.
   - Uses **UPSERT** to avoid duplicate entries.
   - Stores in a separate `vpn_sessions_results` table.

![airflow dag overview](https://github.com/user-attachments/assets/d6a8a90c-afe9-4501-9314-6db3d334fb5b)

---

**Key Benefits**

- **Enterprise VPN tracking:** Companies track user VPN access logs for security audits.
- **Data replication & consistency:** Ensures Phoenix tables stay **in sync** with Oracle.
- **Anonymization:** Partial IP masking keeps data **compliant with security policies**.
- **Scalable Approach:** JDBC + PhoenixDB ensure efficient querying across **large-scale logs**.
