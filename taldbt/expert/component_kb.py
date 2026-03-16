"""
Universal Talend Component Knowledge Base.

This is NOT a runtime classifier. This is EXPERT KNOWLEDGE — what a senior
Talend developer already knows about every component before opening any project.

Source: https://www.talendforge.org/components/index.php?version=255&edition=5&showAll=1
        https://help.talend.com/

Every component maps to a migration_role that tells the engine exactly what to do:
  data_source_db    → SELECT from table, {{ source() }} in dbt
  data_source_file  → read_csv/read_json/read_parquet in DuckDB
  data_source_api   → Temporal activity for extraction → staging table
  data_source_saas  → Temporal activity for SaaS API → staging table
  data_sink_db      → Terminal CTE → {{ config(materialized='table') }}
  data_sink_file    → Terminal → Temporal activity for file write
  transformer       → CTE with expression translation (tMap is king)
  filter            → CTE with WHERE clause
  aggregate         → CTE with GROUP BY
  sort              → CTE with ORDER BY
  dedup             → CTE with ROW_NUMBER() OVER (PARTITION BY ...)
  join              → CTE with JOIN (simple, not tMap)
  union             → CTE with UNION ALL
  normalize         → CTE with UNNEST/LATERAL
  denormalize       → CTE with string_agg/GROUP BY
  pivot             → CTE with PIVOT
  unpivot           → CTE with UNPIVOT
  scd               → dbt snapshot
  custom_code       → LLM translation required
  orchestration     → Temporal workflow
  connection_mgmt   → Skip (infrastructure, no data flow)
  transaction       → Skip (commit/rollback — DuckDB auto-commits)
  logging           → Skip or convert to dbt test
  side_effect       → Temporal activity (email, FTP, cloud ops)
  iterator          → Temporal loop or dbt macro
  state             → dbt var or Temporal workflow state
  quality           → dbt test
  chart             → Skip (visualization)
  unknown           → LLM analyzes from XML + component docs
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ComponentKnowledge:
    role: str           # migration_role from above
    dialect: str = ""   # source SQL dialect: mysql, oracle, mssql, postgresql, bigquery, etc.
    purpose: str = ""   # what it does (from Talend docs)
    dbt_target: str = ""  # source, ref, snapshot, activity, skip
    key_params: tuple = ()  # important XML parameters to extract


# ═══════════════════════════════════════════════════════════
# MASTER KNOWLEDGE BASE
# Built from TalendForge component reference + Talend Help docs.
# ═══════════════════════════════════════════════════════════

# Format: component_name_lowercase → ComponentKnowledge
# For DB families, we register every vendor variant.

_KB: dict[str, ComponentKnowledge] = {}


def _register(names: list[str], **kwargs):
    """Register multiple component names with the same knowledge."""
    ck = ComponentKnowledge(**kwargs)
    for n in names:
        _KB[n.lower()] = ck


# ── DATABASE SOURCES (reads data via SQL query) ──────────

# Each DB vendor has: Input, Output, Connection, Close, Row, Commit, Rollback, BulkExec, SCD, SCDELT
_DB_VENDORS = {
    "mysql":        ("mysql",       "mysql"),
    "mssql":        ("mssql",       "tsql"),
    "oracle":       ("oracle",      "oracle"),
    "postgresql":   ("postgresql",  "postgres"),
    "db2":          ("db2",         "db2"),
    "teradata":     ("teradata",    "teradata"),
    "netezza":      ("netezza",     "netezza"),
    "sybase":       ("sybase",      "sybase"),
    "informix":     ("informix",    "informix"),
    "greenplum":    ("greenplum",   "postgres"),   # GP is PG-based
    "vertica":      ("vertica",     "vertica"),
    "sqlite":       ("sqlite",      "sqlite"),
    "derby":        ("derby",       "derby"),
    "as400":        ("as400",       "db2"),        # AS400 uses DB2 SQL
    "access":       ("access",      "access"),
    "firebird":     ("firebird",    "firebird"),
    "hsql":         ("hsql",        "hsql"),
    "ingres":       ("ingres",      "ingres"),
    "interbase":    ("interbase",   "interbase"),
    "postgresplus": ("postgresplus","postgres"),
    "jdbc":         ("jdbc",        "generic"),    # Generic JDBC — dialect unknown
    # Cloud-hosted variants
    "amazonaurora":   ("amazonaurora",   "mysql"),
    "amazonmysql":    ("amazonmysql",    "mysql"),
    "amazonoracle":   ("amazonoracle",   "oracle"),
    "amazonredshift": ("amazonredshift", "redshift"),
    "azuresynapse":   ("azuresynapse",   "tsql"),
    "snowflake":      ("snowflake",      "snowflake"),
}

for vendor, (prefix, dialect) in _DB_VENDORS.items():
    _register(
        [f"t{prefix}input", f"t{vendor}input"],
        role="data_source_db", dialect=dialect,
        purpose=f"Reads from {vendor} database via SQL query",
        dbt_target="source", key_params=("QUERY", "TABLE", "HOST", "PORT", "DBNAME", "SCHEMA"),
    )
    _register(
        [f"t{prefix}output", f"t{vendor}output"],
        role="data_sink_db", dialect=dialect,
        purpose=f"Writes data to {vendor} database table",
        dbt_target="table", key_params=("TABLE", "ACTION_ON_TABLE", "HOST", "PORT", "DBNAME"),
    )
    _register(
        [f"t{prefix}connection", f"t{vendor}connection"],
        role="connection_mgmt", dialect=dialect,
        purpose=f"Opens reusable connection to {vendor}",
        dbt_target="skip",
    )
    _register(
        [f"t{prefix}close", f"t{vendor}close"],
        role="connection_mgmt", dialect=dialect,
        purpose=f"Closes connection to {vendor}",
        dbt_target="skip",
    )
    _register(
        [f"t{prefix}row", f"t{vendor}row"],
        role="data_source_db", dialect=dialect,
        purpose=f"Executes SQL on {vendor} row-by-row (DDL or DML)",
        dbt_target="source", key_params=("QUERY",),
    )
    _register(
        [f"t{prefix}commit", f"t{vendor}commit"],
        role="transaction",
        purpose=f"Commits transaction on {vendor}",
        dbt_target="skip",
    )
    _register(
        [f"t{prefix}rollback", f"t{vendor}rollback"],
        role="transaction",
        purpose=f"Rolls back transaction on {vendor}",
        dbt_target="skip",
    )
    _register(
        [f"t{prefix}bulkexec", f"t{vendor}bulkexec"],
        role="data_sink_db", dialect=dialect,
        purpose=f"Bulk loads data into {vendor}",
        dbt_target="table",
    )
    _register(
        [f"t{prefix}fastload", f"t{vendor}fastload"],
        role="data_sink_db", dialect=dialect,
        purpose=f"Fast/bulk loads data into {vendor} (high-speed variant)",
        dbt_target="table",
    )
    # SCD
    _register(
        [f"t{prefix}scd", f"t{vendor}scd"],
        role="scd", dialect=dialect,
        purpose=f"Slowly Changing Dimension on {vendor} (Type 1/2)",
        dbt_target="snapshot",
    )
    _register(
        [f"t{prefix}scdelt", f"t{vendor}scdelt"],
        role="scd", dialect=dialect,
        purpose=f"SCD via ELT (server-side SQL) on {vendor}",
        dbt_target="snapshot",
    )

# ── BIG DATA SOURCES ─────────────────────────────────────

_register(["tbigqueryinput"], role="data_source_db", dialect="bigquery",
          purpose="Performs queries on Google BigQuery", dbt_target="source",
          key_params=("QUERY", "DATASET", "TABLE"))
_register(["tbigqueryoutput"], role="data_sink_db", dialect="bigquery",
          purpose="Writes data to Google BigQuery", dbt_target="table")
_register(["tbigquerybulkexec"], role="data_sink_db", dialect="bigquery",
          purpose="Bulk loads data to BigQuery", dbt_target="table")
_register(["tbigquerysqlrow"], role="data_source_db", dialect="bigquery",
          purpose="Executes BigQuery SQL row-by-row", dbt_target="source")
_register(["tbigqueryoutputbulk"], role="data_sink_file",
          purpose="Creates CSV/TXT for BigQuery bulk load", dbt_target="activity")

_register(["thiverow"], role="data_source_db", dialect="hive",
          purpose="Executes Hive SQL", dbt_target="source")
_register(["thiveconnection", "thiveclose"], role="connection_mgmt", dbt_target="skip")

_register(["tneo4jv4input"], role="data_source_db", dialect="cypher",
          purpose="Reads from Neo4j via Cypher query", dbt_target="activity")
_register(["tneo4jv4output"], role="data_sink_db", dialect="cypher",
          purpose="Writes to Neo4j", dbt_target="activity")
_register(["tneo4jv4row"], role="data_source_db", dialect="cypher",
          purpose="Executes Cypher query", dbt_target="activity")
_register(["tneo4jv4connection", "tneo4jv4close"], role="connection_mgmt", dbt_target="skip")


# ── SAAS / BUSINESS SOURCES ──────────────────────────────

_SAAS_SOURCES = {
    "tsalesforceinput":     ("Retrieves data from Salesforce object via SOQL", "salesforce"),
    "tsalesforceoutput":    ("Writes data to Salesforce object", "salesforce"),
    "tsalesforceconnection":("Opens Salesforce connection", "salesforce"),
    "tsalesforcebulkexec":  ("Bulk loads into Salesforce", "salesforce"),
    "tsalesforceoutputbulk":("Generates file for Salesforce bulk load", "salesforce"),
    "tsalesforceoutputbulkexec":("Bulk loads into Salesforce from file", "salesforce"),
    "tsalesforcegetdeleted":("Gets deleted Salesforce records", "salesforce"),
    "tsalesforcegetupdated":("Gets updated Salesforce records", "salesforce"),
    "tsalesforcegetservertimestamp":("Gets Salesforce server timestamp", "salesforce"),
    "tsalesforceeinsteinbulkexec":("Loads into Salesforce Analytics Cloud", "salesforce"),
    "tsalesforceeinsteinoutputbulkexec":("Loads into Salesforce Analytics Cloud from file", "salesforce"),
    "tmarketoinput":        ("Retrieves lead/activity data from Marketo", "marketo"),
    "tmarketooutput":       ("Writes lead data to Marketo", "marketo"),
    "tmarketoconnection":   ("Opens Marketo connection", "marketo"),
    "tmarketocampaign":     ("Retrieves Marketo campaign data", "marketo"),
    "tmarketobulkexec":     ("Bulk imports into Marketo", "marketo"),
    "tmarketolistoperation":("Manages Marketo lists", "marketo"),
    "tnetsuiteinput":       ("Retrieves data from NetSuite via SOAP", "netsuite"),
    "tnetsuiteoutput":      ("Writes data to NetSuite", "netsuite"),
    "tnetsuiteconnection":  ("Opens NetSuite connection", "netsuite"),
    "tnetsuitev2019input":  ("Retrieves data from NetSuite v2019", "netsuite"),
    "tnetsuitev2019output": ("Writes data to NetSuite v2019", "netsuite"),
    "tnetsuitev2019connection":("Opens NetSuite v2019 connection", "netsuite"),
    "tservicenowinput":     ("Reads data from ServiceNow", "servicenow"),
    "tservicenowoutput":    ("Writes data to ServiceNow", "servicenow"),
    "tservicenowconnection":("Opens ServiceNow connection", "servicenow"),
    "tworkdayinput":        ("Retrieves data from Workday", "workday"),
    "tmicrosoftcrminput":   ("Reads from Microsoft CRM", "mscrm"),
    "tmicrosoftcrmoutput":  ("Writes to Microsoft CRM", "mscrm"),
    "tjirainput":           ("Retrieves JIRA issues via JQL", "jira"),
    "tjiraoutput":          ("Creates/updates JIRA issues", "jira"),
    "tldapinput":           ("Queries LDAP directory", "ldap"),
    "tldapoutput":          ("Writes to LDAP directory", "ldap"),
    "tldapconnection":      ("Opens LDAP connection", "ldap"),
    "tldapclose":           ("Closes LDAP connection", "ldap"),
    "tldapattributesinput": ("Lists LDAP object attributes", "ldap"),
    "tldaprenameentry":     ("Renames LDAP entries", "ldap"),
    "tbonitadeploy":        ("Deploys Bonita process", "bonita"),
    "tbonitainstantiateprocess":("Starts Bonita process instance", "bonita"),
    "tsplunkeventcollector":("Sends events to Splunk HEC", "splunk"),
}

for comp, (desc, platform) in _SAAS_SOURCES.items():
    if "connection" in comp or "close" in comp:
        _register([comp], role="connection_mgmt", purpose=desc, dbt_target="skip")
    elif "output" in comp or "exec" in comp or "deploy" in comp or "operation" in comp:
        _register([comp], role="data_sink_saas", purpose=desc, dbt_target="activity",
                  key_params=("MODULE", "OBJECT", "ACTION"))
    else:
        _register([comp], role="data_source_saas", purpose=desc, dbt_target="activity",
                  key_params=("QUERY", "MODULE", "OBJECT"))


# ── SAP (special handling — deep integration) ────────────

_SAP = ["tsapbapi", "tsapcommit", "tsapconnection", "tsaprollback",
        "tsaptableinput", "tsapdatasourceoutput", "tsapdatasourcereceiver",
        "tsapadsoinput", "tsapdsoinput", "tsapdsooutput",
        "tsapidocinput", "tsapidocoutput",
        "tsapinfocubeinput", "tsapinfoobjectinput", "tsapinfoobjectoutput",
        "tsapodpinput"]

for comp in _SAP:
    if "connection" in comp or "commit" in comp or "rollback" in comp:
        _register([comp], role="connection_mgmt", purpose="SAP infrastructure", dbt_target="skip")
    elif "output" in comp:
        _register([comp], role="data_sink_saas", purpose="Writes to SAP", dbt_target="activity")
    else:
        _register([comp], role="data_source_saas", purpose="Reads from SAP", dbt_target="activity",
                  key_params=("TABLE_NAME", "QUERY", "FUNCTION"))


# ── CLOUD STORAGE ────────────────────────────────────────

_CLOUD_OPS = {
    # Google Cloud Storage
    "tgsbucketcreate": "Creates GCS bucket",
    "tgsbucketdelete": "Deletes GCS bucket",
    "tgsbucketexist":  "Checks GCS bucket existence",
    "tgsbucketlist":   "Lists GCS buckets",
    "tgsclose":        "Closes GCS connection",
    "tgsconnection":   "Opens GCS connection",
    "tgscopy":         "Copies GCS objects",
    "tgsdelete":       "Deletes GCS objects",
    "tgsget":          "Downloads from GCS",
    "tgslist":         "Lists GCS objects",
    "tgsput":          "Uploads to GCS",
    # Azure Storage
    "tazurestorageconnection":      "Opens Azure Storage connection",
    "tazurestoragecontainercreate": "Creates Azure container",
    "tazurestoragecontainerdelete": "Deletes Azure container",
    "tazurestoragecontainerexist":  "Checks Azure container",
    "tazurestoragecontainerlist":   "Lists Azure containers",
    "tazurestoragedelete":          "Deletes Azure blobs",
    "tazurestorageget":             "Downloads Azure blobs",
    "tazurestoragelist":            "Lists Azure blobs",
    "tazurestorageput":             "Uploads to Azure Storage",
    "tazurestoragequeuecreate":     "Creates Azure queue",
    "tazurestoragequeuedelete":     "Deletes Azure queue",
    "tazurestoragequeueinput":      "Reads Azure queue messages",
    "tazurestoragequeueinputloop":  "Polls Azure queue",
    "tazurestoragequeuelist":       "Lists Azure queues",
    "tazurestoragequeueoutput":     "Writes to Azure queue",
    "tazurestoragequeuepurge":      "Purges Azure queue",
    # Azure Data Lake
    "tazureadlsgen2input":  "Reads from Azure Data Lake Gen2",
    "tazureadlsgen2output": "Writes to Azure Data Lake Gen2",
    # Amazon EMR
    "tamazonemrlistinstances": "Lists EMR instances",
    "tamazonemrmanage":        "Manages EMR clusters",
    "tamazonemrresize":        "Resizes EMR cluster",
    "tamazonredshiftmanage":   "Manages Redshift clusters",
    # Box
    "tboxconnection": "Opens Box connection",
    "tboxcopy":       "Copies Box files",
    "tboxdelete":     "Deletes Box files",
    "tboxget":        "Downloads from Box",
    "tboxlist":       "Lists Box files",
    "tboxput":        "Uploads to Box",
}

for comp, desc in _CLOUD_OPS.items():
    if "connection" in comp or "close" in comp:
        _register([comp], role="connection_mgmt", purpose=desc, dbt_target="skip")
    elif "input" in comp or "get" in comp or "list" in comp:
        _register([comp], role="data_source_api", purpose=desc, dbt_target="activity")
    else:
        _register([comp], role="side_effect", purpose=desc, dbt_target="activity")


# ── FILE COMPONENTS ──────────────────────────────────────

_register(["tfileinputdelimited", "tfileinputcsv"],
          role="data_source_file", purpose="Reads delimited file (CSV/TSV)",
          dbt_target="source", key_params=("FILENAME", "CSVROWSEPARATOR", "FIELDSEPARATOR", "HEADER"))

_register(["tfileoutputdelimited", "tfileoutputcsv"],
          role="data_sink_file", purpose="Writes delimited file",
          dbt_target="activity", key_params=("FILENAME",))

_register(["tfileinputexcel"],
          role="data_source_file", purpose="Reads Excel file (.xls, .xlsx)",
          dbt_target="source", key_params=("FILENAME", "SHEETNAME", "HEADER"))

_register(["tfileoutputexcel"],
          role="data_sink_file", purpose="Writes Excel file", dbt_target="activity")

_register(["tfileinputjson"],
          role="data_source_file", purpose="Reads JSON file",
          dbt_target="source", key_params=("FILENAME",))

_register(["tfileoutputjson"],
          role="data_sink_file", purpose="Writes JSON file", dbt_target="activity")

_register(["tfileinputxml"],
          role="data_source_file", purpose="Reads XML file with XPath",
          dbt_target="source", key_params=("FILENAME", "XPATH_QUERY"))

_register(["tfileoutputxml"],
          role="data_sink_file", purpose="Writes XML file", dbt_target="activity")

_register(["tfileinputparquet"],
          role="data_source_file", purpose="Reads Parquet file",
          dbt_target="source", key_params=("FILENAME",))

_register(["tfileoutputparquet"],
          role="data_sink_file", purpose="Writes Parquet file", dbt_target="activity")

_register(["tfileinputpositional"],
          role="data_source_file", purpose="Reads fixed-width positional file",
          dbt_target="source", key_params=("FILENAME",))

_register(["tfileoutputpositional"],
          role="data_sink_file", purpose="Writes fixed-width file", dbt_target="activity")

_register(["tfileinputldif"],
          role="data_source_file", purpose="Reads LDIF file",
          dbt_target="source", key_params=("FILENAME",))

_register(["tfileinputfullrow", "tfileoutputfullrow"],
          role="data_source_file", purpose="Reads/writes full row text file",
          dbt_target="source")

_register(["tfilelist"], role="iterator", purpose="Iterates over files in directory",
          dbt_target="activity", key_params=("DIRECTORY",))
_register(["tfilecopy"], role="side_effect", purpose="Copies files", dbt_target="activity")
_register(["tfiledelete"], role="side_effect", purpose="Deletes files", dbt_target="activity")
_register(["tfileexist"], role="side_effect", purpose="Checks file existence", dbt_target="activity")
_register(["tfilerename"], role="side_effect", purpose="Renames files", dbt_target="activity")
_register(["tfileproperties"], role="side_effect", purpose="Gets file properties", dbt_target="activity")
_register(["tfileunarchive"], role="side_effect", purpose="Extracts archives", dbt_target="activity")
_register(["tfilearchive"], role="side_effect", purpose="Creates archives", dbt_target="activity")
_register(["tfiletouch"], role="side_effect", purpose="Creates empty file (touch)", dbt_target="activity")

_register(["treplicate"], role="union",
          purpose="Duplicates input flow to multiple outputs (fan-out). Each output gets identical data.",
          dbt_target="ref")

_register(["tinfiniteloop"], role="orchestration",
          purpose="Runs an infinite loop (polling pattern). Usually paired with tWaitForFile or tSleep.",
          dbt_target="workflow")


# ── DISCOVERED FROM 517 REAL JOBS ────────────────────────
# Components found in production repos that weren't in initial KB

_register(["trowgenerator"], role="data_source_db",
          purpose="Generates rows from inline data (test data, fixed values, sequences)",
          dbt_target="source")
_register(["tfixedflowinput"], role="data_source_db",
          purpose="Provides fixed/inline data rows as input (like VALUES clause)",
          dbt_target="source")
_register(["thashoutput"], role="data_sink_db",
          purpose="Writes rows to in-memory hash table for cross-subjob lookup",
          dbt_target="ref")
_register(["thashinput"], role="data_source_db",
          purpose="Reads from in-memory hash table (written by tHashOutput in earlier subjob)",
          dbt_target="ref")
_register(["tconverttype"], role="transformer",
          purpose="Converts data types between columns (cast/coerce)",
          dbt_target="ref")
_register(["treplace"], role="transformer",
          purpose="Replaces values in columns based on search/replace rules",
          dbt_target="ref")
_register(["tmsgbox"], role="logging",
          purpose="Shows message box dialog (debug/dev only)",
          dbt_target="skip")
_register(["tcontextdump"], role="logging",
          purpose="Dumps all context variables to console/file for debugging",
          dbt_target="skip")
_register(["tcheckpoint", "tcheckpointstart", "tcheckpointend"], role="orchestration",
          purpose="Job checkpoint/restart marker for recovery",
          dbt_target="skip")
_register(["tsleep"], role="orchestration",
          purpose="Pauses execution for specified duration",
          dbt_target="workflow")
_register(["tcreatetable"], role="data_sink_db",
          purpose="Creates a database table (DDL). In dbt: handled by materialization.",
          dbt_target="skip")
_register(["txmlmap"], role="transformer",
          purpose="XML-specific tMap variant for XML document transformation",
          dbt_target="ref")
_register(["textractpositionalfields"], role="transformer",
          purpose="Extracts fields from fixed-width positional data",
          dbt_target="ref")
_register(["tfilefetch"], role="side_effect", purpose="Fetches file from URL/path", dbt_target="activity")
_register(["cmqconnectionfactory"], role="connection_mgmt", purpose="IBM MQ connection factory", dbt_target="skip")
_register(["tflowmetercatcher"], role="logging", purpose="Catches flow meter statistics", dbt_target="skip")
_register(["tfuzzymatch"], role="transformer", purpose="Fuzzy/approximate string matching", dbt_target="ref")
_register(["teltmysqlinput"], role="data_source_db", dialect="mysql",
          purpose="ELT MySQL input (server-side query)", dbt_target="source")
_register(["tfileinputmail"], role="data_source_api", purpose="Reads email attachments as input", dbt_target="activity")
_register(["taddlocationfromip"], role="transformer", purpose="Geo-locates IP addresses", dbt_target="ref")
_register(["tserveralive"], role="side_effect", purpose="Checks if server/port is reachable", dbt_target="activity")
_register(["tadvancedfileoutputxml"], role="data_sink_file", purpose="Writes advanced XML output", dbt_target="activity")

# DI_CNTL_Job_Tracking_Stats is a CUSTOM component from TalendFramework
# It appears 122x in framework-based projects. Skip it (audit/tracking).
_register(["di_cntl_job_tracking_stats"], role="logging",
          purpose="Custom framework component for job execution tracking/audit",
          dbt_target="skip")


# ── INTERNET / API ───────────────────────────────────────

_register(["trestclient"], role="data_source_api",
          purpose="Calls REST API and reads response",
          dbt_target="activity", key_params=("URL", "HTTP_METHOD", "BODY"))
_register(["thttprequest"], role="data_source_api",
          purpose="Makes raw HTTP request", dbt_target="activity",
          key_params=("URI",))
_register(["tsoap"], role="data_source_api",
          purpose="Calls SOAP web service", dbt_target="activity",
          key_params=("ENDPOINT", "OPERATION"))
_register(["twebserviceinput"], role="data_source_api",
          purpose="Reads from web service", dbt_target="activity")
_register(["twebserviceoutput"], role="side_effect",
          purpose="Writes to web service", dbt_target="activity")

# FTP/SFTP
for proto in ["tftp", "tsftp"]:
    _register([f"{proto}get"], role="data_source_api",
              purpose=f"Downloads file via {'FTP' if 'ftp' in proto else 'SFTP'}",
              dbt_target="activity", key_params=("HOST", "REMOTEDIR", "FILENAME"))
    _register([f"{proto}put"], role="side_effect",
              purpose=f"Uploads file via {'FTP' if 'ftp' in proto else 'SFTP'}",
              dbt_target="activity")
    _register([f"{proto}connection"], role="connection_mgmt", dbt_target="skip")
    _register([f"{proto}close"], role="connection_mgmt", dbt_target="skip")
    _register([f"{proto}delete"], role="side_effect", dbt_target="activity")
    _register([f"{proto}rename"], role="side_effect", dbt_target="activity")
    _register([f"{proto}list"], role="data_source_api", dbt_target="activity")
    _register([f"{proto}exist"], role="side_effect", dbt_target="activity")
    _register([f"{proto}filelist"], role="iterator", dbt_target="activity")

# Email
_register(["tsendmail", "tsmtp"], role="side_effect",
          purpose="Sends email", dbt_target="activity")


# ── PROCESSING / TRANSFORMATION ──────────────────────────

_register(["tmap"], role="transformer",
          purpose="Core transformation: joins, lookups, expressions, filters. 80% of business logic.",
          dbt_target="ref", key_params=("UNIQUE_NAME",))

_register(["teltmap"], role="transformer", dialect="pushdown",
          purpose="Server-side ELT transformation (pushes SQL to database, not Java processing)",
          dbt_target="ref")
_register(["teltoutput"], role="data_sink_db", dialect="pushdown",
          purpose="ELT output — writes results of server-side processing",
          dbt_target="table")

_register(["tfilterrow"], role="filter",
          purpose="Filters rows based on conditions",
          dbt_target="ref", key_params=("CONDITIONS",))
_register(["tfiltercolumns"], role="filter",
          purpose="Selects/reorders columns (projection)",
          dbt_target="ref")
_register(["tsamplerow"], role="filter",
          purpose="Samples N rows or every Nth row",
          dbt_target="ref", key_params=("RANGE",))

_register(["taggregaterow", "taggregatesortedrow"], role="aggregate",
          purpose="Aggregates data with GROUP BY and functions (SUM, COUNT, MIN, MAX, AVG, etc.)",
          dbt_target="ref")

_register(["tsortrow"], role="sort",
          purpose="Sorts rows by specified columns",
          dbt_target="ref")

_register(["tuniqrow"], role="dedup",
          purpose="Removes duplicate rows based on key columns",
          dbt_target="ref")
_register(["tfirstrow"], role="dedup",
          purpose="Keeps first N rows per group",
          dbt_target="ref")
_register(["treplacerow"], role="dedup",
          purpose="Replaces rows matching conditions",
          dbt_target="ref")

_register(["tjoin"], role="join",
          purpose="Simple join (like VLOOKUP) between two flows",
          dbt_target="ref")
_register(["tunite"], role="union",
          purpose="UNION ALL of multiple input flows",
          dbt_target="ref")
_register(["tinterceptrow"], role="union",
          purpose="INTERSECT of two flows", dbt_target="ref")

_register(["tnormalize"], role="normalize",
          purpose="Splits multi-value fields into separate rows (UNNEST)",
          dbt_target="ref")
_register(["tdenormalize"], role="denormalize",
          purpose="Combines rows into delimited string (string_agg)",
          dbt_target="ref")
_register(["tpivottocolumnsdelimited"], role="pivot",
          purpose="Pivots rows to columns", dbt_target="ref")
_register(["tunpivot"], role="unpivot",
          purpose="Unpivots columns to rows", dbt_target="ref")

_register(["textractregexfields"], role="transformer",
          purpose="Extracts fields from string using regex",
          dbt_target="ref")
_register(["textractxmlfield"], role="transformer",
          purpose="Extracts values from XML column",
          dbt_target="ref")
_register(["textractdelimitedfields"], role="transformer",
          purpose="Splits delimited string into columns",
          dbt_target="ref")

# Custom code
_register(["tjavarow"], role="custom_code",
          purpose="Custom Java per-row transformation. output_row.X = f(input_row.Y)",
          dbt_target="ref", key_params=("CODE",))
_register(["tjava"], role="custom_code",
          purpose="Free-form Java code block (no row structure)",
          dbt_target="ref", key_params=("CODE",))
_register(["tjavaflex"], role="custom_code",
          purpose="Java with begin/main/end blocks",
          dbt_target="ref", key_params=("CODE_START", "CODE_MAIN", "CODE_END"))
_register(["tgroovy", "tgroovyrow"], role="custom_code",
          purpose="Groovy scripting", dbt_target="ref")


# ── ORCHESTRATION ────────────────────────────────────────

_register(["trunjob"], role="orchestration",
          purpose="Executes a child Talend job",
          dbt_target="workflow", key_params=("PROCESS", "CONTEXT"))
_register(["tparallelize"], role="orchestration",
          purpose="Runs connected components in parallel (fan-out, wait-all or wait-first)",
          dbt_target="workflow", key_params=("ALL",))
_register(["tloop"], role="orchestration",
          purpose="Iterates: for-loop (count), while-loop (condition), or file-list",
          dbt_target="workflow", key_params=("ITERATE_TYPE", "NB_ITERATE"))
_register(["tflowtoiterate"], role="state",
          purpose="Converts data flow rows to globalMap variables for iteration",
          dbt_target="workflow")
_register(["titeratetoflow"], role="state",
          purpose="Converts iteration variables back to data flow rows",
          dbt_target="workflow")
_register(["trunif"], role="orchestration",
          purpose="Conditional execution based on expression evaluation",
          dbt_target="workflow", key_params=("CONDITION",))
_register(["twaitforfile"], role="orchestration",
          purpose="Polls for file existence before proceeding",
          dbt_target="workflow", key_params=("FILENAME", "MAXIMUM_WAITING_TIME"))
_register(["tprejob"], role="orchestration",
          purpose="Runs before the main job (initialization)", dbt_target="workflow")
_register(["tpostjob"], role="orchestration",
          purpose="Runs after the main job (cleanup)", dbt_target="workflow")
_register(["tcontextload"], role="state",
          purpose="Loads context variables from file/database at runtime",
          dbt_target="workflow")
_register(["tsetglobalvar"], role="state",
          purpose="Sets globalMap variables for downstream components",
          dbt_target="workflow")
_register(["tbufferinput", "tbufferoutput"], role="state",
          purpose="In-memory buffer for data sharing between subjobs",
          dbt_target="workflow")


# ── LOGGING / MONITORING ─────────────────────────────────

_register(["tlogrow"], role="logging",
          purpose="Prints row data to console log", dbt_target="skip")
_register(["tlogcatcher"], role="logging",
          purpose="Catches and logs job errors", dbt_target="skip")
_register(["tstatcatcher"], role="logging",
          purpose="Captures job execution statistics", dbt_target="skip")
_register(["tflowmeter", "tflowmeterrow"], role="logging",
          purpose="Counts rows flowing through", dbt_target="skip")
_register(["twarn"], role="logging",
          purpose="Generates warning message", dbt_target="skip")
_register(["tdie"], role="logging",
          purpose="Terminates job with error", dbt_target="skip")
_register(["tassert"], role="logging",
          purpose="Validates assertion condition", dbt_target="skip")
_register(["tassertcatcher"], role="logging",
          purpose="Catches assertion failures", dbt_target="skip")
_register(["tjobinstanceend", "tjobinstancestart"], role="logging",
          purpose="Job instance lifecycle", dbt_target="skip")


# ── QUALITY ──────────────────────────────────────────────

_register(["tpatterncheck"], role="quality",
          purpose="Validates data against quality patterns",
          dbt_target="test")
_register(["tdatamasking"], role="transformer",
          purpose="Masks/anonymizes sensitive data",
          dbt_target="ref")


# ── MESSAGING ────────────────────────────────────────────

_register(["tkafkainput"], role="data_source_api",
          purpose="Reads messages from Kafka topic", dbt_target="activity",
          key_params=("BROKER_LIST", "TOPIC"))
_register(["tkafkaoutput"], role="side_effect",
          purpose="Writes messages to Kafka topic", dbt_target="activity")
_register(["tjmsinput", "tactivemqinput", "trabbitmqinput"], role="data_source_api",
          purpose="Reads from message queue", dbt_target="activity")
_register(["tjmsoutput", "tactivemqoutput", "trabbitmqoutput"], role="side_effect",
          purpose="Writes to message queue", dbt_target="activity")


# ── CHART / VISUALIZATION ────────────────────────────────

_register(["tbarchart", "tlinechart", "tpiechart"], role="chart",
          purpose="Generates chart image", dbt_target="skip")
_register(["tjasperoutput", "tjasperoutputexec"], role="side_effect",
          purpose="Generates Jasper report", dbt_target="activity")


# ═══════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════

def lookup(component_name: str) -> ComponentKnowledge:
    """Look up a component's knowledge. Falls back to suffix-based inference."""
    name = component_name.lower().strip()

    # Exact match
    if name in _KB:
        return _KB[name]

    # Suffix-based inference (handles new/unknown components generically)
    if name.endswith("input"):
        return ComponentKnowledge(role="data_source_db", purpose=f"Unknown input: {component_name}",
                                  dbt_target="source")
    if name.endswith("output"):
        return ComponentKnowledge(role="data_sink_db", purpose=f"Unknown output: {component_name}",
                                  dbt_target="table")
    if name.endswith("connection") or name.endswith("close"):
        return ComponentKnowledge(role="connection_mgmt", purpose=f"Connection: {component_name}",
                                  dbt_target="skip")
    if name.endswith("commit") or name.endswith("rollback"):
        return ComponentKnowledge(role="transaction", dbt_target="skip")
    if name.endswith("row"):
        return ComponentKnowledge(role="data_source_db", purpose=f"Row-level SQL: {component_name}",
                                  dbt_target="source")
    if name.endswith("scd") or name.endswith("scdelt"):
        return ComponentKnowledge(role="scd", dbt_target="snapshot")
    if name.endswith("bulkexec"):
        return ComponentKnowledge(role="data_sink_db", dbt_target="table")
    if name.endswith("list") or name.endswith("exist"):
        return ComponentKnowledge(role="side_effect", dbt_target="activity")

    # Truly unknown — needs LLM analysis
    return ComponentKnowledge(role="unknown", purpose=f"Unknown component: {component_name}",
                              dbt_target="activity")


def get_source_dialect(component_name: str) -> str:
    """Get the SQL dialect for a database component. Empty string if not a DB component."""
    ck = lookup(component_name)
    return ck.dialect


def get_migration_role(component_name: str) -> str:
    """Get the migration role for a component."""
    return lookup(component_name).role


def get_dbt_target(component_name: str) -> str:
    """Get what this component becomes in dbt (source, ref, snapshot, activity, skip)."""
    return lookup(component_name).dbt_target


def is_data_component(component_name: str) -> bool:
    """True if this component produces or consumes data in the flow."""
    role = get_migration_role(component_name)
    return role not in ("connection_mgmt", "transaction", "logging", "side_effect",
                         "chart", "orchestration", "state")


def should_skip(component_name: str) -> bool:
    """True if this component should NOT produce a CTE or model."""
    return get_dbt_target(component_name) == "skip"


def needs_temporal(component_name: str) -> bool:
    """True if this component must become a Temporal activity (API, SaaS, messaging, etc.)."""
    return get_dbt_target(component_name) == "activity"


# Stats
def kb_stats() -> dict:
    """Return stats about the knowledge base coverage."""
    roles = {}
    for ck in _KB.values():
        roles[ck.role] = roles.get(ck.role, 0) + 1
    return {
        "total_components": len(_KB),
        "unique_roles": len(set(ck.role for ck in _KB.values())),
        "by_role": roles,
    }
