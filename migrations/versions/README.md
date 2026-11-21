# Migration revisions

`0001_paper_state` creates the InnoDB tables for metadata, screening decisions, and leased jobs. Use `alembic upgrade head` from the configured CLI container. Downgrading this initial revision drops the tables and their data.

`0002_search_snapshots` adds artifact locations, the active fixture recipe and the published snapshot pointer. The initial recipe preserves the previous embedding job identity. Selecting the test encoder backfills current eligible papers into its own job namespace. Existing revisions, decisions and jobs retain their identities.
