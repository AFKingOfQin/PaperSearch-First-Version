# Migration revisions

`0001_paper_state` creates the InnoDB tables for metadata, screening decisions, and leased jobs. Use `alembic upgrade head` from the configured CLI container. Downgrading this initial revision drops the tables and their data.
