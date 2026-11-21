# AI-generated code (Codex): empty MySQL migration environment for Phase 0.
import os
from pathlib import Path

from alembic import context
from sqlalchemy import create_engine

from papersearch.config import mysql_url, settings

if context.is_offline_mode():
    context.configure(dialect_name="mysql", literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    file = Path(".env")
    config = settings(file) if file.exists() else dict(os.environ)
    with create_engine(mysql_url(config)).connect() as conn:
        context.configure(connection=conn)
        with context.begin_transaction():
            context.run_migrations()
