# AI-generated code (Codex): fixture derivation settings, immutable cache locations and publication pointer.
from alembic import op

revision = "0002_search_snapshots"
down_revision = "0001_paper_state"
branch_labels = None
depends_on = None


def upgrade():
    tables = {
        "derived_artifacts": """
            cache_key CHAR(64) PRIMARY KEY, text_hash CHAR(64) NOT NULL,
            kind VARCHAR(32) NOT NULL, model_id VARCHAR(160) NOT NULL,
            model_revision VARCHAR(64) NOT NULL, preprocessing VARCHAR(32) NOT NULL,
            shard_sha CHAR(64) NOT NULL, slot INT UNSIGNED NOT NULL, vector_sha CHAR(64) NOT NULL,
            UNIQUE(text_hash,kind,model_id,model_revision,preprocessing)
        """,
        "published_snapshots": """
            snapshot_id CHAR(32) PRIMARY KEY, manifest_sha CHAR(64) NOT NULL,
            generation BIGINT UNSIGNED NOT NULL, artifact_epoch BIGINT UNSIGNED NOT NULL
        """,
        "derivation_state": """
            singleton TINYINT PRIMARY KEY CHECK(singleton=1),
            kind VARCHAR(32) NOT NULL, model_id VARCHAR(160) NOT NULL,
            model_revision VARCHAR(64) NOT NULL, preprocessing VARCHAR(32) NOT NULL,
            recipe_epoch BIGINT UNSIGNED NOT NULL DEFAULT 0, artifact_epoch BIGINT UNSIGNED NOT NULL DEFAULT 0,
            store_id CHAR(32), active_snapshot CHAR(32),
            FOREIGN KEY(active_snapshot) REFERENCES published_snapshots(snapshot_id)
        """,
    }
    for name, definition in tables.items():
        op.execute(
            f"CREATE TABLE IF NOT EXISTS {name} ({definition}) ENGINE=InnoDB "
            "DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_0900_bin"
        )
    op.execute("""INSERT INTO derivation_state(singleton,kind,model_id,model_revision,preprocessing)
        VALUES (1,'embedding','BAAI/bge-small-en-v1.5','5c38ec7c405ec4b44b94cc5a9bb96e735b38267a','text-v1')
        ON DUPLICATE KEY UPDATE singleton=1""")


def downgrade():
    for name in ("derivation_state", "published_snapshots", "derived_artifacts"):
        op.drop_table(name)
