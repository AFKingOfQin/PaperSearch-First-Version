# AI-generated code (Codex): initial InnoDB schema for durable metadata and screening.
from alembic import op

revision = "0001_paper_state"
down_revision = None
branch_labels = None
depends_on = None

# Ordered by foreign-key dependency. A current pointer can disappear on conflict;
# the stable paper row and all reviewed revisions remain.
TABLES = {
    "papers": """
        paper_id VARCHAR(128) PRIMARY KEY
    """,
    "paper_revisions": """
        event_id CHAR(64) PRIMARY KEY,
        paper_id VARCHAR(128) NOT NULL,
        version_rank INT UNSIGNED NOT NULL,
        source_updated_at CHAR(27) NOT NULL,
        text_hash CHAR(64) NOT NULL,
        record_json JSON NOT NULL,
        batch_id VARCHAR(256) NOT NULL,
        collected_at CHAR(27) NOT NULL,
        raw_location TEXT NOT NULL,
        raw_sha256 CHAR(64) NOT NULL,
        UNIQUE (paper_id, event_id),
        INDEX revision_priority (paper_id, version_rank, source_updated_at),
        FOREIGN KEY (paper_id) REFERENCES papers(paper_id)
    """,
    "current_revisions": """
        paper_id VARCHAR(128) PRIMARY KEY,
        event_id CHAR(64) NOT NULL,
        FOREIGN KEY (paper_id, event_id) REFERENCES paper_revisions(paper_id, event_id)
    """,
    "processed_events": """
        event_id CHAR(64) PRIMARY KEY,
        outcome ENUM('applied','stale','conflict') NOT NULL,
        FOREIGN KEY (event_id) REFERENCES paper_revisions(event_id)
    """,
    "rejected_events": """
        raw_sha256 CHAR(64) NOT NULL,
        input_format VARCHAR(16) NOT NULL,
        reason VARCHAR(64) NOT NULL,
        raw_location TEXT NOT NULL,
        PRIMARY KEY (raw_sha256, input_format)
    """,
    "system_state": """
        singleton TINYINT PRIMARY KEY CHECK (singleton = 1),
        generation BIGINT UNSIGNED NOT NULL
    """,
    "review_projects": """
        project_id CHAR(36) PRIMARY KEY,
        topic TEXT NOT NULL,
        criteria TEXT NOT NULL
    """,
    "screening_history": """
        history_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        project_id CHAR(36) NOT NULL,
        paper_id VARCHAR(128) NOT NULL,
        request_id CHAR(36) NOT NULL,
        reviewed_event_id CHAR(64) NOT NULL,
        label ENUM('relevant','irrelevant','uncertain') NOT NULL,
        note TEXT NOT NULL,
        saved_at DATETIME(6) NOT NULL,
        UNIQUE (project_id, paper_id, request_id),
        UNIQUE (project_id, paper_id, history_id),
        FOREIGN KEY (project_id) REFERENCES review_projects(project_id),
        FOREIGN KEY (paper_id, reviewed_event_id) REFERENCES paper_revisions(paper_id, event_id)
    """,
    "screening_decisions": """
        project_id CHAR(36) NOT NULL,
        paper_id VARCHAR(128) NOT NULL,
        history_id BIGINT UNSIGNED NOT NULL,
        PRIMARY KEY (project_id, paper_id),
        FOREIGN KEY (project_id, paper_id, history_id)
            REFERENCES screening_history(project_id, paper_id, history_id)
    """,
    "derived_jobs": """
        job_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        paper_id VARCHAR(128) NOT NULL,
        text_hash CHAR(64) NOT NULL,
        kind VARCHAR(32) NOT NULL,
        model_id VARCHAR(160) NOT NULL,
        model_revision VARCHAR(64) NOT NULL,
        preprocessing VARCHAR(32) NOT NULL,
        status ENUM('pending','leased','done','failed') NOT NULL DEFAULT 'pending',
        attempts INT UNSIGNED NOT NULL DEFAULT 0,
        lease_token CHAR(36),
        lease_until DATETIME(6),
        last_error TEXT,
        completed_at DATETIME(6),
        UNIQUE (paper_id, text_hash, kind, model_id, model_revision, preprocessing),
        INDEX claimable (status, lease_until, job_id),
        CHECK ((status = 'leased') = (lease_token IS NOT NULL)),
        CHECK ((status = 'leased') = (lease_until IS NOT NULL)),
        FOREIGN KEY (paper_id) REFERENCES papers(paper_id)
    """,
}


def upgrade():
    # MySQL commits DDL per statement. A retry can finish an interrupted first upgrade.
    for name, definition in TABLES.items():
        op.execute(
            f"CREATE TABLE IF NOT EXISTS {name} ({definition}) "
            "ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_0900_bin"
        )
    op.execute(
        "INSERT INTO system_state VALUES (1, 0) ON DUPLICATE KEY UPDATE singleton=1"
    )


def downgrade():
    for name in reversed(TABLES):
        op.drop_table(name)
