\set ON_ERROR_STOP on

BEGIN;

CREATE TABLE IF NOT EXISTS oauth_clients (
    client_id           TEXT PRIMARY KEY,
    metadata            JSONB NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oauth_authorization_requests (
    request_hash        TEXT PRIMARY KEY,
    client_id           TEXT NOT NULL REFERENCES oauth_clients(client_id) ON DELETE CASCADE,
    redirect_uri        TEXT NOT NULL,
    state               TEXT,
    scopes              TEXT[] NOT NULL,
    code_challenge      TEXT NOT NULL,
    resource            TEXT,
    csrf_hash           TEXT NOT NULL,
    expires_at          TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oauth_connections (
    id                  UUID PRIMARY KEY,
    client_id           TEXT NOT NULL REFERENCES oauth_clients(client_id) ON DELETE CASCADE,
    key_ciphertext      TEXT NOT NULL,
    key_iv              TEXT NOT NULL,
    key_tag             TEXT NOT NULL,
    key_fingerprint     TEXT NOT NULL,
    retention           TEXT NOT NULL,
    last_validated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oauth_schema_metadata (
    singleton           BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    version             INTEGER NOT NULL
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM oauth_schema_metadata
        WHERE singleton = TRUE AND version >= 2
    ) THEN
        ALTER TABLE oauth_connections
            DROP CONSTRAINT IF EXISTS oauth_connections_retention_check;
        ALTER TABLE oauth_connections
            ADD CONSTRAINT oauth_connections_retention_check
            CHECK (retention IN (
                'persistent', 'days_30', 'days_14', 'days_7', 'session'
            ));
    END IF;
END
$$;

INSERT INTO oauth_schema_metadata (singleton, version)
VALUES (TRUE, 2)
ON CONFLICT (singleton) DO UPDATE
SET version = GREATEST(oauth_schema_metadata.version, EXCLUDED.version);

CREATE INDEX IF NOT EXISTS idx_oauth_connections_expiry
    ON oauth_connections (expires_at)
    WHERE expires_at IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_oauth_connections_client_key
    ON oauth_connections (client_id, key_fingerprint);

CREATE TABLE IF NOT EXISTS oauth_authorization_codes (
    code_hash           TEXT PRIMARY KEY,
    client_id           TEXT NOT NULL REFERENCES oauth_clients(client_id) ON DELETE CASCADE,
    connection_id       UUID NOT NULL REFERENCES oauth_connections(id) ON DELETE CASCADE,
    redirect_uri        TEXT NOT NULL,
    scopes              TEXT[] NOT NULL,
    code_challenge      TEXT NOT NULL,
    resource            TEXT,
    expires_at          TIMESTAMPTZ NOT NULL,
    consumed_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oauth_access_tokens (
    token_hash          TEXT PRIMARY KEY,
    client_id           TEXT NOT NULL REFERENCES oauth_clients(client_id) ON DELETE CASCADE,
    connection_id       UUID NOT NULL REFERENCES oauth_connections(id) ON DELETE CASCADE,
    scopes              TEXT[] NOT NULL,
    resource            TEXT,
    expires_at          TIMESTAMPTZ NOT NULL,
    revoked_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_oauth_access_tokens_connection
    ON oauth_access_tokens (connection_id);

CREATE TABLE IF NOT EXISTS oauth_refresh_tokens (
    token_hash          TEXT PRIMARY KEY,
    client_id           TEXT NOT NULL REFERENCES oauth_clients(client_id) ON DELETE CASCADE,
    connection_id       UUID NOT NULL REFERENCES oauth_connections(id) ON DELETE CASCADE,
    family_id           UUID NOT NULL,
    scopes              TEXT[] NOT NULL,
    resource            TEXT,
    consumed_at         TIMESTAMPTZ,
    revoked_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_oauth_refresh_tokens_connection
    ON oauth_refresh_tokens (connection_id);

CREATE INDEX IF NOT EXISTS idx_oauth_refresh_tokens_family
    ON oauth_refresh_tokens (family_id);

CREATE INDEX IF NOT EXISTS idx_oauth_refresh_tokens_consumed
    ON oauth_refresh_tokens (consumed_at)
    WHERE consumed_at IS NOT NULL;

COMMIT;
