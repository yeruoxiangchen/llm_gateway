CREATE TABLE IF NOT EXISTS accounts (
 id CHAR(36) PRIMARY KEY,
 name VARCHAR(120) NOT NULL,
 quota_tokens BIGINT NOT NULL,
 used_tokens BIGINT NOT NULL DEFAULT 0,
 reserved_tokens BIGINT NOT NULL DEFAULT 0,
 created DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS api_keys (
 id CHAR(36) PRIMARY KEY,
 account_id CHAR(36) NOT NULL,
 name VARCHAR(120) NOT NULL,
 key_hash BINARY(32) NOT NULL UNIQUE,
 prefix VARCHAR(20) NOT NULL,
 rpm INT NOT NULL,
 active BOOLEAN NOT NULL DEFAULT TRUE,
 FOREIGN KEY (account_id) REFERENCES accounts(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS providers (
 id CHAR(36) PRIMARY KEY,
 name VARCHAR(120) NOT NULL,
 model_alias VARCHAR(100) NOT NULL,
 upstream_model VARCHAR(160) NOT NULL,
 base_url VARCHAR(500) NOT NULL,
 secret BLOB NOT NULL,
 priority INT NOT NULL DEFAULT 0,
 enabled BOOLEAN NOT NULL DEFAULT TRUE,
 input_price BIGINT NOT NULL DEFAULT 0,
 output_price BIGINT NOT NULL DEFAULT 0,
 INDEX providers_alias(model_alias, enabled)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS owner_leases (
 owner_id CHAR(36) PRIMARY KEY,
 lease_until DATETIME(6) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS requests (
 id CHAR(36) PRIMARY KEY,
 account_id CHAR(36) NOT NULL,
 key_id CHAR(36) NOT NULL,
 idempotency_key VARBINARY(128) NULL,
 payload_hash BINARY(32) NOT NULL,
 model_alias VARCHAR(100) NOT NULL,
 is_stream BOOLEAN NOT NULL,
 state VARCHAR(32) NOT NULL,
 outcome VARCHAR(24) NOT NULL DEFAULT '',
 reservation_tokens BIGINT NOT NULL,
 prompt_tokens BIGINT NULL,
 completion_tokens BIGINT NULL,
 total_tokens BIGINT NULL,
 cost_micro_usd BIGINT NOT NULL DEFAULT 0,
 http_status INT NOT NULL DEFAULT 0,
 error_code VARCHAR(100) NOT NULL DEFAULT '',
 provider_id CHAR(36) NOT NULL DEFAULT '',
 input_price BIGINT NOT NULL DEFAULT 0,
 output_price BIGINT NOT NULL DEFAULT 0,
 response_payload MEDIUMBLOB NULL,
 owner_id CHAR(36) NOT NULL,
 lease_until DATETIME(6) NOT NULL,
 created DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 updated DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 UNIQUE KEY requests_idempotency(account_id, idempotency_key),
 INDEX requests_account(account_id, created),
 INDEX requests_recovery(state, lease_until),
 FOREIGN KEY (account_id) REFERENCES accounts(id),
 FOREIGN KEY (key_id) REFERENCES api_keys(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ledger (
 id BIGINT AUTO_INCREMENT PRIMARY KEY,
 request_id CHAR(36) NOT NULL,
 account_id CHAR(36) NOT NULL,
 event_type VARCHAR(24) NOT NULL,
 used_delta BIGINT NOT NULL,
 reserved_delta BIGINT NOT NULL,
 reason VARCHAR(512) NOT NULL DEFAULT '',
 created DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 UNIQUE KEY ledger_event(request_id, event_type),
 FOREIGN KEY (request_id) REFERENCES requests(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS attempts (
 id BIGINT AUTO_INCREMENT PRIMARY KEY,
 request_id CHAR(36) NOT NULL,
 provider_id CHAR(36) NOT NULL,
 status INT NOT NULL DEFAULT 0,
 error_code VARCHAR(100) NOT NULL DEFAULT '',
 started DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 ended DATETIME(6) NULL,
 FOREIGN KEY (request_id) REFERENCES requests(id)
) ENGINE=InnoDB;
