CREATE SCHEMA bank;

CREATE TABLE bank.transactions (
    transaction_id     text PRIMARY KEY,
    account_token      text        NOT NULL,
    counterparty_token text        NOT NULL,
    amount_cents       bigint      NOT NULL CHECK (amount_cents > 0),
    device_hash        text,
    occurred_at        timestamptz NOT NULL,
    payer_name         text,
    payer_cpf          text
);
CREATE INDEX transactions_occurred_at_idx ON bank.transactions (occurred_at);

CREATE PUBLICATION dbz_transactions FOR TABLE bank.transactions;
