//! Finding a run by what went through it.
//!
//! When a person traces an incident ("this customer says their order
//! failed at noon"), what they hold is a word that went through the run:
//! an email, an order id, a phrase from an error. Each finished run gets
//! one search document, the words of every value its journal recorded
//! (the trigger's input, what every node sent on, every error and log
//! line), built once when the run ends, off the path any run takes. The
//! runs listing matches it (`GET /executions?search=...`), beside its
//! other filters.
//!
//! Words, not substrings: `ada@example.com` finds the runs that carried that
//! address, and several words find the runs carrying all of them (a
//! quoted phrase, the runs carrying it as written). A run still going has
//! no document yet.

/// How much of a run's text its document keeps, in characters, and how
/// much of any one value. The values are read in the order the run
/// recorded them and reading stops at the cap, so a run with a huge journal
/// costs no more than a small one. A document holds each word once with
/// its positions and Postgres refuses one past a megabyte: at four bytes a
/// character, every word distinct and each hyphenated one also kept in
/// parts, this many characters stays well under it.
const MAX_TEXT: i32 = 50_000;
const MAX_VALUE: i32 = 2_000;

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "execution_search",
    tables: &["execution_search"],
    ddl: &[
        // One row per finished recorded run: its words, in the `simple`
        // configuration (every word as written, lowercased, no language's
        // stemming or stop words: an id or a name is found as it is).
        r#"CREATE TABLE IF NOT EXISTS execution_search (
            execution_id TEXT PRIMARY KEY,
            words TSVECTOR NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_execution_search_words ON execution_search USING GIN (words)"#,
        // The text of a run's document: every string and number its
        // journal recorded (never a field's name), row by row in the order
        // recorded, each cut to `p_value_max` characters, up to `p_max` in
        // all. A
        // value holding a NUL character cannot be read as JSONB, so a row
        // with one is read without it; one that still does not read (a
        // NUL spelled out with an escaped backslash before it) adds
        // nothing, and the rest of the run still does.
        r#"CREATE OR REPLACE FUNCTION weft_run_search_text(p_execution_id TEXT, p_max INTEGER, p_value_max INTEGER) RETURNS TEXT AS $$
            DECLARE
                -- Collected, then joined once: growing one string would
                -- copy all of it at every value.
                v_parts TEXT[] := '{}';
                v_length INTEGER := 0;
                v_payload TEXT;
                v_json JSONB;
                v_value TEXT;
            BEGIN
                FOR v_payload IN SELECT payload_json FROM exec_event WHERE execution_id = p_execution_id ORDER BY id LOOP
                    IF strpos(v_payload, '\u0000') > 0 THEN
                        BEGIN
                            v_json := replace(v_payload, '\u0000', '')::jsonb;
                        EXCEPTION WHEN untranslatable_character OR invalid_text_representation THEN
                            v_json := NULL;
                        END;
                        CONTINUE WHEN v_json IS NULL;
                    ELSE
                        v_json := v_payload::jsonb;
                    END IF;
                    -- One row's values at once; the cap is looked at row by
                    -- row, and the last row is cut to it.
                    SELECT string_agg(left(j #>> '{}', p_value_max), ' ') INTO v_value
                    FROM jsonb_path_query(v_json, 'strict $.** ? (@.type() == "string" || @.type() == "number")') j;
                    CONTINUE WHEN v_value IS NULL;
                    v_parts := array_append(v_parts, v_value);
                    v_length := v_length + length(v_value) + CASE WHEN v_length = 0 THEN 0 ELSE 1 END;
                    IF v_length >= p_max THEN
                        RETURN left(array_to_string(v_parts, ' '), p_max);
                    END IF;
                END LOOP;
                RETURN array_to_string(v_parts, ' ');
            END;
            $$ LANGUAGE plpgsql"#,
    ],
    seed: &[],
};

/// Build the search document of the finished run `execution_id`
/// (`weft_run_search_text`). Idempotent: a second build of the same run
/// (another dispatcher's bridge, a retried row) leaves the first.
pub async fn index_finished_run(pool: &sqlx::PgPool, execution_id: weft_core::ExecutionId) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO execution_search (execution_id, words) \
         SELECT ec.execution_id, to_tsvector('simple', weft_run_search_text(ec.execution_id, $2, $3)) \
         FROM execution ec WHERE ec.execution_id = $1 \
         ON CONFLICT (execution_id) DO NOTHING",
    )
    .bind(execution_id.to_string())
    .bind(MAX_TEXT)
    .bind(MAX_VALUE)
    .execute(pool)
    .await?;
    Ok(())
}
