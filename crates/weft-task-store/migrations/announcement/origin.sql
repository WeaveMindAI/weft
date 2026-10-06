CREATE UNLOGGED TABLE IF NOT EXISTS weft_announcement (
            channel TEXT NOT NULL,
            payload TEXT NOT NULL
        );
CREATE OR REPLACE FUNCTION weft_announce(p_channel TEXT, p_payload TEXT) RETURNS VOID AS $$
            INSERT INTO weft_announcement (channel, payload) VALUES (p_channel, p_payload)
            $$ LANGUAGE sql;
CREATE OR REPLACE FUNCTION weft_flush_announcements(p_limit INTEGER) RETURNS INTEGER AS $$
            DECLARE
                v_channels TEXT[];
                v_payloads TEXT[];
            BEGIN
                WITH taken AS (
                    DELETE FROM weft_announcement a
                    WHERE a.ctid IN (SELECT ctid FROM weft_announcement FOR UPDATE SKIP LOCKED LIMIT p_limit)
                    RETURNING channel, payload)
                SELECT array_agg(channel), array_agg(payload) INTO v_channels, v_payloads FROM taken;
                IF v_channels IS NULL THEN
                    RETURN 0;
                END IF;
                PERFORM pg_notify(d.channel, d.payload)
                    FROM (SELECT DISTINCT u.channel, u.payload FROM unnest(v_channels, v_payloads) AS u(channel, payload)) d;
                RETURN array_length(v_channels, 1);
            END;
            $$ LANGUAGE plpgsql;
