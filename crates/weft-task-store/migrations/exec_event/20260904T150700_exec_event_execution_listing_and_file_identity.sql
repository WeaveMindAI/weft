CREATE INDEX idx_execution_color_listing ON execution_color USING btree (tenant_id, started_at_unix DESC, color DESC) WHERE (kind = 'execution'::text);
