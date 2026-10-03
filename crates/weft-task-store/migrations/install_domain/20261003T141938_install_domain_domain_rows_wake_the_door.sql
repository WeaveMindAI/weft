CREATE OR REPLACE FUNCTION install_domain_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_notify('weft_install_domains', '');
                RETURN NULL;
            END;
            $function$
;

CREATE TRIGGER install_domain_changed AFTER INSERT OR DELETE ON install_domain FOR EACH ROW EXECUTE FUNCTION install_domain_notify();
