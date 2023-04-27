CREATE OR REPLACE FUNCTION add_updated_at_column(table_name TEXT)
RETURNS VOID AS $$
BEGIN
    EXECUTE format(
        '
        ALTER TABLE %I 
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW()', table_name
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_updated_at_trigger(table_name TEXT)
RETURNS VOID AS $$
BEGIN
    EXECUTE format(
        'DROP TRIGGER IF EXISTS trg_%I_updated_at ON %I', 
        table_name, 
        table_name
    );
    EXECUTE format(
        '
        CREATE TRIGGER trg_%I_updated_at
        BEFORE UPDATE ON %I
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column()',
        table_name,
        table_name

    );
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    tablename TEXT;
BEGIN
    FOR tablename IN (
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    )
    LOOP
        PERFORM add_updated_at_column(tablename);
        PERFORM create_updated_at_trigger(tablename);
    END LOOP;
END $$;