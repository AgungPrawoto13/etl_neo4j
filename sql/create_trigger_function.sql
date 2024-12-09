-- subject trigger function
CREATE OR REPLACE FUNCTION subjects_changes()
    RETURNS trigger
    LANGUAGE 'plpgsql'
as $BODY$
declare
begin
    if (tg_op = 'INSERT' and tg_table_name = 'subjects') then
        perform pg_notify('subjects_row_added',
        json_build_object(
            'action', 'insert',
            'table', 'subjects',
            'data', row_to_json(new)
          )::text);
    elsif (tg_op = 'UPDATE' and tg_table_name = 'subjects') then
        perform pg_notify('subjects_row_updated',
        json_build_object(
            'action', 'update',
            'table', 'subjects',
            'data', row_to_json(new),
            'old_data', row_to_json(old)
          )::text);
    elsif (tg_op = 'DELETE' and tg_table_name = 'subjects') then
        perform pg_notify('subjects_row_deleted',
        json_build_object(
            'action', 'delete',
            'table', 'subjects',
            'data', row_to_json(old)
           )::text);
    end if;
    return null;
end
$BODY$;
CREATE TRIGGER subjects
    AFTER INSERT OR UPDATE OR DELETE
    ON subjects
    FOR EACH ROW
    EXECUTE PROCEDURE subjects_changes();
 
-- subject_relations trigger function
CREATE OR REPLACE FUNCTION subject_relations_changes()
    RETURNS trigger
    LANGUAGE 'plpgsql'
as $BODY$
declare
begin
    if (tg_op = 'INSERT' and tg_table_name = 'subject_relations') then
        perform pg_notify('subject_relations_row_added',
        json_build_object(
            'action', 'insert',
            'table', 'subject_relations',
            'data', row_to_json(new)
          )::text);
    elsif (tg_op = 'UPDATE' and tg_table_name = 'subject_relations') then
        perform pg_notify('subject_relations_row_updated',
        json_build_object(
            'action', 'update',
            'table', 'subject_relations',
            'data', row_to_json(new),
            'old_data', row_to_json(old)
          )::text);
    elsif (tg_op = 'DELETE' and tg_table_name = 'subject_relations') then
        perform pg_notify('subject_relations_row_deleted',
        json_build_object(
            'action', 'delete',
            'table', 'subject_relations',
            'data', row_to_json(old)
           )::text);
    end if;
    return null;
end
$BODY$;
CREATE TRIGGER subject_relations_changes
    AFTER INSERT OR UPDATE OR DELETE
    ON subject_relations
    FOR EACH ROW
    EXECUTE PROCEDURE subject_relations_changes();

-- links_bc trigger function
CREATE OR REPLACE FUNCTION links_bc_changes()
    RETURNS trigger
    LANGUAGE 'plpgsql'
as $BODY$
declare
begin
    if (tg_op = 'INSERT' and tg_table_name = 'links_bc') then
        perform pg_notify('links_bc_row_added',
        json_build_object(
            'action', 'insert',
            'table', 'links_bc',
            'data', row_to_json(new)
          )::text);
    elsif (tg_op = 'UPDATE' and tg_table_name = 'links_bc') then
        perform pg_notify('links_bc_row_updated',
        json_build_object(
            'action', 'update',
            'table', 'links_bc',
            'data', row_to_json(new),
            'old_data', row_to_json(old)
          )::text);
    elsif (tg_op = 'DELETE' and tg_table_name = 'links_bc') then
        perform pg_notify('links_bc_row_deleted',
        json_build_object(
            'action', 'delete',
            'table', 'links_bc',
            'data', row_to_json(old)
           )::text);
    end if;
    return null;
end
$BODY$;
CREATE TRIGGER links_bc_changes
    AFTER INSERT OR UPDATE OR DELETE
    ON links_bc
    FOR EACH ROW
    EXECUTE PROCEDURE links_bc_changes();
 
-- events_bc trigger function
CREATE OR REPLACE FUNCTION events_bc_changes()
    RETURNS trigger
    LANGUAGE 'plpgsql'
as $BODY$
declare
begin
    if (tg_op = 'INSERT' and tg_table_name = 'events_bc') then
        perform pg_notify('events_bc_row_added',
        json_build_object(
            'action', 'insert',
            'table', 'events_bc',
            'data', row_to_json(new)
          )::text);
    elsif (tg_op = 'UPDATE' and tg_table_name = 'events_bc') then
        perform pg_notify('events_bc_row_updated',
        json_build_object(
            'action', 'update',
            'table', 'events_bc',
            'data', row_to_json(new),
            'old_data', row_to_json(old)
          )::text);
    elsif (tg_op = 'DELETE' and tg_table_name = 'events_bc') then
        perform pg_notify('events_bc_row_deleted',
        json_build_object(
            'action', 'delete',
            'table', 'events_bc',
            'data', row_to_json(old)
           )::text);
    end if;
    return null;
end
$BODY$;
CREATE TRIGGER events_bc_changes
    AFTER INSERT OR UPDATE OR DELETE
    ON events_bc
    FOR EACH ROW
    EXECUTE PROCEDURE events_bc_changes();

-- links_bc_traverse trigger function
CREATE OR REPLACE FUNCTION links_bc_traverse_changes()
    RETURNS trigger
    LANGUAGE 'plpgsql'
as $BODY$
declare
begin
    if (tg_op = 'INSERT' and tg_table_name = 'links_bc_traverse') then
        perform pg_notify('links_bc_traverse_row_added',
        json_build_object(
            'action', 'insert',
            'table', 'links_bc_traverse',
            'data', row_to_json(new)
          )::text);
    elsif (tg_op = 'UPDATE' and tg_table_name = 'links_bc_traverse') then
        perform pg_notify('links_bc_traverse_row_updated',
        json_build_object(
            'action', 'update',
            'table', 'links_bc_traverse',
            'data', row_to_json(new),
            'old_data', row_to_json(old)
          )::text);
    elsif (tg_op = 'DELETE' and tg_table_name = 'links_bc_traverse') then
        perform pg_notify('links_bc_traverse_row_deleted',
        json_build_object(
            'action', 'delete',
            'table', 'links_bc_traverse',
            'data', row_to_json(old)
           )::text);
    end if;
    return null;
end
$BODY$;
CREATE TRIGGER links_bc_traverse
    AFTER INSERT OR UPDATE OR DELETE
    ON links_bc_traverse
    FOR EACH ROW
    EXECUTE PROCEDURE links_bc_traverse_changes();