-- Add migration script here
CREATE OR REPLACE FUNCTION notify_new_job() RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify('new_job', NEW.id::text);
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_inserted
AFTER INSERT ON jobs
FOR EACH ROW
EXECUTE FUNCTION notify_new_job();