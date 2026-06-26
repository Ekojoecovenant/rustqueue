-- Add migration script here
CREATE TRIGGER job_updated
AFTER UPDATE ON jobs
FOR EACH ROW
WHEN (NEW.status = 'pending' AND OLD.status != 'pending')
EXECUTE FUNCTION notify_new_job();