-- SQL to create the application logging table in Teradata
-- Adjust database and table name as needed

CREATE TABLE LOGDB.ApplicationLogs (
    CreateDate DATE NOT NULL,
    CreateTime TIME(3) NOT NULL,
    CreateTS TIMESTAMP(3) NOT NULL,
    UserName VARCHAR(50) NOT NULL,
    EventType VARCHAR(20) NOT NULL,  -- INFO, WARNING, ERROR, DEBUG
    EventText VARCHAR(10000)
)
PRIMARY INDEX (CreateTS);

-- Optional: Create index on EventType for filtering by log level
CREATE INDEX ApplicationLogs_EventType (EventType) ON LOGDB.ApplicationLogs;

-- Optional: Create index on CreateDate for date-based queries
CREATE INDEX ApplicationLogs_Date (CreateDate) ON LOGDB.ApplicationLogs;

-- Grant permissions to the logging user
GRANT INSERT ON LOGDB.ApplicationLogs TO log_user;
