CREATE DATABASE prototype2;
USE prototype2;

-- Create the table
CREATE TABLE PipelineRuns (
    PipelineName VARCHAR(50),
    RunStart DATETIME,
    RunEnd DATETIME,
    Duration VARCHAR(20),
    TriggeredBy VARCHAR(50),
    Status VARCHAR(20),
    Error TEXT,
    Run VARCHAR(50),
    Parameters TEXT,
    Annotations TEXT,
    RunID VARCHAR(50)
);

-- Row 1: pipeline1 - Failed
INSERT INTO PipelineRuns VALUES
('pipeline1', '2025-06-10 09:10:00', '2025-06-10 09:11:00', '60s', 'Manual trigger', 'Failed', 'Mock failure error 1', 'Original', '[]', '', 'uuid-101');

-- Row 2: pipeline2 - Succeeded
INSERT INTO PipelineRuns VALUES
('pipeline2', '2025-06-10 09:15:00', '2025-06-10 09:16:00', '60s', 'Manual trigger', 'Succeeded', '', 'Original', '[]', '', 'uuid-102');

-- Row 3: pipeline3 - Failed
INSERT INTO PipelineRuns VALUES
('pipeline3', '2025-06-10 09:20:00', '2025-06-10 09:21:00', '60s', 'Manual trigger', 'Failed', 'Mock failure error 3', 'Original', '[]', '', 'uuid-103');

-- Row 4: pipeline2 - Failed
INSERT INTO PipelineRuns VALUES
('pipeline2', '2025-06-10 09:25:00', '2025-06-10 09:26:00', '60s', 'Manual trigger', 'Failed', 'Mock failure error 4', 'Original', '[]', '', 'uuid-104');

-- Row 5: pipeline1 - Succeeded
INSERT INTO PipelineRuns VALUES
('pipeline1', '2025-06-10 09:30:00', '2025-06-10 09:31:00', '60s', 'Manual trigger', 'Succeeded', '', 'Original', '[]', '', 'uuid-105');


--code will auto create the currentStatus table
drop table pipelineRuns;
drop table currentRuns;
select * from currentStatus;
select * from pipelineRuns;