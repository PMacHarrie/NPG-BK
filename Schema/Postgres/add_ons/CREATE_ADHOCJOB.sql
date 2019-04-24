CREATE TABLE ondemandjob ( 
    odjobid                   BIGINT NOT NULL PRIMARY KEY, 
    odalgorithmname           VARCHAR(255),
    odalgorithmversion        VARCHAR(25),
    odjobspec                 JSON, 
    odjobenqueuetime          TIMESTAMP, 
    odjobstarttime            TIMESTAMP, 
    odjobcompletiontime       TIMESTAMP, 
    odjobstatus               VARCHAR(35), 
    odalgorithmreturncode     INT, 
    oddataselectionreturncode INT, 
    odjobhighesterrorclass    VARCHAR(8), 
    odjobpid                  INT, 
    odjobcpu_util             FLOAT, 
    odjobmem_util             FLOAT, 
    odjobio_util              FLOAT, 
    odjoboutput               JSON 
); 
