drop index if exists il_fileid;
drop table if exists ingest_log;

create table ingest_log (
        platformName            VARCHAR(255) null,
        productId               BIGINT null,
        productShortName        VARCHAR(255) NULL,
        productType             VARCHAR(50) NULL,
        productSubType          VARCHAR(50) NULL,
        fileId                  BIGINT NULL,
        fileName                VARCHAR(255) NULL,
        fileSize                INTEGER NULL,
        fileInsertTime          TIMESTAMP NULL,
        fileStartTime           TIMESTAMP NULL,
        fileEndTime             TIMESTAMP NULL,
        fileBeginOrbitNum       INTEGER NULL,
        fileEndOrbitNum         INTEGER NULL,
        product_time_coverage   VARCHAR(255) NULL,
        productFileFormat	    VARCHAR(255) NULL,
        productCoverageGapInterval_DS   interval day to second,
        if_fileorigcreatetime   TIMESTAMP NULL,
        if_filedetectedtime     TIMESTAMP NULL,
        if_filesourcecreatetime TIMESTAMP NULL,
        if_filereceipttime      TIMESTAMP NULL,
        if_filemessagecreatetime TIMESTAMP NULL,
        lastInputOrigCreateTime    TIMESTAMP NULL,
        lastInputDetectedTime      TIMESTAMP NULL,
        lastInputSourceCreateTime  TIMESTAMP NULL,
        lastInputReceiptTime       TIMESTAMP NULL,
        lastInputMessageCreateTime TIMESTAMP NULL,
        ancestorCount           INT NULL,
        row_lastupdatetime      TIMESTAMP NULL
);

create index il_fileid on ingest_log(fileId);
create index il_filename on ingest_log(fileName);

drop table if exists ingest_log_temp;

create table ingest_log_temp (
        platformName            VARCHAR(255) null,
        productId               BIGINT null,
        productShortName        VARCHAR(255) NULL,
        productType             VARCHAR(50) NULL,
        productSubType          VARCHAR(50) NULL,
        fileId                  BIGINT NULL,
        fileName                VARCHAR(255) NULL,
        fileSize                INTEGER NULL,
        fileInsertTime          TIMESTAMP NULL,
        fileStartTime           TIMESTAMP NULL,
        fileEndTime             TIMESTAMP NULL,
        fileBeginOrbitNum       INTEGER NULL,
        fileEndOrbitNum         INTEGER NULL,
        product_time_coverage   VARCHAR(255) NULL,
        productFileFormat       VARCHAR(255) NULL,
        productCoverageGapInterval_DS   interval day to second
);


drop index if exists jl_ppji;
drop index if exists jl_pji;
drop table if exists job_log;

create table job_log (
        prJobId                 bigint null,
        prodPartialJobId        bigint null,
        prJobEnqueueTime        TIMESTAMP null,
        prJobStartTime          TIMESTAMP null,
        prJobCompletionTime     TIMESTAMP NULL,
        prJobStatus             VARCHAR(35) NULL,
        prAlgorithmReturnCode   INTEGER NULL,
        prDataSelectionReturnCode       INTEGER NULL,
        prJobCPU_Util           REAL NULL,
        prJobMem_Util           REAL NULL,
        prJobIO_Util            REAL NULL,
        prJobWorkerNode         VARCHAR(255) NULL,
        jobClass                INTEGER NULL,
        jobPriority             INTEGER NULL,
        pjsStartTime            TIMESTAMP NULL,
        pjsObsStartTime         TIMESTAMP NULL,
        pjsObsEndTime           TIMESTAMP NULL,
        pjsCompletionStatus     VARCHAR(255) NULL,
        prId                    BIGINT NULL,
        prRuleName              VARCHAR(255) NULL,
        algorithmId             BIGINT NULL,
        algorithm               VARCHAR(271) NULL,
        row_lastupdatetime      TIMESTAMP NULL
);


create index jl_ppji on job_log (prodPartialJobId);
create index jl_pji on job_log (prJobId);
create index jl_prjobcompletiontime on job_log(prJobCompletionTime);

drop table if exists job_log_temp;

create table job_log_temp (
        prJobId                 bigint null,
        prodPartialJobId        bigint null,
        prJobEnqueueTime        TIMESTAMP null,
        prJobStartTime          TIMESTAMP null,
        prJobCompletionTime     TIMESTAMP NULL,
        prJobStatus             VARCHAR(35) NULL,
        prAlgorithmReturnCode   INTEGER NULL,
        prDataSelectionReturnCode       INTEGER NULL,
        prJobCPU_Util           REAL NULL,
        prJobMem_Util           REAL NULL,
        prJobIO_Util            REAL NULL,
        prJobWorkerNode         VARCHAR(255) NULL,
        jobClass                INTEGER NULL,
        jobPriority             INTEGER NULL,
        pjsStartTime            TIMESTAMP NULL,
        pjsObsStartTime         TIMESTAMP NULL,
        pjsObsEndTime           TIMESTAMP NULL,
        pjsCompletionStatus     VARCHAR(255) NULL,
        prId                    BIGINT NULL,
        prRuleName              VARCHAR(255) NULL,
        algorithmId             BIGINT NULL,
        algorithm               VARCHAR(271) NULL
);

drop index if exists pil_jf;
drop index if exists pil_f;
drop table if exists pgs_in_log;

create table pgs_in_log (
        prJobId bigint null,
        fileId  bigint null,
        prisneed VARCHAR(25) null,
        rowInsertTime timestamp NULL
);

create index pil_jf on pgs_in_log(prJobId, fileId);
create index pil_f  on pgs_in_log(fileId);

drop table if exists pgs_in_log_temp;

create table pgs_in_log_temp (
        prJobId bigint null,
        fileId  bigint null,
        prisneed VARCHAR(25) null
);

drop index if exists pol_jf;
drop index if exists pol_f;
drop table if exists pgs_out_log;

create table pgs_out_log (
        prJobId bigint null,
        fileId  bigint null,
        rowInsertTime timestamp NULL
);

create index pol_jf on pgs_out_log(prJobId, fileId);
create index pol_f on pgs_out_log(fileId);

drop table if exists pgs_out_log_temp;

create table pgs_out_log_temp (
        prJobId bigint null,
        fileId  bigint null
);


drop table if exists ConfigurationRegistry;
create table ConfigurationRegistry (
        cfgParameterName varchar(255) not null,
        cfgParameterValue varchar(255) null
);

insert into ConfigurationRegistry values ('dbStatisticsExpirationHr', '2160');
insert into ConfigurationRegistry values ('last_popmetric_update', null);
insert into ConfigurationRegistry values ('last_filelatency_update', null);


