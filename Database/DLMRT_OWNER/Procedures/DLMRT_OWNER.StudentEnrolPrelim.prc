DROP PROCEDURE DLMRT_OWNER."StudentEnrolPrelim"
/

--
-- "StudentEnrolPrelim"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."StudentEnrolPrelim"
        (
                i_MartId                        in  Varchar2    Default 'DLAB',
                i_ProcessName                   in  Varchar2    Default 'StudentEnrolPrelim',
                i_Institution                   in  Varchar2
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Loads table DLMRT_OWNER.IR_STUDENT_ENROL_PRELIM
--
-- V03  SMT-8358 11/26/2019     Greg Kampf
--                              Added columns:
--                              o DOCTORATE_TYPE_CODE
--                              o ADMIT_TERM_CODE
--                              o ADMIT_TERM_DESCR
--                              Renamed columns:
--                              o ACADEMIC_LOAD_CODE to FT_PT_CODE
--                              o ACADEMIC_LOAD_DESCR to FT_PT_DESCR
--                              o HYBRID_STUDENT_FLAG to MIXED_MODE_INSTRUCTION_FLAG
--
-- V02  SMT-8358 11/06/2019     Greg Kampf
--                              Use table CENSUS_STATUS to control which census data to move
--                              from stage to IR_STUDENT_ENROL_PRELIM.
--                              Support new partitioning of table IR_STUDENT_ENROL_PRELIM.
--
-- V01  SMT-8358 10/21/2019     Greg Kampf
--
------------------------------------------------------------------------

        dtProcessStart                  Date            := SYSDATE;
        intProcessSid                   Integer;
        strMessage01                    Varchar2(32767);
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strQuote                        Varchar2(1)     := chr(39);
        strSqlCommand                   Varchar2(32767) := '';
        strSqlDynamic                   Varchar2(32767) := '';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;
        intYear                         Integer;
        strControlRowExists             Varchar2(1);
        strPartitionName                Varchar2(128);
        strPartitionValuesList          Varchar2(50);
        strCarriageReturn               Varchar2(1)     := chr(13);
        intErrorCount                   Integer         := 0;
        intRowNum                       Integer;
        intHeaderRowCount               Integer         := 0;
        bolError                        Boolean;
        bolTransformError               Boolean;
        intInsertCount                  Integer         := 0;
        intUpdateCount                  Integer         := 0;
        intFailedRowCount               Integer         := 0;
        strStatusRowExists              Varchar2(1);
        intFailedRowMax                 Integer         := 100;
        strDataTimestampRowExists       Varchar2(1);
        strCampusId                     Varchar2(3);
        strInterfaceStatus              Varchar2(30);
        intInterfaceProcessSid          Integer;
        dtInterfaceTime                 Date;
        dtInterfaceStartTime            Date;
        dtInterfaceStopTime             Date;
        bolInterfaceFound               Boolean;
        strUploadType                   Varchar2(10)    := 'ENROLLMENT';

BEGIN
strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (i_ProcessName);

COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => i_MartId,
                i_ProcessName           => i_ProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Procedure DLMRT_OWNER."StudentEnrolPrelim" arguments:'
                || strNewLine || '                     i_MartId: ' || i_MartId
                || strNewLine || '                i_ProcessName: ' || i_ProcessName
                || strNewLine || '                i_Institution: ' || i_Institution;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strCampusId     := substr(i_Institution,3,3);


-- Using DLMRT_OWNER.CENSUS_STATUS identify the institution, census period and census sequences
-- that are ready to be loaded into the preliminary table.
-- For those institution, census period and census sequences combinations truncate their partitions.
strSqlCommand   := 'For recPartition';
For recPartition in
        (
        SELECT  INSTITUTION,
                CENSUS_PERIOD,
                CENSUS_SEQ
          FROM  DLMRT_OWNER.CENSUS_STATUS
         WHERE  UPLOAD_TYPE             = strUploadType
           AND  READY_FOR_PRELIM        = 'Y'
           AND  INSTITUTION             = i_Institution
        ORDER BY
                INSTITUTION,
                CENSUS_PERIOD,
                CENSUS_SEQ
        )
Loop
        strPartitionName        := recPartition.INSTITUTION || '_' || recPartition.CENSUS_PERIOD || '_' || TO_CHAR(recPartition.CENSUS_SEQ);
        strPartitionValuesList  := strQuote || recPartition.INSTITUTION || strQuote || ',' || strQuote || recPartition.CENSUS_PERIOD || strQuote || ',' || TO_CHAR(recPartition.CENSUS_SEQ);

        strMessage01    := 'Processing partition ' || strPartitionName;
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

        strSqlCommand   := 'SMTCMN_PART.LIST_PARTITION_CREATE';
        COMMON_OWNER.SMTCMN_PART.LIST_PARTITION_CREATE
                (       i_TableOwner                    => 'DLMRT_OWNER',
                        i_TableName                     => 'IR_STUDENT_ENROL_PRELIM',
                        i_PartitionName                 => strPartitionName,
                        i_TestMode                      => False,
                        i_PartitionValuesList           => strPartitionValuesList
                );

        strSqlDynamic   := 'ALTER TABLE DLMRT_OWNER.IR_STUDENT_ENROL_PRELIM TRUNCATE PARTITION "' || strPartitionName || '" UPDATE INDEXES';
        strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
        COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                        i_SqlStatement                  => strSqlDynamic,
                        i_MaxTries                      => 10,
                        i_WaitSeconds                   => 10,
                        o_Tries                         => intTries
                );

        strSqlCommand   := 'SMT_INDEX.ALL_UNUSABLE';
        COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE
                (
                        i_TableOwner                    => 'DLMRT_OWNER',
                        i_TableName                     => 'IR_STUDENT_ENROL_PRELIM',
                        i_IncludeJoinedTables           => True,
                        i_IncludePartitionedIndexes     => True,
                        i_PartitionName                 => strPartitionName,
                        i_BitmapsOnly                   => True,
                        i_IndexNameNotLike              => 'PK%'
                );
End Loop; -- recPartition

strMessage01 := 'Inserting DLMRT_OWNER.IR_STUDENT_ENROL_PRELIM rows...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
strSqlCommand   := 'INSERT DLMRT_OWNER.IR_STUDENT_ENROL_PRELIM';
INSERT  /*+APPEND*/
  INTO  DLMRT_OWNER.IR_STUDENT_ENROL_PRELIM
        (
        INSTITUTION,
        CAMPUS,
        CENSUS_PERIOD,
        CENSUS_PERIOD_DESCR,
        CENSUS_SEQ,
        PERSON_ID,
        ACADEMIC_CAREER_PS,
        STUDENT_CAREER_NUMBER_PS,
        TERM_CODE_PS,
        HEADCOUNT,
        CAREER_LEVEL_CODE,
        PROGRAM_TYPE,
        EDUCATION_LEVEL_CODE,
        EDUCATION_LEVEL_DESCR,
        DOCTORATE_TYPE_CODE,
        CES_STUDENT_FLAG,
        REPORTED_TO_IPEDS_FLAG,
        ADMIT_TYPE,
        ADMIT_TERM_CODE,
        ADMIT_TERM_DESCR,
        FIRST_GENERATION_FLAG,
        NEW_OR_CONTINUING,
        PRIMARY_COLLEGE_CODE,
        PRIMARY_COLLEGE_DESCR,
        PRIMARY_MAJOR_CODE,
        PRIMARY_MAJOR_DESCR,
        PRIMARY_MAJOR_CIP_CODE,
        PRIMARY_MAJOR_PLAN_TYPE,
        STEM_FLAG,
        PRIMARY_MAJOR_SUB_PLAN_TYPE,
        PRIMARY_MAJOR_SUB_PLAN_CODE,
        PRIMARY_MAJOR_SUB_PLAN_DESCR,
        PRIMARY_MAJOR_SUB_PLAN_CIP_CODE,
        ACADEMIC_LEVEL_BOT_CODE,
        ACADEMIC_LEVEL_BOT_DESCR,
        ACADEMIC_LEVEL_EOT_CODE,
        ACADEMIC_LEVEL_EOT_DESCR,
        EXPECTED_GRAD_TERM_CODE,
        EXPECTED_GRAD_TERM_DESCR,
        CUMULATIVE_CREDITS,
        CUMULATIVE_GPA,
        FT_PT_CODE,
        FT_PT_DESCR,
        TOTAL_CREDITS,
        ONLINE_CREDITS,
        NON_ONLINE_CREDITS,
        ONLINE_ONLY_STUDENT_FLAG,
        MIXED_MODE_INSTRUCTION_FLAG,
        CE_CREDITS,
        NON_CE_CREDITS,
        TOTAL_FTE,
        ONLINE_FTE,
        CE_FTE,
        CLASS_COUNT,
        ONLINE_CLASS_COUNT,
        CE_CLASS_COUNT,
        ALL_REGISTRATION_COUNT,
        ONLINE_REGISTRATION_COUNT,
        CE_REGISTRATION_COUNT,
        PELL_RECIPIENT_FLAG,
        INSERT_TIME
        )
SELECT  
        STG.INSTITUTION,
        STG.CAMPUS,
        STG.CENSUS_PERIOD,
        STG.CENSUS_PERIOD_DESCR,
        STG.CENSUS_SEQ,
        STG.PERSON_ID,
        STG.ACADEMIC_CAREER_PS,
        STG.STUDENT_CAREER_NUMBER_PS,
        STG.TERM_CODE_PS,
        STG.HEADCOUNT,
        STG.CAREER_LEVEL_CODE,
        STG.PROGRAM_TYPE,
        STG.EDUCATION_LEVEL_CODE,
        STG.EDUCATION_LEVEL_DESCR,
        STG.DOCTORATE_TYPE_CODE,
        STG.CES_STUDENT_FLAG,
        STG.REPORTED_TO_IPEDS_FLAG,
        STG.ADMIT_TYPE,
        STG.ADMIT_TERM_CODE,
        STG.ADMIT_TERM_DESCR,
        STG.FIRST_GENERATION_FLAG,
        STG.NEW_OR_CONTINUING,
        STG.PRIMARY_COLLEGE_CODE,
        STG.PRIMARY_COLLEGE_DESCR,
        STG.PRIMARY_MAJOR_CODE,
        STG.PRIMARY_MAJOR_DESCR,
        STG.PRIMARY_MAJOR_CIP_CODE,
        STG.PRIMARY_MAJOR_PLAN_TYPE,
        STG.STEM_FLAG,
        STG.PRIMARY_MAJOR_SUB_PLAN_TYPE,
        STG.PRIMARY_MAJOR_SUB_PLAN_CODE,
        STG.PRIMARY_MAJOR_SUB_PLAN_DESCR,
        STG.PRIMARY_MAJOR_SUB_PLAN_CIP_CODE,
        STG.ACADEMIC_LEVEL_BOT_CODE,
        STG.ACADEMIC_LEVEL_BOT_DESCR,
        STG.ACADEMIC_LEVEL_EOT_CODE,
        STG.ACADEMIC_LEVEL_EOT_DESCR,
        STG.EXPECTED_GRAD_TERM_CODE,
        STG.EXPECTED_GRAD_TERM_DESCR,
        STG.CUMULATIVE_CREDITS,
        STG.CUMULATIVE_GPA,
        STG.FT_PT_CODE,
        STG.FT_PT_DESCR,
        STG.TOTAL_CREDITS,
        STG.ONLINE_CREDITS,
        STG.NON_ONLINE_CREDITS,
        STG.ONLINE_ONLY_STUDENT_FLAG,
        STG.MIXED_MODE_INSTRUCTION_FLAG,
        STG.CE_CREDITS,
        STG.NON_CE_CREDITS,
        STG.TOTAL_FTE,
        STG.ONLINE_FTE,
        STG.CE_FTE,
        STG.CLASS_COUNT,
        STG.ONLINE_CLASS_COUNT,
        STG.CE_CLASS_COUNT,
        STG.ALL_REGISTRATION_COUNT,
        STG.ONLINE_REGISTRATION_COUNT,
        STG.CE_REGISTRATION_COUNT,
        STG.PELL_RECIPIENT_FLAG,
        SYSDATE                         INSERT_TIME
  FROM  DLSTG_OWNER.ENROLLMENT_S2       STG,
        DLMRT_OWNER.CENSUS_STATUS       CST
 WHERE  CST.UPLOAD_TYPE                 = strUploadType
   AND  CST.INSTITUTION                 = i_Institution
   AND  CST.READY_FOR_PRELIM            = 'Y'
   AND  STG.INSTITUTION                 = CST.INSTITUTION
   AND  STG.CENSUS_PERIOD               = CST.CENSUS_PERIOD
   AND  STG.CENSUS_SEQ                  = CST.CENSUS_SEQ
;
intRowCount     := SQL%ROWCOUNT;

strMessage01    := '# of rows inserted: ' || TO_CHAR(intRowCount);
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'IR_STUDENT_ENROL_PRELIM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand   := 'COMMIT';
COMMIT;

strSqlCommand   := 'SMT_INDEX.ALL_REBUILD';
COMMON_OWNER.SMT_INDEX.ALL_REBUILD
                (
                i_TableOwner            => 'DLMRT_OWNER',
                i_TableName             => 'IR_STUDENT_ENROL_PRELIM',
                i_UnusableOnly          => True,
                i_AllJoinedTables       => False,
                i_ParallelDegree        => 1
                );


strMessage01    := 'Gathering statistics for DLMRT_OWNER.IR_STUDENT_ENROL_PRELIM...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'DBMS_STATS.GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS
        (
                OWNNAME                 => 'DLMRT_OWNER',
                TABNAME                 => 'IR_STUDENT_ENROL_PRELIM',
                DEGREE                  => 8,
                ESTIMATE_PERCENT        => DBMS_STATS.AUTO_SAMPLE_SIZE,
                METHOD_OPT              => 'FOR ALL COLUMNS SIZE AUTO',
                GRANULARITY             => 'AUTO',
                FORCE                   => True,
                NO_INVALIDATE           => False
        );

strMessage01    := 'Statistics gathered.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

EXCEPTION
    WHEN OTHERS THEN
        numSqlCode := SQLCODE;
        strSqlErrm := SQLERRM;
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => numSqlCode,
                        i_SqlErrm      => strSqlErrm
                );

END "StudentEnrolPrelim";
/
