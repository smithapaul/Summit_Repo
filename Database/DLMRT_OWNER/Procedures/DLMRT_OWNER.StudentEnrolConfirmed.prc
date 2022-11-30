DROP PROCEDURE DLMRT_OWNER."StudentEnrolConfirmed"
/

--
-- "StudentEnrolConfirmed"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."StudentEnrolConfirmed"
        (
                i_MartId        in  Varchar2    Default 'DLAB',
                i_ProcessName   in  Varchar2    Default 'StudentEnrolConfirmed',
                i_Institution   in  Varchar2
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Loads table DLMRT_OWNER.IR_STUDENT_ENROL_CONFIRMED
--
-- V04  SMT-8410 01/22/2020     Greg Kampf
--                              Added APPROVED_FOR_CONFIRM = Y to CENSUS_STATUS criteria.
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
--                              from prelim to IR_STUDENT_ENROL_CONFIRMED.
--
-- V01  SMT-8358 10/21/2019     Greg Kampf
--
------------------------------------------------------------------------

        dtProcessStart                  Date            := SYSDATE;
        intProcessSid                   Integer;
        strMessage01                    Varchar2(32767);
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
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
        strCarriageReturn               Varchar2(1)     := chr(13);
        intErrorCount                   Integer         := 0;
        intRowNum                       Integer;
        intHeaderRowCount               Integer         := 0;
        bolError                        Boolean;
        bolTransformError               Boolean;
        intInsertCount                  Integer         := 0;
        intFailedRowCount               Integer         := 0;
        intFailedRowMax                 Integer         := 100;
        strDataTimestampRowExists       Varchar2(1);
        strInterfaceStatus              Varchar2(30);
        intInterfaceProcessSid          Integer;
        dtInterfaceTime                 Date;
        dtInterfaceStartTime            Date;
        dtInterfaceStopTime             Date;
        bolInterfaceFound               Boolean;
        strPartitionValuesList          Varchar2(50);
        strQuote                        Varchar2(1)     := chr(39);
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

strMessage01    := 'Procedure DLMRT_OWNER."StudentEnrolConfirmed" arguments:'
                || strNewLine || '                     i_MartId: ' || i_MartId
                || strNewLine || '                i_ProcessName: ' || i_ProcessName
                || strNewLine || '                i_Institution: ' || i_Institution;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

-- Truncate the partitions associated with the specified institution / census period / census sequences in the preliminary table
strSqlCommand   := 'For recPartition';
For recPartition in
        (
        SELECT  INSTITUTION,
                CENSUS_PERIOD,
                CENSUS_SEQ
          FROM  DLMRT_OWNER.CENSUS_STATUS
         WHERE  UPLOAD_TYPE             = strUploadType
           AND  INSTITUTION             = i_Institution
           AND  READY_FOR_CONFIRM       = 'Y'
           AND  APPROVED_FOR_CONFIRM    = 'Y'
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

        COMMON_OWNER.SMTCMN_PART.LIST_PARTITION_CREATE
                (       i_TableOwner                    => 'DLMRT_OWNER',
                        i_TableName                     => 'IR_STUDENT_ENROL_CONFIRMED',
                        i_PartitionName                 => strPartitionName,
                        i_TestMode                      => False,
                        i_PartitionValuesList           => strPartitionValuesList
                );

        strSqlDynamic   := 'ALTER TABLE DLMRT_OWNER.IR_STUDENT_ENROL_CONFIRMED TRUNCATE PARTITION "' || strPartitionName || '" UPDATE INDEXES';
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
                        i_TableName                     => 'IR_STUDENT_ENROL_CONFIRMED',
                        i_IncludeJoinedTables           => True,
                        i_IncludePartitionedIndexes     => True,
                        i_PartitionName                 => strPartitionName,
                        i_BitmapsOnly                   => True,
                        i_IndexNameNotLike              => 'PK%'
                );
End Loop; -- recPartition

strMessage01 := 'Inserting DLMRT_OWNER.IR_STUDENT_ENROL_CONFIRMED rows...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
INSERT  /*+APPEND*/
  INTO  DLMRT_OWNER.IR_STUDENT_ENROL_CONFIRMED
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
        PLM.INSTITUTION,
        PLM.CAMPUS,
        PLM.CENSUS_PERIOD,
        PLM.CENSUS_PERIOD_DESCR,
        PLM.CENSUS_SEQ,
        PLM.PERSON_ID,
        PLM.ACADEMIC_CAREER_PS,
        PLM.STUDENT_CAREER_NUMBER_PS,
        PLM.TERM_CODE_PS,
        PLM.HEADCOUNT,
        PLM.CAREER_LEVEL_CODE,
        PLM.PROGRAM_TYPE,
        PLM.EDUCATION_LEVEL_CODE,
        PLM.EDUCATION_LEVEL_DESCR,
        PLM.DOCTORATE_TYPE_CODE,
        PLM.CES_STUDENT_FLAG,
        PLM.REPORTED_TO_IPEDS_FLAG,
        PLM.ADMIT_TYPE,
        PLM.ADMIT_TERM_CODE,
        PLM.ADMIT_TERM_DESCR,
        PLM.FIRST_GENERATION_FLAG,
        PLM.NEW_OR_CONTINUING,
        PLM.PRIMARY_COLLEGE_CODE,
        PLM.PRIMARY_COLLEGE_DESCR,
        PLM.PRIMARY_MAJOR_CODE,
        PLM.PRIMARY_MAJOR_DESCR,
        PLM.PRIMARY_MAJOR_CIP_CODE,
        PLM.PRIMARY_MAJOR_PLAN_TYPE,
        PLM.STEM_FLAG,
        PLM.PRIMARY_MAJOR_SUB_PLAN_TYPE,
        PLM.PRIMARY_MAJOR_SUB_PLAN_CODE,
        PLM.PRIMARY_MAJOR_SUB_PLAN_DESCR,
        PLM.PRIMARY_MAJOR_SUB_PLAN_CIP_CODE,
        PLM.ACADEMIC_LEVEL_BOT_CODE,
        PLM.ACADEMIC_LEVEL_BOT_DESCR,
        PLM.ACADEMIC_LEVEL_EOT_CODE,
        PLM.ACADEMIC_LEVEL_EOT_DESCR,
        PLM.EXPECTED_GRAD_TERM_CODE,
        PLM.EXPECTED_GRAD_TERM_DESCR,
        PLM.CUMULATIVE_CREDITS,
        PLM.CUMULATIVE_GPA,
        PLM.FT_PT_CODE,
        PLM.FT_PT_DESCR,
        PLM.TOTAL_CREDITS,
        PLM.ONLINE_CREDITS,
        PLM.NON_ONLINE_CREDITS,
        PLM.MIXED_MODE_INSTRUCTION_FLAG,
        PLM.CE_CREDITS,
        PLM.NON_CE_CREDITS,
        PLM.TOTAL_FTE,
        PLM.ONLINE_FTE,
        PLM.CE_FTE,
        PLM.CLASS_COUNT,
        PLM.ONLINE_CLASS_COUNT,
        PLM.CE_CLASS_COUNT,
        PLM.ALL_REGISTRATION_COUNT,
        PLM.ONLINE_REGISTRATION_COUNT,
        PLM.CE_REGISTRATION_COUNT,
        PLM.PELL_RECIPIENT_FLAG,
        SYSDATE                                 INSERT_TIME
  FROM  DLMRT_OWNER.IR_STUDENT_ENROL_PRELIM     PLM,
        DLMRT_OWNER.CENSUS_STATUS               CST
 WHERE  CST.UPLOAD_TYPE                 = strUploadType
   AND  CST.INSTITUTION                 = i_Institution
   AND  CST.READY_FOR_CONFIRM           = 'Y'
   AND  CST.APPROVED_FOR_CONFIRM        = 'Y'
   AND  PLM.INSTITUTION                 = CST.INSTITUTION
   AND  PLM.CENSUS_PERIOD               = CST.CENSUS_PERIOD
   AND  PLM.CENSUS_SEQ                  = CST.CENSUS_SEQ
;
intRowCount     := SQL%ROWCOUNT;

strMessage01    := '# of rows inserted: ' || TO_CHAR(intRowCount);
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'COMMIT';
COMMIT;

strSqlCommand   := 'SMT_INDEX.ALL_REBUILD';
COMMON_OWNER.SMT_INDEX.ALL_REBUILD
        (
                i_TableOwner            => 'DLMRT_OWNER',
                i_TableName             => 'IR_STUDENT_ENROL_CONFIRMED',
                i_UnusableOnly          => True,
                i_AllJoinedTables       => True,
                i_ParallelDegree        => 1
        );
                
                
strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName       => 'IR_STUDENT_ENROL_CONFIRMED',
                i_Action                => 'INSERT',
                i_RowCount              => intRowCount
        );


strMessage01    := 'Gathering statistics for DLMRT_OWNER.IR_STUDENT_ENROL_CONFIRMED...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'DBMS_STATS.GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS
        (
                OWNNAME                 => 'DLMRT_OWNER',
                TABNAME                 => 'IR_STUDENT_ENROL_CONFIRMED',
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

END "StudentEnrolConfirmed";
/
