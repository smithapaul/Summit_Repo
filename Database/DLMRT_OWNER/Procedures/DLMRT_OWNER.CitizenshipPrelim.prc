DROP PROCEDURE DLMRT_OWNER."CitizenshipPrelim"
/

--
-- "CitizenshipPrelim"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."CitizenshipPrelim"
        (
                i_MartId                        in  Varchar2    Default 'DLAB',
                i_ProcessName                   in  Varchar2    Default 'CitizenshipPrelim',
                i_Institution                   in  Varchar2
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Loads table DLMRT_OWNER.IR_CITIZENSHIP_DTL_PRELIM
--
-- V02  SMT-8358 11/07/2019     Greg Kampf
--                              Use table CENSUS_STATUS to control which census data to move
--                              from stage to IR_PERSON_PRELIM.
--                              Support new partitioning of table IR_CITIZENSHIP_DTL_PRELIM.
--
-- V01  SMT-8358 10/22/2019     Greg Kampf
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
        strPartitionValuesList          Varchar2(50);
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
        strCampusId                     Varchar2(3);
        strInterfaceStatus              Varchar2(30);
        intInterfaceProcessSid          Integer;
        dtInterfaceTime                 Date;
        dtInterfaceStartTime            Date;
        dtInterfaceStopTime             Date;
        bolInterfaceFound               Boolean;
        strCountryCodeUs                Varchar2(2)     := 'US';
        strCountryDescrUs               Varchar2(13)    := 'United States';
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

strMessage01    := 'Procedure DLMRT_OWNER."CitizenshipPrelim" arguments:'
                || strNewLine || '                     i_MartId: ' || i_MartId
                || strNewLine || '                i_ProcessName: ' || i_ProcessName
                || strNewLine || '                i_Institution: ' || i_Institution;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

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
                        i_TableName                     => 'IR_CITIZENSHIP_DTL_PRELIM',
                        i_PartitionName                 => strPartitionName,
                        i_TestMode                      => False,
                        i_PartitionValuesList           => strPartitionValuesList
                );

        strSqlDynamic   := 'ALTER TABLE DLMRT_OWNER.IR_CITIZENSHIP_DTL_PRELIM TRUNCATE PARTITION "' || strPartitionName || '" UPDATE INDEXES';
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
                        i_TableName                     => 'IR_CITIZENSHIP_DTL_PRELIM',
                        i_IncludeJoinedTables           => True,
                        i_IncludePartitionedIndexes     => True,
                        i_PartitionName                 => strPartitionName,
                        i_BitmapsOnly                   => True,
                        i_IndexNameNotLike              => 'PK%'
                );
End Loop; -- recPartition


strMessage01 := 'Inserting DLMRT_OWNER.IR_CITIZENSHIP_DTL_PRELIM rows...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
strSqlCommand   := 'INSERT DLMRT_OWNER.IR_CITIZENSHIP_DTL_PRELIM';
INSERT  /*+APPEND*/
  INTO  DLMRT_OWNER.IR_CITIZENSHIP_DTL_PRELIM
        (
        INSTITUTION,
        CAMPUS,
        CENSUS_PERIOD,
        CENSUS_PERIOD_DESCR,
        CENSUS_SEQ,
        PERSON_ID,
        COUNTRY_2CHAR,
        COUNTRY_DESCR,
        CITIZENSHIP_CODE,
        CITIZENSHIP_STATUS,
        INSERT_TIME
        )
SELECT  
        STG.INSTITUTION,
        STG.CAMPUS,
        STG.CENSUS_PERIOD,
        STG.CENSUS_PERIOD_DESCR,
        STG.CENSUS_SEQ,
        STG.PERSON_ID,
        strCountryCodeUs                COUNTRY_2CHAR,
        strCountryDescrUs               COUNTRY_DESCR,
        STG.US_CITIZENSHIP_CODE         CITIZENSHIP_CODE,
        STG.US_CITIZENSHIP_STATUS       CITIZENSHIP_STATUS,
        SYSDATE                         INSERT_TIME
  FROM  DLSTG_OWNER.ENROLLMENT_S2       STG,
        DLMRT_OWNER.CENSUS_STATUS       CST
 WHERE  CST.UPLOAD_TYPE                 = strUploadType
   AND  CST.INSTITUTION                 = i_Institution
   AND  CST.READY_FOR_PRELIM            = 'Y'
   AND  STG.INSTITUTION                 = CST.INSTITUTION
   AND  STG.CENSUS_PERIOD               = CST.CENSUS_PERIOD
   AND  STG.CENSUS_SEQ                  = CST.CENSUS_SEQ
UNION ALL
SELECT
        STG.INSTITUTION,
        STG.CAMPUS,
        STG.CENSUS_PERIOD,
        STG.CENSUS_PERIOD_DESCR,
        STG.CENSUS_SEQ,
        STG.PERSON_ID,
        STG.COUNTRY_2CHAR_NON_US_1      COUNTRY_2CHAR,
        STG.COUNTRY_DESCR_NON_US_1      COUNTRY_DESCR,
        STG.CITIZENSHIP_CODE_NON_US_1   CITIZENSHIP_CODE,
        STG.CITIZENSHIP_STATUS_NON_US_1 CITIZENSHIP_STATUS,
        SYSDATE                         INSERT_TIME
  FROM  DLSTG_OWNER.ENROLLMENT_S2       STG,
        DLMRT_OWNER.CENSUS_STATUS       CST
 WHERE  CST.UPLOAD_TYPE                 = strUploadType
   AND  CST.INSTITUTION                 = i_Institution
   AND  CST.READY_FOR_PRELIM            = 'Y'
   AND  STG.INSTITUTION                 = CST.INSTITUTION
   AND  STG.CENSUS_PERIOD               = CST.CENSUS_PERIOD
   AND  STG.CENSUS_SEQ                  = CST.CENSUS_SEQ
   AND  STG.COUNTRY_2CHAR_NON_US_1 IS NOT NULL
UNION ALL
SELECT
        STG.INSTITUTION,
        STG.CAMPUS,
        STG.CENSUS_PERIOD,
        STG.CENSUS_PERIOD_DESCR,
        STG.CENSUS_SEQ,
        STG.PERSON_ID,
        STG.COUNTRY_2CHAR_NON_US_2      COUNTRY_2CHAR,
        STG.COUNTRY_DESCR_NON_US_2      COUNTRY_DESCR,
        STG.CITIZENSHIP_CODE_NON_US_2   CITIZENSHIP_CODE,
        STG.CITIZENSHIP_STATUS_NON_US_2 CITIZENSHIP_STATUS,
        SYSDATE                         INSERT_TIME
  FROM  DLSTG_OWNER.ENROLLMENT_S2       STG,
        DLMRT_OWNER.CENSUS_STATUS       CST
 WHERE  CST.UPLOAD_TYPE                 = strUploadType
   AND  CST.INSTITUTION                 = i_Institution
   AND  CST.READY_FOR_PRELIM            = 'Y'
   AND  STG.INSTITUTION                 = CST.INSTITUTION
   AND  STG.CENSUS_PERIOD               = CST.CENSUS_PERIOD
   AND  STG.CENSUS_SEQ                  = CST.CENSUS_SEQ
   AND  STG.COUNTRY_2CHAR_NON_US_2 IS NOT NULL
UNION ALL
SELECT
        STG.INSTITUTION,
        STG.CAMPUS,
        STG.CENSUS_PERIOD,
        STG.CENSUS_PERIOD_DESCR,
        STG.CENSUS_SEQ,
        STG.PERSON_ID,
        STG.COUNTRY_2CHAR_NON_US_3      COUNTRY_2CHAR,
        STG.COUNTRY_DESCR_NON_US_3      COUNTRY_DESCR,
        STG.CITIZENSHIP_CODE_NON_US_3   CITIZENSHIP_CODE,
        STG.CITIZENSHIP_STATUS_NON_US_3 CITIZENSHIP_STATUS,
        SYSDATE                         INSERT_TIME
  FROM  DLSTG_OWNER.ENROLLMENT_S2       STG,
        DLMRT_OWNER.CENSUS_STATUS       CST
 WHERE  CST.UPLOAD_TYPE                 = strUploadType
   AND  CST.INSTITUTION                 = i_Institution
   AND  CST.READY_FOR_PRELIM            = 'Y'
   AND  STG.INSTITUTION                 = CST.INSTITUTION
   AND  STG.CENSUS_PERIOD               = CST.CENSUS_PERIOD
   AND  STG.CENSUS_SEQ                  = CST.CENSUS_SEQ
   AND  STG.COUNTRY_2CHAR_NON_US_3 IS NOT NULL
;
intRowCount     := SQL%ROWCOUNT;

strSqlCommand   := 'COMMIT';
COMMIT;

strMessage01    := '# of rows inserted: ' || TO_CHAR(intRowCount);
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'IR_CITIZENSHIP_DTL_PRELIM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand   := 'SMT_INDEX.ALL_REBUILD';
COMMON_OWNER.SMT_INDEX.ALL_REBUILD
                (
                i_TableOwner            => 'DLMRT_OWNER',
                i_TableName             => 'IR_CITIZENSHIP_DTL_PRELIM',
                i_UnusableOnly          => True,
                i_AllJoinedTables       => True,
                i_ParallelDegree        => 1
                );

strMessage01    := 'Gathering statistics for DLMRT_OWNER.IR_CITIZENSHIP_DTL_PRELIM...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'DBMS_STATS.GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS
        (
                OWNNAME                 => 'DLMRT_OWNER',
                TABNAME                 => 'IR_CITIZENSHIP_DTL_PRELIM',
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

END "CitizenshipPrelim";
/
