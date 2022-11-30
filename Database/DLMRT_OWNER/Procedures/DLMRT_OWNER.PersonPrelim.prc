DROP PROCEDURE DLMRT_OWNER."PersonPrelim"
/

--
-- "PersonPrelim"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."PersonPrelim"
        (
                i_MartId                        in  Varchar2    Default 'DLAB',
                i_ProcessName                   in  Varchar2    Default 'PersonPrelim',
                i_Institution                   in  Varchar2
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Loads table DLMRT_OWNER.IR_PERSON_PRELIM
--
-- V04  CASE-23700      03/21/2020      Greg Kampf
--                                      Support new columns RESIDENCY_CODE_INOUTSTATE, RESIDENCY_DESC_INOUTSTATE
--
-- V03  SMT-8358 11/07/2019     Greg Kampf
--                              Use table CENSUS_STATUS to control which census data to move
--                              from stage to IR_PERSON_PRELIM.
--                              Support new partitioning of table IR_PERSON_PRELIM.
--
-- V02  SMT-8358 10/18/2019     Greg Kampf
--                              Load data from DLSTG_OWNER.ENROLLMENT_S2 instead of UPLOAD_S1.
--
-- V01  SMT-8358 10/08/2019     Greg Kampf
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

strMessage01    := 'Procedure DLMRT_OWNER."PersonPrelim" arguments:'
                || strNewLine || '                     i_MartId: ' || i_MartId
                || strNewLine || '                i_ProcessName: ' || i_ProcessName
                || strNewLine || '                i_Institution: ' || i_Institution;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strCampusId     := substr(i_Institution,3,3);
strPartitionName:= 'INST_' || i_Institution;

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
                        i_TableName                     => 'IR_PERSON_PRELIM',
                        i_PartitionName                 => strPartitionName,
                        i_TestMode                      => False,
                        i_PartitionValuesList           => strPartitionValuesList
                );

        strSqlDynamic   := 'ALTER TABLE DLMRT_OWNER.IR_PERSON_PRELIM TRUNCATE PARTITION "' || strPartitionName || '" UPDATE INDEXES';
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
                        i_TableName                     => 'IR_PERSON_PRELIM',
                        i_IncludeJoinedTables           => True,
                        i_IncludePartitionedIndexes     => True,
                        i_PartitionName                 => strPartitionName,
                        i_BitmapsOnly                   => True,
                        i_IndexNameNotLike              => 'PK%'
                );
End Loop; -- recPartition

strMessage01    := 'Inserting DLMRT_OWNER.IR_PERSON_PRELIM rows...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'INSERT DLMRT_OWNER.IR_PERSON_PRELIM';
INSERT  /*+APPEND*/
  INTO  DLMRT_OWNER.IR_PERSON_PRELIM
        (
        INSTITUTION,
        CAMPUS,
        CENSUS_PERIOD,
        CENSUS_PERIOD_DESCR,
        CENSUS_SEQ,
        PERSON_ID,
        UMASS_GUID,
        BIRTH_DATE,
        CAMPUS_CENSUS_DATE,
        IPEDS_AGE_RANGE_CODE,
        IPEDS_AGE_RANGE_DESCR,
        GENDER_CODE,
        GENDER_DESCR,
        US_CITIZENSHIP_CODE,
        US_CITIZENSHIP_STATUS,
        COUNTRY_2CHAR_NON_US,
        COUNTRY_DESCR_NON_US,
        FED_ETHNICITY_CODE,
        FED_ETHNICITY_DESCR,
        STATE_ETHNICITY_CODE,
        STATE_ETHNICITY_DESCR,
        UNDER_REPRESENTED_MINORITY,
        MILITARY_STATUS,
        COUNTRY_2CHAR_PERM_ADDRESS,
        COUNTRY_DESCR_PERM_ADDRESS,
        STATE_PERM_ADDRESS,
        POSTAL_CODE_PERM_ADDRESS,
        COUNTY_PERM_ADDRESS,
        CAMPUS_RESIDENT,
        RESIDENCY_CODE_SA_TUITION,
        RESIDENCY_DESC_SA_TUITION,
        RESIDENCY_CODE_TUITION,
        RESIDENCY_DESC_TUITION,
        RESIDENCY_CODE_INOUTSTATE,
        RESIDENCY_DESC_INOUTSTATE,
        INSERT_TIME
        )
SELECT  
        STG.INSTITUTION,
        STG.CAMPUS,
        STG.CENSUS_PERIOD,
        STG.CENSUS_PERIOD_DESCR,
        STG.CENSUS_SEQ,
        STG.PERSON_ID,
        STG.UMASS_GUID,
        STG.BIRTH_DATE,
        STG.CAMPUS_CENSUS_DATE,
        STG.IPEDS_AGE_RANGE_CODE,
        STG.IPEDS_AGE_RANGE_DESCR,
        STG.GENDER_CODE,
        STG.GENDER_DESCR,
        STG.US_CITIZENSHIP_CODE,
        STG.US_CITIZENSHIP_STATUS,
        STG.COUNTRY_2CHAR_NON_US,
        STG.COUNTRY_DESCR_NON_US,
        STG.FED_ETHNICITY_CODE,
        STG.FED_ETHNICITY_DESCR,
        STG.STATE_ETHNICITY_CODE,
        STG.STATE_ETHNICITY_DESCR,
        STG.UNDER_REPRESENTED_MINORITY,
        STG.MILITARY_STATUS,
        STG.COUNTRY_2CHAR_PERM_ADDRESS,
        STG.COUNTRY_DESCR_PERM_ADDRESS,
        STG.STATE_PERM_ADDRESS,
        STG.POSTAL_CODE_PERM_ADDRESS,
        STG.COUNTY_PERM_ADDRESS,
        STG.CAMPUS_RESIDENT,
        STG.RESIDENCY_CODE_SA_TUITION,
        STG.RESIDENCY_DESC_SA_TUITION,
        STG.RESIDENCY_CODE_TUITION,
        STG.RESIDENCY_DESC_TUITION,
        STG.RESIDENCY_CODE_INOUTSTATE,
        STG.RESIDENCY_DESC_INOUTSTATE,
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
                i_TargetTableName   => 'IR_PERSON_PRELIM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand   := 'COMMIT';
COMMIT;

strSqlCommand   := 'SMT_INDEX.ALL_REBUILD';
COMMON_OWNER.SMT_INDEX.ALL_REBUILD
                (
                i_TableOwner            => 'DLMRT_OWNER',
                i_TableName             => 'IR_PERSON_PRELIM',
                i_UnusableOnly          => True,
                i_AllJoinedTables       => True,
                i_ParallelDegree        => 1
                );

strMessage01    := 'Gathering statistics for DLMRT_OWNER.IR_PERSON_PRELIM...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'DBMS_STATS.GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS
        (
                OWNNAME                 => 'DLMRT_OWNER',
                TABNAME                 => 'IR_PERSON_PRELIM',
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

END "PersonPrelim";
/
