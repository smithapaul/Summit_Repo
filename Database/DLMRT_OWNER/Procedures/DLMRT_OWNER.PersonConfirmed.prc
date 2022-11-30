DROP PROCEDURE DLMRT_OWNER."PersonConfirmed"
/

--
-- "PersonConfirmed"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."PersonConfirmed"
        (
                i_MartId                        in  Varchar2    Default 'DLAB',
                i_ProcessName                   in  Varchar2    Default 'PersonConfirmed',
                i_Institution                   in  Varchar2
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Loads table DLMRT_OWNER.IR_PERSON_CONFIRMED
--
-- V05  CASE-23700      03/21/2020      Greg Kampf
--                                      Support new columns RESIDENCY_CODE_INOUTSTATE, RESIDENCY_DESC_INOUTSTATE
--
-- V04  SMT-8410 01/22/2020     Greg Kampf
--                              Added APPROVED_FOR_CONFIRM = Y to CENSUS_STATUS criteria.
--
-- V03  SMT-8358 11/20/2019     Greg Kampf
--                              Limit the data loaded from the preliminary to just the periods
--                              that table CENSUS_STATUS indicates are ready for confirm.
--                              Necessary when the data in the preliminary table is out-of-synce with
--                              table CENSUS_STATUS.
--
-- V02  SMT-8358 11/07/2019     Greg Kampf
--                              Use table CENSUS_STATUS to control which census data to move
--                              from prelim to IR_PERSON_CONFIRMED.
--
-- V01  SMT-8358 10/21/2019     Greg Kampf
--
------------------------------------------------------------------------

        dtProcessStart                  Date            := SYSDATE;
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
                i_ProcessStartTime      => dtProcessStart
        );

strMessage01    := 'Procedure DLMRT_OWNER."PersonConfirmed" arguments:'
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
                        i_TableName                     => 'IR_PERSON_CONFIRMED',
                        i_PartitionName                 => strPartitionName,
                        i_TestMode                      => False,
                        i_PartitionValuesList           => strPartitionValuesList
                );

        strSqlDynamic   := 'ALTER TABLE DLMRT_OWNER.IR_PERSON_CONFIRMED TRUNCATE PARTITION "' || strPartitionName || '" UPDATE INDEXES';
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
                        i_TableName                     => 'IR_PERSON_CONFIRMED',
                        i_IncludeJoinedTables           => True,
                        i_IncludePartitionedIndexes     => True,
                        i_PartitionName                 => strPartitionName,
                        i_BitmapsOnly                   => True,
                        i_IndexNameNotLike              => 'PK%'
                );
End Loop; -- recPartition

strMessage01 := 'Inserting DLMRT_OWNER.IR_PERSON_CONFIRMED rows...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'INSERT INTO DLMRT_OWNER.IR_PERSON_CONFIRMED';
INSERT  /*+APPEND*/
  INTO  DLMRT_OWNER.IR_PERSON_CONFIRMED
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
        PLM.INSTITUTION,
        PLM.CAMPUS,
        PLM.CENSUS_PERIOD,
        PLM.CENSUS_PERIOD_DESCR,
        PLM.CENSUS_SEQ,
        PLM.PERSON_ID,
        PLM.UMASS_GUID,
        PLM.BIRTH_DATE,
        PLM.CAMPUS_CENSUS_DATE,
        PLM.IPEDS_AGE_RANGE_CODE,
        PLM.IPEDS_AGE_RANGE_DESCR,
        PLM.GENDER_CODE,
        PLM.GENDER_DESCR,
        PLM.US_CITIZENSHIP_CODE,
        PLM.US_CITIZENSHIP_STATUS,
        PLM.COUNTRY_2CHAR_NON_US,
        PLM.COUNTRY_DESCR_NON_US,
        PLM.FED_ETHNICITY_CODE,
        PLM.FED_ETHNICITY_DESCR,
        PLM.STATE_ETHNICITY_CODE,
        PLM.STATE_ETHNICITY_DESCR,
        PLM.UNDER_REPRESENTED_MINORITY,
        PLM.MILITARY_STATUS,
        PLM.COUNTRY_2CHAR_PERM_ADDRESS,
        PLM.COUNTRY_DESCR_PERM_ADDRESS,
        PLM.STATE_PERM_ADDRESS,
        PLM.POSTAL_CODE_PERM_ADDRESS,
        PLM.COUNTY_PERM_ADDRESS,
        PLM.CAMPUS_RESIDENT,
        PLM.RESIDENCY_CODE_SA_TUITION,
        PLM.RESIDENCY_DESC_SA_TUITION,
        PLM.RESIDENCY_CODE_TUITION,
        PLM.RESIDENCY_DESC_TUITION,
        PLM.RESIDENCY_CODE_INOUTSTATE,
        PLM.RESIDENCY_DESC_INOUTSTATE,
        SYSDATE                         INSERT_TIME
  FROM  DLMRT_OWNER.IR_PERSON_PRELIM    PLM,
        DLMRT_OWNER.CENSUS_STATUS       CST
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
                i_TableName             => 'IR_PERSON_CONFIRMED',
                i_UnusableOnly          => True,
                i_AllJoinedTables       => True,
                i_ParallelDegree        => 1
                );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'IR_PERSON_CONFIRMED',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Gathering statistics for DLMRT_OWNER.IR_PERSON_CONFIRMED...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'DBMS_STATS.GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS
        (
                OWNNAME                 => 'DLMRT_OWNER',
                TABNAME                 => 'IR_PERSON_CONFIRMED',
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

END "PersonConfirmed";
/
