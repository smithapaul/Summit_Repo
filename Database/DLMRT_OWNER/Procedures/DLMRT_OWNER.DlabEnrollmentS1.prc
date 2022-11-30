DROP PROCEDURE DLMRT_OWNER."DlabEnrollmentS1"
/

--
-- "DlabEnrollmentS1"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."DlabEnrollmentS1"
        (
                i_MartId                in  Varchar2    Default 'DLAB',
                i_ProcessName           in  Varchar2    Default 'EnrollmentS2',
                i_Institution           in  Varchar2
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Load table UPLOAD_S1 (UPLOAD_ID 'DLAB_ENROLLMENT_campus') from ENROLLMENT_campus_DLAB_EXT
-- where campus = AMH, BOS, DAR or LOW.
--
-- V02  SMT-8410 01/21/2020     Greg Kampf
--                              Set new column CENSUS_STATUS.APPROVED_FOR_CONFIRM to N.
--
-- V01  SMT-8358 11/21/2019     Greg Kampf
--                              Replace DlabEnrollmentBosS1 with this procedure which works for all campuses.
--
------------------------------------------------------------------------

        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(32767);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   := '';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strLineFeed                     Varchar2(2)     := chr(10);
        strCarriageReturn               Varchar2(2)     := chr(13);
        strQuote                        Varchar2(1)     := chr(39);
        strSqlCommand                   Varchar2(32767) := '';
        strSqlDynamic                   Varchar2(32767) := '';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intInsertCount                  Integer;
        intUpdateCount                  Integer;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;
        strControlRowExists             Varchar2(1);
        strPartitionExists              Varchar2(1);
        strCampusId                     Varchar2(3);
        strUploadType                   Varchar2(10)    := 'ENROLLMENT';
        strUploadId                     Varchar2(30);
        strPartitionName                Varchar2(128);
        strFileName                     Varchar2(4000);
        strFileNameNotFixed             Varchar2(4000);
        dtFileAsOfTime                  Date;
        strStatusRowExists              Varchar2(1);
        strLineBreakPlaceHolder         Varchar2(20)    := '<line break removed>';
        intCensusSequence               Integer;
        strTimeMask                     Varchar2(22)    := 'DD-MON-YYYY HH24:MI:SS';
        strProcessStartTime             Varchar2(20);
        strExternalTableName            Varchar2(128);


BEGIN
strSqlCommand   := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (i_ProcessName);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => i_MartId,
                i_ProcessName           => i_ProcessName,
                i_ProcessStartTime      => dtProcessStart
        );

strMessage01    := 'Procedure DLSTG_OWNER."EnrollmentS2" arguments:'
                || strNewLine || '       i_MartId: ' || i_MartId
                || strNewLine || '  i_ProcessName: ' || i_ProcessName
                || strNewLine || '  i_Institution: ' || i_Institution;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strProcessStartTime     := to_char(dtProcessStart,strTimeMask);
strCampusId             := substr(i_Institution,3,3);
strUploadId             := 'DLAB_ENROLLMENT_' || strCampusId;
strExternalTableName    := 'ENROLLMENT_' || strCampusId || '_DLAB_EXT';
strPartitionName        := 'UPLOAD_' || strUploadId;

strSqlCommand := 'SELECT strControlRowExists, strPartitionExists';
SELECT  CASE
          WHEN  UPLOAD_ID IS NULL
          THEN  'N'
          ELSE  'Y'
        END     CONTROL_ROW_EXISTS,
        CASE
          WHEN  PARTITION_NAME IS NULL
          THEN  'N'
          ELSE  'Y'
        END     PARTITION_EXISTS
  INTO  strControlRowExists,
        strPartitionExists
  FROM  (
        SELECT  (
                SELECT  CTL.UPLOAD_ID
                  FROM  COMMON_OWNER.UPLOAD_CONTROL CTL
                 WHERE  CTL.UPLOAD_ID  = strUploadId
                )                                       UPLOAD_ID
        FROM  DUAL
        ),
        (
        SELECT  (
                SELECT  PRT.PARTITION_NAME
                  FROM  ALL_TAB_PARTITIONS PRT
                 WHERE  PRT.TABLE_OWNER         = 'COMMON_OWNER'
                   AND  PRT.TABLE_NAME          = 'UPLOAD_S1'
                   AND  PRT.PARTITION_NAME      = strPartitionName
                )                                       PARTITION_NAME
        FROM  DUAL
        )
;

strSqlCommand   := 'Validate Upload';
If  strControlRowExists = 'N'
Then
        RAISE_APPLICATION_ERROR( -20001, 'Upload ' || strUploadId || ' does not exist in control table UPLOAD_CONTROL.');
End If;

strSqlCommand   := 'Validate partition existence';
If  strPartitionExists = 'N'
Then
        RAISE_APPLICATION_ERROR( -20001, 'Parition ' || strPartitionName || ' does not exist in table UPLOAD_S1.');
End If;

strSqlCommand   := 'SELECT strFileName';
SELECT  (
        SELECT  LOC.LOCATION
          FROM  ALL_EXTERNAL_LOCATIONS LOC
         WHERE  LOC.OWNER       = 'COMMON_OWNER'
           AND  LOC.TABLE_NAME  = strExternalTableName
        )       FILE_NAME
  INTO  strFileName
  FROM  DUAL;

strMessage01 := 'Name of COMMON_OWNER.' || strExternalTableName || strQuote || 's file: ' || strFileName;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strFileNameNotFixed     := replace(strFileName,'-Fixed','');

strSqlCommand   := 'UPDATE COMMON_OWNER.UPLOAD_CONTROL';
UPDATE  COMMON_OWNER.UPLOAD_CONTROL
   SET  UPLOAD_STATUS           = 'STARTED',
        LAST_UPLOAD_TIME        = dtProcessStart,
        UPDATED_BY              = i_ProcessName,
        UPDATE_TIME             = SYSDATE
 WHERE  UPLOAD_ID       = strUploadId
;

strSqlCommand   := 'COMMIT';
COMMIT;


strSqlCommand   := 'SELECT dtFileAsOfTime';
SELECT  (
        SELECT  MODIFIED_TIME
          FROM  COMMON_OWNER.CMN_FILES_LIST_VW
         WHERE  FILE_NAME = strFileName
        )       MODIFIED_TIME
  INTO  dtFileAsOfTime
  FROM  DUAL;

strMessage01    := 'Modified date/time of COMMON_OWNER.' || strExternalTableName || strQuote || 's file: ' || to_char(dtFileAsOfTime,strTimeMask);
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strMessage01    := 'Loading partition ' || strPartitionName || ' of table UPLOAD_S1';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'ALTER TABLE COMMON_OWNER.UPLOAD_S1 TRUNCATE PARTITION ' || strPartitionName || ' UPDATE INDEXES';
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
                i_TableOwner                    => 'COMMON_OWNER',
                i_TableName                     => 'UPLOAD_S1',
                i_IncludeJoinedTables           => False,
                i_IncludePartitionedIndexes     => True,
                i_PartitionName                 => strPartitionName,
                i_BitmapsOnly                   => True,
                i_IndexNameNotLike              => 'PK%'
                );

strSqlDynamic   := '
INSERT  /*+APPEND*/
  INTO  COMMON_OWNER.UPLOAD_S1
        (
        UPLOAD_ID,
        USER_ID,
        FILE_NAME,
        RECORD_NUMBER,
        INSERT_TIME,
        COLUMN_001,
        COLUMN_002,
        COLUMN_003,
        COLUMN_004,
        COLUMN_005,
        COLUMN_006,
        COLUMN_007,
        COLUMN_008,
        COLUMN_009,
        COLUMN_010,
        COLUMN_011,
        COLUMN_012,
        COLUMN_013,
        COLUMN_014,
        COLUMN_015,
        COLUMN_016,
        COLUMN_017,
        COLUMN_018,
        COLUMN_019,
        COLUMN_020,
        COLUMN_021,
        COLUMN_022,
        COLUMN_023,
        COLUMN_024,
        COLUMN_025,
        COLUMN_026,
        COLUMN_027,
        COLUMN_028,
        COLUMN_029,
        COLUMN_030,
        COLUMN_031,
        COLUMN_032,
        COLUMN_033,
        COLUMN_034,
        COLUMN_035,
        COLUMN_036,
        COLUMN_037,
        COLUMN_038,
        COLUMN_039,
        COLUMN_040,
        COLUMN_041,
        COLUMN_042,
        COLUMN_043,
        COLUMN_044,
        COLUMN_045,
        COLUMN_046,
        COLUMN_047,
        COLUMN_048,
        COLUMN_049,
        COLUMN_050,
        COLUMN_051,
        COLUMN_052,
        COLUMN_053,
        COLUMN_054,
        COLUMN_055,
        COLUMN_056,
        COLUMN_057,
        COLUMN_058,
        COLUMN_059,
        COLUMN_060,
        COLUMN_061,
        COLUMN_062,
        COLUMN_063,
        COLUMN_064,
        COLUMN_065,
        COLUMN_066,
        COLUMN_067,
        COLUMN_068
        )
SELECT
        ' || strQuote || strUploadId || strQuote || '                                                                   UPLOAD_ID,
        ' || strQuote || i_ProcessName || strQuote  || '                                                                USER_ID,
        ' || strQuote || strFileNameNotFixed || strQuote  || '                                                          FILE_NAME,
        rownum                                                                                                          RECORD_NUMBER,
        to_date(' || strQuote || strProcessStartTime || strQuote || ', ' || strQuote || strTimeMask || strQuote || ')   INSERT_TIME,
        INST_ORIG                       COLUMN_001,
        PERIOD_ORIG                     COLUMN_002,
        SEQ_ORIG                        COLUMN_003,
        PERSON_ID_ORIG                  COLUMN_004,
        GUID_ORIG                       COLUMN_005,
        BIRTHDATE_ORIG                  COLUMN_006,
        CENSUS_RUN_DATE_ORIG            COLUMN_007,
        SEX_ORIG                        COLUMN_008,
        CIT_ORIG                        COLUMN_009,
        NONUSCITCNTRY1_ORIG             COLUMN_010,
        NONUSCITCNTRY2_ORIG             COLUMN_011,
        NONUSCITCNTRY3_ORIG             COLUMN_012,
        RACE_FEDERAL_ORIG               COLUMN_013,
        RACE_STATE_ORIG                 COLUMN_014,
        URM_ORIG                        COLUMN_015,
        MILITARY_ORIG                   COLUMN_016,
        PERM_COUNTRY_2CHAR_ORIG         COLUMN_017,
        PERM_STATE_ORIG                 COLUMN_018,
        PERM_POSTAL_ORIG                COLUMN_019,
        PERM_COUNTY_ORIG                COLUMN_020,
        DORM_ORIG                       COLUMN_021,
        RESIDENCY_TUITION_ORIG          COLUMN_022,
        CAREER_ORIG                     COLUMN_023,
        STDNT_CAR_NUM_ORIG              COLUMN_024,
        STRM_ORIG                       COLUMN_025,
        CARLVL_ORIG                     COLUMN_026,
        PROGTYPE_ORIG                   COLUMN_027,
        EDLVL_ORIG                      COLUMN_028,
        DOCTYP_ORIG                     COLUMN_029,
        CESTDNT_ORIG                    COLUMN_030,
        REPIPEDS_ORIG                   COLUMN_031,
        ADMIT_TYPE_ORIG                 COLUMN_032,
        ADMITTERM_ORIG                  COLUMN_033,
        FIRSTGEN_ORIG                   COLUMN_034,
        NEW_ORIG                        COLUMN_035,
        COLLEGE_CODE_ORIG               COLUMN_036,
        COLLEGE_LONG_ORIG               COLUMN_037,
        MAJOR1_ORIG                     COLUMN_038,
        MAJOR_LONG1_ORIG                COLUMN_039,
        MAJOR_CIP_SHORT1_ORIG           COLUMN_040,
        MAJOR_TYPE_ORIG                 COLUMN_041,
        MAJOR_SUB_PLAN_TYPE1_ORIG       COLUMN_042,
        MAJOR_SUB_PLAN1_ORIG            COLUMN_043,
        MAJOR_SUB_PLAN_LONG1_ORIG       COLUMN_044,
        MAJOR_SUB_PLAN_CIP1_ORIG        COLUMN_045,
        ACADEMIC_LEVEL_TERM_BEGIN_ORIG  COLUMN_046,
        ACADEMIC_LEVEL_TERM_END_ORIG    COLUMN_047,
        EXP_GRAD_TERM_CD_ORIG           COLUMN_048,
        CREDITS_TOTAL_ORIG              COLUMN_049,
        GPA_CUMULATIVE_ORIG             COLUMN_050,
        FTPT_FLAG_ORIG                  COLUMN_051,
        CREDITS_TAKEN_ORIG              COLUMN_052,
        ONLINE_CREDITS_TAKEN_ORIG       COLUMN_053,
        FTFCREDITS_ORIG                 COLUMN_054,
        ONLINE_ONLY_ORIG                COLUMN_055,
        MIXED_ORIG                      COLUMN_056,
        CE_CREDITS_TAKEN_ORIG           COLUMN_057,
        DAY_CREDITS_TAKEN_ORIG          COLUMN_058,
        FTE_ORIG                        COLUMN_059,
        ONLINEFTE_ORIG                  COLUMN_060,
        CEFTE_ORIG                      COLUMN_061,
        NUM_CRS_ORIG                    COLUMN_062,
        NUM_CRS_ONLINE_ORIG             COLUMN_063,
        NUM_CRS_CE_ORIG                 COLUMN_064,
        CRSREG_ORIG                     COLUMN_065,
        CRSREG_ONLINE_ORIG              COLUMN_066,
        CRSREG_CE_ORIG                  COLUMN_067,
        PELL_RECIPIENT_ORIG             COLUMN_068
  FROM  COMMON_OWNER.' || strExternalTableName || ' EXT'
;

strMessage01    := 'Inserting UPLOAD_S1 rows...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: INSERT INTO COMMON_OWNER.UPLOAD_S1';
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
        (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 100,
                i_WaitSeconds           => 10,
                i_ShowStatement         => True,
                i_ShowSuccess           => True,
                i_RollbackOnRetry       => True,
                o_Tries                 => intTries
        );

intRowCount     := to_number(COMMON_OWNER.SMT_CONTEXT.GET_ATTRIBUTE(i_AttributeName => 'RowCount'));

strSqlCommand   := 'UPDATE COMMON_OWNER.UPLOAD_CONTROL';
UPDATE  COMMON_OWNER.UPLOAD_CONTROL
   SET  UPLOAD_STATUS           = 'SUCCESS',
        LAST_UPLOAD_TIME        = dtProcessStart,
        UPDATED_BY              = i_ProcessName,
        UPDATE_TIME             = SYSDATE
 WHERE  UPLOAD_ID       = strUploadId
;

strSqlCommand   := 'COMMIT';
COMMIT;

strMessage01    := '# of rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName       => 'UPLOAD_S1',
                i_Action                => 'INSERT',
                i_RowCount              => intRowCount,
                i_Comments              => 'Partition: ' || strPartitionName
        );

strSqlCommand   := 'SMT_INDEX.ALL_REBUILD';
COMMON_OWNER.SMT_INDEX.ALL_REBUILD
                (
                i_TableOwner            => 'COMMON_OWNER',
                i_TableName             => 'UPLOAD_S1',
                i_UnusableOnly          => True,
                i_AllJoinedTables       => False,
                i_ParallelDegree        => 4
                );

intInsertCount  := 0;
intUpdateCount  := 0;

strSqlCommand   := 'For recCencsusPeriod';
For recCencsusPeriod in
        (
        SELECT  INL.INSTITUTION,
                INL.CENSUS_PERIOD,
                INL.CENSUS_SEQ,
                COUNT(*)                UPLOAD_ROW_COUNT
          FROM  
                (
                SELECT  
                        COLUMN_001      INSTITUTION,
                        COLUMN_002      CENSUS_PERIOD,
                        COLUMN_003      CENSUS_SEQ
                  FROM  COMMON_OWNER.UPLOAD_S1  UPL
                 WHERE  UPL.UPLOAD_ID           = strUploadId
                   AND  UPL.RECORD_NUMBER      <> 1
                ) INL
        GROUP BY
                INL.INSTITUTION,
                INL.CENSUS_PERIOD,
                INL.CENSUS_SEQ
        ORDER BY
                INL.INSTITUTION,
                INL.CENSUS_PERIOD,
                INL.CENSUS_SEQ
        )
Loop
        Begin
                intCensusSequence       := to_number(recCencsusPeriod.CENSUS_SEQ);
                Exception
                  When  Others
                  Then  intCensusSequence := 0;
        End;
        strSqlCommand   := 'SELECT strStatusRowExists';
        SELECT  (
                NVL     (       (
                                SELECT  MAX('Y')
                                  FROM  DLMRT_OWNER.CENSUS_STATUS CST
                                 WHERE  CST.UPLOAD_TYPE         = strUploadType
                                   AND  CST.INSTITUTION         = recCencsusPeriod.INSTITUTION
                                   AND  CST.CENSUS_PERIOD       = recCencsusPeriod.CENSUS_PERIOD
                                   AND  CST.CENSUS_SEQ          = intCensusSequence
                                ),
                                'N'
                        )
                )       ROW_EXISTS
          INTO  strStatusRowExists
          FROM  DUAL;
        If  strStatusRowExists = 'Y'
        Then
                strMessage01    := 'Updating DLMRT_OWNER.CENSUS_STATUS (INSTITUTION:' || recCencsusPeriod.INSTITUTION || ' CENSUS_PERIOD:' || recCencsusPeriod.CENSUS_PERIOD || ' CENSUS_SEQ: ' || to_char(intCensusSequence);
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                strSqlCommand   := 'UPDATE DLMRT_OWNER.CENSUS_STATUS';
                UPDATE  DLMRT_OWNER.CENSUS_STATUS
                   SET  UPLOAD_ID               = strUploadId,
                        FILE_NAME               = strFileNameNotFixed,
                        UPLOAD_STATUS           = 'LOADED',
                        UPLOAD_TIME             = dtProcessStart,
                        APPROVED_FOR_CONFIRM    = 'N',
                        UPLOAD_ROW_COUNT        = recCencsusPeriod.UPLOAD_ROW_COUNT,
                        LAST_UPDATE_TIME        = SYSDATE,
                        LAST_UPDATE_BY          = i_ProcessName
                 WHERE  UPLOAD_TYPE     = strUploadType
                   AND  INSTITUTION     = recCencsusPeriod.INSTITUTION
                   AND  CENSUS_PERIOD   = recCencsusPeriod.CENSUS_PERIOD
                   AND  CENSUS_SEQ      = intCensusSequence
                ;
                intUpdateCount  := intUpdateCount + SQL%ROWCOUNT;
        Else
                strMessage01    := 'Inserting DLMRT_OWNER.CENSUS_STATUS row (INSTITUTION:' || recCencsusPeriod.INSTITUTION || ' CENSUS_PERIOD:' || recCencsusPeriod.CENSUS_PERIOD || ' CENSUS_SEQ: ' || to_char(intCensusSequence);
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                strSqlCommand   := 'INSERT DLMRT_OWNER.CENSUS_STATUS';
                INSERT
                  INTO  DLMRT_OWNER.CENSUS_STATUS
                        (
                        UPLOAD_TYPE,
                        INSTITUTION,
                        CENSUS_PERIOD,
                        CENSUS_SEQ,
                        FILE_NAME,
                        UPLOAD_ID,
                        UPLOAD_STATUS,
                        STAGE_STATUS,
                        PRELIM_STATUS,
                        CONFIRMED_STATUS,
                        APPROVED_FOR_CONFIRM,
                        UPLOAD_TIME,
                        STAGE_LOAD_TIME,
                        PRELIM_LOAD_TIME,
                        CONFIRMED_LOAD_TIME,
                        UPLOAD_ROW_COUNT,
                        STAGE_ROW_COUNT,
                        PRELIM_ROW_COUNT,
                        CONFIRMED_ROW_COUNT,
                        LAST_UPDATE_BY,
                        LAST_UPDATE_TIME
                        )
                SELECT
                        strUploadType                           UPLOAD_TYPE,
                        recCencsusPeriod.INSTITUTION            INSTITUTION,
                        recCencsusPeriod.CENSUS_PERIOD          CENSUS_PERIOD,
                        intCensusSequence                       CENSUS_SEQ,
                        strFileNameNotFixed                     FILE_NAME,
                        strUploadId                             UPLOAD_ID,
                        'LOADED'                                UPLOAD_STATUS,
                        Null                                    PRELIM_STATUS,
                        Null                                    STAGE_STATUS,
                        Null                                    CONFIRMED_STATUS,
                        'N'                                     APPROVED_FOR_CONFIRM,
                        dtProcessStart                          UPLOAD_TIME,
                        Null                                    STAGE_LOAD_TIME,
                        Null                                    PRELIM_LOAD_TIME,
                        Null                                    CONFIRMED_LOAD_TIME,
                        recCencsusPeriod.UPLOAD_ROW_COUNT       UPLOAD_ROW_COUNT,
                        Null                                    STAGE_ROW_COUNT,
                        Null                                    PRELIM_ROW_COUNT,
                        Null                                    CONFIRMED_ROW_COUNT,
                        i_ProcessName                           LAST_UPDATE_BY,
                        SYSDATE                                 LAST_UPDATE_TIME
                  FROM  DUAL
                ;
                intInsertCount  := intInsertCount + SQL%ROWCOUNT;
        End If;
End Loop; -- recCencsusPeriod

strSqlCommand   := 'COMMIT';
COMMIT;

strMessage01    := '# of rows updated: ' || TO_CHAR(intUpdateCount);
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strMessage01    := '# of rows inserted: ' || TO_CHAR(intInsertCount);
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'CENSUS_STATUS',
                i_Action            => 'UPDATE',
                i_RowCount          => intUpdateCount
        );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'CENSUS_STATUS',
                i_Action            => 'INSERT',
                i_RowCount          => intInsertCount
        );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := i_ProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN

        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END "DlabEnrollmentS1";
/
