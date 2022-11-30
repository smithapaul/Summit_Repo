DROP PROCEDURE DLMRT_OWNER."EnrollmentConfirmComplete"
/

--
-- "EnrollmentConfirmComplete"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."EnrollmentConfirmComplete"
        (
                i_MartId                        in  Varchar2    Default 'DLAB',
                i_ProcessName                   in  Varchar2    Default 'EnrollmentConfirmComplete',
                i_Institution                   in  Varchar2
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Completes the enrollment confirmation by truncating the data's partitions in the _PRELIM tables
-- and updating the associated CENSUS_STATUS rows.
--
-- V03  CASE-28490      04/24/2020      Greg Kampf
--                                      Set new columns in CENSUS_STATUS
--                                      o FILE_NAME_CONFIRM
--                                      o UPLOAD_TIME_CONFIRM
--
-- V02  SMT-8410 01/21/2020     Greg Kampf
--                              Added APPROVED_FOR_CONFIRM = Y to CENSUS_STATUS criteria.
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
        intUpdateCount                  Integer         := 0;
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
        strStatusRowExists              Varchar2(1);

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

strMessage01    := 'Procedure DLMRT_OWNER."EnrollmentConfirmComplete" arguments:'
                || strNewLine || '                     i_MartId: ' || i_MartId
                || strNewLine || '                i_ProcessName: ' || i_ProcessName
                || strNewLine || '                i_Institution: ' || i_Institution;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

-- Now that it has been confirmed get rid of the data from the preliminary table.
strSqlCommand   := 'For recPartition';
For recPrelimPartition in
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
        strPartitionName        := recPrelimPartition.INSTITUTION || '_' || recPrelimPartition.CENSUS_PERIOD || '_' || TO_CHAR(recPrelimPartition.CENSUS_SEQ);

        strSqlDynamic   := 'ALTER TABLE DLMRT_OWNER.IR_STUDENT_ENROL_PRELIM TRUNCATE PARTITION "' || strPartitionName || '" UPDATE INDEXES';
        strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
        COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                        i_SqlStatement                  => strSqlDynamic,
                        i_MaxTries                      => 10,
                        i_WaitSeconds                   => 10,
                        o_Tries                         => intTries
                );
End Loop; -- recPrelimPartition


intInsertCount  := 0;
intUpdateCount  := 0;

strSqlCommand   := 'For recCensusPeriod';
For recCensusPeriod in
        (
        SELECT  CST.INSTITUTION,
                CST.CENSUS_PERIOD,
                CST.CENSUS_SEQ,
                'REMOVED'       PRELIM_STATUS,
                'LOADED'        CONFIRMED_STATUS,
                COUNT(*)        CONFIRMED_ROW_COUNT
          FROM  DLMRT_OWNER.IR_STUDENT_ENROL_CONFIRMED  CNF,
                DLMRT_OWNER.CENSUS_STATUS               CST
         WHERE  CST.UPLOAD_TYPE                 = strUploadType
           AND  CST.READY_FOR_CONFIRM           = 'Y'
           AND  CST.APPROVED_FOR_CONFIRM        = 'Y'
           AND  CST.INSTITUTION                 = i_Institution
           AND  CNF.INSTITUTION                 = CST.INSTITUTION
           AND  CNF.CENSUS_PERIOD               = CST.CENSUS_PERIOD
           AND  CNF.CENSUS_SEQ                  = CST.CENSUS_SEQ
        GROUP BY
                CST.INSTITUTION,
                CST.CENSUS_PERIOD,
                CST.CENSUS_SEQ
        ORDER BY
                CST.INSTITUTION,
                CST.CENSUS_PERIOD,
                CST.CENSUS_SEQ
        )
Loop
        strSqlCommand   := 'SELECT strStatusRowExists';
        SELECT  (
                NVL     (       (
                                SELECT  MAX('Y')
                                  FROM  DLMRT_OWNER.CENSUS_STATUS CST
                                 WHERE  CST.UPLOAD_TYPE         = strUploadType
                                   AND  CST.INSTITUTION         = recCensusPeriod.INSTITUTION
                                   AND  CST.CENSUS_PERIOD       = recCensusPeriod.CENSUS_PERIOD
                                   AND  CST.CENSUS_SEQ          = recCensusPeriod.CENSUS_SEQ
                                ),
                                'N'
                        )
                )       ROW_EXISTS
          INTO  strStatusRowExists
          FROM  DUAL;
        If  strStatusRowExists = 'Y'
        Then
                strMessage01    := 'Updating DLMRT_OWNER.CENSUS_STATUS (INSTITUTION:' || recCensusPeriod.INSTITUTION || ' CENSUS_PERIOD:' || recCensusPeriod.CENSUS_PERIOD || ' CENSUS_SEQ: ' || to_char(recCensusPeriod.CENSUS_SEQ);
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                strSqlCommand   := 'UPDATE DLMRT_OWNER.CENSUS_STATUS';
                UPDATE  DLMRT_OWNER.CENSUS_STATUS
                   SET  PRELIM_STATUS           = recCensusPeriod.PRELIM_STATUS,
                        CONFIRMED_STATUS        = recCensusPeriod.CONFIRMED_STATUS,
                        CONFIRMED_LOAD_TIME     = dtProcessStart,
                        CONFIRMED_ROW_COUNT     = recCensusPeriod.CONFIRMED_ROW_COUNT,
                        FILE_NAME_CONFIRM       = FILE_NAME_PRELIM,
                        UPLOAD_TIME_CONFIRM     = UPLOAD_TIME_PRELIM,
                        LAST_UPDATE_TIME        = SYSDATE,
                        LAST_UPDATE_BY          = i_ProcessName
                 WHERE  UPLOAD_TYPE     = strUploadType
                   AND  INSTITUTION     = recCensusPeriod.INSTITUTION
                   AND  CENSUS_PERIOD   = recCensusPeriod.CENSUS_PERIOD
                   AND  CENSUS_SEQ      = recCensusPeriod.CENSUS_SEQ
                ;
                intUpdateCount  := intUpdateCount + SQL%ROWCOUNT;
        Else
                strMessage01    := 'Inserting DLMRT_OWNER.CENSUS_STATUS row (INSTITUTION:' || recCensusPeriod.INSTITUTION || ' CENSUS_PERIOD:' || recCensusPeriod.CENSUS_PERIOD || ' CENSUS_SEQ: ' || to_char(recCensusPeriod.CENSUS_SEQ);
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                strSqlCommand   := 'INSERT DLMRT_OWNER.CENSUS_STATUS';
                INSERT
                  INTO  DLMRT_OWNER.CENSUS_STATUS
                        (
                        UPLOAD_TYPE,
                        INSTITUTION,
                        CENSUS_PERIOD,
                        CENSUS_SEQ,
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
                        recCensusPeriod.INSTITUTION             INSTITUTION,
                        recCensusPeriod.CENSUS_PERIOD           CENSUS_PERIOD,
                        recCensusPeriod.CENSUS_SEQ              CENSUS_SEQ,
                        Null                                    UPLOAD_ID,
                        Null                                    UPLOAD_STATUS,
                        Null                                    STAGE_STATUS,
                        recCensusPeriod.PRELIM_STATUS           PRELIM_STATUS,
                        recCensusPeriod.CONFIRMED_STATUS        CONFIRMED_STATUS,
                        Null                                    APPROVED_FOR_CONFIRM,
                        Null                                    UPLOAD_TIME,
                        Null                                    STAGE_LOAD_TIME,
                        Null                                    PRELIM_LOAD_TIME,
                        dtProcessStart                          CONFIRMED_LOAD_TIME,
                        Null                                    UPLOAD_ROW_COUNT,
                        Null                                    STAGE_ROW_COUNT,
                        Null                                    PRELIM_ROW_COUNT,
                        recCensusPeriod.CONFIRMED_ROW_COUNT     CONFIRMED_ROW_COUNT,
                        i_ProcessName                           LAST_UPDATE_BY,
                        SYSDATE                                 LAST_UPDATE_TIME
                  FROM  DUAL
                ;
                intInsertCount  := intInsertCount + SQL%ROWCOUNT;
        End If;
End Loop; -- recCensusPeriod

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

END "EnrollmentConfirmComplete";
/
