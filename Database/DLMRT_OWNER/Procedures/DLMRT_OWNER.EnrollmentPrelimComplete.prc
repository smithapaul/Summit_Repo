DROP PROCEDURE DLMRT_OWNER."EnrollmentPrelimComplete"
/

--
-- "EnrollmentPrelimComplete"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."EnrollmentPrelimComplete"
        (
                i_MartId                        in  Varchar2    Default 'DLAB',
                i_ProcessName                   in  Varchar2    Default 'EnrollmentPrelimComplete',
                i_Institution                   in  Varchar2,
                i_ProcessNameInterface          in  Varchar2
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Completes the enrollment preliminary loads by updating the associated CENSUS_STATUS rows.
--
-- V04  CASE-28490      04/24/2020      Greg Kampf
--                                      Set new columns in CENSUS_STATUS
--                                      o FILE_NAME_PRELIM
--                                      o UPLOAD_TIME_PRELIM
-- V03  SMT-8410 01/21/2020     Greg Kampf
--                              Set new column CENSUS_STATUS.APPROVED_FOR_CONFIRM to N.
--
-- V02  SMT-8358 11/12/2019     Greg Kampf
--                              Remove notification.  That is no done in a separate procedure.
--
-- V01  SMT-8358 11/07/2019     Greg Kampf
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
        strUploadId                     Varchar2(19);
        strUploadType                   Varchar2(10)    := 'ENROLLMENT';
        strAdditionalMessage            Varchar2(2000);
        intCensusCountSuccess           Integer         := 0;
        intCensusCountReject            Integer         := 0;

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

strMessage01    := 'Procedure DLMRT_OWNER."EnrollmentPrelimComplete" arguments:'
                || strNewLine || '                     i_MartId: ' || i_MartId
                || strNewLine || '                i_ProcessName: ' || i_ProcessName
                || strNewLine || '                i_Institution: ' || i_Institution
                || strNewLine || '       i_ProcessNameInterface: ' || i_ProcessNameInterface;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strCampusId     := substr(i_Institution,3,3);
strUploadId     := 'DLAB_ENROLLMENT_' || strCampusId;
strPartitionName:= 'INST_' || i_Institution;

intUpdateCount  := 0;

strSqlCommand   := 'For recCensusPeriod';
For recCensusPeriod in
        (
        SELECT  CST.INSTITUTION,
                CST.CENSUS_PERIOD,
                CST.CENSUS_SEQ,
                MAX(CST.STAGE_STATUS)   STAGE_STATUS,
                MAX(CST.UPLOAD_TIME)    UPLOAD_TIME,
                'LOADED'                PRELIM_STATUS,
                COUNT(*)                PRELIM_ROW_COUNT
          FROM  DLMRT_OWNER.IR_STUDENT_ENROL_PRELIM     PLM,
                DLMRT_OWNER.CENSUS_STATUS               CST
         WHERE  CST.UPLOAD_TYPE                 = strUploadType
           AND  CST.READY_FOR_PRELIM            = 'Y'
           AND  CST.INSTITUTION                 = i_Institution
           AND  PLM.INSTITUTION                 = CST.INSTITUTION
           AND  PLM.CENSUS_PERIOD               = CST.CENSUS_PERIOD
           AND  PLM.CENSUS_SEQ                  = CST.CENSUS_SEQ
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
        strMessage01    := 'Updating DLMRT_OWNER.CENSUS_STATUS (INSTITUTION:' || recCensusPeriod.INSTITUTION || ' CENSUS_PERIOD:' || recCensusPeriod.CENSUS_PERIOD || ' CENSUS_SEQ: ' || to_char(recCensusPeriod.CENSUS_SEQ);
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

        strSqlCommand   := 'UPDATE DLMRT_OWNER.CENSUS_STATUS';
        UPDATE  DLMRT_OWNER.CENSUS_STATUS
           SET  PRELIM_STATUS           = recCensusPeriod.PRELIM_STATUS,
                APPROVED_FOR_CONFIRM    = 'N',
                FILE_NAME_PRELIM        = FILE_NAME,
                UPLOAD_TIME_PRELIM      = UPLOAD_TIME,
                PRELIM_LOAD_TIME        = dtProcessStart,
                PRELIM_ROW_COUNT        = recCensusPeriod.PRELIM_ROW_COUNT,
                LAST_UPDATE_TIME        = SYSDATE,
                LAST_UPDATE_BY          = i_ProcessName
         WHERE  UPLOAD_TYPE     = strUploadType
           AND  INSTITUTION     = recCensusPeriod.INSTITUTION
           AND  CENSUS_PERIOD   = recCensusPeriod.CENSUS_PERIOD
           AND  CENSUS_SEQ      = recCensusPeriod.CENSUS_SEQ
        ;
        intUpdateCount          := intUpdateCount + SQL%ROWCOUNT;
        intCensusCountSuccess   := intCensusCountSuccess + 1;
End Loop; -- recCensusPeriod

strSqlCommand   := 'COMMIT';
COMMIT;

strMessage01    := '# of rows updated: ' || TO_CHAR(intUpdateCount);
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'CENSUS_STATUS',
                i_Action            => 'UPDATE',
                i_RowCount          => intUpdateCount
        );

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

END "EnrollmentPrelimComplete";
/
