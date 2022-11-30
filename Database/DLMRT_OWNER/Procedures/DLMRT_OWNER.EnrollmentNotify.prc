DROP PROCEDURE DLMRT_OWNER."EnrollmentNotify"
/

--
-- "EnrollmentNotify"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."EnrollmentNotify"
        (
                i_MartId                        in  Varchar2    Default 'DLAB',
                i_ProcessName                   in  Varchar2    Default 'EnrollmentNotify',
                i_Institution                   in  Varchar2,
                i_ProcessNameInterface          in  Varchar2
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Notify users by email of preliminary and failed enrollment uploads.
--
-- V04  CASE-28490      04/24/2020      Greg Kampf
--                                      Make use of new columns for accurate reporting of
--                                      rejected files that have periods that are already
--                                      in the preliminary tables.
--                                      o FILE_NAME_PRELIM
--                                      o UPLOAD_TIME_PRELIM
--
-- V03  SMT-8358 11/21/2019     Greg Kampf
--                              Increase size of reject row count edit mask.
--
-- V02  SMT-8358 11/13/2019     Greg Kampf
--                              Add file name to message.
--
-- V01  SMT-8358 11/12/2019     Greg Kampf
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
        strAdditionalMessage            Varchar2(4000);
        intCensusCountSuccess           Integer         := 0;
        intCensusCountReject            Integer         := 0;
        strSuccessList                  Varchar2(4000)  := '';
        strRejectList                   Varchar2(4000)  := '';

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

strMessage01    := 'Procedure DLMRT_OWNER."EnrollmentNotify" arguments:'
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
                MAX(CST.FILE_NAME_PRELIM)       FILE_NAME_PRELIM,
                MAX(CST.STAGE_STATUS)           STAGE_STATUS,
                MAX(CST.UPLOAD_TIME_PRELIM)     UPLOAD_TIME_PRELIM,
                'LOADED'                        PRELIM_STATUS,
                COUNT(*)                        PRELIM_ROW_COUNT
          FROM  DLMRT_OWNER.IR_STUDENT_ENROL_PRELIM     PLM,
                DLMRT_OWNER.CENSUS_STATUS               CST
         WHERE  CST.UPLOAD_TYPE                 = strUploadType
           AND  CST.READY_FOR_CONFIRM           = 'Y'
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
        strSuccessList  := strSuccessList
                        || strNewLine   || recCensusPeriod.CENSUS_PERIOD || '-' || to_char(recCensusPeriod.CENSUS_SEQ)
                        || '   Upload Time: ' || to_char(recCensusPeriod.UPLOAD_TIME_PRELIM,'DD-MON-YYYY HH24:MI:SS')
                        || '   Row Count: ' || to_char(recCensusPeriod.PRELIM_ROW_COUNT, '999,999')
                        || '   File: ' || recCensusPeriod.FILE_NAME_PRELIM;

        intCensusCountSuccess   := intCensusCountSuccess + 1;
End Loop; -- recCensusPeriod

strSqlCommand   := 'For recRejectPeriod';
For recRejectPeriod in
        (
        SELECT  CST.INSTITUTION,
                CST.CENSUS_PERIOD,
                CST.CENSUS_SEQ,
                CST.STAGE_STATUS,
                CST.FILE_NAME,
                CST.UPLOAD_TIME,
                CST.UPLOAD_ROW_COUNT - CST.STAGE_ROW_COUNT      REJECT_ROW_COUNT
          FROM  DLMRT_OWNER.CENSUS_STATUS CST
         WHERE  CST.UPLOAD_TYPE         = strUploadType
           AND  CST.READY_FOR_PRELIM    = 'N'
           AND  CST.INSTITUTION         = i_Institution
           AND  CST.STAGE_STATUS        = 'REJECTED'
        ORDER BY
                CST.INSTITUTION,
                CST.CENSUS_PERIOD,
                CST.CENSUS_SEQ
        )
Loop
        strRejectList   := strRejectList
                        || strNewLine   || recRejectPeriod.CENSUS_PERIOD || '-' || to_char(recRejectPeriod.CENSUS_SEQ)
                        || '   Upload Time: ' || to_char(recRejectPeriod.UPLOAD_TIME,'DD-MON-YYYY HH24:MI:SS') || '   Reject Row Count: ' || to_char(recRejectPeriod.REJECT_ROW_COUNT, '999,999') || '   File: ' || recRejectPeriod.FILE_NAME;

        intCensusCountReject    := intCensusCountReject + 1;
End Loop; -- recRejectPeriod

If  intCensusCountSuccess > 0
Then
        strAdditionalMessage    := strNewLine || strNewLine || 'Preliminary data is available for review.' || strNewLine
                                || strNewLine || 'Periods with successfully loaded preliminary data:'
                                || strNewLine || strSuccessList;
End If;

If  intCensusCountReject > 0
Then
        strAdditionalMessage    := strAdditionalMessage
                                || strNewLine || strNewLine || 'Rejected periods:'
                                || strNewLine || strRejectList;
End If;

strSqlCommand   := 'SMT_INTERFACE.INTERFACE_NOTIFY';
COMMON_OWNER.SMT_INTERFACE.INTERFACE_NOTIFY
        (
        i_MartId                => i_MartId,
        i_ProcessNameInterface  => i_ProcessNameInterface,
        i_UploadId              => strUploadId,
        i_AdditionalMessage     => strAdditionalMessage
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

END "EnrollmentNotify";
/
