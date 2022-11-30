DROP PROCEDURE DLMRT_OWNER."ConfirmApprove"
/

--
-- "ConfirmApprove"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."ConfirmApprove"
        (
                i_MartId                        in  Varchar2    Default 'DlabOd',
                i_ProcessName                   in  Varchar2    Default 'ConfirmApprove',
                i_UploadType                    in  Varchar2    Default 'ENROLLMENT',
                i_Institution                   in  Varchar2,
                i_CensusList                    in  Varchar2
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Approves census periods for confirmation by updating the CENSUS_STATUS.APPROVED_FOR_CONFIRM
-- column to Y.
--
-- i_CensusList contains a comma separated list of census persiods and sequences in format PPPPP-SEQ where
-- PPPPP is the census period and SEQ is the sequence number.
--
-- V01  SMT-8410 01/27/2020     Greg Kampf
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
        strStatusRowExists              Varchar2(1);
        intCensusSeq                    Integer;
        strReadyForConfirm              Varchar2(1);

BEGIN
strSqlCommand   := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (i_ProcessName);

COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => i_MartId,
                i_ProcessName           => i_ProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Procedure DLMRT_OWNER."ConfirmApprove" arguments:'
                || strNewLine || '       i_MartId: ' || i_MartId
                || strNewLine || '  i_ProcessName: ' || i_ProcessName
                || strNewLine || '   i_UploadType: ' || i_UploadType
                || strNewLine || '  i_Institution: ' || i_Institution
                || strNewLine || '   i_CensusList: ' || i_CensusList;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

-- Validations
strSqlCommand := 'Validate i_UploadType';
If  i_UploadType <> 'ENROLLMENT'
Then
        RAISE_APPLICATION_ERROR( -20001, 'Invalid i_UploadType value "' || i_UploadType || '", must be "ENROLLMENT"');
End If;

strSqlCommand := 'Validate i_Institution';
If  i_Institution not in ('UMAMH', 'UMBOS', 'UMDAR', 'UMLOW')
Then
        RAISE_APPLICATION_ERROR( -20001, 'Invalid i_Institution value "' || i_Institution || '", must be "UMAMH", "UMBOS", "UMDAR" or "UMLOW".');
End If;

strSqlCommand := 'For recCensusValidation';
For recCensusValidation in
        (
        SELECT  INL.CENSUS,
                SUBSTR(INL.CENSUS,1,5)                          CENSUS_PERIOD,
                SUBSTR(INL.CENSUS,7,LENGTH(INL.CENSUS) - 6)     CENSUS_SEQ_STR
          FROM  (
                select  regexp_substr(i_CensusList,'[^,]+', 1, level) "CENSUS"
                  from  dual 
                connect BY regexp_substr(i_CensusList, '[^,]+', 1, level) 
                        is not null
                ) INL
        )
Loop
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => 'Validating census ' || recCensusValidation.CENSUS || '...');
        
        Begin
                intCensusSeq    := to_number(recCensusValidation.CENSUS_SEQ_STR);
        Exception
            When Others Then
                intCensusSeq    := Null;
        End;
        strSqlCommand   := 'Validate census sequence';
        If  intCensusSeq is Null
        Then
                RAISE_APPLICATION_ERROR(-20001, 'Could not determine census sequence.  Invalid format in i_CensusList element "' || recCensusValidation.CENSUS || '".');
        End If;

        strSqlCommand   := 'Lookup Census';
        SELECT  (
                SELECT  READY_FOR_CONFIRM
                  FROM  DLMRT_OWNER.CENSUS_STATUS CEN
                 WHERE  CEN.UPLOAD_TYPE         = i_UploadType
                   AND  CEN.INSTITUTION         = i_Institution
                   AND  CEN.CENSUS_PERIOD       = recCensusValidation.CENSUS_PERIOD
                   AND  CEN.CENSUS_SEQ          = intCensusSeq
                )       READY_FOR_CONFIRM
          INTO  strReadyForConfirm
          FROM  DUAL;

        strSqlCommand   := 'Validate census existence';
        If  strReadyForConfirm is null
        Then
                RAISE_APPLICATION_ERROR(-20001, 'Census ' || i_UploadType || '/' || i_Institution || '/' || recCensusValidation.CENSUS || ' does not exist.');
        End If;

        strSqlCommand   := 'Validate census ready for confirm';
        If  strReadyForConfirm <> 'Y'
        Then
                RAISE_APPLICATION_ERROR(-20001, 'Census ' || i_UploadType || '/' || i_Institution || '/' || recCensusValidation.CENSUS || ' is not ready form confirm.');
        End If;

        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => 'Census validated');
End Loop; -- recCensusValidation

For recCensusApprove in
        (
        SELECT  INL.CENSUS,
                SUBSTR(INL.CENSUS,1,5)                          CENSUS_PERIOD,
                SUBSTR(INL.CENSUS,7,LENGTH(INL.CENSUS) - 6)     CENSUS_SEQ_STR
          FROM  (
                select  regexp_substr(i_CensusList,'[^,]+', 1, level) "CENSUS"
                  from  dual 
                connect BY regexp_substr(i_CensusList, '[^,]+', 1, level) 
                        is not null
                ) INL
        )
Loop
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => 'Approving census ' || recCensusApprove.CENSUS || ' for confirmation...');

        intCensusSeq    := to_number(recCensusApprove.CENSUS_SEQ_STR);

        strSqlCommand   := 'UPDATE DLMRT_OWNER.CENSUS_STATUS';
        UPDATE  DLMRT_OWNER.CENSUS_STATUS
           SET  APPROVED_FOR_CONFIRM    = 'Y',
                LAST_UPDATE_TIME        = SYSDATE,
                LAST_UPDATE_BY          = i_ProcessName
         WHERE  UPLOAD_TYPE     = i_UploadType
           AND  INSTITUTION     = i_Institution
           AND  CENSUS_PERIOD   = recCensusApprove.CENSUS_PERIOD
           AND  CENSUS_SEQ      = intCensusSeq
        ;
        intUpdateCount  := intUpdateCount + SQL%ROWCOUNT;

        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => 'Census approved');
End Loop; -- recCensusApprove

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
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END "ConfirmApprove";
/
