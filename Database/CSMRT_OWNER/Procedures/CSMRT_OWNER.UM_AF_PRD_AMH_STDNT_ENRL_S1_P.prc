CREATE OR REPLACE PROCEDURE             "UM_AF_PRD_AMH_STDNT_ENRL_S1_P"
        (
                i_ContextBlock          in  Varchar2    Default Null,
                i_UploadId              in  Varchar2,
                i_ExpectedRowCount      in  Varchar2    Default Null,
                o_Status                out Varchar2,
                o_RowCountSuccess       out Integer,
                o_RowCountFail          out Integer,
                o_ErrorCount            out Integer
        )
IS

------------------------------------------------------------------------
--
-- Validates load of table COMMON_OWNER.UPLOAD_S1 for Amherst student enrollment data.
--
-- V01  SMT-xxxx 01/05/2021,    George Adams
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_AF_PRD_AMH_STDNT_ENRL_S1_P';
        dtProcessStart                  Date            := SYSDATE;
        dtParentProcessStartTime        Date;
        intProcessSid                   Integer;
        strMessage01                    Varchar2(32767);
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) := '';
        strSqlDynamic                   Varchar2(32767) := '';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        intHeaderRowCount               Integer         := 0;
        intInsertCount                  Integer         := 0;
        intFailedRowCount               Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(32767);
        intTries                        Integer;
        intYear                         Integer;
        strControlRowExists             Varchar2(1);
        strCarriageReturn               Varchar2(1)     := chr(13);
--        rtpTarget                       FSSTG_OWNER.BUYWAYS_AUTH_S2%ROWTYPE; -- Creates a record with columns matching those in the target table
        intErrorCount                   Integer         := 0;
        intRowNum                       Integer;
        bolError                        Boolean;
        bolFatalError                   Boolean         := False;
        intFailedRowMax                 Integer         := 10;
        strDataTimestampRowExists       Varchar2(1);
        strDocGUID                      Varchar2(50);
        strFileName                     Varchar2(256);
        dtFileAsOfTime                  Date            := SYSDATE;
        numErrorPercent                 Number;
        strTimeMask                     Varchar2(22)    := 'DD-MON-YYYY HH24:MI:SS';
        intExpectedRowCount             Integer;
        strHeadingUserName              Varchar2(4000);
        strHeadingFirstName             Varchar2(4000);
        strHeadingLastName              Varchar2(4000);
        strHeadingPhoneNumber           Varchar2(4000);
        strHeadingAreaCode              Varchar2(4000);
        strHeadingPhoneNoAreaCode       Varchar2(4000);
        strHeadingPhoneExtension        Varchar2(4000);
        strHeadingEmailAddress          Varchar2(4000);
        strHeadingBusinessUnit          Varchar2(4000);
        strHeadingDepartment            Varchar2(4000);
        strHeadingPosition              Varchar2(4000);
        strHeadingUserStatus            Varchar2(4000);
        strHeadingAutomaticRoles        Varchar2(4000);
        strHeadingAssignedRoles         Varchar2(4000);
        bolPrematureExit                Boolean         := False;

BEGIN

If  i_ContextBlock is null
Then
        strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_INIT';
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
                (
                        i_MartId                => strMartId,
                        i_ProcessName           => strProcessName,
                        i_ProcessStartTime      => dtProcessStart
                );
Else
        strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_CONTEXT';
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_CONTEXT
                (
                        i_ContextBlock          => i_ContextBlock
                );
End If;

strMessage01    := 'Procedure CSMRT_OWNER.' || strProcessName || ' input arguments:'
                || strNewLine || '    i_ContextBlock: ' || i_ContextBlock
                || strNewLine || '        i_UploadId: ' || i_UploadId
                || strNewLine || 'i_ExpectedRowCount: ' || i_ExpectedRowCount;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strMartId                       := COMMON_OWNER.SMT_CONTEXT.GET_ATTRIBUTE(i_AttributeName =>'MartId');
strProcessName                  := COMMON_OWNER.SMT_CONTEXT.GET_ATTRIBUTE(i_AttributeName =>'ProcessName');
dtParentProcessStartTime        := TO_DATE(COMMON_OWNER.SMT_CONTEXT.GET_ATTRIBUTE(i_AttributeName => 'ProcessStartTime'),strTimeMask);

Begin
        intExpectedRowCount     := TO_NUMBER(i_ExpectedRowCount);
Exception
    When others
    Then
        intExpectedRowCount     := Null;
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => 'i_ExpectedRowCount not numeric.  Number of rows processed will not be validated.');
End;

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SELECT ';
SELECT  CASE
          WHEN  UPLOAD_ID IS NULL
          THEN  'N'
          ELSE  'Y'
        END     CONTROL_ROW_EXISTS,
        FILE_NAME
  INTO  strControlRowExists,
        strFileName
  FROM  (
        SELECT  MAX(CTL.UPLOAD_ID)                      UPLOAD_ID,
                MAX(CTL.FILE_NAME)                      FILE_NAME
          FROM  COMMON_OWNER.UPLOAD_CONTROL CTL
         WHERE  CTL.UPLOAD_ID  = i_UploadId
        )
;

strSqlCommand   := 'Validate Upload';
If  strControlRowExists = 'N'
Then
        RAISE_APPLICATION_ERROR( -20001, 'Upload ' || i_UploadId || ' does not exist in control table UPLOAD_CONTROL.');
End If;

strSqlCommand   := 'SMT_CONTEXT.SET_ATTRIBUTE';
COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceRowNumber', i_AttributeValue=> to_char(0));

strSqlCommand := 'SMT_INTERFACE.INTERFACE_INIT';
COMMON_OWNER.SMT_INTERFACE.INTERFACE_INIT
        (       i_SourceTableOwner      => 'COMMON_OWNER',
                i_SourceTableName       => 'UPLOAD_S1',
                i_SourceFileName        => strFileName,
                i_TargetTableOwner      => 'CSSTG_OWNER',
                i_TargetTableName       => 'UM_AF_AMH_STUDENT_ENROLL_S2'
        );

strSqlCommand   := 'Get DOC_GUID and INS_COUNT from COMMON_OWNER.UPLOAD_S1.';
select max(DOC_GUID) DOC_GUID,
       count(*) INS_COUNT
  into strDocGUID,
       intInsertCount 
  from COMMON_OWNER.UPLOAD_S1
 where UPLOAD_ID = i_UploadId
--   and COLUMN_001 = 'UMAMH'   -- Include first line of file with column names. 
;

strSqlCommand   := 'Get SRC_COUNT from COMMON_OWNER.UPLOAD_S0.';
select count(*) SRC_COUNT
  into intExpectedRowCount
  from COMMON_OWNER.UPLOAD_S0
 where DOC_GUID = strDocGUID
;

        intFailedRowCount   := intExpectedRowCount - intInsertCount;

        intTotalRowCount    := intInsertCount + intFailedRowCount + intHeaderRowCount;

If  intTotalRowCount <> intExpectedRowCount
Then
        intErrorCount   := intErrorCount + 1;
        bolError        := True;
        bolFatalError   := True;
        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                (
                        i_ErrorSequence         => intErrorCount,
                        i_ErrorDescription      => 'Fatal Error',
                        i_ErrorMessage          => 'Number of rows processed (' || TRIM(TO_CHAR(intTotalRowCount,'999,999,999')) || ') does not equal the number of rows expected (' || TRIM(TO_CHAR(intExpectedRowCount,'999,999,999')) || ')',
                        i_ErrorCode             => SQLCODE
                );
End If;

If  intTotalRowCount = 0
Then
        numErrorPercent         := 0;
Else
        numErrorPercent         := (intFailedRowCount / intTotalRowCount) * 100;
End If;

strMessage01    := 'Error %: ' || TO_CHAR(numErrorPercent,'999.99');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

If  numErrorPercent >= 10
Then
        bolFatalError   := True;
        intErrorCount   := intErrorCount + 1;
        bolError        := True;
        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                (
                        i_ErrorSequence         => intErrorCount,
                        i_ErrorDescription      => 'Fatal Error',
                        i_ErrorMessage          => 'Upload had 10% or more rows rejected due to errors.',
                        i_ErrorCode             => SQLCODE
                );
End If;

o_RowCountSuccess       := intInsertCount;
o_RowCountFail          := intFailedRowCount;
o_ErrorCount            := intErrorCount;

If  bolFatalError
Then
--      A fatal eror condition was determined. Mark the interface as failed.
        strSqlCommand   := 'SMT_INTERFACE.INTERFACE_FAILURE';
        COMMON_OWNER.SMT_INTERFACE.INTERFACE_FAILURE
                (       i_SuccessRowCount       => intInsertCount,
                        i_FailedRowCount        => intFailedRowCount,
                        i_EofProcessed          => Case When bolPrematureExit Then 'N' Else 'Y' End
                );

        o_Status                := 'FAILED';

        strMessage01    := 'Upload has failed.';

        If  i_ContextBlock is Null
        Then
                strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_FAILURE';
                COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_FAILURE
                               (i_SqlCommand    => Null,
                                i_ErrorText     => strMessage01,
                                i_ErrorCode     => Null,
                                i_ErrorMessage  => Null
                               );

        End If;
        strSqlCommand   := 'RAISE_APPLICATION_ERROR due to fatal error';
        RAISE_APPLICATION_ERROR( -20001, strMessage01);
Else
        strSqlCommand   := 'SMT_INTERFACE.INTERFACE_SUCCESS';
        COMMON_OWNER.SMT_INTERFACE.INTERFACE_SUCCESS
                (       i_SuccessRowCount       => intInsertCount,
                        i_FailedRowCount        => intFailedRowCount
                );

        o_Status                := 'SUCCESS';

        If  i_ContextBlock is Null
        Then
                strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
                COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;
                strMessage01    := strProcessName || ' is complete.';
        Else
                strMessage01    := 'UM_AF_PRD_AMH_STDNT_ENRL_S1_P is complete.';
        End If;
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
End If;

EXCEPTION
    WHEN OTHERS THEN
        numSqlCode := SQLCODE;
        strSqlErrm := SQLERRM;

        intErrorCount   := intErrorCount + 1;
        bolError        := True;
        COMMON_OWNER.SMT_INTERFACE.LOG_ERROR
                (       i_ErrorSequence         => intErrorCount,
                        i_ErrorDescription      => 'General error',
                        i_ErrorMessage          => strSqlErrm,
                        i_ErrorCode             => numSqlCode
                );

        If  i_ContextBlock is Null
        Then
                COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                        (
                                i_SqlCommand   => strSqlCommand,
                                i_SqlCode      => numSqlCode,
                                i_SqlErrm      => strSqlErrm
                        );
        Else
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strProcessName || ' has failed' || strNewLine || strSqlErrm);
        End If;

END UM_AF_PRD_AMH_STDNT_ENRL_S1_P;
/
