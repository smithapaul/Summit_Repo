DROP PROCEDURE CSMRT_OWNER.NSC_STUDENT_TRACKER_S1_P
/

--
-- NSC_STUDENT_TRACKER_S1_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.NSC_STUDENT_TRACKER_S1_P 
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
-- Upload file and insert data into CSSTG_OWNER.NSC_STUDENT_TRACKER_S1. 
--
-- Minimum validation for file upload. Validates UPLOAD_ID and optional expected row count only.  
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'UPLD';
        strProcessOwner                 Varchar2(100)   := 'CSSTG_OWNER';  
        strProcessName                  Varchar2(100)   := 'NSC_STUDENT_TRACKER_S1';  
        dtProcessStart                  Date            := SYSDATE;
        dtParentProcessStartTime        Date;
        strMessage01                    Varchar2(32767);
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) := '';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        intHeaderRowCount               Integer         := 0;
        intInsertCount                  Integer         := 0;
        intFailedRowCount               Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(32767);
        strControlRowExists             Varchar2(1);
        strCarriageReturn               Varchar2(1)     := chr(13);
        intErrorCount                   Integer         := 0;
        bolError                        Boolean;
        bolFatalError                   Boolean         := False;
        strDocGUID                      Varchar2(50);
        strFileName                     Varchar2(256);
        strTimeMask                     Varchar2(22)    := 'DD-MON-YYYY HH24:MI:SS';
        intExpectedRowCount             Integer;
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

strMessage01    := 'Procedure '||strProcessOwner||'.'||strProcessName||' input arguments:' 
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
        nvl(FILE_NAME,'NewFileName.csv') FILE_NAME
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
        RAISE_APPLICATION_ERROR( -20001, 'Upload ' || i_UploadId || ' is not a valid Upload ID.');
End If;

strSqlCommand   := 'SMT_CONTEXT.SET_ATTRIBUTE';
COMMON_OWNER.SMT_CONTEXT.SET_ATTRIBUTE(i_AttributeName => 'InterfaceRowNumber', i_AttributeValue=> to_char(0));

strSqlCommand := 'SMT_INTERFACE.INTERFACE_INIT';  
COMMON_OWNER.SMT_INTERFACE.INTERFACE_INIT  
        (       i_SourceTableOwner      => 'COMMON_OWNER',
                i_SourceTableName       => 'UPLOAD_S1',
                i_SourceFileName        => strFileName,
                i_TargetTableOwner      => 'CSSTG_OWNER',           -- Add these to UPLOAD_CONTROL???
                i_TargetTableName       => 'NSC_STUDENT_TRACKER_S1' -- Add these to UPLOAD_CONTROL???
        );

strSqlCommand   := 'Get DOC_GUID and INS_COUNT from COMMON_OWNER.UPLOAD_S1.';  
select max(DOC_GUID) DOC_GUID,
       count(*) INS_COUNT
  into strDocGUID,
       intInsertCount 
  from COMMON_OWNER.UPLOAD_S1
 where UPLOAD_ID = i_UploadId
;

If      intExpectedRowCount is NULL or intExpectedRowCount = 0
Then    intExpectedRowCount := intInsertCount;  
End If;

        intFailedRowCount   := intExpectedRowCount - intInsertCount;

        intTotalRowCount    := intInsertCount + intFailedRowCount;  

If      intTotalRowCount <> intExpectedRowCount
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
                strMessage01    := strProcessName||' is complete.';  
        End If;
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
End If;

strSqlCommand   := 'insert into CSSTG_OWNER.NSC_STUDENT_TRACKER_S1';
insert /*+ append */ into CSSTG_OWNER.NSC_STUDENT_TRACKER_S1
select COLUMN_001 UNIQUE_ID,
       COLUMN_002 FIRST_NAME,
       COLUMN_003 MIDDLE_INITIAL,
       COLUMN_004 LAST_NAME,
       COLUMN_005 SUFFIX,
       COLUMN_006 RETURN_FIELD,
       COLUMN_007 RECORD_FOUND,
       case when COMMON_OWNER.ISDATE(COLUMN_008,'YYYYMMDD') = 0 then to_date(COLUMN_008,'YYYYMMDD') else NULL end SEARCH_DATE,
       COLUMN_009 COLLEGE_ID,
       COLUMN_010 COLLEGE_NAME,
       COLUMN_011 COLLEGE_STATE,
       COLUMN_012 YEAR_2_4,
       COLUMN_013 PUB_PRIVATE,
       case when COMMON_OWNER.ISDATE(COLUMN_014,'YYYYMMDD') = 0 then to_date(COLUMN_014,'YYYYMMDD') else NULL end ENROLL_BEGIN_DATE,
       case when COMMON_OWNER.ISDATE(COLUMN_015,'YYYYMMDD') = 0 then to_date(COLUMN_015,'YYYYMMDD') else NULL end ENROLL_END_DATE,
       COLUMN_016 ENROLL_STATUS,
       COLUMN_017 CLASS_LEVEL,
       COLUMN_018 ENROLL_MAJOR_1,
       COLUMN_019 ENROLL_CIP_1,
       COLUMN_020 ENROLL_MAJOR_2,
       COLUMN_021 ENROLL_CIP_2,
       COLUMN_022 GRADUATED_FLG,
       case when COMMON_OWNER.ISDATE(COLUMN_023,'YYYYMMDD') = 0 then to_date(COLUMN_023,'YYYYMMDD') else NULL end GRAD_DATE,
       COLUMN_024 DEGREE_TITLE,
       COLUMN_025 DEG_MAJOR_1,
       COLUMN_026 DEG_CIP_1,
       COLUMN_027 DEG_MAJOR_2,
       COLUMN_028 DEG_CIP_2,
       COLUMN_029 DEG_MAJOR_3,
       COLUMN_030 DEG_CIP_3,
       COLUMN_031 DEG_MAJOR_4,
       COLUMN_032 DEG_CIP_4,
       COLUMN_033 COLLEGE_SEQ_NBR,
       substr(COLUMN_006,1,5) INSTITUTION_CD,
       decode(substr(FILE_NAME,25,2),'SE',substr(COLUMN_006,7,8),'') PERSON_ID,
       decode(substr(FILE_NAME,25,2),'DA',substr(COLUMN_006,7,8),'') APPL_NBR,
       decode(substr(FILE_NAME,25,2),'DA',substr(COLUMN_006,16,4),'') TERM_CD,
       substr(FILE_NAME,25,2) SEARCH_TYPE,
       FILE_NAME,
       case when COMMON_OWNER.ISDATE(substr(FILE_NAME,28,8),'MMDDYYYY') = 0 then to_date(substr(FILE_NAME,28,8),'MMDDYYYY') else NULL end FILE_DATE,
       RECORD_NUMBER,
       SYSDATE INSERT_TIME
  from COMMON_OWNER.UPLOAD_S1  
 where UPLOAD_ID = i_UploadId
   and RECORD_NUMBER > 1
;       
       
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of NSC_STUDENT_TRACKER_S1 rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'NSC_STUDENT_TRACKER_S1',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

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

END NSC_STUDENT_TRACKER_S1_P;
/
