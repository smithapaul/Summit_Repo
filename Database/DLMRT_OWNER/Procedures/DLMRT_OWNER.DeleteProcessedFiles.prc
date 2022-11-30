DROP PROCEDURE DLMRT_OWNER."DeleteProcessedFiles"
/

--
-- "DeleteProcessedFiles"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."DeleteProcessedFiles"
        (
                i_MartId                        in  Varchar2    Default 'DLAB',
                i_ProcessName                   in  Varchar2    Default 'DeleteProcessedFiles',
                i_Institution                   in  Varchar2
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Deletes processed enrollment files.
--
-- V01  SMT-8358 11/15/2019     Greg Kampf
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
        strDataTimestampRowExists       Varchar2(1);
        strCampusId                     Varchar2(3);
        strUploadId                     Varchar2(19);
        strUploadType                   Varchar2(10)    := 'ENROLLMENT';
        strFileNameSearch               Varchar2(50);
        strFileNameFixed                Varchar2(150);
        strFileNameOld                  Varchar2(150);
        strFileNameNew                  Varchar2(150);
        intFileCount                    Integer         := 0;
        strMartIdOfJobGroup             Varchar2(15);
        strJobGroupName                 Varchar2(128);
        strTableName                    Varchar2(128);

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

strMessage01    := 'Procedure DLMRT_OWNER."DeleteProcessedFiles" arguments:'
                || strNewLine || '      i_MartId: ' || i_MartId
                || strNewLine || ' i_ProcessName: ' || i_ProcessName
                || strNewLine || ' i_Institution: ' || i_Institution;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strCampusId     := substr(i_Institution,3,3);
strUploadId     := 'DLAB_ENROLLMENT_' || strCampusId;

strSqlCommand   := 'SELECT UPC.FILE_NAME_SEARCH FROM COMMON_OWNER.UPLOAD_CONTROL UPC';
SELECT  UPC.FILE_NAME_SEARCH
  INTO  strFileNameSearch
  FROM  COMMON_OWNER.UPLOAD_CONTROL UPC
 WHERE  UPC.UPLOAD_ID = strUploadId
;

strFileNameSearch       := strFileNameSearch || '%-Processed_%';

strSqlCommand   := 'For recProcessedFile';
For recProcessedFile in
        (
        SELECT  FILE_NAME,
                MODIFIED_TIME,
                FILE_SIZE
          FROM  COMMON_OWNER.CMN_FILES_LIST_VW
         WHERE  FILE_NAME LIKE strFileNameSearch
        ORDER BY
                FILE_NAME
        )
Loop
        intFileCount    := intFileCount + 1;

        strMessage01    := 'Deleting ' || recProcessedFile.FILE_NAME || '...';
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

        strSqlCommand   := 'UTL_FILE.FREMOVE (' || recProcessedFile.FILE_NAME || ')';
        UTL_FILE.FREMOVE
                (
                location        => 'CMN_FILES',
                filename        => recProcessedFile.FILE_NAME
                );

        strMessage01    := 'File deleted';
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
End Loop; --recProcessedFile

strMessage01    := 'Number of files deleted: ' || TO_CHAR(intFileCount);
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

END "DeleteProcessedFiles";
/
