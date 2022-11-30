DROP PROCEDURE DLMRT_OWNER."EnrollmentUploadWrapper"
/

--
-- "EnrollmentUploadWrapper"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."EnrollmentUploadWrapper"
        (
                i_MartId                        in  Varchar2    Default 'DLAB',
                i_ProcessName                   in  Varchar2    Default 'EnrollmentUploadWrapper',
                i_Institution                   in  Varchar2,
                i_RenameFiles                   in  Varchar2    Default 'TRUE'
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Run the enrollment processes for each enrollment file.
--
-- V02  SMT-8358 11/21/2019     Greg Kampf
--                              Add argument i_RenameFiles.  When true do rename the
--                              files after they are processed.
--                              False is intended for unit testing.
--
-- V01  SMT-8358 11/14/2019     Greg Kampf
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
        strFileNameSearch               Varchar2(50);
        strFileNameFixed                Varchar2(150);
        strFileNameOld                  Varchar2(150);
        strFileNameNew                  Varchar2(150);
        intFileCount                    Integer         := 0;
        strMartIdOfJobGroup             Varchar2(15);
        strJobGroupName                 Varchar2(128);
        strTableName                    Varchar2(128);
        bolRenameFiles                  Boolean;

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

strMessage01    := 'Procedure DLMRT_OWNER."EnrollmentUploadWrapper" arguments:'
                || strNewLine || '      i_MartId: ' || i_MartId
                || strNewLine || ' i_ProcessName: ' || i_ProcessName
                || strNewLine || ' i_Institution: ' || i_Institution
                || strNewLine || ' i_RenameFiles: ' || i_RenameFiles;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

bolRenameFiles  := Case
                        When    upper(i_RenameFiles) = 'TRUE'
                        Then    True
                        Else    False
                    End;
strCampusId     := substr(i_Institution,3,3);
strUploadId     := 'DLAB_ENROLLMENT_' || strCampusId;

strSqlCommand   := 'SELECT UPC.FILE_NAME_SEARCH FROM COMMON_OWNER.UPLOAD_CONTROL UPC';
SELECT  UPC.FILE_NAME_SEARCH,
        UPC.MART_ID,
        UPC.JOB_GROUP_NAME
  INTO  strFileNameSearch,
        strMartIdOfJobGroup,
        strJobGroupName
  FROM  COMMON_OWNER.UPLOAD_CONTROL UPC
 WHERE  UPC.UPLOAD_ID = strUploadId
;

strMessage01    := 'Items from UPLOAD_CONTROL for upload ' || strUploadId || ':'
                || strNewLine || '   strFileNameSearch: ' || strFileNameSearch
                || strNewLine || ' strMartIdOfJobGroup: ' || strMartIdOfJobGroup
                || strNewLine || '     strJobGroupName: ' || strJobGroupName;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'For recUploadFile';
For recUploadFile in
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
        strMessage01    := 'Processing file ' || recUploadFile.FILE_NAME
                        || strNewLine || ' Modified time: ' || TO_CHAR(recUploadFile.MODIFIED_TIME, 'DD-MON-YYYY HH24:MI:SS')
                        || strNewLine || '          Size: ' || TO_CHAR(recUploadFile.FILE_SIZE);
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

        -- Alter the external table so that it uses (the fixed version of) the file
        strTableName    := 'ENROLLMENT_' || strCampusId || '_DLAB_EXT';
        strFileNameFixed:= recUploadFile.FILE_NAME || '-Fixed';
        strSqlDynamic   := 'ALTER TABLE COMMON_OWNER.' || strTableName || ' LOCATION (' || strQuote || strFileNameFixed || strQuote || ')';
        strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
        COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                        (       i_SqlStatement  => strSqlDynamic,
                                i_MaxTries      => 10,
                                i_WaitSeconds   => 10,
                                o_Tries         => intTries
                        );

        -- Update UPLOAD_CONTROL with the name of the file
        strSqlCommand   := 'UPDATE COMMON_OWNER.UPLOAD_CONTROL';
        UPDATE  COMMON_OWNER.UPLOAD_CONTROL
           SET  FILE_NAME       = recUploadFile.FILE_NAME,
                UPDATED_BY      = i_ProcessName,
                UPDATE_TIME     = SYSDATE
         WHERE  UPLOAD_ID       = strUploadId
        ;

        strSqlCommand   := 'COMMIT';
        COMMIT;

        strMessage01    := 'Running job group ' || strMartIdOfJobGroup || '.' || strJobGroupName;
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

        strSqlCommand   := 'SMT_JOB.RUN_JOB_GROUP';
        COMMON_OWNER.SMT_JOB.RUN_JOB_GROUP
                (
                        i_ProcessName           => 'RUN_JOB_GROUP-' || strMartIdOfJobGroup|| '.' || strJobGroupName,
                        i_MartId                => strMartIdOfJobGroup,
                        i_JobGroup              => strJobGroupName
                );

        strMessage01    := 'Job group finished';
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

        If  bolRenameFiles
        Then
                -- Rename the original and fixed files so that they do not get reprocessed
                strFileNameOld  := recUploadFile.FILE_NAME;
                strFileNameNew  := recUploadFile.FILE_NAME || '-Processed_' || TO_CHAR(SYSDATE,'YYYY-MM-DD_HH24-MI-SS');
                strMessage01    := 'Renaming ' || strFileNameOld || ' to ' || strFileNameNew || '...';
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                strSqlCommand   := 'UTL_FILE.FRENAME (from ' || strFileNameOld || ' to ' || strFileNameNew || ')';
                UTL_FILE.FRENAME
                        (
                        src_location    => 'CMN_FILES',
                        src_filename    => strFileNameOld, 
                        dest_location   => 'CMN_FILES',
                        dest_filename   => strFileNameNew,
                        overwrite       => False
                        );

                strMessage01    := 'File renamed';
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                strFileNameOld  := strFileNameFixed;
                strFileNameNew  := strFileNameFixed || '-Processed_' || TO_CHAR(SYSDATE,'YYYY-MM-DD_HH24-MI-SS');
                strMessage01    := 'Renaming ' || strFileNameOld || ' to ' || strFileNameNew || '...';
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                strSqlCommand   := 'UTL_FILE.FRENAME (from ' || strFileNameOld || ' to ' || strFileNameNew || ')';
                UTL_FILE.FRENAME
                        (
                        src_location    => 'CMN_FILES',
                        src_filename    => strFileNameOld, 
                        dest_location   => 'CMN_FILES',
                        dest_filename   => strFileNameNew,
                        overwrite       => False
                        );

                strMessage01    := 'File renamed';
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
        End If;
End Loop; --recUploadFile

strMessage01    := 'Number of files processed: ' || TO_CHAR(intFileCount);
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

END "EnrollmentUploadWrapper";
/
