CREATE OR REPLACE PROCEDURE             "SchedSuccessNew" 

        (
               i_MartId                in  Varchar2
        )
 AUTHID CURRENT_USER
 IS
------------------------------------------------------------------------
--
-- V01  SMT-7581 01/08/2018     Jim Doucette
--                              Converted from SchedSuccessFdm procedure
--                              Argument driven procedure accepting MartID to update
--                              start of batch process in the Summit Process Log.
--
------------------------------------------------------------------------

        strProcessName                  Varchar2(100)   := 'SchedSuccessNew';
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) := '';
        strClientInfo                   Varchar2(100);

        
BEGIN
strClientInfo   := strProcessName;
strSqlCommand   := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => i_MartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart
        );

strMessage01    := 'Procedure COMMON_OWNER."SchedSuccessNew" arguments:'
                || strNewLine || '         i_MartId: ' || i_MartId;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);	

strSqlCommand   := 'SMT_PROCESS_LOG.SCHEDULE_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.SCHEDULE_SUCCESS;

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN

        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END "SchedSuccessNew";
/
