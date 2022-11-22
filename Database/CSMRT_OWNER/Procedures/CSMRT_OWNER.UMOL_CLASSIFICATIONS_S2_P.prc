DROP PROCEDURE CSMRT_OWNER.UMOL_CLASSIFICATIONS_S2_P
/

--
-- UMOL_CLASSIFICATIONS_S2_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UMOL_CLASSIFICATIONS_S2_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Jim Doucette
--
-- Loads S2 table UMOL_CLASSIFICATIONS_S2 from S1 table UMOL_CLASSIFICATIONS_S1.
--
-- V01  CASE-xxxxx 08/28/2020,    Jim Doucette
--                                New
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UMOL_CLASSIFICATIONS_S2';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Merging data into CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2 T
using (select /*+ full(S) */
       BB_SOURCE, 
       PK1, 
       TITLE, 
       BATCH_UID, 
       ROW_STATUS, 
       DATA_SRC_PK1, 
       PARENT_PK1
  from CSSTG_OWNER.UMOL_CLASSIFICATIONS_S1) S
 on ( 
    T.BB_SOURCE = S.BB_SOURCE and 
    T.PK1 = S.PK1)
 when matched then update set
    T.TITLE = S.TITLE,
    T.BATCH_UID = S.BATCH_UID,
    T.ROW_STATUS = S.ROW_STATUS,
    T.DATA_SRC_PK1 = S.DATA_SRC_PK1,
    T.PARENT_PK1 = S.PARENT_PK1,
    T.DELETE_FLAG = 'N',
    T.UPDATE_TIME = SYSDATE
where 
    nvl(trim(T.TITLE),0) <> nvl(trim(S.TITLE),0) or 
    nvl(trim(T.BATCH_UID),0) <> nvl(trim(S.BATCH_UID),0) or 
    nvl(trim(T.ROW_STATUS),0) <> nvl(trim(S.ROW_STATUS),0) or 
    nvl(trim(T.DATA_SRC_PK1),0) <> nvl(trim(S.DATA_SRC_PK1),0) or 
    nvl(trim(T.PARENT_PK1),0) <> nvl(trim(S.PARENT_PK1),0)
when not matched then 
insert (
    T.BB_SOURCE,
    T.PK1,
    T.TITLE, 
    T.BATCH_UID, 
    T.ROW_STATUS, 
    T.DATA_SRC_PK1,
    T.PARENT_PK1,
    T.DELETE_FLAG, 
    T.INSERT_TIME,
    T.UPDATE_TIME
) 
values (
    S.BB_SOURCE,
    S.PK1,
    S.TITLE, 
    S.BATCH_UID, 
    S.ROW_STATUS, 
    S.DATA_SRC_PK1,
    S.PARENT_PK1,
    'N', 
    SYSDATE,
    SYSDATE);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UMOL_CLASSIFICATIONS_S2 rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UMOL_CLASSIFICATIONS_S2',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
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

END UMOL_CLASSIFICATIONS_S2_P;
/
