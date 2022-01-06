CREATE OR REPLACE PROCEDURE             "PS_STDNT_RESPONSE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_STDNT_RESPONSE from PeopleSoft table PS_STDNT_RESPONSE.
--
-- V01  SMT-xxxx 07/11/2017,    Jim Doucette
--                              Converted from PS_STDNT_RESPONSE.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_STDNT_RESPONSE';
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

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_STDNT_RESPONSE'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_STDNT_RESPONSE@SASOURCE S)
 where TABLE_NAME = 'PS_STDNT_RESPONSE'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_STDNT_RESPONSE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_STDNT_RESPONSE';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_STDNT_RESPONSE T 
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR, 
    nvl(trim(ADM_APPL_NBR),'-') ADM_APPL_NBR, 
    nvl(SEQNUM,0) SEQNUM, 
    RESPONSE_REASON RESPONSE_REASON,
    to_date(to_char(case when RESPONSE_DT < '01-JAN-1800' then NULL else RESPONSE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') RESPONSE_DT, 
    EXT_ORG_ID EXT_ORG_ID,
    DESCR DESCR
from SYSADM.PS_STDNT_RESPONSE@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_RESPONSE') ) S 
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.STDNT_CAR_NBR = S.STDNT_CAR_NBR and 
    T.ADM_APPL_NBR = S.ADM_APPL_NBR and 
    T.SEQNUM = S.SEQNUM and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.RESPONSE_REASON = S.RESPONSE_REASON,
    T.RESPONSE_DT = S.RESPONSE_DT,
    T.EXT_ORG_ID = S.EXT_ORG_ID,
    T.DESCR = S.DESCR,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    nvl(trim(T.RESPONSE_REASON),0) <> nvl(trim(S.RESPONSE_REASON),0) or 
    nvl(trim(T.RESPONSE_DT),0) <> nvl(trim(S.RESPONSE_DT),0) or 
    nvl(trim(T.EXT_ORG_ID),0) <> nvl(trim(S.EXT_ORG_ID),0) or 
    nvl(trim(T.DESCR),0) <> nvl(trim(S.DESCR),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.STDNT_CAR_NBR,
    T.ADM_APPL_NBR, 
    T.SEQNUM, 
    T.SRC_SYS_ID, 
    T.RESPONSE_REASON,
    T.RESPONSE_DT,
    T.EXT_ORG_ID, 
    T.DESCR,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.ACAD_CAREER,
    S.STDNT_CAR_NBR,
    S.ADM_APPL_NBR, 
    S.SEQNUM, 
    'CS90', 
    S.RESPONSE_REASON,
    S.RESPONSE_DT,
    S.EXT_ORG_ID, 
    S.DESCR,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_RESPONSE rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_RESPONSE',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_STDNT_RESPONSE';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_RESPONSE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_RESPONSE';
update CSSTG_OWNER.PS_STDNT_RESPONSE T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, SEQNUM
   from CSSTG_OWNER.PS_STDNT_RESPONSE T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_RESPONSE') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, SEQNUM
   from SYSADM.PS_STDNT_RESPONSE@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_RESPONSE') = 'Y' 
   ) S
 where T.EMPLID= S.EMPLID
    AND T.ACAD_CAREER = S.ACAD_CAREER
    AND T.STDNT_CAR_NBR = S.STDNT_CAR_NBR
    AND T.ADM_APPL_NBR = S.ADM_APPL_NBR
    AND T.SEQNUM = S.SEQNUM
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_RESPONSE rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_RESPONSE',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_STDNT_RESPONSE'
;

strSqlCommand := 'commit';
commit;


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

END PS_STDNT_RESPONSE_P;
/
