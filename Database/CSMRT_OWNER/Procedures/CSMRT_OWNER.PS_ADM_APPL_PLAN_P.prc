CREATE OR REPLACE PROCEDURE             "PS_ADM_APPL_PLAN_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ADM_APPL_PLAN from PeopleSoft table PS_ADM_APPL_PLAN.
--
-- V01  SMT-xxxx 05/11/2017,    Jim Doucette
--                              Converted from PS_ADM_APPL_PLAN.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ADM_APPL_PLAN';
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
 where TABLE_NAME = 'PS_ADM_APPL_PLAN'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ADM_APPL_PLAN@SASOURCE S)
 where TABLE_NAME = 'PS_ADM_APPL_PLAN'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ADM_APPL_PLAN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ADM_APPL_PLAN';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ADM_APPL_PLAN T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR, 
    nvl(trim(ADM_APPL_NBR),'-') ADM_APPL_NBR, 
    nvl(APPL_PROG_NBR,0) APPL_PROG_NBR, 
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL 
                    else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
    nvl(EFFSEQ,0) EFFSEQ, 
    nvl(trim(ACAD_PLAN),'-') ACAD_PLAN, 
    to_date(to_char(case when DECLARE_DT < '01-JAN-1800' then NULL 
                    else DECLARE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DECLARE_DT,
    nvl(PLAN_SEQUENCE,0) PLAN_SEQUENCE, 
    nvl(trim(REQ_TERM),'-') REQ_TERM
from SYSADM.PS_ADM_APPL_PLAN@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_PLAN') 
  and (EMPLID BETWEEN '00000000' AND '99999999'
  and LENGTH (TRIM (EMPLID)) = 8)) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.STDNT_CAR_NBR = S.STDNT_CAR_NBR and 
    T.ADM_APPL_NBR = S.ADM_APPL_NBR and 
    T.APPL_PROG_NBR = S.APPL_PROG_NBR and 
    T.EFFDT = S.EFFDT and 
    T.EFFSEQ = S.EFFSEQ and 
    T.ACAD_PLAN = S.ACAD_PLAN and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.DECLARE_DT = S.DECLARE_DT,
    T.PLAN_SEQUENCE = S.PLAN_SEQUENCE,
    T.REQ_TERM = S.REQ_TERM,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    nvl(trim(T.DECLARE_DT),0) <> nvl(trim(S.DECLARE_DT),0) or 
    T.PLAN_SEQUENCE <> S.PLAN_SEQUENCE or 
    trim(T.REQ_TERM) <> trim(S.REQ_TERM) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.STDNT_CAR_NBR,
    T.ADM_APPL_NBR, 
    T.APPL_PROG_NBR,
    T.EFFDT,
    T.EFFSEQ, 
    T.ACAD_PLAN,
    T.SRC_SYS_ID, 
    T.DECLARE_DT, 
    T.PLAN_SEQUENCE,
    T.REQ_TERM, 
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
    S.APPL_PROG_NBR,
    S.EFFDT,
    S.EFFSEQ, 
    S.ACAD_PLAN,
    'CS90', 
    S.DECLARE_DT, 
    S.PLAN_SEQUENCE,
    S.REQ_TERM, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_APPL_PLAN rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_APPL_PLAN',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ADM_APPL_PLAN';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ADM_APPL_PLAN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ADM_APPL_PLAN';
update CSSTG_OWNER.PS_ADM_APPL_PLAN T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, APPL_PROG_NBR, EFFDT, EFFSEQ, ACAD_PLAN
   from CSSTG_OWNER.PS_ADM_APPL_PLAN T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_PLAN') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, APPL_PROG_NBR, EFFDT, EFFSEQ, ACAD_PLAN
   from SYSADM.PS_ADM_APPL_PLAN@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_PLAN') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.STDNT_CAR_NBR = S.STDNT_CAR_NBR
   and T.ADM_APPL_NBR = S.ADM_APPL_NBR
   and T.APPL_PROG_NBR = S.APPL_PROG_NBR
   and T.EFFDT = S.EFFDT
   and T.EFFSEQ = S.EFFSEQ
   and T.ACAD_PLAN = S.ACAD_PLAN
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_APPL_PLAN rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_APPL_PLAN',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ADM_APPL_PLAN'
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

END PS_ADM_APPL_PLAN_P;
/
