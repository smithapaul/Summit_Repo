DROP PROCEDURE CSMRT_OWNER.PS_ADM_APPL_PROG_P
/

--
-- PS_ADM_APPL_PROG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_ADM_APPL_PROG_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ADM_APPL_PROG'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_ADM_APPL_PROG', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ADM_APPL_PROG'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ADM_APPL_PROG from PeopleSoft table PS_ADM_APPL_PROG.
--
-- V01  SMT-xxxx 05/16/2017,    Jim Doucette
--                              Converted from PS_ADM_APPL_PROG.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ADM_APPL_PROG';
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
 where TABLE_NAME = 'PS_ADM_APPL_PROG'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ADM_APPL_PROG@SASOURCE S)
 where TABLE_NAME = 'PS_ADM_APPL_PROG'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ADM_APPL_PROG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ADM_APPL_PROG';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ADM_APPL_PROG T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR, 
    nvl(trim(ADM_APPL_NBR),'-') ADM_APPL_NBR, 
    nvl(APPL_PROG_NBR,0) APPL_PROG_NBR, 
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
    nvl(EFFSEQ,0) EFFSEQ, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(ACAD_PROG),'-') ACAD_PROG, 
    nvl(trim(PROG_STATUS),'-') PROG_STATUS, 
    nvl(trim(PROG_ACTION),'-') PROG_ACTION, 
    to_date(to_char(case when ACTION_DT < '01-JAN-1800' then NULL else ACTION_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ACTION_DT, 
    nvl(trim(PROG_REASON),'-') PROG_REASON, 
    nvl(trim(ADMIT_TERM),'-') ADMIT_TERM, 
    nvl(trim(EXP_GRAD_TERM),'-') EXP_GRAD_TERM, 
    nvl(trim(REQ_TERM),'-') REQ_TERM, 
    nvl(trim(ACAD_LOAD_APPR),'-') ACAD_LOAD_APPR, 
    nvl(trim(CAMPUS),'-') CAMPUS, 
    nvl(trim(ACAD_PROG_DUAL),'-') ACAD_PROG_DUAL, 
    nvl(trim(JOINT_PROG_APPR),'-') JOINT_PROG_APPR
  from SYSADM.PS_ADM_APPL_PROG@SASOURCE S 
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_PROG')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.STDNT_CAR_NBR = S.STDNT_CAR_NBR and 
    T.ADM_APPL_NBR = S.ADM_APPL_NBR and 
    T.APPL_PROG_NBR = S.APPL_PROG_NBR and 
    T.EFFDT = S.EFFDT and 
    T.EFFSEQ = S.EFFSEQ and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.INSTITUTION = S.INSTITUTION,
    T.ACAD_PROG = S.ACAD_PROG,
    T.PROG_STATUS = S.PROG_STATUS,
    T.PROG_ACTION = S.PROG_ACTION,
    T.ACTION_DT = S.ACTION_DT,
    T.PROG_REASON = S.PROG_REASON,
    T.ADMIT_TERM = S.ADMIT_TERM,
    T.EXP_GRAD_TERM = S.EXP_GRAD_TERM,
    T.REQ_TERM = S.REQ_TERM,
    T.ACAD_LOAD_APPR = S.ACAD_LOAD_APPR,
    T.CAMPUS = S.CAMPUS,
    T.ACAD_PROG_DUAL = S.ACAD_PROG_DUAL,
    T.JOINT_PROG_APPR = S.JOINT_PROG_APPR,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.INSTITUTION <> S.INSTITUTION or 
    T.ACAD_PROG <> S.ACAD_PROG or 
    T.PROG_STATUS <> S.PROG_STATUS or 
    T.PROG_ACTION <> S.PROG_ACTION or 
    nvl(trim(T.ACTION_DT),0) <> nvl(trim(S.ACTION_DT),0) or 
    T.PROG_REASON <> S.PROG_REASON or 
    T.ADMIT_TERM <> S.ADMIT_TERM or 
    T.EXP_GRAD_TERM <> S.EXP_GRAD_TERM or 
    T.REQ_TERM <> S.REQ_TERM or 
    T.ACAD_LOAD_APPR <> S.ACAD_LOAD_APPR or 
    T.CAMPUS <> S.CAMPUS or 
    T.ACAD_PROG_DUAL <> S.ACAD_PROG_DUAL or 
    T.JOINT_PROG_APPR <> S.JOINT_PROG_APPR or 
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
    T.SRC_SYS_ID, 
    T.INSTITUTION,
    T.ACAD_PROG,
    T.PROG_STATUS,
    T.PROG_ACTION,
    T.ACTION_DT,
    T.PROG_REASON,
    T.ADMIT_TERM, 
    T.EXP_GRAD_TERM,
    T.REQ_TERM, 
    T.ACAD_LOAD_APPR, 
    T.CAMPUS, 
    T.ACAD_PROG_DUAL, 
    T.JOINT_PROG_APPR,
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
    'CS90', 
    S.INSTITUTION,
    S.ACAD_PROG,
    S.PROG_STATUS,
    S.PROG_ACTION,
    S.ACTION_DT,
    S.PROG_REASON,
    S.ADMIT_TERM, 
    S.EXP_GRAD_TERM,
    S.REQ_TERM, 
    S.ACAD_LOAD_APPR, 
    S.CAMPUS, 
    S.ACAD_PROG_DUAL, 
    S.JOINT_PROG_APPR,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_APPL_PROG rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_APPL_PROG',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ADM_APPL_PROG';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ADM_APPL_PROG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ADM_APPL_PROG';
update CSSTG_OWNER.PS_ADM_APPL_PROG T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, APPL_PROG_NBR, EFFDT, EFFSEQ
   from CSSTG_OWNER.PS_ADM_APPL_PROG T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_PROG') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, APPL_PROG_NBR, EFFDT, EFFSEQ
   from SYSADM.PS_ADM_APPL_PROG@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_PROG') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.STDNT_CAR_NBR = S.STDNT_CAR_NBR
   and T.ADM_APPL_NBR = S.ADM_APPL_NBR
   and T.APPL_PROG_NBR = S.APPL_PROG_NBR
   and T.EFFDT = S.EFFDT
   and T.EFFSEQ = S.EFFSEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_APPL_PROG rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_APPL_PROG',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ADM_APPL_PROG'
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

END PS_ADM_APPL_PROG_P;
/
