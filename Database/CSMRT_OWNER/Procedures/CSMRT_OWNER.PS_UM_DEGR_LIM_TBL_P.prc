DROP PROCEDURE CSMRT_OWNER.PS_UM_DEGR_LIM_TBL_P
/

--
-- PS_UM_DEGR_LIM_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_UM_DEGR_LIM_TBL_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_UM_DEGR_LIM_TBL'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_UM_DEGR_LIM_TBL', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_UM_DEGR_LIM_TBL'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_UM_DEGR_LIM_TBL from PeopleSoft table PS_UM_DEGR_LIM_TBL.
--
-- V01  SMT-xxxx 05/16/2017,    Jim Doucette
--                              Converted from PS_UM_DEGR_LIM_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_UM_DEGR_LIM_TBL';
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
 where TABLE_NAME = 'PS_UM_DEGR_LIM_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_UM_DEGR_LIM_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_UM_DEGR_LIM_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_UM_DEGR_LIM_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_UM_DEGR_LIM_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_UM_DEGR_LIM_TBL T
using (select /*+ full(S) */
    nvl(trim(INSTITUTION),'-') INSTITUTION,
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
    nvl(trim(ACAD_PROG),'-') ACAD_PROG,
    nvl(trim(ACAD_PLAN),'-') ACAD_PLAN,
    nvl(trim(ACAD_SUB_PLAN),'-') ACAD_SUB_PLAN,
    to_date(to_char(EFFDT,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
    YEARS YEARS
from SYSADM.PS_UM_DEGR_LIM_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_DEGR_LIM_TBL') ) S
   on (
    T.INSTITUTION = S.INSTITUTION and
    T.ACAD_CAREER = S.ACAD_CAREER and
    T.ACAD_PROG = S.ACAD_PROG and
    T.ACAD_PLAN = S.ACAD_PLAN and
    T.ACAD_SUB_PLAN = S.ACAD_SUB_PLAN and
    T.EFFDT = S.EFFDT and
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.YEARS = S.YEARS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID   = 1234
where
    nvl(trim(T.YEARS),0) <> nvl(trim(S.YEARS),0) or
    T.DATA_ORIGIN = 'D'
when not matched then
insert (
    T.INSTITUTION,
    T.ACAD_CAREER,
    T.ACAD_PROG,
    T.ACAD_PLAN,
    T.ACAD_SUB_PLAN,
    T.EFFDT,
    T.SRC_SYS_ID,
    T.YEARS,
    T.LOAD_ERROR,
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    )
values (
    S.INSTITUTION,
    S.ACAD_CAREER,
    S.ACAD_PROG,
    S.ACAD_PLAN,
    S.ACAD_SUB_PLAN,
    S.EFFDT,
    'CS90',
    S.YEARS,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_DEGR_LIM_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_DEGR_LIM_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_UM_DEGR_LIM_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_UM_DEGR_LIM_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_UM_DEGR_LIM_TBL';
update CSSTG_OWNER.PS_UM_DEGR_LIM_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, ACAD_CAREER, ACAD_PROG, ACAD_PLAN, ACAD_SUB_PLAN, EFFDT
   from CSSTG_OWNER.PS_UM_DEGR_LIM_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_DEGR_LIM_TBL') = 'Y'
  minus
 select INSTITUTION, ACAD_CAREER, ACAD_PROG, ACAD_PLAN, nvl(trim(ACAD_SUB_PLAN),'-'), EFFDT
   from SYSADM.PS_UM_DEGR_LIM_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_DEGR_LIM_TBL') = 'Y'
   ) S
 where T.INSTITUTION = S.INSTITUTION
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.ACAD_PROG = S.ACAD_PROG
   and T.ACAD_PLAN = S.ACAD_PLAN
   and T.ACAD_SUB_PLAN = S.ACAD_SUB_PLAN
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_DEGR_LIM_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_DEGR_LIM_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_UM_DEGR_LIM_TBL'
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

END PS_UM_DEGR_LIM_TBL_P;
/
