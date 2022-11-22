DROP PROCEDURE CSMRT_OWNER.PS_CLST_MAIN_TBL_P
/

--
-- PS_CLST_MAIN_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_CLST_MAIN_TBL_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_CLST_MAIN_TBL'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_CLST_MAIN_TBL', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_CLST_MAIN_TBL'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_CLST_MAIN_TBL from PeopleSoft table PS_CLST_MAIN_TBL.
--
-- V01  SMT-xxxx 05/16/2017,    Jim Doucette
--                              Converted from PS_CLST_MAIN_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_CLST_MAIN_TBL';
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
 where TABLE_NAME = 'PS_CLST_MAIN_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_CLST_MAIN_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_CLST_MAIN_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_CLST_MAIN_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_CLST_MAIN_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_CLST_MAIN_TBL T
using (select /*+ full(S) */
    nvl(trim(COURSE_LIST),'-') COURSE_LIST, 
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/
    YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    DESCR DESCR,
    DESCRSHORT DESCRSHORT,
    RQRMNT_USEAGE RQRMNT_USEAGE,
    INSTITUTION INSTITUTION,
    ACAD_CAREER ACAD_CAREER,
    ACAD_GROUP ACAD_GROUP,
    ACAD_PROG ACAD_PROG,
    ACAD_PLAN ACAD_PLAN,
    ACAD_SUB_PLAN ACAD_SUB_PLAN,
    SUBJECT SUBJECT,
    CATALOG_NBR CATALOG_NBR,
    DESCR254A DESCR254A
    from SYSADM.PS_CLST_MAIN_TBL@SASOURCE S 
    where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLST_MAIN_TBL') ) S 
 on ( 
    T.COURSE_LIST = S.COURSE_LIST and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
    when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.RQRMNT_USEAGE = S.RQRMNT_USEAGE,
    T.INSTITUTION = S.INSTITUTION,
    T.ACAD_CAREER = S.ACAD_CAREER,
    T.ACAD_GROUP = S.ACAD_GROUP,
    T.ACAD_PROG = S.ACAD_PROG,
    T.ACAD_PLAN = S.ACAD_PLAN,
    T.ACAD_SUB_PLAN = S.ACAD_SUB_PLAN,
    T.SUBJECT = S.SUBJECT,
    T.CATALOG_NBR = S.CATALOG_NBR,
    T.DESCR254A = S.DESCR254A,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    nvl(trim(T.DESCR),0) <> nvl(trim(S.DESCR),0) or 
    nvl(trim(T.DESCRSHORT),0) <> nvl(trim(S.DESCRSHORT),0) or 
    nvl(trim(T.RQRMNT_USEAGE),0) <> nvl(trim(S.RQRMNT_USEAGE),0) or 
    nvl(trim(T.INSTITUTION),0) <> nvl(trim(S.INSTITUTION),0) or 
    nvl(trim(T.ACAD_CAREER),0) <> nvl(trim(S.ACAD_CAREER),0) or 
    nvl(trim(T.ACAD_GROUP),0) <> nvl(trim(S.ACAD_GROUP),0) or 
    nvl(trim(T.ACAD_PROG),0) <> nvl(trim(S.ACAD_PROG),0) or 
    nvl(trim(T.ACAD_PLAN),0) <> nvl(trim(S.ACAD_PLAN),0) or 
    nvl(trim(T.ACAD_SUB_PLAN),0) <> nvl(trim(S.ACAD_SUB_PLAN),0) or 
    nvl(trim(T.SUBJECT),0) <> nvl(trim(S.SUBJECT),0) or 
    nvl(trim(T.CATALOG_NBR),0) <> nvl(trim(S.CATALOG_NBR),0) or 
    nvl(trim(T.DESCR254A),0) <> nvl(trim(S.DESCR254A),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.COURSE_LIST,
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.DESCR,
    T.DESCRSHORT, 
    T.RQRMNT_USEAGE,
    T.INSTITUTION,
    T.ACAD_CAREER,
    T.ACAD_GROUP, 
    T.ACAD_PROG,
    T.ACAD_PLAN,
    T.ACAD_SUB_PLAN,
    T.SUBJECT,
    T.CATALOG_NBR,
    T.DESCR254A,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.COURSE_LIST,
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.DESCR,
    S.DESCRSHORT, 
    S.RQRMNT_USEAGE,
    S.INSTITUTION,
    S.ACAD_CAREER,
    S.ACAD_GROUP, 
    S.ACAD_PROG,
    S.ACAD_PLAN,
    S.ACAD_SUB_PLAN,
    S.SUBJECT,
    S.CATALOG_NBR,
    S.DESCR254A,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);



strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLST_MAIN_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLST_MAIN_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_CLST_MAIN_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_CLST_MAIN_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_CLST_MAIN_TBL';
update CSSTG_OWNER.PS_CLST_MAIN_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select COURSE_LIST, EFFDT
   from CSSTG_OWNER.PS_CLST_MAIN_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLST_MAIN_TBL') = 'Y'
  minus
 select COURSE_LIST, EFFDT
   from SYSADM.PS_CLST_MAIN_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLST_MAIN_TBL') = 'Y'
   ) S
 where T.COURSE_LIST = S.COURSE_LIST
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLST_MAIN_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLST_MAIN_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_CLST_MAIN_TBL'
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

END PS_CLST_MAIN_TBL_P;
/
