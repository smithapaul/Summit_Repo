DROP PROCEDURE CSMRT_OWNER.AM_PS_ACAD_ORG_TBL_P
/

--
-- AM_PS_ACAD_ORG_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_ACAD_ORG_TBL_P" IS

------------------------------------------------------------------------
--Preethi Lodha
--
-- Loads stage table PS_ACAD_ORG_TBL from PeopleSoft table PS_ACAD_ORG_TBL.
--
-- V01  SMT-xxxx 07/12/2017,    Preethi Lodha
--                              Converted from PS_ACAD_ORG_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_ACAD_ORG_TBL';
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

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_ACAD_ORG_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ACAD_ORG_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_ACAD_ORG_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_ACAD_ORG_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_ACAD_ORG_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_ACAD_ORG_TBL T
using (select /*+ full(S) */
nvl(trim(ACAD_ORG),'-') ACAD_ORG,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(EFF_STATUS),'-') EFF_STATUS,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(DESCRSHORT),'-') DESCRSHORT,
nvl(trim(DESCRFORMAL),'-') DESCRFORMAL,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(CAMPUS),'-') CAMPUS,
nvl(trim(MANAGER_ID),'-') MANAGER_ID,
nvl(trim(INSTR_EDIT),'-') INSTR_EDIT,
nvl(trim(CAMPUS_EDIT),'-') CAMPUS_EDIT,
nvl(trim(SUBJECT_EDIT),'-') SUBJECT_EDIT,
nvl(trim(COURSE_EDIT),'-') COURSE_EDIT
from SYSADM.PS_ACAD_ORG_TBL@AMSOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_ORG_TBL') ) S
   on (
T.ACAD_ORG = S.ACAD_ORG and
T.EFFDT = S.EFFDT and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.EFF_STATUS = S.EFF_STATUS,
T.DESCR = S.DESCR,
T.DESCRSHORT = S.DESCRSHORT,
T.DESCRFORMAL = S.DESCRFORMAL,
T.INSTITUTION = S.INSTITUTION,
T.CAMPUS = S.CAMPUS,
T.MANAGER_ID = S.MANAGER_ID,
T.INSTR_EDIT = S.INSTR_EDIT,
T.CAMPUS_EDIT = S.CAMPUS_EDIT,
T.SUBJECT_EDIT = S.SUBJECT_EDIT,
T.COURSE_EDIT = S.COURSE_EDIT,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.EFF_STATUS <> S.EFF_STATUS or
T.DESCR <> S.DESCR or
T.DESCRSHORT <> S.DESCRSHORT or
T.DESCRFORMAL <> S.DESCRFORMAL or
T.INSTITUTION <> S.INSTITUTION or
T.CAMPUS <> S.CAMPUS or
T.MANAGER_ID <> S.MANAGER_ID or
T.INSTR_EDIT <> S.INSTR_EDIT or
T.CAMPUS_EDIT <> S.CAMPUS_EDIT or
T.SUBJECT_EDIT <> S.SUBJECT_EDIT or
T.COURSE_EDIT <> S.COURSE_EDIT or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.ACAD_ORG,
T.EFFDT,
T.SRC_SYS_ID,
T.EFF_STATUS,
T.DESCR,
T.DESCRSHORT,
T.DESCRFORMAL,
T.INSTITUTION,
T.CAMPUS,
T.MANAGER_ID,
T.INSTR_EDIT,
T.CAMPUS_EDIT,
T.SUBJECT_EDIT,
T.COURSE_EDIT,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.ACAD_ORG,
S.EFFDT,
'CS90',
S.EFF_STATUS,
S.DESCR,
S.DESCRSHORT,
S.DESCRFORMAL,
S.INSTITUTION,
S.CAMPUS,
S.MANAGER_ID,
S.INSTR_EDIT,
S.CAMPUS_EDIT,
S.SUBJECT_EDIT,
S.COURSE_EDIT,
'N',
'S',
sysdate,
sysdate,
1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_ORG_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_ORG_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ACAD_ORG_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_ACAD_ORG_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_ACAD_ORG_TBL';

update AMSTG_OWNER.PS_ACAD_ORG_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select ACAD_ORG, EFFDT
   from AMSTG_OWNER.PS_ACAD_ORG_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_ORG_TBL') = 'Y'
  minus
 select ACAD_ORG, EFFDT
   from SYSADM.PS_ACAD_ORG_TBL@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_ORG_TBL') = 'Y' 
   ) S
 where T.ACAD_ORG = S.ACAD_ORG    
    AND T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_ORG_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_ORG_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ACAD_ORG_TBL'
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

END AM_PS_ACAD_ORG_TBL_P;
/
