CREATE OR REPLACE PROCEDURE             "PS_RECRUITER_CAT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_RECRUITER_CAT from PeopleSoft table PS_RECRUITER_CAT.
--
 --V01  SMT-xxxx 08/16/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_RECRUITER_CAT';
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
 where TABLE_NAME = 'PS_RECRUITER_CAT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_RECRUITER_CAT@SASOURCE S)
 where TABLE_NAME = 'PS_RECRUITER_CAT'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_RECRUITER_CAT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_RECRUITER_CAT';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_RECRUITER_CAT T
using (select /*+ full(S) */
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(RECRUITER_ID),'-') RECRUITER_ID,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(ASSIGNMENT_STAGE),'-') ASSIGNMENT_STAGE,
nvl(trim(RECRUITMENT_CAT),'-') RECRUITMENT_CAT
from SYSADM.PS_RECRUITER_CAT@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RECRUITER_CAT') ) S
   on (
T.INSTITUTION = S.INSTITUTION and
T.ACAD_CAREER = S.ACAD_CAREER and
T.RECRUITER_ID = S.RECRUITER_ID and
T.EFFDT = S.EFFDT and
T.ASSIGNMENT_STAGE = S.ASSIGNMENT_STAGE and
T.RECRUITMENT_CAT = S.RECRUITMENT_CAT and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.INSTITUTION,
T.ACAD_CAREER,
T.RECRUITER_ID,
T.EFFDT,
T.ASSIGNMENT_STAGE,
T.RECRUITMENT_CAT,
T.SRC_SYS_ID,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.INSTITUTION,
S.ACAD_CAREER,
S.RECRUITER_ID,
S.EFFDT,
S.ASSIGNMENT_STAGE,
S.RECRUITMENT_CAT,
'CS90',
'N',
'S',
sysdate,
sysdate,
1234);
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_RECRUITER_CAT rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_RECRUITER_CAT',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_RECRUITER_CAT';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_RECRUITER_CAT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_RECRUITER_CAT';
update CSSTG_OWNER.PS_RECRUITER_CAT T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, ACAD_CAREER, RECRUITER_ID, EFFDT, ASSIGNMENT_STAGE, RECRUITMENT_CAT
   from CSSTG_OWNER.PS_RECRUITER_CAT T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RECRUITER_CAT') = 'Y'
  minus
 select INSTITUTION, ACAD_CAREER, RECRUITER_ID, EFFDT, ASSIGNMENT_STAGE, RECRUITMENT_CAT
   from SYSADM.PS_RECRUITER_CAT@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RECRUITER_CAT') = 'Y'
   ) S
 where T.INSTITUTION = S.INSTITUTION
  and T.ACAD_CAREER = S.ACAD_CAREER
  and T.RECRUITER_ID = S.RECRUITER_ID
      and T.EFFDT = S.EFFDT
      and T.ASSIGNMENT_STAGE = S.ASSIGNMENT_STAGE
      and T.RECRUITMENT_CAT = S.RECRUITMENT_CAT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_RECRUITER_CAT rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_RECRUITER_CAT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_RECRUITER_CAT'
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

END PS_RECRUITER_CAT_P;
/
