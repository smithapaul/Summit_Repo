CREATE OR REPLACE PROCEDURE             "PS_STDNT_ADVR_HIST_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_STDNT_ADVR_HIST'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_STDNT_ADVR_HIST', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_STDNT_ADVR_HIST'
   */


   ------------------------------------------------------------------------
   --
   --
   -- Loads stage table PS_STDNT_ADVR_HIST from PeopleSoft table PS_STDNT_ADVR_HIST.
   --
   -- V01  SMT-xxxx 08/09/2017,    Preethi Lodha
   --                              Converted from PS_STDNT_ADVR_HIST.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_STDNT_ADVR_HIST';
   intProcessSid      INTEGER;
   dtProcessStart     DATE := SYSDATE;
   strMessage01       VARCHAR2 (4000);
   strMessage02       VARCHAR2 (512);
   strMessage03       VARCHAR2 (512) := '';
   strNewLine         VARCHAR2 (2) := CHR (13) || CHR (10);
   strSqlCommand      VARCHAR2 (32767) := '';
   strSqlDynamic      VARCHAR2 (32767) := '';
   strClientInfo      VARCHAR2 (100);
   intRowCount        INTEGER;
   intTotalRowCount   INTEGER := 0;
   numSqlCode         NUMBER;
   strSqlErrm         VARCHAR2 (4000);
   intTries           INTEGER;
BEGIN
   strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
   DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT (
      i_MartId             => strMartId,
      i_ProcessName        => strProcessName,
      i_ProcessStartTime   => dtProcessStart,
      o_ProcessSid         => intProcessSid);

   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);


   strSqlCommand := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Reading', START_DT = SYSDATE, END_DT = NULL
    WHERE TABLE_NAME = 'PS_STDNT_ADVR_HIST';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_STDNT_ADVR_HIST@SASOURCE S)
    WHERE TABLE_NAME = 'PS_STDNT_ADVR_HIST';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_STDNT_ADVR_HIST';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_STDNT_ADVR_HIST';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_STDNT_ADVR_HIST T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTITUTION),'-') INSTITUTION,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(ADVISOR_ROLE),'-') ADVISOR_ROLE,
nvl(STDNT_ADVISOR_NBR,0) STDNT_ADVISOR_NBR,
nvl(trim(ACAD_PROG),'-') ACAD_PROG,
nvl(trim(ADVISOR_ID),'-') ADVISOR_ID,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(ACAD_PLAN),'-') ACAD_PLAN,
nvl(trim(APPROVE_ENRLMT),'-') APPROVE_ENRLMT,
nvl(trim(APPROVE_GRAD),'-') APPROVE_GRAD,
nvl(trim(GRAD_APPROVED),'-') GRAD_APPROVED,
nvl(trim(COMMITTEE_ID),'-') COMMITTEE_ID,
nvl(trim(COMM_PERS_CD),'-') COMM_PERS_CD
from SYSADM.PS_STDNT_ADVR_HIST@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_ADVR_HIST') ) S
   on (
T.EMPLID = S.EMPLID and
T.INSTITUTION = S.INSTITUTION and
T.EFFDT = S.EFFDT and
T.ADVISOR_ROLE = S.ADVISOR_ROLE and
T.STDNT_ADVISOR_NBR = S.STDNT_ADVISOR_NBR and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.ACAD_PROG = S.ACAD_PROG,
T.ADVISOR_ID = S.ADVISOR_ID,
T.ACAD_CAREER = S.ACAD_CAREER,
T.ACAD_PLAN = S.ACAD_PLAN,
T.APPROVE_ENRLMT = S.APPROVE_ENRLMT,
T.APPROVE_GRAD = S.APPROVE_GRAD,
T.GRAD_APPROVED = S.GRAD_APPROVED,
T.COMMITTEE_ID = S.COMMITTEE_ID,
T.COMM_PERS_CD = S.COMM_PERS_CD,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.ACAD_PROG <> S.ACAD_PROG or
T.ADVISOR_ID <> S.ADVISOR_ID or
T.ACAD_CAREER <> S.ACAD_CAREER or
T.ACAD_PLAN <> S.ACAD_PLAN or
T.APPROVE_ENRLMT <> S.APPROVE_ENRLMT or
T.APPROVE_GRAD <> S.APPROVE_GRAD or
T.GRAD_APPROVED <> S.GRAD_APPROVED or
T.COMMITTEE_ID <> S.COMMITTEE_ID or
T.COMM_PERS_CD <> S.COMM_PERS_CD or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.INSTITUTION,
T.EFFDT,
T.ADVISOR_ROLE,
T.STDNT_ADVISOR_NBR,
T.SRC_SYS_ID,
T.ACAD_PROG,
T.ADVISOR_ID,
T.ACAD_CAREER,
T.ACAD_PLAN,
T.APPROVE_ENRLMT,
T.APPROVE_GRAD,
T.GRAD_APPROVED,
T.COMMITTEE_ID,
T.COMM_PERS_CD,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.EMPLID,
S.INSTITUTION,
S.EFFDT,
S.ADVISOR_ROLE,
S.STDNT_ADVISOR_NBR,
'CS90',
S.ACAD_PROG,
S.ADVISOR_ID,
S.ACAD_CAREER,
S.ACAD_PLAN,
S.APPROVE_ENRLMT,
S.APPROVE_GRAD,
S.GRAD_APPROVED,
S.COMMITTEE_ID,
S.COMM_PERS_CD,
'N',
'S',
sysdate,
sysdate,
1234);
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_STDNT_ADVR_HIST rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_STDNT_ADVR_HIST',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_STDNT_ADVR_HIST';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_ADVR_HIST';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_ADVR_HIST';

update CSSTG_OWNER.PS_STDNT_ADVR_HIST T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, EFFDT, ADVISOR_ROLE, STDNT_ADVISOR_NBR
   from CSSTG_OWNER.PS_STDNT_ADVR_HIST T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_ADVR_HIST') = 'Y'
  minus
 select EMPLID, INSTITUTION, EFFDT, ADVISOR_ROLE, STDNT_ADVISOR_NBR
   from SYSADM.PS_STDNT_ADVR_HIST@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_ADVR_HIST') = 'Y' 
   ) S
 where T.INSTITUTION = S.INSTITUTION   
  AND T.EMPLID = S.EMPLID
  AND T.EFFDT = S.EFFDT
  AND T.ADVISOR_ROLE = S.ADVISOR_ROLE
  AND T.STDNT_ADVISOR_NBR = S.STDNT_ADVISOR_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_STDNT_ADVR_HIST rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_STDNT_ADVR_HIST',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_STDNT_ADVR_HIST';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

   strMessage01 := strProcessName || ' is complete.';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);
EXCEPTION
   WHEN OTHERS
   THEN
      COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
         i_SqlCommand   => strSqlCommand,
         i_SqlCode      => SQLCODE,
         i_SqlErrm      => SQLERRM);
END PS_STDNT_ADVR_HIST_P;
/
