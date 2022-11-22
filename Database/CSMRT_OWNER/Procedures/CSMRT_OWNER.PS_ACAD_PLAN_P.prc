DROP PROCEDURE CSMRT_OWNER.PS_ACAD_PLAN_P
/

--
-- PS_ACAD_PLAN_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_ACAD_PLAN_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_ACAD_PLAN'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_ACAD_PLAN', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_ACAD_PLAN'
   */


   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_ACAD_PLAN from PeopleSoft table PS_ACAD_PLAN.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_ACAD_PLAN.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_ACAD_PLAN';
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
    WHERE TABLE_NAME = 'PS_ACAD_PLAN';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_ACAD_PLAN@SASOURCE S)
    WHERE TABLE_NAME = 'PS_ACAD_PLAN';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_ACAD_PLAN';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_ACAD_PLAN';

merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ACAD_PLAN T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(EFFSEQ,0) EFFSEQ,
nvl(trim(ACAD_PLAN),'-') ACAD_PLAN,
to_date(to_char(case when DECLARE_DT < '01-JAN-1800' then NULL else DECLARE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DECLARE_DT,
nvl(PLAN_SEQUENCE,0) PLAN_SEQUENCE,
nvl(trim(REQ_TERM),'-') REQ_TERM,
nvl(trim(COMPLETION_TERM),'-') COMPLETION_TERM,
nvl(trim(STDNT_DEGR),'-') STDNT_DEGR,
nvl(trim(DEGR_CHKOUT_STAT),'-') DEGR_CHKOUT_STAT,
nvl(trim(ADVIS_STATUS),'-') ADVIS_STATUS,
nvl(SSR_APT_INSTANCE,0) SSR_APT_INSTANCE,
nvl(trim(SSR_YR_OF_PROG),'-') SSR_YR_OF_PROG,
nvl(trim(SCC_ROW_ADD_OPRID),'-') SCC_ROW_ADD_OPRID,
to_date(to_char(case when SCC_ROW_ADD_DTTM < '01-JAN-1800' then NULL else SCC_ROW_ADD_DTTM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SCC_ROW_ADD_DTTM,
nvl(trim(SCC_ROW_UPD_OPRID),'-') SCC_ROW_UPD_OPRID,
to_date(to_char(case when SCC_ROW_UPD_DTTM < '01-JAN-1800' then NULL else SCC_ROW_UPD_DTTM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SCC_ROW_UPD_DTTM
from SYSADM.PS_ACAD_PLAN@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_PLAN') ) S
   on (
T.EMPLID = S.EMPLID and
T.ACAD_CAREER = S.ACAD_CAREER and
T.STDNT_CAR_NBR = S.STDNT_CAR_NBR and
T.EFFDT = S.EFFDT and
T.EFFSEQ = S.EFFSEQ and
T.ACAD_PLAN = S.ACAD_PLAN and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.DECLARE_DT = S.DECLARE_DT,
T.PLAN_SEQUENCE = S.PLAN_SEQUENCE,
T.REQ_TERM = S.REQ_TERM,
T.COMPLETION_TERM = S.COMPLETION_TERM,
T.STDNT_DEGR = S.STDNT_DEGR,
T.DEGR_CHKOUT_STAT = S.DEGR_CHKOUT_STAT,
T.ADVIS_STATUS = S.ADVIS_STATUS,
T.SSR_APT_INSTANCE = S.SSR_APT_INSTANCE,
T.SSR_YR_OF_PROG = S.SSR_YR_OF_PROG,
T.SCC_ROW_ADD_OPRID = S.SCC_ROW_ADD_OPRID,
T.SCC_ROW_ADD_DTTM = S.SCC_ROW_ADD_DTTM,
T.SCC_ROW_UPD_OPRID = S.SCC_ROW_UPD_OPRID,
T.SCC_ROW_UPD_DTTM = S.SCC_ROW_UPD_DTTM,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.DECLARE_DT <> S.DECLARE_DT or
T.PLAN_SEQUENCE <> S.PLAN_SEQUENCE or
T.REQ_TERM <> S.REQ_TERM or
T.COMPLETION_TERM <> S.COMPLETION_TERM or
T.STDNT_DEGR <> S.STDNT_DEGR or
T.DEGR_CHKOUT_STAT <> S.DEGR_CHKOUT_STAT or
T.ADVIS_STATUS <> S.ADVIS_STATUS or
T.SSR_APT_INSTANCE <> S.SSR_APT_INSTANCE or
T.SSR_YR_OF_PROG <> S.SSR_YR_OF_PROG or
T.SCC_ROW_ADD_OPRID <> S.SCC_ROW_ADD_OPRID or
nvl(trim(T.SCC_ROW_ADD_DTTM),0) <> nvl(trim(S.SCC_ROW_ADD_DTTM),0) or
T.SCC_ROW_UPD_OPRID <> S.SCC_ROW_UPD_OPRID or
nvl(trim(T.SCC_ROW_UPD_DTTM),0) <> nvl(trim(S.SCC_ROW_UPD_DTTM),0) or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.ACAD_CAREER,
T.STDNT_CAR_NBR,
T.EFFDT,
T.EFFSEQ,
T.ACAD_PLAN,
T.SRC_SYS_ID,
T.DECLARE_DT,
T.PLAN_SEQUENCE,
T.REQ_TERM,
T.COMPLETION_TERM,
T.STDNT_DEGR,
T.DEGR_CHKOUT_STAT,
T.ADVIS_STATUS,
T.SSR_APT_INSTANCE,
T.SSR_YR_OF_PROG,
T.SCC_ROW_ADD_OPRID,
T.SCC_ROW_ADD_DTTM,
T.SCC_ROW_UPD_OPRID,
T.SCC_ROW_UPD_DTTM,
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
S.EFFDT,
S.EFFSEQ,
S.ACAD_PLAN,
'CS90',
S.DECLARE_DT,
S.PLAN_SEQUENCE,
S.REQ_TERM,
S.COMPLETION_TERM,
S.STDNT_DEGR,
S.DEGR_CHKOUT_STAT,
S.ADVIS_STATUS,
S.SSR_APT_INSTANCE,
S.SSR_YR_OF_PROG,
S.SCC_ROW_ADD_OPRID,
S.SCC_ROW_ADD_DTTM,
S.SCC_ROW_UPD_OPRID,
S.SCC_ROW_UPD_DTTM,
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
         '# of PS_ACAD_PLAN rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_ACAD_PLAN',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_ACAD_PLAN';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_PLAN';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_PLAN';


update CSSTG_OWNER.PS_ACAD_PLAN T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, EFFDT, EFFSEQ, ACAD_PLAN
   from CSSTG_OWNER.PS_ACAD_PLAN T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_PLAN') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, EFFDT, EFFSEQ, ACAD_PLAN
   from SYSADM.PS_ACAD_PLAN@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_PLAN') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID   
  AND T.ACAD_CAREER = S.ACAD_CAREER
  AND T.STDNT_CAR_NBR = S.STDNT_CAR_NBR
  AND T.EFFDT = S.EFFDT
  AND T.EFFSEQ = S.EFFSEQ
  AND T.ACAD_PLAN = S.ACAD_PLAN
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_ACAD_PLAN rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_ACAD_PLAN',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_ACAD_PLAN';

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
END PS_ACAD_PLAN_P;
/
