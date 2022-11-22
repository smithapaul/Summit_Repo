DROP PROCEDURE CSMRT_OWNER.AM_PS_GRADE_TBL_P
/

--
-- AM_PS_GRADE_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_GRADE_TBL_P" IS

   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_GRADE_TBL from PeopleSoft table PS_GRADE_TBL.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_GRADE_TBL.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_GRADE_TBL';
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

   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);


   strSqlCommand := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Reading', START_DT = SYSDATE, END_DT = NULL
    WHERE TABLE_NAME = 'PS_GRADE_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_GRADE_TBL@AMSOURCE S)
    WHERE TABLE_NAME = 'PS_GRADE_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into AMSTG_OWNER.PS_GRADE_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into AMSTG_OWNER.PS_GRADE_TBL';

merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_GRADE_TBL T
using (select /*+ full(S) */
nvl(trim(SETID),'-') SETID,
nvl(trim(GRADING_SCHEME),'-') GRADING_SCHEME,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(GRADING_BASIS),'-') GRADING_BASIS,
nvl(trim(CRSE_GRADE_INPUT),'-') CRSE_GRADE_INPUT,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(DESCRSHORT),'-') DESCRSHORT,
nvl(trim(GRADE_CONVERT),'-') GRADE_CONVERT,
nvl(GRADE_POINTS,0) GRADE_POINTS,
nvl(trim(EARN_CREDIT),'-') EARN_CREDIT,
nvl(trim(INCLUDE_IN_GPA),'-') INCLUDE_IN_GPA,
nvl(trim(IN_PROGRESS_GRD),'-') IN_PROGRESS_GRD,
nvl(trim(VALID_ATTEMPT),'-') VALID_ATTEMPT,
nvl(trim(GRADE_CATEGORY),'-') GRADE_CATEGORY,
nvl(trim(EXCLUDE_PRGRSS_UNT),'-') EXCLUDE_PRGRSS_UNT,
nvl(trim(SSR_GRADE_FLAG),'-') SSR_GRADE_FLAG
from SYSADM.PS_GRADE_TBL@AMSOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRADE_TBL') ) S
   on (
T.SETID = S.SETID and
T.GRADING_SCHEME = S.GRADING_SCHEME and
T.EFFDT = S.EFFDT and
T.GRADING_BASIS = S.GRADING_BASIS and
T.CRSE_GRADE_INPUT = S.CRSE_GRADE_INPUT and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.DESCR = S.DESCR,
T.DESCRSHORT = S.DESCRSHORT,
T.GRADE_CONVERT = S.GRADE_CONVERT,
T.GRADE_POINTS = S.GRADE_POINTS,
T.EARN_CREDIT = S.EARN_CREDIT,
T.INCLUDE_IN_GPA = S.INCLUDE_IN_GPA,
T.IN_PROGRESS_GRD = S.IN_PROGRESS_GRD,
T.VALID_ATTEMPT = S.VALID_ATTEMPT,
T.GRADE_CATEGORY = S.GRADE_CATEGORY,
T.EXCLUDE_PRGRSS_UNT = S.EXCLUDE_PRGRSS_UNT,
T.SSR_GRADE_FLAG = S.SSR_GRADE_FLAG,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.DESCR <> S.DESCR or
T.DESCRSHORT <> S.DESCRSHORT or
T.GRADE_CONVERT <> S.GRADE_CONVERT or
T.GRADE_POINTS <> S.GRADE_POINTS or
T.EARN_CREDIT <> S.EARN_CREDIT or
T.INCLUDE_IN_GPA <> S.INCLUDE_IN_GPA or
T.IN_PROGRESS_GRD <> S.IN_PROGRESS_GRD or
T.VALID_ATTEMPT <> S.VALID_ATTEMPT or
T.GRADE_CATEGORY <> S.GRADE_CATEGORY or
T.EXCLUDE_PRGRSS_UNT <> S.EXCLUDE_PRGRSS_UNT or
T.SSR_GRADE_FLAG <> S.SSR_GRADE_FLAG or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.SETID,
T.GRADING_SCHEME,
T.EFFDT,
T.GRADING_BASIS,
T.CRSE_GRADE_INPUT,
T.SRC_SYS_ID,
T.DESCR,
T.DESCRSHORT,
T.GRADE_CONVERT,
T.GRADE_POINTS,
T.EARN_CREDIT,
T.INCLUDE_IN_GPA,
T.IN_PROGRESS_GRD,
T.VALID_ATTEMPT,
T.GRADE_CATEGORY,
T.EXCLUDE_PRGRSS_UNT,
T.SSR_GRADE_FLAG,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.SETID,
S.GRADING_SCHEME,
S.EFFDT,
S.GRADING_BASIS,
S.CRSE_GRADE_INPUT,
'CS90',
S.DESCR,
S.DESCRSHORT,
S.GRADE_CONVERT,
S.GRADE_POINTS,
S.EARN_CREDIT,
S.INCLUDE_IN_GPA,
S.IN_PROGRESS_GRD,
S.VALID_ATTEMPT,
S.GRADE_CATEGORY,
S.EXCLUDE_PRGRSS_UNT,
S.SSR_GRADE_FLAG,
'N',
'S',
sysdate,
sysdate,
1234);
COMMIT;

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_GRADE_TBL rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_GRADE_TBL',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_GRADE_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_GRADE_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on AMSTG_OWNER.PS_GRADE_TBL';


update AMSTG_OWNER.PS_GRADE_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, GRADING_SCHEME, EFFDT, GRADING_BASIS, CRSE_GRADE_INPUT
   from AMSTG_OWNER.PS_GRADE_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRADE_TBL') = 'Y'
  minus
 select SETID, GRADING_SCHEME, EFFDT, GRADING_BASIS, CRSE_GRADE_INPUT
   from SYSADM.PS_GRADE_TBL@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRADE_TBL') = 'Y' 
   ) S
 where T.SETID = S.SETID   
  AND T.GRADING_SCHEME = S.GRADING_SCHEME 
    AND T.EFFDT = S.EFFDT
     AND T.GRADING_BASIS = S.GRADING_BASIS
     AND T.CRSE_GRADE_INPUT = S.CRSE_GRADE_INPUT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_GRADE_TBL rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_GRADE_TBL',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_GRADE_TBL';

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
END AM_PS_GRADE_TBL_P;
/
