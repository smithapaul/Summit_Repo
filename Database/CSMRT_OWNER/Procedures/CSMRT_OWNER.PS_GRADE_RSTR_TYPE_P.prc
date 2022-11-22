DROP PROCEDURE CSMRT_OWNER.PS_GRADE_RSTR_TYPE_P
/

--
-- PS_GRADE_RSTR_TYPE_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_GRADE_RSTR_TYPE_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_GRADE_RSTR_TYPE'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_GRADE_RSTR_TYPE', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_GRADE_RSTR_TYPE'
   */


   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_GRADE_RSTR_TYPE from PeopleSoft table PS_GRADE_RSTR_TYPE.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_GRADE_RSTR_TYPE.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_GRADE_RSTR_TYPE';
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
    WHERE TABLE_NAME = 'PS_GRADE_RSTR_TYPE';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_GRADE_RSTR_TYPE@SASOURCE S)
    WHERE TABLE_NAME = 'PS_GRADE_RSTR_TYPE';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_GRADE_RSTR_TYPE';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_GRADE_RSTR_TYPE';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_GRADE_RSTR_TYPE T
using (select /*+ full(S) */
nvl(trim(STRM),'-') STRM,
nvl(CLASS_NBR,0) CLASS_NBR,
nvl(GRD_RSTR_TYPE_SEQ,0) GRD_RSTR_TYPE_SEQ,
nvl(trim(GRADE_ROSTER_TYPE),'-') GRADE_ROSTER_TYPE,
nvl(trim(GRADING_STATUS),'-') GRADING_STATUS,
nvl(trim(GR_APPROVAL_STATUS),'-') GR_APPROVAL_STATUS,
to_date(to_char(case when APPROVAL_DATE < '01-JAN-1800' then NULL else APPROVAL_DATE end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') APPROVAL_DATE,
to_date(to_char(case when POSTING_DATE < '01-JAN-1800' then NULL else POSTING_DATE end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') POSTING_DATE,
nvl(trim(PARTIAL_POST),'-') PARTIAL_POST,
nvl(trim(OVRD_GRADE_ROSTER),'-') OVRD_GRADE_ROSTER,
nvl(PROCESS_INSTANCE,0) PROCESS_INSTANCE,
nvl(trim(DESCR),'-') DESCR
from SYSADM.PS_GRADE_RSTR_TYPE@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRADE_RSTR_TYPE') ) S
   on (
T.STRM = S.STRM and
T.CLASS_NBR = S.CLASS_NBR and
T.GRD_RSTR_TYPE_SEQ = S.GRD_RSTR_TYPE_SEQ and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.GRADE_ROSTER_TYPE = S.GRADE_ROSTER_TYPE,
T.GRADING_STATUS = S.GRADING_STATUS,
T.GR_APPROVAL_STATUS = S.GR_APPROVAL_STATUS,
T.APPROVAL_DATE = S.APPROVAL_DATE,
T.POSTING_DATE = S.POSTING_DATE,
T.PARTIAL_POST = S.PARTIAL_POST,
T.OVRD_GRADE_ROSTER = S.OVRD_GRADE_ROSTER,
T.PROCESS_INSTANCE = S.PROCESS_INSTANCE,
T.DESCR = S.DESCR,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.GRADE_ROSTER_TYPE <> S.GRADE_ROSTER_TYPE or
T.GRADING_STATUS <> S.GRADING_STATUS or
T.GR_APPROVAL_STATUS <> S.GR_APPROVAL_STATUS or
nvl(trim(T.APPROVAL_DATE),0) <> nvl(trim(S.APPROVAL_DATE),0) or
nvl(trim(T.POSTING_DATE),0) <> nvl(trim(S.POSTING_DATE),0) or
T.PARTIAL_POST <> S.PARTIAL_POST or
T.OVRD_GRADE_ROSTER <> S.OVRD_GRADE_ROSTER or
T.PROCESS_INSTANCE <> S.PROCESS_INSTANCE or
T.DESCR <> S.DESCR or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.STRM,
T.CLASS_NBR,
T.GRD_RSTR_TYPE_SEQ,
T.SRC_SYS_ID,
T.GRADE_ROSTER_TYPE,
T.GRADING_STATUS,
T.GR_APPROVAL_STATUS,
T.APPROVAL_DATE,
T.POSTING_DATE,
T.PARTIAL_POST,
T.OVRD_GRADE_ROSTER,
T.PROCESS_INSTANCE,
T.DESCR,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.STRM,
S.CLASS_NBR,
S.GRD_RSTR_TYPE_SEQ,
'CS90',
S.GRADE_ROSTER_TYPE,
S.GRADING_STATUS,
S.GR_APPROVAL_STATUS,
S.APPROVAL_DATE,
S.POSTING_DATE,
S.PARTIAL_POST,
S.OVRD_GRADE_ROSTER,
S.PROCESS_INSTANCE,
S.DESCR,
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
         '# of PS_GRADE_RSTR_TYPE rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_GRADE_RSTR_TYPE',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_GRADE_RSTR_TYPE';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_GRADE_RSTR_TYPE';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_GRADE_RSTR_TYPE';

update CSSTG_OWNER.PS_GRADE_RSTR_TYPE T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select STRM, CLASS_NBR, GRD_RSTR_TYPE_SEQ
   from CSSTG_OWNER.PS_GRADE_RSTR_TYPE T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRADE_RSTR_TYPE') = 'Y'
  minus
 select STRM, CLASS_NBR, GRD_RSTR_TYPE_SEQ
   from SYSADM.PS_GRADE_RSTR_TYPE@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRADE_RSTR_TYPE') = 'Y' 
   ) S
 where T.CLASS_NBR = S.CLASS_NBR   
  AND T.GRD_RSTR_TYPE_SEQ = S.GRD_RSTR_TYPE_SEQ
  AND T.STRM = S.STRM
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_GRADE_RSTR_TYPE rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_GRADE_RSTR_TYPE',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_GRADE_RSTR_TYPE';

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
END PS_GRADE_RSTR_TYPE_P;
/
