DROP PROCEDURE CSMRT_OWNER.PS_CLASS_INSTR_P
/

--
-- PS_CLASS_INSTR_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_CLASS_INSTR_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_CLASS_INSTR'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_CLASS_INSTR', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_CLASS_INSTR'
   */


   ------------------------------------------------------------------------
   --
   --
   -- Loads stage table PS_CLASS_INSTR from PeopleSoft table PS_CLASS_INSTR.
   --
   -- V01  SMT-xxxx 08/09/2017,    Preethi Lodha
   --                              Converted from PS_CLASS_INSTR.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_CLASS_INSTR';
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
    WHERE TABLE_NAME = 'PS_CLASS_INSTR';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_CLASS_INSTR@SASOURCE S)
    WHERE TABLE_NAME = 'PS_CLASS_INSTR';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_CLASS_INSTR';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_CLASS_INSTR';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_CLASS_INSTR T
using (select /*+ full(S) */
nvl(trim(CRSE_ID),'-') CRSE_ID,
nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR,
nvl(trim(STRM),'-') STRM,
nvl(trim(SESSION_CODE),'-') SESSION_CODE,
nvl(trim(CLASS_SECTION),'-') CLASS_SECTION,
nvl(CLASS_MTG_NBR,0) CLASS_MTG_NBR,
nvl(INSTR_ASSIGN_SEQ,0) INSTR_ASSIGN_SEQ,
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTR_ROLE),'-') INSTR_ROLE,
nvl(trim(GRADE_RSTR_ACCESS),'-') GRADE_RSTR_ACCESS,
nvl(CONTACT_MINUTES,0) CONTACT_MINUTES,
nvl(trim(SCHED_PRINT_INSTR),'-') SCHED_PRINT_INSTR,
nvl(INSTR_LOAD_FACTOR,0) INSTR_LOAD_FACTOR,
nvl(EMPL_RCD,0) EMPL_RCD,
nvl(trim(ASSIGN_TYPE),'-') ASSIGN_TYPE,
nvl(WEEK_WORKLOAD_HRS,0) WEEK_WORKLOAD_HRS,
nvl(ASSIGNMENT_PCT,0) ASSIGNMENT_PCT,
nvl(trim(AUTO_CALC_WRKLD),'-') AUTO_CALC_WRKLD
from SYSADM.PS_CLASS_INSTR@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_INSTR') ) S
   on (
T.CRSE_ID = S.CRSE_ID and
T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR and
T.STRM = S.STRM and
T.SESSION_CODE = S.SESSION_CODE and
T.CLASS_SECTION = S.CLASS_SECTION and
T.CLASS_MTG_NBR = S.CLASS_MTG_NBR and
T.INSTR_ASSIGN_SEQ = S.INSTR_ASSIGN_SEQ and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.EMPLID = S.EMPLID,
T.INSTR_ROLE = S.INSTR_ROLE,
T.GRADE_RSTR_ACCESS = S.GRADE_RSTR_ACCESS,
T.CONTACT_MINUTES = S.CONTACT_MINUTES,
T.SCHED_PRINT_INSTR = S.SCHED_PRINT_INSTR,
T.INSTR_LOAD_FACTOR = S.INSTR_LOAD_FACTOR,
T.EMPL_RCD = S.EMPL_RCD,
T.ASSIGN_TYPE = S.ASSIGN_TYPE,
T.WEEK_WORKLOAD_HRS = S.WEEK_WORKLOAD_HRS,
T.ASSIGNMENT_PCT = S.ASSIGNMENT_PCT,
T.AUTO_CALC_WRKLD = S.AUTO_CALC_WRKLD,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.EMPLID <> S.EMPLID or
T.INSTR_ROLE <> S.INSTR_ROLE or
T.GRADE_RSTR_ACCESS <> S.GRADE_RSTR_ACCESS or
T.CONTACT_MINUTES <> S.CONTACT_MINUTES or
T.SCHED_PRINT_INSTR <> S.SCHED_PRINT_INSTR or
T.INSTR_LOAD_FACTOR <> S.INSTR_LOAD_FACTOR or
T.EMPL_RCD <> S.EMPL_RCD or
T.ASSIGN_TYPE <> S.ASSIGN_TYPE or
T.WEEK_WORKLOAD_HRS <> S.WEEK_WORKLOAD_HRS or
T.ASSIGNMENT_PCT <> S.ASSIGNMENT_PCT or
T.AUTO_CALC_WRKLD <> S.AUTO_CALC_WRKLD or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.CRSE_ID,
T.CRSE_OFFER_NBR,
T.STRM,
T.SESSION_CODE,
T.CLASS_SECTION,
T.CLASS_MTG_NBR,
T.INSTR_ASSIGN_SEQ,
T.SRC_SYS_ID,
T.EMPLID,
T.INSTR_ROLE,
T.GRADE_RSTR_ACCESS,
T.CONTACT_MINUTES,
T.SCHED_PRINT_INSTR,
T.INSTR_LOAD_FACTOR,
T.EMPL_RCD,
T.ASSIGN_TYPE,
T.WEEK_WORKLOAD_HRS,
T.ASSIGNMENT_PCT,
T.AUTO_CALC_WRKLD,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.CRSE_ID,
S.CRSE_OFFER_NBR,
S.STRM,
S.SESSION_CODE,
S.CLASS_SECTION,
S.CLASS_MTG_NBR,
S.INSTR_ASSIGN_SEQ,
'CS90',
S.EMPLID,
S.INSTR_ROLE,
S.GRADE_RSTR_ACCESS,
S.CONTACT_MINUTES,
S.SCHED_PRINT_INSTR,
S.INSTR_LOAD_FACTOR,
S.EMPL_RCD,
S.ASSIGN_TYPE,
S.WEEK_WORKLOAD_HRS,
S.ASSIGNMENT_PCT,
S.AUTO_CALC_WRKLD,
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
         '# of PS_CLASS_INSTR rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_CLASS_INSTR',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_CLASS_INSTR';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_CLASS_INSTR';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_CLASS_INSTR';

update CSSTG_OWNER.PS_CLASS_INSTR T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CRSE_ID, CRSE_OFFER_NBR, STRM, SESSION_CODE, CLASS_SECTION, CLASS_MTG_NBR, INSTR_ASSIGN_SEQ
   from CSSTG_OWNER.PS_CLASS_INSTR T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_INSTR') = 'Y'
  minus
 select CRSE_ID, CRSE_OFFER_NBR, STRM, SESSION_CODE, CLASS_SECTION, CLASS_MTG_NBR, INSTR_ASSIGN_SEQ
   from SYSADM.PS_CLASS_INSTR@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_INSTR') = 'Y' 
   ) S
 where T.CRSE_ID = S.CRSE_ID   
  AND T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR
  AND T.STRM = S.STRM
  AND T.SESSION_CODE = S.SESSION_CODE
  AND T.CLASS_SECTION = S.CLASS_SECTION
  AND T.CLASS_MTG_NBR = S.CLASS_MTG_NBR
   AND T.INSTR_ASSIGN_SEQ = S.INSTR_ASSIGN_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_CLASS_INSTR rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_CLASS_INSTR',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_CLASS_INSTR';

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
END PS_CLASS_INSTR_P;
/
