DROP PROCEDURE CSMRT_OWNER.PS_CS_CHKLST_TBL_P
/

--
-- PS_CS_CHKLST_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_CS_CHKLST_TBL_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_CS_CHKLST_TBL'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_CS_CHKLST_TBL', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_CS_CHKLST_TBL'
   */


   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_CS_CHKLST_TBL from PeopleSoft table PS_CS_CHKLST_TBL.
   --
   -- V01  SMT-xxxx 08/08/2017,    Jim Doucette
   --                              Converted from PS_CS_CHKLST_TBL.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_CS_CHKLST_TBL';
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
    WHERE TABLE_NAME = 'PS_CS_CHKLST_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_CS_CHKLST_TBL@SASOURCE S)
    WHERE TABLE_NAME = 'PS_CS_CHKLST_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_CS_CHKLST_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

strSqlCommand := 'merge into CSSTG_OWNER.PS_CS_CHKLST_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_CS_CHKLST_TBL T
using (select /*+ full(S) */
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(CHECKLIST_CD),'-') CHECKLIST_CD, 
    EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(ADMIN_FUNCTION),'-') ADMIN_FUNCTION, 
    nvl(trim(SCC_CHECKLIST_TYPE),'-') SCC_CHECKLIST_TYPE, 
    nvl(trim(TRACKING_GROUP),'-') TRACKING_GROUP, 
    DEFAULT_DUE_DT,
    nvl(DUE_DAYS,0) DUE_DAYS, 
    nvl(trim(COMM_KEY),'-') COMM_KEY, 
    nvl(trim(SCC_TODO_SS_DISP),'-') SCC_TODO_SS_DISP
from SYSADM.PS_CS_CHKLST_TBL@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CS_CHKLST_TBL') ) S
 on ( 
    T.INSTITUTION = S.INSTITUTION and 
    T.CHECKLIST_CD = S.CHECKLIST_CD and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.ADMIN_FUNCTION = S.ADMIN_FUNCTION,
    T.SCC_CHECKLIST_TYPE = S.SCC_CHECKLIST_TYPE,
    T.TRACKING_GROUP = S.TRACKING_GROUP,
    T.DEFAULT_DUE_DT = S.DEFAULT_DUE_DT,
    T.DUE_DAYS = S.DUE_DAYS,
    T.COMM_KEY = S.COMM_KEY,
    T.SCC_TODO_SS_DISP = S.SCC_TODO_SS_DISP,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.ADMIN_FUNCTION <> S.ADMIN_FUNCTION or 
    T.SCC_CHECKLIST_TYPE <> S.SCC_CHECKLIST_TYPE or 
    T.TRACKING_GROUP <> S.TRACKING_GROUP or 
    nvl(trim(T.DEFAULT_DUE_DT),0) <> nvl(trim(S.DEFAULT_DUE_DT),0) or 
    T.DUE_DAYS <> S.DUE_DAYS or 
    T.COMM_KEY <> S.COMM_KEY or 
    T.SCC_TODO_SS_DISP <> S.SCC_TODO_SS_DISP or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.INSTITUTION,
    T.CHECKLIST_CD, 
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.DESCR,
    T.DESCRSHORT, 
    T.ADMIN_FUNCTION, 
    T.SCC_CHECKLIST_TYPE, 
    T.TRACKING_GROUP, 
    T.DEFAULT_DUE_DT, 
    T.DUE_DAYS, 
    T.COMM_KEY, 
    T.SCC_TODO_SS_DISP, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.INSTITUTION,
    S.CHECKLIST_CD, 
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.DESCR,
    S.DESCRSHORT, 
    S.ADMIN_FUNCTION, 
    S.SCC_CHECKLIST_TYPE, 
    S.TRACKING_GROUP, 
    S.DEFAULT_DUE_DT, 
    S.DUE_DAYS, 
    S.COMM_KEY, 
    S.SCC_TODO_SS_DISP, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_CS_CHKLST_TBL rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_CS_CHKLST_TBL',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_CS_CHKLST_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_CS_CHKLST_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_CS_CHKLST_TBL';
update CSSTG_OWNER.PS_CS_CHKLST_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, CHECKLIST_CD, EFFDT
   from CSSTG_OWNER.PS_CS_CHKLST_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CS_CHKLST_TBL') = 'Y'
  minus
 select INSTITUTION, CHECKLIST_CD, EFFDT
   from SYSADM.PS_CS_CHKLST_TBL@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CS_CHKLST_TBL') = 'Y'
   ) S
 where T.INSTITUTION = S.INSTITUTION
   and T.CHECKLIST_CD = S.CHECKLIST_CD
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_CS_CHKLST_TBL rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_CS_CHKLST_TBL',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_CS_CHKLST_TBL';

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
         
END PS_CS_CHKLST_TBL_P;
/
