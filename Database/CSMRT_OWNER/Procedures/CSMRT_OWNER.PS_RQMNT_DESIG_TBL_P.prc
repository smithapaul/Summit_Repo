DROP PROCEDURE CSMRT_OWNER.PS_RQMNT_DESIG_TBL_P
/

--
-- PS_RQMNT_DESIG_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_RQMNT_DESIG_TBL_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_RQMNT_DESIG_TBL'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_RQMNT_DESIG_TBL', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_RQMNT_DESIG_TBL'
   */


   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_RQMNT_DESIG_TBL from PeopleSoft table PS_RQMNT_DESIG_TBL.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_RQMNT_DESIG_TBL.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_RQMNT_DESIG_TBL';
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
    WHERE TABLE_NAME = 'PS_RQMNT_DESIG_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_RQMNT_DESIG_TBL@SASOURCE S)
    WHERE TABLE_NAME = 'PS_RQMNT_DESIG_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_RQMNT_DESIG_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_RQMNT_DESIG_TBL';

merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_RQMNT_DESIG_TBL T
using (select /*+ full(S) */
nvl(trim(RQMNT_DESIGNTN),'-') RQMNT_DESIGNTN,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(EFF_STATUS),'-') EFF_STATUS,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(DESCRSHORT),'-') DESCRSHORT,
nvl(trim(DESCRFORMAL),'-') DESCRFORMAL,
nvl(trim(CATALOG_PRINT),'-') CATALOG_PRINT,
nvl(trim(SCHEDULE_PRINT),'-') SCHEDULE_PRINT,
nvl(trim(TSCRPT_PRINT),'-') TSCRPT_PRINT,
nvl(trim(AT_STDNT_OPTION),'-') AT_STDNT_OPTION,
nvl(trim(ACAD_PLAN),'-') ACAD_PLAN,
nvl(trim(SEPARATE_CRSE_GRD),'-') SEPARATE_CRSE_GRD,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(SAA_DISPLAY_OPTION),'-') SAA_DISPLAY_OPTION
from SYSADM.PS_RQMNT_DESIG_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RQMNT_DESIG_TBL') ) S
   on (
T.RQMNT_DESIGNTN = S.RQMNT_DESIGNTN and
T.EFFDT = S.EFFDT and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.EFF_STATUS = S.EFF_STATUS,
T.DESCR = S.DESCR,
T.DESCRSHORT = S.DESCRSHORT,
T.DESCRFORMAL = S.DESCRFORMAL,
T.CATALOG_PRINT = S.CATALOG_PRINT,
T.SCHEDULE_PRINT = S.SCHEDULE_PRINT,
T.TSCRPT_PRINT = S.TSCRPT_PRINT,
T.AT_STDNT_OPTION = S.AT_STDNT_OPTION,
T.ACAD_PLAN = S.ACAD_PLAN,
T.SEPARATE_CRSE_GRD = S.SEPARATE_CRSE_GRD,
T.INSTITUTION = S.INSTITUTION,
T.SAA_DISPLAY_OPTION = S.SAA_DISPLAY_OPTION,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.EFF_STATUS <> S.EFF_STATUS or
T.DESCR <> S.DESCR or
T.DESCRSHORT <> S.DESCRSHORT or
T.DESCRFORMAL <> S.DESCRFORMAL or
T.CATALOG_PRINT <> S.CATALOG_PRINT or
T.SCHEDULE_PRINT <> S.SCHEDULE_PRINT or
T.TSCRPT_PRINT <> S.TSCRPT_PRINT or
T.AT_STDNT_OPTION <> S.AT_STDNT_OPTION or
T.ACAD_PLAN <> S.ACAD_PLAN or
T.SEPARATE_CRSE_GRD <> S.SEPARATE_CRSE_GRD or
T.INSTITUTION <> S.INSTITUTION or
T.SAA_DISPLAY_OPTION <> S.SAA_DISPLAY_OPTION or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.RQMNT_DESIGNTN,
T.EFFDT,
T.SRC_SYS_ID,
T.EFF_STATUS,
T.DESCR,
T.DESCRSHORT,
T.DESCRFORMAL,
T.CATALOG_PRINT,
T.SCHEDULE_PRINT,
T.TSCRPT_PRINT,
T.AT_STDNT_OPTION,
T.ACAD_PLAN,
T.SEPARATE_CRSE_GRD,
T.INSTITUTION,
T.SAA_DISPLAY_OPTION,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.RQMNT_DESIGNTN,
S.EFFDT,
'CS90',
S.EFF_STATUS,
S.DESCR,
S.DESCRSHORT,
S.DESCRFORMAL,
S.CATALOG_PRINT,
S.SCHEDULE_PRINT,
S.TSCRPT_PRINT,
S.AT_STDNT_OPTION,
S.ACAD_PLAN,
S.SEPARATE_CRSE_GRD,
S.INSTITUTION,
S.SAA_DISPLAY_OPTION,
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
         '# of PS_RQMNT_DESIG_TBL rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_RQMNT_DESIG_TBL',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_RQMNT_DESIG_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_RQMNT_DESIG_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_RQMNT_DESIG_TBL';

update CSSTG_OWNER.PS_RQMNT_DESIG_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select RQMNT_DESIGNTN, EFFDT
   from CSSTG_OWNER.PS_RQMNT_DESIG_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RQMNT_DESIG_TBL') = 'Y'
  minus
 select RQMNT_DESIGNTN, EFFDT
   from SYSADM.PS_RQMNT_DESIG_TBL@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RQMNT_DESIG_TBL') = 'Y' 
   ) S
 where T.RQMNT_DESIGNTN = S.RQMNT_DESIGNTN  
  AND T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_RQMNT_DESIG_TBL rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_RQMNT_DESIG_TBL',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_RQMNT_DESIG_TBL';

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
END PS_RQMNT_DESIG_TBL_P;
/
