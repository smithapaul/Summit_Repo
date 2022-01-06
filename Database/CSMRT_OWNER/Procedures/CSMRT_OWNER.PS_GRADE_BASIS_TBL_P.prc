CREATE OR REPLACE PROCEDURE             "PS_GRADE_BASIS_TBL_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_GRADE_BASIS_TBL'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_GRADE_BASIS_TBL', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_GRADE_BASIS_TBL'
   */


   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_GRADE_BASIS_TBL from PeopleSoft table PS_GRADE_BASIS_TBL.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_GRADE_BASIS_TBL.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_GRADE_BASIS_TBL';
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
    WHERE TABLE_NAME = 'PS_GRADE_BASIS_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_GRADE_BASIS_TBL@SASOURCE S)
    WHERE TABLE_NAME = 'PS_GRADE_BASIS_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_GRADE_BASIS_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_GRADE_BASIS_TBL';

merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_GRADE_BASIS_TBL T
using (select /*+ full(S) */
nvl(trim(SETID),'-') SETID,
nvl(trim(GRADING_SCHEME),'-') GRADING_SCHEME,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(GRADING_BASIS),'-') GRADING_BASIS,
nvl(trim(DESCRFORMAL),'-') DESCRFORMAL,
nvl(trim(INCLUDE_IN_GPA),'-') INCLUDE_IN_GPA,
nvl(trim(PRINT_ON_XCRIPT),'-') PRINT_ON_XCRIPT,
nvl(trim(GRADING_BASIS_CONV),'-') GRADING_BASIS_CONV,
nvl(trim(ELECT_GRADE_BASIS),'-') ELECT_GRADE_BASIS,
nvl(trim(GRADE_BASIS_CHOICE),'-') GRADE_BASIS_CHOICE,
nvl(trim(PRINT_GRD_DESCR),'-') PRINT_GRD_DESCR,
nvl(trim(AUDIT_GRADE_BASIS),'-') AUDIT_GRADE_BASIS,
nvl(trim(GRADE_REQUIRED),'-') GRADE_REQUIRED,
nvl(trim(DROP_PEN_GRADE),'-') DROP_PEN_GRADE,
nvl(trim(DROP_PEN_GRADE_2),'-') DROP_PEN_GRADE_2,
nvl(trim(WD_W_PEN_GRADE),'-') WD_W_PEN_GRADE,
nvl(trim(WD_W_PEN2_GRADE),'-') WD_W_PEN2_GRADE
from SYSADM.PS_GRADE_BASIS_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRADE_BASIS_TBL') ) S
   on (
T.SETID = S.SETID and
T.GRADING_SCHEME = S.GRADING_SCHEME and
T.EFFDT = S.EFFDT and
T.GRADING_BASIS = S.GRADING_BASIS and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.DESCRFORMAL = S.DESCRFORMAL,
T.INCLUDE_IN_GPA = S.INCLUDE_IN_GPA,
T.PRINT_ON_XCRIPT = S.PRINT_ON_XCRIPT,
T.GRADING_BASIS_CONV = S.GRADING_BASIS_CONV,
T.ELECT_GRADE_BASIS = S.ELECT_GRADE_BASIS,
T.GRADE_BASIS_CHOICE = S.GRADE_BASIS_CHOICE,
T.PRINT_GRD_DESCR = S.PRINT_GRD_DESCR,
T.AUDIT_GRADE_BASIS = S.AUDIT_GRADE_BASIS,
T.GRADE_REQUIRED = S.GRADE_REQUIRED,
T.DROP_PEN_GRADE = S.DROP_PEN_GRADE,
T.DROP_PEN_GRADE_2 = S.DROP_PEN_GRADE_2,
T.WD_W_PEN_GRADE = S.WD_W_PEN_GRADE,
T.WD_W_PEN2_GRADE = S.WD_W_PEN2_GRADE,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.DESCRFORMAL <> S.DESCRFORMAL or
T.INCLUDE_IN_GPA <> S.INCLUDE_IN_GPA or
T.PRINT_ON_XCRIPT <> S.PRINT_ON_XCRIPT or
T.GRADING_BASIS_CONV <> S.GRADING_BASIS_CONV or
T.ELECT_GRADE_BASIS <> S.ELECT_GRADE_BASIS or
T.GRADE_BASIS_CHOICE <> S.GRADE_BASIS_CHOICE or
T.PRINT_GRD_DESCR <> S.PRINT_GRD_DESCR or
T.AUDIT_GRADE_BASIS <> S.AUDIT_GRADE_BASIS or
T.GRADE_REQUIRED <> S.GRADE_REQUIRED or
T.DROP_PEN_GRADE <> S.DROP_PEN_GRADE or
T.DROP_PEN_GRADE_2 <> S.DROP_PEN_GRADE_2 or
T.WD_W_PEN_GRADE <> S.WD_W_PEN_GRADE or
T.WD_W_PEN2_GRADE <> S.WD_W_PEN2_GRADE or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.SETID,
T.GRADING_SCHEME,
T.EFFDT,
T.GRADING_BASIS,
T.SRC_SYS_ID,
T.DESCRFORMAL,
T.INCLUDE_IN_GPA,
T.PRINT_ON_XCRIPT,
T.GRADING_BASIS_CONV,
T.ELECT_GRADE_BASIS,
T.GRADE_BASIS_CHOICE,
T.PRINT_GRD_DESCR,
T.AUDIT_GRADE_BASIS,
T.GRADE_REQUIRED,
T.DROP_PEN_GRADE,
T.DROP_PEN_GRADE_2,
T.WD_W_PEN_GRADE,
T.WD_W_PEN2_GRADE,
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
'CS90',
S.DESCRFORMAL,
S.INCLUDE_IN_GPA,
S.PRINT_ON_XCRIPT,
S.GRADING_BASIS_CONV,
S.ELECT_GRADE_BASIS,
S.GRADE_BASIS_CHOICE,
S.PRINT_GRD_DESCR,
S.AUDIT_GRADE_BASIS,
S.GRADE_REQUIRED,
S.DROP_PEN_GRADE,
S.DROP_PEN_GRADE_2,
S.WD_W_PEN_GRADE,
S.WD_W_PEN2_GRADE,
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
         '# of PS_GRADE_BASIS_TBL rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_GRADE_BASIS_TBL',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_GRADE_BASIS_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_GRADE_BASIS_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_GRADE_BASIS_TBL';


 update CSSTG_OWNER.PS_GRADE_BASIS_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, GRADING_SCHEME, EFFDT, GRADING_BASIS
   from CSSTG_OWNER.PS_GRADE_BASIS_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRADE_BASIS_TBL') = 'Y'
  minus
 select SETID, GRADING_SCHEME, EFFDT, GRADING_BASIS
   from SYSADM.PS_GRADE_BASIS_TBL@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRADE_BASIS_TBL') = 'Y' 
   ) S
 where T.SETID = S.SETID   
  AND T.GRADING_SCHEME = S.GRADING_SCHEME 
    AND T.EFFDT = S.EFFDT
     AND T.GRADING_BASIS = S.GRADING_BASIS
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_GRADE_BASIS_TBL rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_GRADE_BASIS_TBL',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_GRADE_BASIS_TBL';

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
END PS_GRADE_BASIS_TBL_P;
/
