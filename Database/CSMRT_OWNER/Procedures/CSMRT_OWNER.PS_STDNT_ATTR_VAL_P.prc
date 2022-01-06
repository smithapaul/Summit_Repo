CREATE OR REPLACE PROCEDURE             "PS_STDNT_ATTR_VAL_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_STDNT_ATTR_VAL'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_STDNT_ATTR_VAL', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_STDNT_ATTR_VAL'
   */


   ------------------------------------------------------------------------
   --
   --
   -- Loads stage table PS_STDNT_ATTR_VAL from PeopleSoft table PS_STDNT_ATTR_VAL.
   --
   -- V01  SMT-xxxx 08/09/2017,    Preethi Lodha
   --                              Converted from PS_STDNT_ATTR_VAL.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_STDNT_ATTR_VAL';
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
    WHERE TABLE_NAME = 'PS_STDNT_ATTR_VAL';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_STDNT_ATTR_VAL@SASOURCE S)
    WHERE TABLE_NAME = 'PS_STDNT_ATTR_VAL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_STDNT_ATTR_VAL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_STDNT_ATTR_VAL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_STDNT_ATTR_VAL T
using (select /*+ full(S) */
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(STDNT_ATTR),'-') STDNT_ATTR,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(STDNT_ATTR_VALUE),'-') STDNT_ATTR_VALUE,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(DESCRSHORT),'-') DESCRSHORT,
nvl(STDNT_ATTR_VAL_PRM,0) STDNT_ATTR_VAL_PRM
from SYSADM.PS_STDNT_ATTR_VAL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_ATTR_VAL') ) S
   on (
T.INSTITUTION = S.INSTITUTION and
T.STDNT_ATTR = S.STDNT_ATTR and
T.EFFDT = S.EFFDT and
T.STDNT_ATTR_VALUE = S.STDNT_ATTR_VALUE and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.DESCR = S.DESCR,
T.DESCRSHORT = S.DESCRSHORT,
T.STDNT_ATTR_VAL_PRM = S.STDNT_ATTR_VAL_PRM,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.DESCR <> S.DESCR or
T.DESCRSHORT <> S.DESCRSHORT or
T.STDNT_ATTR_VAL_PRM <> S.STDNT_ATTR_VAL_PRM or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.INSTITUTION,
T.STDNT_ATTR,
T.EFFDT,
T.STDNT_ATTR_VALUE,
T.SRC_SYS_ID,
T.DESCR,
T.DESCRSHORT,
T.STDNT_ATTR_VAL_PRM,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.INSTITUTION,
S.STDNT_ATTR,
S.EFFDT,
S.STDNT_ATTR_VALUE,
'CS90',
S.DESCR,
S.DESCRSHORT,
S.STDNT_ATTR_VAL_PRM,
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
         '# of PS_STDNT_ATTR_VAL rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_STDNT_ATTR_VAL',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_STDNT_ATTR_VAL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_ATTR_VAL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_ATTR_VAL';

update CSSTG_OWNER.PS_STDNT_ATTR_VAL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, STDNT_ATTR, EFFDT, STDNT_ATTR_VALUE
   from CSSTG_OWNER.PS_STDNT_ATTR_VAL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_ATTR_VAL') = 'Y'
  minus
 select INSTITUTION, STDNT_ATTR, EFFDT, STDNT_ATTR_VALUE
   from SYSADM.PS_STDNT_ATTR_VAL@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_ATTR_VAL') = 'Y' 
   ) S
 where T.INSTITUTION = S.INSTITUTION   
  AND T.EFFDT = S.EFFDT
  AND T.STDNT_ATTR = S.STDNT_ATTR
  AND T.STDNT_ATTR_VALUE = S.STDNT_ATTR_VALUE
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_STDNT_ATTR_VAL rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_STDNT_ATTR_VAL',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_STDNT_ATTR_VAL';

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
END PS_STDNT_ATTR_VAL_P;
/
