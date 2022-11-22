DROP PROCEDURE CSMRT_OWNER.PS_ACAD_ORG_FS_OWN_P
/

--
-- PS_ACAD_ORG_FS_OWN_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.PS_ACAD_ORG_FS_OWN_P
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_ACAD_ORG_FS_OWN'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_ACAD_ORG_FS_OWN', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_ACAD_ORG_FS_OWN'
   */


   ------------------------------------------------------------------------
   -- Smitha Paul
   --
   -- Loads stage table PS_ACAD_ORG_FS_OWN from PeopleSoft table PS_ACAD_ORG_FS_OWN.

   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_ACAD_ORG_FS_OWN';
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
    WHERE TABLE_NAME = 'PS_ACAD_ORG_FS_OWN';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_ACAD_ORG_FS_OWN@SASOURCE S)
    WHERE TABLE_NAME = 'PS_ACAD_ORG_FS_OWN';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_ACAD_ORG_FS_OWN';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_ACAD_ORG_FS_OWN';
   
   
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ACAD_ORG_FS_OWN T
using (select /*+ full(S) */
nvl(trim(ACAD_ORG),'-') ACAD_ORG,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(BUSINESS_UNIT),'-') BUSINESS_UNIT,
nvl(trim(DEPTID),'-') DEPTID,
nvl(PERCENT_OWNED,0) PERCENT_OWNED
from SYSADM.PS_ACAD_ORG_FS_OWN@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_ORG_FS_OWN')
AND nvl(trim(BUSINESS_UNIT),'-') <> 'UMLOW' ) S
   on (
T.BUSINESS_UNIT = S.BUSINESS_UNIT and
T.DEPTID = S.DEPTID and
T.EFFDT = S.EFFDT and
T.ACAD_ORG = S.ACAD_ORG and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.PERCENT_OWNED = S.PERCENT_OWNED,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate
where
T.PERCENT_OWNED <> S.PERCENT_OWNED or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.ACAD_ORG,
T.BUSINESS_UNIT,
T.DEPTID,
T.EFFDT,
T.SRC_SYS_ID,
T.PERCENT_OWNED,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM
)
values (
S.ACAD_ORG,
S.BUSINESS_UNIT,
S.DEPTID,
S.EFFDT,
'CS90',
S.PERCENT_OWNED,
'S',
sysdate,
sysdate
);
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_ACAD_ORG_FS_OWN rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_ACAD_ORG_FS_OWN',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_ACAD_ORG_FS_OWN';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_ORG_FS_OWN';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_ORG_FS_OWN';

update CSSTG_OWNER.PS_ACAD_ORG_FS_OWN T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(    select 1 from
(select BUSINESS_UNIT, DEPTID, EFFDT, ACAD_ORG,PERCENT_OWNED
   from CSSTG_OWNER.PS_ACAD_ORG_FS_OWN T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_ORG_FS_OWN') = 'Y'
  minus
 select BUSINESS_UNIT, DEPTID, EFFDT, ACAD_ORG,PERCENT_OWNED
   from SYSADM.PS_ACAD_ORG_FS_OWN@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_ORG_FS_OWN') = 'Y' 
   ) S
 where T.BUSINESS_UNIT = S.BUSINESS_UNIT   
  AND T.ACAD_ORG = S.ACAD_ORG
  AND T.EFFDT = S.EFFDT
  AND T.DEPTID = S.DEPTID
   AND T.PERCENT_OWNED = S.PERCENT_OWNED
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_ACAD_ORG_FS_OWN rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_ACAD_ORG_FS_OWN',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_ACAD_ORG_FS_OWN';

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
END PS_ACAD_ORG_FS_OWN_P;
/
