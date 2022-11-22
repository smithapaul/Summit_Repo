DROP PROCEDURE CSMRT_OWNER.AM_PS_SCTN_CMBND_TBL_P
/

--
-- AM_PS_SCTN_CMBND_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_SCTN_CMBND_TBL_P" IS

   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_SCTN_CMBND_TBL from PeopleSoft table PS_SCTN_CMBND_TBL.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_SCTN_CMBND_TBL.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_SCTN_CMBND_TBL';
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
    WHERE TABLE_NAME = 'PS_SCTN_CMBND_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_SCTN_CMBND_TBL@AMSOURCE S)
    WHERE TABLE_NAME = 'PS_SCTN_CMBND_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into AMSTG_OWNER.PS_SCTN_CMBND_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into AMSTG_OWNER.PS_SCTN_CMBND_TBL';

merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_SCTN_CMBND_TBL T
using (select /*+ full(S) */
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(STRM),'-') STRM,
nvl(trim(SESSION_CODE),'-') SESSION_CODE,
nvl(trim(SCTN_COMBINED_ID),'-') SCTN_COMBINED_ID,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(DESCRSHORT),'-') DESCRSHORT,
nvl(ENRL_CAP,0) ENRL_CAP,
nvl(ENRL_TOT,0) ENRL_TOT,
nvl(WAIT_CAP,0) WAIT_CAP,
nvl(WAIT_TOT,0) WAIT_TOT,
nvl(ROOM_CAP_REQUEST,0) ROOM_CAP_REQUEST,
nvl(trim(PERM_COMBINATION),'-') PERM_COMBINATION,
nvl(trim(COMBINATION_TYPE),'-') COMBINATION_TYPE,
nvl(trim(SKIP_MTGPAT_EDIT),'-') SKIP_MTGPAT_EDIT
from SYSADM.PS_SCTN_CMBND_TBL@AMSOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SCTN_CMBND_TBL') ) S
   on (
T.INSTITUTION = S.INSTITUTION and
T.STRM = S.STRM and
T.SESSION_CODE = S.SESSION_CODE and
T.SCTN_COMBINED_ID = S.SCTN_COMBINED_ID and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.DESCR = S.DESCR,
T.DESCRSHORT = S.DESCRSHORT,
T.ENRL_CAP = S.ENRL_CAP,
T.ENRL_TOT = S.ENRL_TOT,
T.WAIT_CAP = S.WAIT_CAP,
T.WAIT_TOT = S.WAIT_TOT,
T.ROOM_CAP_REQUEST = S.ROOM_CAP_REQUEST,
T.PERM_COMBINATION = S.PERM_COMBINATION,
T.COMBINATION_TYPE = S.COMBINATION_TYPE,
T.SKIP_MTGPAT_EDIT = S.SKIP_MTGPAT_EDIT,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.DESCR <> S.DESCR or
T.DESCRSHORT <> S.DESCRSHORT or
T.ENRL_CAP <> S.ENRL_CAP or
T.ENRL_TOT <> S.ENRL_TOT or
T.WAIT_CAP <> S.WAIT_CAP or
T.WAIT_TOT <> S.WAIT_TOT or
T.ROOM_CAP_REQUEST <> S.ROOM_CAP_REQUEST or
T.PERM_COMBINATION <> S.PERM_COMBINATION or
T.COMBINATION_TYPE <> S.COMBINATION_TYPE or
T.SKIP_MTGPAT_EDIT <> S.SKIP_MTGPAT_EDIT or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.INSTITUTION,
T.STRM,
T.SESSION_CODE,
T.SCTN_COMBINED_ID,
T.SRC_SYS_ID,
T.DESCR,
T.DESCRSHORT,
T.ENRL_CAP,
T.ENRL_TOT,
T.WAIT_CAP,
T.WAIT_TOT,
T.ROOM_CAP_REQUEST,
T.PERM_COMBINATION,
T.COMBINATION_TYPE,
T.SKIP_MTGPAT_EDIT,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.INSTITUTION,
S.STRM,
S.SESSION_CODE,
S.SCTN_COMBINED_ID,
'CS90',
S.DESCR,
S.DESCRSHORT,
S.ENRL_CAP,
S.ENRL_TOT,
S.WAIT_CAP,
S.WAIT_TOT,
S.ROOM_CAP_REQUEST,
S.PERM_COMBINATION,
S.COMBINATION_TYPE,
S.SKIP_MTGPAT_EDIT,
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
         '# of PS_SCTN_CMBND_TBL rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_SCTN_CMBND_TBL',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_SCTN_CMBND_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_SCTN_CMBND_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on AMSTG_OWNER.PS_SCTN_CMBND_TBL';


update AMSTG_OWNER.PS_SCTN_CMBND_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, STRM, SESSION_CODE, SCTN_COMBINED_ID
   from AMSTG_OWNER.PS_SCTN_CMBND_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SCTN_CMBND_TBL') = 'Y'
  minus
 select INSTITUTION, STRM, SESSION_CODE, SCTN_COMBINED_ID
   from SYSADM.PS_SCTN_CMBND_TBL@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SCTN_CMBND_TBL') = 'Y' 
   ) S
 where T.INSTITUTION = S.INSTITUTION   
  AND T.STRM = S.STRM
   AND T.SESSION_CODE = S.SESSION_CODE
     AND T.SCTN_COMBINED_ID = S.SCTN_COMBINED_ID
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_SCTN_CMBND_TBL rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_SCTN_CMBND_TBL',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_SCTN_CMBND_TBL';

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
END AM_PS_SCTN_CMBND_TBL_P;
/
