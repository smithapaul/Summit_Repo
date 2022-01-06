CREATE OR REPLACE PROCEDURE             "PS_SEL_VALUE_TBL_P"
   AUTHID CURRENT_USER
IS

   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_SEL_VALUE_TBL from PeopleSoft table PS_SEL_VALUE_TBL.
   --
   -- V01  SMT-xxxx 01/09/2018,    James Doucette
   --                              New Stage table.
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_SEL_VALUE_TBL';
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
    WHERE TABLE_NAME = 'PS_SEL_VALUE_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_SEL_VALUE_TBL@SASOURCE S)
    WHERE TABLE_NAME = 'PS_SEL_VALUE_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_SEL_VALUE_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_SEL_VALUE_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_SEL_VALUE_TBL T 
using (select /*+ full(S) */ 
	nvl(trim(BUSINESS_UNIT),'-') BUSINESS_UNIT,
	nvl(trim(CRITERIA),'-') CRITERIA,
	EFFDT,
	nvl(SEQNO,0) SEQNO,
	nvl(trim(SELECT_VALUE),'-') SELECT_VALUE
from SYSADM.PS_SEL_VALUE_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SEL_VALUE_TBL') ) S 
 on (
	T.BUSINESS_UNIT = S.BUSINESS_UNIT and
	T.CRITERIA = S.CRITERIA and
	T.EFFDT = S.EFFDT and
	T.SEQNO = S.SEQNO and
	T.SELECT_VALUE = S.SELECT_VALUE and
	T.SRC_SYS_ID = 'CS90') 
when matched then update set 
	T.DATA_ORIGIN = 'S', 
	T.LASTUPD_EW_DTTM = sysdate, 
	T.BATCH_SID = 1234 
where
	T.DATA_ORIGIN = 'D'
when not matched then
insert ( 
	T.BUSINESS_UNIT, 
	T.CRITERIA,
	T.EFFDT, 
	T.SEQNO, 
	T.SELECT_VALUE,
	T.SRC_SYS_ID,
	T.LOAD_ERROR,
	T.DATA_ORIGIN, 
	T.CREATED_EW_DTTM, 
	T.LASTUPD_EW_DTTM, 
	T.BATCH_SID
	)
values ( 
	S.BUSINESS_UNIT, 
	S.CRITERIA,
	S.EFFDT, 
	S.SEQNO, 
	S.SELECT_VALUE,
	'CS90',
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
         '# of PS_SEL_VALUE_TBL rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_SEL_VALUE_TBL',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_SEL_VALUE_TBL';

   strSqlCommand := 'commit';
   COMMIT;


strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_SEL_VALUE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_SEL_VALUE_TBL';
update CSSTG_OWNER.PS_SEL_VALUE_TBL T
  set T.DATA_ORIGIN = 'D',
      T.LASTUPD_EW_DTTM = SYSDATE
where T.DATA_ORIGIN <> 'D'
  and exists 
(select 1 from
(select BUSINESS_UNIT, CRITERIA, EFFDT, SEQNO, SELECT_VALUE
   from CSSTG_OWNER.PS_SEL_VALUE_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SEL_VALUE_TBL') = 'Y'
  minus
 select BUSINESS_UNIT, CRITERIA, EFFDT, SEQNO, SELECT_VALUE
   from SYSADM.PS_SEL_VALUE_TBL@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SEL_VALUE_TBL') = 'Y' 
   ) S
 where T.BUSINESS_UNIT = S.BUSINESS_UNIT   
  and T.CRITERIA = S.CRITERIA
  and T.EFFDT = S.EFFDT
  and T.SEQNO = S.SEQNO
  and T.SELECT_VALUE = S.SELECT_VALUE
  and T.SRC_SYS_ID = 'CS90' 
   ) 
;

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_SEL_VALUE_TBL rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_SEL_VALUE_TBL',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_SEL_VALUE_TBL';

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
END PS_SEL_VALUE_TBL_P;
/
