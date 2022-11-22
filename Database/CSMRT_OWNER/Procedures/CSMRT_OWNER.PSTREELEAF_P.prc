DROP PROCEDURE CSMRT_OWNER.PSTREELEAF_P
/

--
-- PSTREELEAF_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PSTREELEAF_P"
   AUTHID CURRENT_USER
IS

   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PSTREELEAF from PeopleSoft table PSTREELEAF. 
   --
   -- V01  SMT-xxxx 01/09/2018,    James Doucette
   --                              New Stage table.
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PSTREELEAF';
   intProcessSid      INTEGER;
   dtProcessStart     DATE := SYSDATE;
   strMessage01       VARCHAR2 (4000);
   strMessage02       VARCHAR2 (512);
   strMessage03       VARCHAR2 (512) := '';
   strNewLine         VARCHAR2 (2) := CHR (13) || CHR (
   10);
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
    WHERE TABLE_NAME = 'PSTREELEAF';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PSTREELEAF@SASOURCE S)
    WHERE TABLE_NAME = 'PSTREELEAF';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PSTREELEAF';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PSTREELEAF';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PSTREELEAF T 
using (select /*+ full(S) */ 
	nvl(trim(SETID),'-') SETID,
	nvl(trim(SETCNTRLVALUE),'-') SETCNTRLVALUE,
	nvl(trim(TREE_NAME),'-') TREE_NAME,
	EFFDT,
	nvl(TREE_NODE_NUM,0) TREE_NODE_NUM,
	nvl(trim(RANGE_FROM),'-') RANGE_FROM,
	nvl(trim(RANGE_TO),'-') RANGE_TO,
	nvl(trim(TREE_BRANCH),'-') TREE_BRANCH,
	nvl(trim(DYNAMIC_RANGE),'-') DYNAMIC_RANGE,
	nvl(trim(OLD_TREE_NODE_NUM),'-') OLD_TREE_NODE_NUM,
	nvl(trim(LEAF_IMAGE),'-') LEAF_IMAGE
from SYSADM.PSTREELEAF@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PSTREELEAF') ) S 
 on (
	T.SETID = S.SETID and
	T.SETCNTRLVALUE = S.SETCNTRLVALUE and
	T.TREE_NAME = S.TREE_NAME and
	T.EFFDT = S.EFFDT and
	T.TREE_NODE_NUM = S.TREE_NODE_NUM and
	T.RANGE_FROM = S.RANGE_FROM and
	T.RANGE_TO = S.RANGE_TO and
	T.TREE_BRANCH = S.TREE_BRANCH and
	T.SRC_SYS_ID = 'CS90') 
when matched then update set 
	T.DYNAMIC_RANGE = S.DYNAMIC_RANGE, 
	T.OLD_TREE_NODE_NUM = S.OLD_TREE_NODE_NUM, 
	T.LEAF_IMAGE = S.LEAF_IMAGE, 
	T.DATA_ORIGIN = 'S', 
	T.LASTUPD_EW_DTTM = sysdate, 
	T.BATCH_SID = 1234 
where
	T.DYNAMIC_RANGE <> S.DYNAMIC_RANGE or
	T.OLD_TREE_NODE_NUM <> S.OLD_TREE_NODE_NUM or
	T.LEAF_IMAGE <> S.LEAF_IMAGE or
	T.DATA_ORIGIN = 'D'
when not matched then
insert ( 
	T.SETID, 
	T.SETCNTRLVALUE, 
	T.TREE_NAME, 
	T.EFFDT, 
	T.TREE_NODE_NUM, 
	T.RANGE_FROM,
	T.RANGE_TO,
	T.TREE_BRANCH, 
	T.SRC_SYS_ID,
	T.DYNAMIC_RANGE, 
	T.OLD_TREE_NODE_NUM, 
	T.LEAF_IMAGE,
	T.LOAD_ERROR,
	T.DATA_ORIGIN, 
	T.CREATED_EW_DTTM, 
	T.LASTUPD_EW_DTTM, 
	T.BATCH_SID
	)
values ( 
	S.SETID, 
	S.SETCNTRLVALUE, 
	S.TREE_NAME, 
	S.EFFDT, 
	S.TREE_NODE_NUM, 
	S.RANGE_FROM,
	S.RANGE_TO,
	S.TREE_BRANCH, 
	'CS90',
	S.DYNAMIC_RANGE, 
	S.OLD_TREE_NODE_NUM, 
	S.LEAF_IMAGE,
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
         '# of PSTREELEAF rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PSTREELEAF',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PSTREELEAF';

   strSqlCommand := 'commit';
   COMMIT;


strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PSTREELEAF';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PSTREELEAF';
update CSSTG_OWNER.PSTREELEAF T
  set T.DATA_ORIGIN = 'D',
      T.LASTUPD_EW_DTTM = SYSDATE
where T.DATA_ORIGIN <> 'D'
  and exists 
(select 1 from
(select SETID, SETCNTRLVALUE, TREE_NAME, EFFDT, TREE_NODE_NUM, RANGE_FROM, RANGE_TO, TREE_BRANCH
   from CSSTG_OWNER.PSTREELEAF T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PSTREELEAF') = 'Y'
  minus
 select nvl(trim(SETID),'-') SETID,
	    nvl(trim(SETCNTRLVALUE),'-') SETCNTRLVALUE,
	    nvl(trim(TREE_NAME),'-') TREE_NAME,
	    EFFDT,
	    nvl(TREE_NODE_NUM,0) TREE_NODE_NUM,
	    nvl(trim(RANGE_FROM),'-') RANGE_FROM,
	    nvl(trim(RANGE_TO),'-') RANGE_TO,
	    nvl(trim(TREE_BRANCH),'-') TREE_BRANCH
   from SYSADM.PSTREELEAF@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PSTREELEAF') = 'Y' 
   ) S
 where T.SETID = S.SETID   
  and T.SETCNTRLVALUE = S.SETCNTRLVALUE
  and T.TREE_NAME = S.TREE_NAME
  and T.EFFDT = S.EFFDT
  and T.TREE_NODE_NUM = S.TREE_NODE_NUM
  and T.RANGE_FROM = S.RANGE_FROM
  and T.RANGE_TO = S.RANGE_TO
  and T.TREE_BRANCH = S.TREE_BRANCH
  and T.SRC_SYS_ID = 'CS90' 
   ) 
;

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PSTREELEAF rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PSTREELEAF',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PSTREELEAF';

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
END PSTREELEAF_P;
/
