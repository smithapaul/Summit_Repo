DROP PROCEDURE CSMRT_OWNER.AM_PSMSGCATDEFN_P
/

--
-- AM_PSMSGCATDEFN_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PSMSGCATDEFN_P IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PSMSGCATDEFN from PeopleSoft table PSMSGCATDEFN.
--
-- V01  SMT-xxxx 11/14/2017,    James Doucette
--                              New Stage Table
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PSMSGCATDEFN';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN
strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PSMSGCATDEFN'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PSMSGCATDEFN@AMSOURCE S)
 where TABLE_NAME = 'PSMSGCATDEFN'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_PSMSGCATDEFN';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PSMSGCATDEFN'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
INSERT /*+ append */
  INTO AMSTG_OWNER.PS_T_PSMSGCATDEFN
SELECT /*+ full(S) */
       MESSAGE_SET_NBR, 
       MESSAGE_NBR, 
       MESSAGE_TEXT, 
       MSG_SEVERITY, 
       LAST_UPDATE_DTTM,
       TO_CHAR (SUBSTR (TRIM (DESCRLONG), 1, 4000)) DESCRLONG,
       to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PSMSGCATDEFN@AMSOURCE S;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PSMSGCATDEFN'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PSMSGCATDEFN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PSMSGCATDEFN';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PSMSGCATDEFN T
using (select /*+ full(S) */
	nvl(MESSAGE_SET_NBR,0) MESSAGE_SET_NBR, 
       nvl(MESSAGE_NBR,0) MESSAGE_NBR, 
       nvl(trim(MESSAGE_TEXT),'-') MESSAGE_TEXT, 
       nvl(trim(MSG_SEVERITY),'-') MSG_SEVERITY, 
       to_date(trunc(LAST_UPDATE_DTTM)) LAST_UPDATE_DTTM,
       DESCRLONG
from AMSTG_OWNER.PS_T_PSMSGCATDEFN S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PSMSGCATDEFN') ) S
 on ( 
	T.MESSAGE_SET_NBR = S.MESSAGE_SET_NBR and 
	T.MESSAGE_NBR = S.MESSAGE_NBR and 
	T.SRC_SYS_ID = 'CS90')
	when matched then update set
	T.MESSAGE_TEXT = S.MESSAGE_TEXT,
	T.MSG_SEVERITY = S.MSG_SEVERITY,
	T.LAST_UPDATE_DTTM = S.LAST_UPDATE_DTTM,
	T.DESCRLONG = S.DESCRLONG,
	T.DATA_ORIGIN = 'S',
	T.LASTUPD_EW_DTTM = sysdate,
	T.BATCH_SID = 1234
where 
	T.MESSAGE_TEXT <> S.MESSAGE_TEXT or 
	T.MSG_SEVERITY <> S.MSG_SEVERITY or 
	nvl(trim(T.LAST_UPDATE_DTTM),0) <> nvl(trim(S.LAST_UPDATE_DTTM),0) or 
	nvl(trim(T.DESCRLONG),0) <> nvl(trim(S.DESCRLONG),0) or 
	T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
	T.MESSAGE_SET_NBR,
	T.MESSAGE_NBR,
	T.SRC_SYS_ID, 
	T.MESSAGE_TEXT, 
	T.MSG_SEVERITY, 
	T.LAST_UPDATE_DTTM, 
	T.LOAD_ERROR, 
	T.DATA_ORIGIN,
	T.CREATED_EW_DTTM,
	T.LASTUPD_EW_DTTM,
	T.BATCH_SID,
	T.DESCRLONG
	) 
values (
	S.MESSAGE_SET_NBR,
	S.MESSAGE_NBR,
	'CS90', 
	S.MESSAGE_TEXT, 
	S.MSG_SEVERITY, 
	S.LAST_UPDATE_DTTM, 
	'N',
	'S',
	sysdate,
	sysdate,
	1234,
	S.DESCRLONG)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PSMSGCATDEFN rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PSMSGCATDEFN',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PSMSGCATDEFN';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PSMSGCATDEFN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PSMSGCATDEFN';
update AMSTG_OWNER.PSMSGCATDEFN T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select MESSAGE_SET_NBR, MESSAGE_NBR
   from AMSTG_OWNER.PSMSGCATDEFN T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PSMSGCATDEFN') = 'Y'
  minus
 select MESSAGE_SET_NBR, MESSAGE_NBR
   from SYSADM.PSMSGCATDEFN@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PSMSGCATDEFN') = 'Y'
   ) S
 where T.MESSAGE_SET_NBR = S.MESSAGE_SET_NBR
   and T.MESSAGE_NBR = S.MESSAGE_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PSMSGCATDEFN rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PSMSGCATDEFN',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PSMSGCATDEFN'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
   WHEN OTHERS
   THEN
      COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
         i_SqlCommand   => strSqlCommand,
         i_SqlCode      => SQLCODE,
         i_SqlErrm      => SQLERRM);

END AM_PSMSGCATDEFN_P;
/
