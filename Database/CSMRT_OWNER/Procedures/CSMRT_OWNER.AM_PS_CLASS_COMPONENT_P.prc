DROP PROCEDURE CSMRT_OWNER.AM_PS_CLASS_COMPONENT_P
/

--
-- AM_PS_CLASS_COMPONENT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_CLASS_COMPONENT_P" IS

------------------------------------------------------------------------
-- James Doucette
--
-- Loads stage table PS_CLASS_COMPONENT from PeopleSoft table PS_CLASS_COMPONENT.
--
 --V01  SMT-xxxx 1/23/2018,     James Doucette
--                              New Stage Table
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_CLASS_COMPONENT';
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
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_CLASS_COMPONENT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_CLASS_COMPONENT@AMSOURCE S)
 where TABLE_NAME = 'PS_CLASS_COMPONENT'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_CLASS_COMPONENT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_CLASS_COMPONENT';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_CLASS_COMPONENT T 
using (select /*+ full(S) */ 
	nvl(trim(CRSE_ID),'-') CRSE_ID,
	nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR,
	nvl(trim(STRM),'-') STRM,
	nvl(trim(SESSION_CODE),'-') SESSION_CODE,
	nvl(ASSOCIATED_CLASS,0) ASSOCIATED_CLASS,
	nvl(trim(SSR_COMPONENT),'-') SSR_COMPONENT,
	nvl(trim(OPTIONAL_SECTION),'-') OPTIONAL_SECTION,
	nvl(CONTACT_HOURS,0) CONTACT_HOURS,
	nvl(trim(FINAL_EXAM),'-') FINAL_EXAM,
	nvl(trim(AUTO_CREATE_CMPNT),'-') AUTO_CREATE_CMPNT,
	nvl(WEEK_WORKLOAD_HRS,0) WEEK_WORKLOAD_HRS
from SYSADM.PS_CLASS_COMPONENT@AMSOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_COMPONENT') ) S 
 on (
	T.CRSE_ID = S.CRSE_ID and
	T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR and
	T.STRM = S.STRM and
	T.SESSION_CODE = S.SESSION_CODE and
	T.ASSOCIATED_CLASS = S.ASSOCIATED_CLASS and
	T.SSR_COMPONENT = S.SSR_COMPONENT and
	T.SRC_SYS_ID = 'CS90') 
when matched then update set 
	T.OPTIONAL_SECTION = S.OPTIONAL_SECTION, 
	T.CONTACT_HOURS = S.CONTACT_HOURS, 
	T.FINAL_EXAM = S.FINAL_EXAM, 
	T.AUTO_CREATE_CMPNT = S.AUTO_CREATE_CMPNT, 
	T.WEEK_WORKLOAD_HRS = S.WEEK_WORKLOAD_HRS, 
	T.DATA_ORIGIN = 'S', 
	T.LASTUPD_EW_DTTM = sysdate, 
	T.BATCH_SID = 1234 
where
	T.OPTIONAL_SECTION <> S.OPTIONAL_SECTION or
	T.CONTACT_HOURS <> S.CONTACT_HOURS or
	T.FINAL_EXAM <> S.FINAL_EXAM or
	T.AUTO_CREATE_CMPNT <> S.AUTO_CREATE_CMPNT or
	T.WEEK_WORKLOAD_HRS <> S.WEEK_WORKLOAD_HRS or
	T.DATA_ORIGIN = 'D'
when not matched then
insert ( 
	T.CRSE_ID, 
	T.CRSE_OFFER_NBR,
	T.STRM,
	T.SESSION_CODE,
	T.ASSOCIATED_CLASS,
	T.SSR_COMPONENT, 
	T.SRC_SYS_ID,
	T.OPTIONAL_SECTION,
	T.CONTACT_HOURS, 
	T.FINAL_EXAM,
	T.AUTO_CREATE_CMPNT, 
	T.WEEK_WORKLOAD_HRS, 
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
	S.ASSOCIATED_CLASS,
	S.SSR_COMPONENT, 
	'CS90',
	S.OPTIONAL_SECTION,
	S.CONTACT_HOURS, 
	S.FINAL_EXAM,
	S.AUTO_CREATE_CMPNT, 
	S.WEEK_WORKLOAD_HRS, 
	'N', 
	'S', 
	sysdate, 
	sysdate, 
	1234)
; 

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLASS_COMPONENT rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLASS_COMPONENT',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_CLASS_COMPONENT';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_CLASS_COMPONENT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_CLASS_COMPONENT';
update AMSTG_OWNER.PS_CLASS_COMPONENT T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CRSE_ID, CRSE_OFFER_NBR, STRM, SESSION_CODE, ASSOCIATED_CLASS, SSR_COMPONENT
   from AMSTG_OWNER.PS_CLASS_COMPONENT T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_COMPONENT') = 'Y'
  minus
 select CRSE_ID, CRSE_OFFER_NBR, STRM, SESSION_CODE, ASSOCIATED_CLASS, SSR_COMPONENT
   from SYSADM.PS_CLASS_COMPONENT@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_COMPONENT') = 'Y' 
   ) S
 where T.CRSE_ID = S.CRSE_ID
   and T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR
   and T.STRM = S.STRM
   and T.SESSION_CODE = S.SESSION_CODE
   and T.ASSOCIATED_CLASS = S.ASSOCIATED_CLASS
   and T.SSR_COMPONENT = S.SSR_COMPONENT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLASS_COMPONENT rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLASS_COMPONENT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_CLASS_COMPONENT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
    WHEN OTHERS THEN

        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END AM_PS_CLASS_COMPONENT_P;
/
