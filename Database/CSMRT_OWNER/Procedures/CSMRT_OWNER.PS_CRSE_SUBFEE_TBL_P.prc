DROP PROCEDURE CSMRT_OWNER.PS_CRSE_SUBFEE_TBL_P
/

--
-- PS_CRSE_SUBFEE_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_CRSE_SUBFEE_TBL_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_CRSE_SUBFEE_TBL';

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_CRSE_SUBFEE_TBL', 'Y');

COMMIT; 

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_CRSE_SUBFEE_TBL';
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_CRSE_SUBFEE_TBL from PeopleSoft table PS_CRSE_SUBFEE_TBL.
--
-- V01  SMT-7550 11/28/2017,    Jim Doucette
--                              New Table
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_CRSE_SUBFEE_TBL';
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

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_CRSE_SUBFEE_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_CRSE_SUBFEE_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_CRSE_SUBFEE_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_CRSE_SUBFEE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_CRSE_SUBFEE_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_CRSE_SUBFEE_TBL T 
using (select /*+ full(S) */ 
	nvl(trim(SETID),'-') SETID,
	nvl(trim(CRSE_ID),'-') CRSE_ID,
	nvl(trim(SSR_COMPONENT),'-') SSR_COMPONENT,
	nvl(trim(INSTITUTION),'-') INSTITUTION,
	nvl(trim(CAMPUS),'-') CAMPUS,
	nvl(trim(LOCATION),'-') LOCATION,
	nvl(trim(STRM),'-') STRM,
	nvl(trim(SESSION_CODE),'-') SESSION_CODE,
	nvl(trim(ACCOUNT_TYPE_SF),'-') ACCOUNT_TYPE_SF,
	nvl(trim(ITEM_TYPE),'-') ITEM_TYPE,
	nvl(trim(SSF_CRITR_EQUTN_SW),'-') SSF_CRITR_EQUTN_SW,
	nvl(trim(FEE_TRIGGER),'-') FEE_TRIGGER,
	nvl(trim(EQUATION_NAME),'-') EQUATION_NAME,
	nvl(AMT_PER_UNIT,0) AMT_PER_UNIT,
	nvl(FLAT_AMT,0) FLAT_AMT,
	nvl(trim(ADJ_TERM_CD),'-') ADJ_TERM_CD,
	nvl(trim(DUE_DATE_CODE),'-') DUE_DATE_CODE,
	nvl(trim(WAIVER_GROUP),'-') WAIVER_GROUP
from SYSADM.PS_CRSE_SUBFEE_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_SUBFEE_TBL') ) S 
 on (
	T.SETID = S.SETID and
	T.CRSE_ID = S.CRSE_ID and
	T.SSR_COMPONENT = S.SSR_COMPONENT and
	T.INSTITUTION = S.INSTITUTION and
	T.CAMPUS = S.CAMPUS and
	T.LOCATION = S.LOCATION and
	T.STRM = S.STRM and
	T.SESSION_CODE = S.SESSION_CODE and
	T.ACCOUNT_TYPE_SF = S.ACCOUNT_TYPE_SF and
	T.ITEM_TYPE = S.ITEM_TYPE and
	T.SRC_SYS_ID = 'CS90') 
when matched then update set 
	T.SSF_CRITR_EQUTN_SW = S.SSF_CRITR_EQUTN_SW, 
	T.FEE_TRIGGER = S.FEE_TRIGGER, 
	T.EQUATION_NAME = S.EQUATION_NAME, 
	T.AMT_PER_UNIT = S.AMT_PER_UNIT, 
	T.FLAT_AMT = S.FLAT_AMT, 
	T.ADJ_TERM_CD = S.ADJ_TERM_CD, 
	T.DUE_DATE_CODE = S.DUE_DATE_CODE, 
	T.WAIVER_GROUP = S.WAIVER_GROUP, 
	T.DATA_ORIGIN = 'S', 
	T.LASTUPD_EW_DTTM = sysdate, 
	T.BATCH_SID = 1234 
where
	T.SSF_CRITR_EQUTN_SW <> S.SSF_CRITR_EQUTN_SW or
	T.FEE_TRIGGER <> S.FEE_TRIGGER or
	T.EQUATION_NAME <> S.EQUATION_NAME or
	T.AMT_PER_UNIT <> S.AMT_PER_UNIT or
	T.FLAT_AMT <> S.FLAT_AMT or
	T.ADJ_TERM_CD <> S.ADJ_TERM_CD or
	T.DUE_DATE_CODE <> S.DUE_DATE_CODE or
	T.WAIVER_GROUP <> S.WAIVER_GROUP or
	T.DATA_ORIGIN = 'D'
when not matched then
insert ( 
	T.SETID, 
	T.CRSE_ID, 
	T.SSR_COMPONENT, 
	T.INSTITUTION, 
	T.CAMPUS,
	T.LOCATION,
	T.STRM,
	T.SESSION_CODE,
	T.ACCOUNT_TYPE_SF, 
	T.ITEM_TYPE, 
	T.SRC_SYS_ID,
	T.SSF_CRITR_EQUTN_SW,
	T.FEE_TRIGGER, 
	T.EQUATION_NAME, 
	T.AMT_PER_UNIT,
	T.FLAT_AMT,
	T.ADJ_TERM_CD, 
	T.DUE_DATE_CODE, 
	T.WAIVER_GROUP,
	T.LOAD_ERROR,
	T.DATA_ORIGIN, 
	T.CREATED_EW_DTTM, 
	T.LASTUPD_EW_DTTM, 
	T.BATCH_SID
	)
values ( 
	S.SETID, 
	S.CRSE_ID, 
	S.SSR_COMPONENT, 
	S.INSTITUTION, 
	S.CAMPUS,
	S.LOCATION,
	S.STRM,
	S.SESSION_CODE,
	S.ACCOUNT_TYPE_SF, 
	S.ITEM_TYPE, 
	'CS90',
	S.SSF_CRITR_EQUTN_SW,
	S.FEE_TRIGGER, 
	S.EQUATION_NAME, 
	S.AMT_PER_UNIT,
	S.FLAT_AMT,
	S.ADJ_TERM_CD, 
	S.DUE_DATE_CODE, 
	S.WAIVER_GROUP,
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

strMessage01    := '# of PS_CRSE_SUBFEE_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CRSE_SUBFEE_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_CRSE_SUBFEE_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_CRSE_SUBFEE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_CRSE_SUBFEE_TBL';
update CSSTG_OWNER.PS_CRSE_SUBFEE_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, CRSE_ID, SSR_COMPONENT, INSTITUTION, CAMPUS, LOCATION, STRM, SESSION_CODE, ACCOUNT_TYPE_SF, ITEM_TYPE
   from CSSTG_OWNER.PS_CRSE_SUBFEE_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_SUBFEE_TBL') = 'Y'
  minus
 select nvl(trim(SETID),'-') SETID,
	nvl(trim(CRSE_ID),'-') CRSE_ID,
	nvl(trim(SSR_COMPONENT),'-') SSR_COMPONENT,
	nvl(trim(INSTITUTION),'-') INSTITUTION,
	nvl(trim(CAMPUS),'-') CAMPUS,
	nvl(trim(LOCATION),'-') LOCATION,
	nvl(trim(STRM),'-') STRM,
	nvl(trim(SESSION_CODE),'-') SESSION_CODE,
	nvl(trim(ACCOUNT_TYPE_SF),'-') ACCOUNT_TYPE_SF,
	nvl(trim(ITEM_TYPE),'-') ITEM_TYPE
   from SYSADM.PS_CRSE_SUBFEE_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_SUBFEE_TBL') = 'Y'
   ) S
 where T.SETID = S.SETID
   and T.CRSE_ID = S.CRSE_ID
   and T.SSR_COMPONENT = S.SSR_COMPONENT
   and T.INSTITUTION = S.INSTITUTION
   and T.CAMPUS = S.CAMPUS
   and T.LOCATION = S.LOCATION
   and T.STRM = S.STRM
   and T.SESSION_CODE = S.SESSION_CODE
   and T.ACCOUNT_TYPE_SF = S.ACCOUNT_TYPE_SF
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CRSE_SUBFEE_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CRSE_SUBFEE_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_CRSE_SUBFEE_TBL'
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

END PS_CRSE_SUBFEE_TBL_P;
/
