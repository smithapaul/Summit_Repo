DROP PROCEDURE CSMRT_OWNER.AM_PS_TERM_FE_CD_TBL_P
/

--
-- AM_PS_TERM_FE_CD_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_TERM_FE_CD_TBL_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_TERM_FE_CD_TBL from PeopleSoft table PS_TERM_FE_CD_TBL.
--
-- V01  SMT-7707 03/07/2018,    Jim Doucette
--                              New Stage Table
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_TERM_FE_CD_TBL';
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
 where TABLE_NAME = 'PS_TERM_FE_CD_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_TERM_FE_CD_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_TERM_FE_CD_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_TERM_FE_CD_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_TERM_FE_CD_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_TERM_FE_CD_TBL T
using (select /*+ full(S) */ 
	nvl(trim(SETID),'-') SETID,
	nvl(trim(FEE_CODE),'-') FEE_CODE,
	nvl(trim(EFF_STATUS),'-') EFF_STATUS,
	nvl(trim(DESCR),'-') DESCR,
	nvl(trim(DESCR254),'-') DESCR254,
	nvl(trim(CHARGE_BY),'-') CHARGE_BY,
	nvl(trim(FEE_CLASS),'-') FEE_CLASS,
	nvl(trim(CURRENCY_CD),'-') CURRENCY_CD,
	nvl(trim(USE_UNIT),'-') USE_UNIT,
	nvl(trim(USE_ANTIC_UNIT_IND),'-') USE_ANTIC_UNIT_IND
from SYSADM.PS_TERM_FE_CD_TBL@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TERM_FE_CD_TBL') ) S
 on (
	T.SETID = S.SETID and
	T.FEE_CODE = S.FEE_CODE and
	T.SRC_SYS_ID = 'CS90') 
when matched then update set 
	T.EFF_STATUS = S.EFF_STATUS, 
	T.DESCR = S.DESCR, 
	T.DESCR254 = S.DESCR254, 
	T.CHARGE_BY = S.CHARGE_BY, 
	T.FEE_CLASS = S.FEE_CLASS, 
	T.CURRENCY_CD = S.CURRENCY_CD, 
	T.USE_UNIT = S.USE_UNIT, 
	T.USE_ANTIC_UNIT_IND = S.USE_ANTIC_UNIT_IND, 
	T.DATA_ORIGIN = 'S', 
	T.LASTUPD_EW_DTTM = sysdate, 
	T.BATCH_SID = 1234 
where
	T.EFF_STATUS <> S.EFF_STATUS or
	T.DESCR <> S.DESCR or
	T.DESCR254 <> S.DESCR254 or
	T.CHARGE_BY <> S.CHARGE_BY or
	T.FEE_CLASS <> S.FEE_CLASS or
	T.CURRENCY_CD <> S.CURRENCY_CD or
	T.USE_UNIT <> S.USE_UNIT or
	T.USE_ANTIC_UNIT_IND <> S.USE_ANTIC_UNIT_IND or
	T.DATA_ORIGIN = 'D'
when not matched then
insert ( 
	T.SETID, 
	T.FEE_CODE,
	T.SRC_SYS_ID,
	T.EFF_STATUS,
	T.DESCR, 
	T.DESCR254,
	T.CHARGE_BY, 
	T.FEE_CLASS, 
	T.CURRENCY_CD, 
	T.USE_UNIT,
	T.USE_ANTIC_UNIT_IND,
	T.LOAD_ERROR,
	T.DATA_ORIGIN, 
	T.CREATED_EW_DTTM, 
	T.LASTUPD_EW_DTTM, 
	T.BATCH_SID
	)
values ( 
	S.SETID, 
	S.FEE_CODE,
	'CS90',
	S.EFF_STATUS,
	S.DESCR, 
	S.DESCR254,
	S.CHARGE_BY, 
	S.FEE_CLASS, 
	S.CURRENCY_CD, 
	S.USE_UNIT,
	S.USE_ANTIC_UNIT_IND,
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

strMessage01    := '# of PS_TERM_FE_CD_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TERM_FE_CD_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_TERM_FE_CD_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_TERM_FE_CD_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_TERM_FE_CD_TBL';
update AMSTG_OWNER.PS_TERM_FE_CD_TBL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, FEE_CODE
   from AMSTG_OWNER.PS_TERM_FE_CD_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TERM_FE_CD_TBL') = 'Y'
  minus
 select SETID, FEE_CODE
   from SYSADM.PS_TERM_FE_CD_TBL@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TERM_FE_CD_TBL') = 'Y'
   ) S
 where T.SETID = S.SETID
   and T.FEE_CODE = S.FEE_CODE
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_TERM_FE_CD_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TERM_FE_CD_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_TERM_FE_CD_TBL'
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

END AM_PS_TERM_FE_CD_TBL_P;
/
