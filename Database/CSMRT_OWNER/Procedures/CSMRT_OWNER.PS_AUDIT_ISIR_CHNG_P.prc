CREATE OR REPLACE PROCEDURE             "PS_AUDIT_ISIR_CHNG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_AUDIT_ISIR_CHNG from PeopleSoft table PS_AUDIT_ISIR_CHNG.
--
-- V01  CASE-83341 12/14/2020,    Jim Doucette
--                              
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_AUDIT_ISIR_CHNG';
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
 where TABLE_NAME = 'PS_AUDIT_ISIR_CHNG'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_AUDIT_ISIR_CHNG@SASOURCE S)
 where TABLE_NAME = 'PS_AUDIT_ISIR_CHNG'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_AUDIT_ISIR_CHNG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_AUDIT_ISIR_CHNG';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_AUDIT_ISIR_CHNG T
using (select /*+ full(S) */
       nvl(trim(EMPLID),'-') EMPLID, 
	   nvl(trim(INSTITUTION),'-') INSTITUTION, 
	   nvl(trim(AID_YEAR),'-') AID_YEAR,
	   DTTM_STAMP,
       nvl(trim(ISIR_FIELD_NUM),'-') ISIR_FIELD_NUM,
	   ISIR_TXN_NBR,
	   OPRID,
	   OLDVALUE,
	   NEWVALUE,
	   CORRECTION_STATUS,
	   CORR_STAT_DT
  from SYSADM.PS_AUDIT_ISIR_CHNG@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_AUDIT_ISIR_CHNG') ) S
    on (
       T.EMPLID = S.EMPLID and
       T.INSTITUTION = S.INSTITUTION and
       T.AID_YEAR = S.AID_YEAR and
       T.DTTM_STAMP = S.DTTM_STAMP and
       T.ISIR_FIELD_NUM = S.ISIR_FIELD_NUM and
       T.SRC_SYS_ID = 'CS90')
when matched then update set
       T.ISIR_TXN_NBR = S.ISIR_TXN_NBR,
	   T.OPRID = S.OPRID,
       T.OLDVALUE = S.OLDVALUE,
       T.NEWVALUE = S.NEWVALUE,
       T.CORRECTION_STATUS = S.CORRECTION_STATUS,
       T.CORR_STAT_DT = S.CORR_STAT_DT,
       T.DATA_ORIGIN = 'S',
       T.LASTUPD_EW_DTTM = sysdate
where
       T.ISIR_TXN_NBR <> S.ISIR_TXN_NBR or
	   T.OPRID <> S.OPRID or
       T.OLDVALUE <> S.OLDVALUE or
       T.NEWVALUE <> S.NEWVALUE or
       T.CORRECTION_STATUS <> S.CORRECTION_STATUS or
       T.CORR_STAT_DT <> S.CORR_STAT_DT or
       T.DATA_ORIGIN = 'D'
when not matched then
insert (
       T.EMPLID,
       T.INSTITUTION,
       T.AID_YEAR,
       T.DTTM_STAMP,
	   T.ISIR_FIELD_NUM,
       T.SRC_SYS_ID,
       T.ISIR_TXN_NBR,
	   T.OPRID,
       T.OLDVALUE,
       T.NEWVALUE,
       T.CORRECTION_STATUS,
       T.CORR_STAT_DT,
       T.DATA_ORIGIN,
       T.CREATED_EW_DTTM,
       T.LASTUPD_EW_DTTM
       )
values (
       S.EMPLID,
       S.INSTITUTION,
       S.AID_YEAR,
       S.DTTM_STAMP,
	   S.ISIR_FIELD_NUM,
       'CS90',
       S.ISIR_TXN_NBR,
	   S.OPRID,
       S.OLDVALUE,
       S.NEWVALUE,
       S.CORRECTION_STATUS,
       S.CORR_STAT_DT,
       'S',
       sysdate,
       sysdate
       )
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_AUDIT_ISIR_CHNG rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_AUDIT_ISIR_CHNG',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_AUDIT_ISIR_CHNG';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_AUDIT_ISIR_CHNG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_AUDIT_ISIR_CHNG';
update CSSTG_OWNER.PS_AUDIT_ISIR_CHNG T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, DTTM_STAMP, ISIR_FIELD_NUM
   from CSSTG_OWNER.PS_AUDIT_ISIR_CHNG T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_AUDIT_ISIR_CHNG') = 'Y'
  minus
 select nvl(trim(EMPLID),'-'), nvl(trim(INSTITUTION),'-'), nvl(trim(AID_YEAR),'-'), DTTM_STAMP, nvl(trim(ISIR_FIELD_NUM),'-')
   from SYSADM.PS_AUDIT_ISIR_CHNG@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_AUDIT_ISIR_CHNG') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.DTTM_STAMP = S.DTTM_STAMP
   and T.ISIR_FIELD_NUM = S.ISIR_FIELD_NUM
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_AUDIT_ISIR_CHNG rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_AUDIT_ISIR_CHNG',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_AUDIT_ISIR_CHNG'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
    WHEN OTHERS THEN
        numSqlCode := SQLCODE;
        strSqlErrm := SQLERRM;

        ROLLBACK;
  
        strMessage01 := 'Error code: ' || TO_CHAR(SQLCODE) || ' Error Message: ' || SQLERRM;
        strMessage02 := TO_CHAR(SQLCODE);
  
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_FAILURE
                       (i_SqlCommand    => strSqlCommand,
                        i_ErrorText     => strMessage01,
                        i_ErrorCode     => strMessage02,
                        i_ErrorMessage  => strSqlErrm
                       );
               
        strMessage01 := 'Error...'
                        || strNewLine   || 'SQL Command:   ' || strSqlCommand
                        || strNewLine   || 'Error code:    ' || numSqlCode
                        || strNewLine   || 'Error Message: ' || strSqlErrm;

        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
        RAISE_APPLICATION_ERROR( -20001, strMessage01);

END PS_AUDIT_ISIR_CHNG_P;
/
