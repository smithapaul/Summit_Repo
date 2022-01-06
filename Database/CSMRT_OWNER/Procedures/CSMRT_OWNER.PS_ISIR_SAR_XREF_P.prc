CREATE OR REPLACE PROCEDURE             "PS_ISIR_SAR_XREF_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ISIR_SAR_XREF from PeopleSoft table PS_ISIR_SAR_XREF.
--
-- V01  CASE-83341 12/14/2020,    Jim Doucette
--                              
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ISIR_SAR_XREF';
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
 where TABLE_NAME = 'PS_ISIR_SAR_XREF'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ISIR_SAR_XREF@SASOURCE S)
 where TABLE_NAME = 'PS_ISIR_SAR_XREF'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ISIR_SAR_XREF';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ISIR_SAR_XREF';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ISIR_SAR_XREF T
using (select /*+ full(S) */
       nvl(trim(AID_YEAR),'-') AID_YEAR, 
	   nvl(trim(RECNAME),'-') RECNAME, 
	   nvl(trim(FIELDNAME),'-') FIELDNAME,
	   nvl(trim(ISIR_FIELD_NUM),'-') ISIR_FIELD_NUM,
       nvl(trim(EFF_STATUS),'-') EFF_STATUS,
	   nvl(trim(FAFSA_QUESTION_NUM),'-') FAFSA_QUESTION_NUM,
	   ISIR_START_POS,
	   ISIR_FIELD_LENGTH,
	   nvl(trim(ISIR_FIELD_TYPE),'-')  ISIR_FIELD_TYPE,
	   nvl(trim(ISIR_CORR_TO_BLANK),'-')  ISIR_CORR_TO_BLANK,
	   nvl(trim(DESCR),'-') DESCR
  from SYSADM.PS_ISIR_SAR_XREF@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ISIR_SAR_XREF') ) S
    on (
       T.AID_YEAR = S.AID_YEAR and
       T.RECNAME = S.RECNAME and
       T.FIELDNAME = S.FIELDNAME and
       T.SRC_SYS_ID = 'CS90')
when matched then update set
       T.ISIR_FIELD_NUM = S.ISIR_FIELD_NUM,
	   T.EFF_STATUS = S.EFF_STATUS,
       T.FAFSA_QUESTION_NUM = S.FAFSA_QUESTION_NUM,
       T.ISIR_START_POS = S.ISIR_START_POS,
       T.ISIR_FIELD_LENGTH = S.ISIR_FIELD_LENGTH,
       T.ISIR_FIELD_TYPE = S.ISIR_FIELD_TYPE,
	   T.ISIR_CORR_TO_BLANK = S.ISIR_CORR_TO_BLANK,
	   T.DESCR = S.DESCR,
       T.DATA_ORIGIN = 'S',
       T.LASTUPD_EW_DTTM = sysdate
where
       T.ISIR_FIELD_NUM <> S.ISIR_FIELD_NUM or
	   T.EFF_STATUS <> S.EFF_STATUS or
       T.FAFSA_QUESTION_NUM <> S.FAFSA_QUESTION_NUM or
       T.ISIR_START_POS <> S.ISIR_START_POS or
       T.ISIR_FIELD_LENGTH <> S.ISIR_FIELD_LENGTH or
       T.ISIR_FIELD_TYPE <> S.ISIR_FIELD_TYPE or
	   T.ISIR_CORR_TO_BLANK <> S.ISIR_CORR_TO_BLANK or
	   T.DESCR <> S.DESCR or
       T.DATA_ORIGIN = 'D'
when not matched then
insert (
       T.AID_YEAR,
       T.RECNAME,
       T.FIELDNAME,
       T.SRC_SYS_ID,
       T.ISIR_FIELD_NUM,
	   T.EFF_STATUS,
       T.FAFSA_QUESTION_NUM,
       T.ISIR_START_POS,
       T.ISIR_FIELD_LENGTH,
       T.ISIR_FIELD_TYPE,
	   T.ISIR_CORR_TO_BLANK,
       T.DESCR,
       T.DATA_ORIGIN,
       T.CREATED_EW_DTTM,
       T.LASTUPD_EW_DTTM
       )
values (
       S.AID_YEAR,
       S.RECNAME,
       S.FIELDNAME,
       'CS90',
       S.ISIR_FIELD_NUM,
	   S.EFF_STATUS,
       S.FAFSA_QUESTION_NUM,
       S.ISIR_START_POS,
       S.ISIR_FIELD_LENGTH,
       S.ISIR_FIELD_TYPE,
	   S.ISIR_CORR_TO_BLANK,
       S.DESCR,
       'S',
       sysdate,
       sysdate
       )
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ISIR_SAR_XREF rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ISIR_SAR_XREF',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ISIR_SAR_XREF';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ISIR_SAR_XREF';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ISIR_SAR_XREF';
update CSSTG_OWNER.PS_ISIR_SAR_XREF T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select AID_YEAR, RECNAME, FIELDNAME
   from CSSTG_OWNER.PS_ISIR_SAR_XREF T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ISIR_SAR_XREF') = 'Y'
  minus
 select nvl(trim(AID_YEAR),'-'), nvl(trim(RECNAME),'-'), nvl(trim(FIELDNAME),'-')
   from SYSADM.PS_ISIR_SAR_XREF@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ISIR_SAR_XREF') = 'Y'
   ) S
 where T.AID_YEAR = S.AID_YEAR
   and T.RECNAME = S.RECNAME
   and T.FIELDNAME = S.FIELDNAME
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ISIR_SAR_XREF rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ISIR_SAR_XREF',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ISIR_SAR_XREF'
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

END PS_ISIR_SAR_XREF_P;
/
