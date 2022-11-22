DROP PROCEDURE CSMRT_OWNER.AM_PS_VAR_DATA_AVAK_P
/

--
-- AM_PS_VAR_DATA_AVAK_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_VAR_DATA_AVAK_P" IS

------------------------------------------------------------------------
--
-- Loads stage table PS_VAR_DATA_AVAK from PeopleSoft table PS_VAR_DATA_AVAK.
--
-- V01  SMT-xxxx 09/13/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_VAR_DATA_AVAK';
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
 where TABLE_NAME = 'PS_VAR_DATA_AVAK'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_VAR_DATA_AVAK@AMSOURCE S)
 where TABLE_NAME = 'PS_VAR_DATA_AVAK'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_VAR_DATA_AVAK';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_VAR_DATA_AVAK';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_VAR_DATA_AVAK T
using (select /*+ full(S) */
    nvl(trim(COMMON_ID),'-') COMMON_ID, 
    nvl(VAR_DATA_SEQ,0) VAR_DATA_SEQ, 
    nvl(trim(SESSION_NO),'-') SESSION_NO, 
    nvl(trim(GIFT_NO),'-') GIFT_NO, 
    nvl(trim(RECOGNITION_TYPE),'-') RECOGNITION_TYPE, 
    nvl(trim(DESIGNATION),'-') DESIGNATION, 
    nvl(trim(INTV_CD),'-') INTV_CD, 
    nvl(trim(BUSINESS_UNIT),'-') BUSINESS_UNIT
from SYSADM.PS_VAR_DATA_AVAK@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_VAR_DATA_AVAK')
  and COMMON_ID between '00000000' and '99999999'
  and length(COMMON_ID) = 8  ) S
 on ( 
    T.COMMON_ID = S.COMMON_ID and 
    T.VAR_DATA_SEQ = S.VAR_DATA_SEQ and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.SESSION_NO = S.SESSION_NO,
    T.GIFT_NO = S.GIFT_NO,
    T.RECOGNITION_TYPE = S.RECOGNITION_TYPE,
    T.DESIGNATION = S.DESIGNATION,
    T.INTV_CD = S.INTV_CD,
    T.BUSINESS_UNIT = S.BUSINESS_UNIT,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.SESSION_NO <> S.SESSION_NO or 
    T.GIFT_NO <> S.GIFT_NO or 
    T.RECOGNITION_TYPE <> S.RECOGNITION_TYPE or 
    T.DESIGNATION <> S.DESIGNATION or 
    T.INTV_CD <> S.INTV_CD or 
    T.BUSINESS_UNIT <> S.BUSINESS_UNIT or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.COMMON_ID,
    T.VAR_DATA_SEQ, 
    T.SRC_SYS_ID, 
    T.SESSION_NO, 
    T.GIFT_NO,
    T.RECOGNITION_TYPE, 
    T.DESIGNATION,
    T.INTV_CD,
    T.BUSINESS_UNIT,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.COMMON_ID,
    S.VAR_DATA_SEQ, 
    'CS90', 
    S.SESSION_NO, 
    S.GIFT_NO,
    S.RECOGNITION_TYPE, 
    S.DESIGNATION,
    S.INTV_CD,
    S.BUSINESS_UNIT,
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

strMessage01    := '# of PS_VAR_DATA_AVAK rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_VAR_DATA_AVAK',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_VAR_DATA_AVAK';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_VAR_DATA_AVAK';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_VAR_DATA_AVAK';
update AMSTG_OWNER.PS_VAR_DATA_AVAK T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select COMMON_ID, VAR_DATA_SEQ
   from AMSTG_OWNER.PS_VAR_DATA_AVAK T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_VAR_DATA_AVAK') = 'Y'
  minus
 select COMMON_ID, VAR_DATA_SEQ
   from SYSADM.PS_VAR_DATA_AVAK@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_VAR_DATA_AVAK') = 'Y' 
   ) S
 where T.COMMON_ID = S.COMMON_ID   
   AND T.VAR_DATA_SEQ = S.VAR_DATA_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_VAR_DATA_AVAK rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_VAR_DATA_AVAK',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_VAR_DATA_AVAK'
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

END AM_PS_VAR_DATA_AVAK_P;
/
