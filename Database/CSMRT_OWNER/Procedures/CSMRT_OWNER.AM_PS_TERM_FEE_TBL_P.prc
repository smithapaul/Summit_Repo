DROP PROCEDURE CSMRT_OWNER.AM_PS_TERM_FEE_TBL_P
/

--
-- AM_PS_TERM_FEE_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_TERM_FEE_TBL_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_TERM_FEE_TBL from PeopleSoft table PS_TERM_FEE_TBL.
--
-- V01  SMT-xxxx 04/04/2017,    Jim Doucette
--                              Converted from PS_TERM_FEE_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_TERM_FEE_TBL';
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
 where TABLE_NAME = 'PS_TERM_FEE_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_TERM_FEE_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_TERM_FEE_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_TERM_FEE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_TERM_FEE_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_TERM_FEE_TBL T                   
using (select /*+ full(S) */                                                    
    nvl(trim(SETID),'-') SETID,                                                     
    nvl(trim(FEE_CODE),'-') FEE_CODE,                                               
    nvl(trim(STRM),'-') STRM,                                                       
    nvl(trim(SESSION_CODE),'-') SESSION_CODE,                                       
    nvl(trim(ACCOUNT_TYPE_SF),'-') ACCOUNT_TYPE_SF,                                 
    nvl(trim(ITEM_TYPE),'-') ITEM_TYPE,                                             
    nvl(trim(ANTIC_ITEM_TYPE),'-') ANTIC_ITEM_TYPE,                                 
    to_date(to_char(case when USE_ANTIC_STOP_DT < '01-JAN-1800' then NULL 
                    else USE_ANTIC_STOP_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') USE_ANTIC_STOP_DT, 
    nvl(trim(CHARGE_WAIT_LIST),'-') CHARGE_WAIT_LIST, 
    nvl(trim(SSF_CRITR_EQUTN_SW),'-') SSF_CRITR_EQUTN_SW, 
    nvl(trim(FEE_TRIGGER),'-') FEE_TRIGGER, 
    nvl(MIN_AMOUNT,0) MIN_AMOUNT, 
    nvl(MAX_AMOUNT,0) MAX_AMOUNT, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    nvl(trim(ADJ_TERM_CD),'-') ADJ_TERM_CD, 
    nvl(trim(DUE_DATE_CODE),'-') DUE_DATE_CODE, 
    nvl(trim(MIN_MAX_FEE_CODE),'-') MIN_MAX_FEE_CODE, 
    nvl(trim(DYNAMIC_ORG),'-') DYNAMIC_ORG, 
    nvl(trim(TRACK_CLASS_PRICE),'-') TRACK_CLASS_PRICE, 
    nvl(trim(TRACK_SUB_FEE),'-') TRACK_SUB_FEE, 
    nvl(trim(GL_FROM_SUBFEE),'-') GL_FROM_SUBFEE, 
    nvl(trim(AUDIT_RT_FLAG),'-') AUDIT_RT_FLAG, 
    nvl(trim(AUDIT_CALC_FLAG),'-') AUDIT_CALC_FLAG, 
    nvl(trim(SSF_EXCLUDE_HECS),'-') SSF_EXCLUDE_HECS
from SYSADM.PS_TERM_FEE_TBL@AMSOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TERM_FEE_TBL') ) S 
 on ( 
    T.SETID = S.SETID and 
    T.FEE_CODE = S.FEE_CODE and 
    T.STRM = S.STRM and 
    T.SESSION_CODE = S.SESSION_CODE and
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.ACCOUNT_TYPE_SF = S.ACCOUNT_TYPE_SF,
    T.ITEM_TYPE = S.ITEM_TYPE,
    T.ANTIC_ITEM_TYPE = S.ANTIC_ITEM_TYPE,
    T.USE_ANTIC_STOP_DT = S.USE_ANTIC_STOP_DT,
    T.CHARGE_WAIT_LIST = S.CHARGE_WAIT_LIST,
    T.SSF_CRITR_EQUTN_SW = S.SSF_CRITR_EQUTN_SW,
    T.FEE_TRIGGER = S.FEE_TRIGGER,
    T.MIN_AMOUNT = S.MIN_AMOUNT,
    T.MAX_AMOUNT = S.MAX_AMOUNT,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.ADJ_TERM_CD = S.ADJ_TERM_CD,
    T.DUE_DATE_CODE = S.DUE_DATE_CODE,
    T.MIN_MAX_FEE_CODE = S.MIN_MAX_FEE_CODE,
    T.DYNAMIC_ORG = S.DYNAMIC_ORG,
    T.TRACK_CLASS_PRICE = S.TRACK_CLASS_PRICE,
    T.TRACK_SUB_FEE = S.TRACK_SUB_FEE,
    T.GL_FROM_SUBFEE = S.GL_FROM_SUBFEE,
    T.AUDIT_RT_FLAG = S.AUDIT_RT_FLAG,
    T.AUDIT_CALC_FLAG = S.AUDIT_CALC_FLAG,
    T.SSF_EXCLUDE_HECS = S.SSF_EXCLUDE_HECS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.ACCOUNT_TYPE_SF <> S.ACCOUNT_TYPE_SF or 
    T.ITEM_TYPE <> S.ITEM_TYPE or 
    T.ANTIC_ITEM_TYPE <> S.ANTIC_ITEM_TYPE or 
    nvl(trim(T.USE_ANTIC_STOP_DT),0) <> nvl(trim(S.USE_ANTIC_STOP_DT),0) or 
    T.CHARGE_WAIT_LIST <> S.CHARGE_WAIT_LIST or 
    T.SSF_CRITR_EQUTN_SW <> S.SSF_CRITR_EQUTN_SW or 
    T.FEE_TRIGGER <> S.FEE_TRIGGER or 
    T.MIN_AMOUNT <> S.MIN_AMOUNT or 
    T.MAX_AMOUNT <> S.MAX_AMOUNT or 
    T.CURRENCY_CD <> S.CURRENCY_CD or 
    T.ADJ_TERM_CD <> S.ADJ_TERM_CD or 
    T.DUE_DATE_CODE <> S.DUE_DATE_CODE or 
    T.MIN_MAX_FEE_CODE <> S.MIN_MAX_FEE_CODE or 
    T.DYNAMIC_ORG <> S.DYNAMIC_ORG or 
    T.TRACK_CLASS_PRICE <> S.TRACK_CLASS_PRICE or 
    T.TRACK_SUB_FEE <> S.TRACK_SUB_FEE or 
    T.GL_FROM_SUBFEE <> S.GL_FROM_SUBFEE or 
    T.AUDIT_RT_FLAG <> S.AUDIT_RT_FLAG or 
    T.AUDIT_CALC_FLAG <> S.AUDIT_CALC_FLAG or 
    T.SSF_EXCLUDE_HECS <> S.SSF_EXCLUDE_HECS or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.SETID,
    T.FEE_CODE, 
    T.STRM, 
    T.SESSION_CODE, 
    T.SRC_SYS_ID,
    T.ACCOUNT_TYPE_SF,
    T.ITEM_TYPE,
    T.ANTIC_ITEM_TYPE,
    T.USE_ANTIC_STOP_DT,
    T.CHARGE_WAIT_LIST, 
    T.SSF_CRITR_EQUTN_SW, 
    T.FEE_TRIGGER,
    T.MIN_AMOUNT, 
    T.MAX_AMOUNT, 
    T.CURRENCY_CD,
    T.ADJ_TERM_CD,
    T.DUE_DATE_CODE,
    T.MIN_MAX_FEE_CODE, 
    T.DYNAMIC_ORG,
    T.TRACK_CLASS_PRICE,
    T.TRACK_SUB_FEE,
    T.GL_FROM_SUBFEE, 
    T.AUDIT_RT_FLAG,
    T.AUDIT_CALC_FLAG,
    T.SSF_EXCLUDE_HECS, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.SETID,
    S.FEE_CODE, 
    S.STRM, 
    S.SESSION_CODE, 
    'CS90',
    S.ACCOUNT_TYPE_SF,
    S.ITEM_TYPE,
    S.ANTIC_ITEM_TYPE,
    S.USE_ANTIC_STOP_DT,
    S.CHARGE_WAIT_LIST, 
    S.SSF_CRITR_EQUTN_SW, 
    S.FEE_TRIGGER,
    S.MIN_AMOUNT, 
    S.MAX_AMOUNT, 
    S.CURRENCY_CD,
    S.ADJ_TERM_CD,
    S.DUE_DATE_CODE,
    S.MIN_MAX_FEE_CODE, 
    S.DYNAMIC_ORG,
    S.TRACK_CLASS_PRICE,
    S.TRACK_SUB_FEE,
    S.GL_FROM_SUBFEE, 
    S.AUDIT_RT_FLAG,
    S.AUDIT_CALC_FLAG,
    S.SSF_EXCLUDE_HECS, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_TERM_FEE_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TERM_FEE_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_TERM_FEE_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_TERM_FEE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_TERM_FEE_TBL';
update AMSTG_OWNER.PS_TERM_FEE_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select nvl(trim(SETID),'-') SETID,                                                     
        nvl(trim(FEE_CODE),'-') FEE_CODE,                                               
        nvl(trim(STRM),'-') STRM,                                                       
        nvl(trim(SESSION_CODE),'-') SESSION_CODE
   from AMSTG_OWNER.PS_TERM_FEE_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TERM_FEE_TBL') = 'Y'
  minus
 select nvl(trim(SETID),'-') SETID,                                                     
        nvl(trim(FEE_CODE),'-') FEE_CODE,                                               
        nvl(trim(STRM),'-') STRM,                                                       
        nvl(trim(SESSION_CODE),'-') SESSION_CODE
   from SYSADM.PS_TERM_FEE_TBL@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TERM_FEE_TBL') = 'Y'
   ) S
 where T.SETID = S.SETID
   and T.FEE_CODE = S.FEE_CODE
   and T.STRM = S.STRM
   and T.SESSION_CODE = S.SESSION_CODE
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_TERM_FEE_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TERM_FEE_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_TERM_FEE_TBL'
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

END AM_PS_TERM_FEE_TBL_P;
/
