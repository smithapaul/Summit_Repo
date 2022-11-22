DROP PROCEDURE CSMRT_OWNER.AM_PS_SEL_GROUP_TBL_P
/

--
-- AM_PS_SEL_GROUP_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_SEL_GROUP_TBL_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_SEL_GROUP_TBL from PeopleSoft table PS_SEL_GROUP_TBL.
--
 --V01  SMT-xxxx 09/11/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_SEL_GROUP_TBL';
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
 where TABLE_NAME = 'PS_SEL_GROUP_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_SEL_GROUP_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_SEL_GROUP_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_SEL_GROUP_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_SEL_GROUP_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_SEL_GROUP_TBL T
using (select /*+ full(S) */
    nvl(trim(BUSINESS_UNIT),'-') BUSINESS_UNIT, 
    nvl(trim(SEL_GROUP),'-') SEL_GROUP, 
    EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(DESCR254A),'-') DESCR254A, 
    nvl(PRIORITY,0) PRIORITY, 
    nvl(trim(TUIT_CALC_GROUP),'-') TUIT_CALC_GROUP, 
    nvl(trim(FA_CALC_GROUP),'-') FA_CALC_GROUP, 
    nvl(trim(TRANS_FEE_CD),'-') TRANS_FEE_CD, 
    nvl(trim(PRO_RATA_ADJ),'-') PRO_RATA_ADJ, 
    nvl(trim(PRO_RATA_SRVC_IND),'-') PRO_RATA_SRVC_IND, 
    nvl(trim(PRO_RATA_SRVC_RSN),'-') PRO_RATA_SRVC_RSN, 
    nvl(trim(ACCREDIT_AGNCY_ADJ),'-') ACCREDIT_AGNCY_ADJ, 
    nvl(trim(ACCR_AGEN_SRVC_IND),'-') ACCR_AGEN_SRVC_IND, 
    nvl(trim(ACCR_AGEN_SRVC_RSN),'-') ACCR_AGEN_SRVC_RSN, 
    nvl(trim(STATE_ADJ),'-') STATE_ADJ, 
    nvl(trim(STATE_SRVC_IND),'-') STATE_SRVC_IND, 
    nvl(trim(STATE_SRVC_RSN),'-') STATE_SRVC_RSN, 
    nvl(trim(FED_REFUND_ADJ),'-') FED_REFUND_ADJ, 
    nvl(trim(FEDERAL_SRVC_IND),'-') FEDERAL_SRVC_IND, 
    nvl(trim(FEDERAL_SRVC_RSN),'-') FEDERAL_SRVC_RSN, 
    nvl(trim(INSTITUTION_ADJ),'-') INSTITUTION_ADJ, 
    nvl(trim(INSTITUTN_SRVC_IND),'-') INSTITUTN_SRVC_IND, 
    nvl(trim(INSTITUTN_SRVC_RSN),'-') INSTITUTN_SRVC_RSN, 
    nvl(trim(NSFA_INST_ADJ),'-') NSFA_INST_ADJ, 
    nvl(trim(NSF_INST_SRVC_IND),'-') NSF_INST_SRVC_IND, 
    nvl(trim(NSF_INST_SRVC_RSN),'-') NSF_INST_SRVC_RSN, 
    nvl(trim(NSFA_1ST_INST_ADJ),'-') NSFA_1ST_INST_ADJ, 
    nvl(trim(NSF_1_INS_SRVC_IND),'-') NSF_1_INS_SRVC_IND, 
    nvl(trim(NSF_1_INS_SRVC_RSN),'-') NSF_1_INS_SRVC_RSN, 
    nvl(trim(NSFA_1ST_CAR_ADJ),'-') NSFA_1ST_CAR_ADJ, 
    nvl(trim(NSF_1_CAR_SRVC_IND),'-') NSF_1_CAR_SRVC_IND, 
    nvl(trim(NSF_1_CAR_SRVC_RSN),'-') NSF_1_CAR_SRVC_RSN, 
    nvl(trim(SRVC_IND_CD),'-') SRVC_IND_CD, 
    nvl(trim(SRVC_IND_REASON),'-') SRVC_IND_REASON, 
    nvl(trim(LOCK_IN_FLG),'-') LOCK_IN_FLG, 
    nvl(trim(SSF_HECS_CODE),'-') SSF_HECS_CODE
from SYSADM.PS_SEL_GROUP_TBL@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SEL_GROUP_TBL') ) S
 on ( 
    T.BUSINESS_UNIT = S.BUSINESS_UNIT and 
    T.SEL_GROUP = S.SEL_GROUP and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.DESCR254A = S.DESCR254A,
    T.PRIORITY = S.PRIORITY,
    T.TUIT_CALC_GROUP = S.TUIT_CALC_GROUP,
    T.FA_CALC_GROUP = S.FA_CALC_GROUP,
    T.TRANS_FEE_CD = S.TRANS_FEE_CD,
    T.PRO_RATA_ADJ = S.PRO_RATA_ADJ,
    T.PRO_RATA_SRVC_IND = S.PRO_RATA_SRVC_IND,
    T.PRO_RATA_SRVC_RSN = S.PRO_RATA_SRVC_RSN,
    T.ACCREDIT_AGNCY_ADJ = S.ACCREDIT_AGNCY_ADJ,
    T.ACCR_AGEN_SRVC_IND = S.ACCR_AGEN_SRVC_IND,
    T.ACCR_AGEN_SRVC_RSN = S.ACCR_AGEN_SRVC_RSN,
    T.STATE_ADJ = S.STATE_ADJ,
    T.STATE_SRVC_IND = S.STATE_SRVC_IND,
    T.STATE_SRVC_RSN = S.STATE_SRVC_RSN,
    T.FED_REFUND_ADJ = S.FED_REFUND_ADJ,
    T.FEDERAL_SRVC_IND = S.FEDERAL_SRVC_IND,
    T.FEDERAL_SRVC_RSN = S.FEDERAL_SRVC_RSN,
    T.INSTITUTION_ADJ = S.INSTITUTION_ADJ,
    T.INSTITUTN_SRVC_IND = S.INSTITUTN_SRVC_IND,
    T.INSTITUTN_SRVC_RSN = S.INSTITUTN_SRVC_RSN,
    T.NSFA_INST_ADJ = S.NSFA_INST_ADJ,
    T.NSF_INST_SRVC_IND = S.NSF_INST_SRVC_IND,
    T.NSF_INST_SRVC_RSN = S.NSF_INST_SRVC_RSN,
    T.NSFA_1ST_INST_ADJ = S.NSFA_1ST_INST_ADJ,
    T.NSF_1_INS_SRVC_IND = S.NSF_1_INS_SRVC_IND,
    T.NSF_1_INS_SRVC_RSN = S.NSF_1_INS_SRVC_RSN,
    T.NSFA_1ST_CAR_ADJ = S.NSFA_1ST_CAR_ADJ,
    T.NSF_1_CAR_SRVC_IND = S.NSF_1_CAR_SRVC_IND,
    T.NSF_1_CAR_SRVC_RSN = S.NSF_1_CAR_SRVC_RSN,
    T.SRVC_IND_CD = S.SRVC_IND_CD,
    T.SRVC_IND_REASON = S.SRVC_IND_REASON,
    T.LOCK_IN_FLG = S.LOCK_IN_FLG,
    T.SSF_HECS_CODE = S.SSF_HECS_CODE,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.DESCR254A <> S.DESCR254A or 
    T.PRIORITY <> S.PRIORITY or 
    T.TUIT_CALC_GROUP <> S.TUIT_CALC_GROUP or 
    T.FA_CALC_GROUP <> S.FA_CALC_GROUP or 
    T.TRANS_FEE_CD <> S.TRANS_FEE_CD or 
    T.PRO_RATA_ADJ <> S.PRO_RATA_ADJ or 
    T.PRO_RATA_SRVC_IND <> S.PRO_RATA_SRVC_IND or 
    T.PRO_RATA_SRVC_RSN <> S.PRO_RATA_SRVC_RSN or 
    T.ACCREDIT_AGNCY_ADJ <> S.ACCREDIT_AGNCY_ADJ or 
    T.ACCR_AGEN_SRVC_IND <> S.ACCR_AGEN_SRVC_IND or 
    T.ACCR_AGEN_SRVC_RSN <> S.ACCR_AGEN_SRVC_RSN or 
    T.STATE_ADJ <> S.STATE_ADJ or 
    T.STATE_SRVC_IND <> S.STATE_SRVC_IND or 
    T.STATE_SRVC_RSN <> S.STATE_SRVC_RSN or 
    T.FED_REFUND_ADJ <> S.FED_REFUND_ADJ or 
    T.FEDERAL_SRVC_IND <> S.FEDERAL_SRVC_IND or 
    T.FEDERAL_SRVC_RSN <> S.FEDERAL_SRVC_RSN or 
    T.INSTITUTION_ADJ <> S.INSTITUTION_ADJ or 
    T.INSTITUTN_SRVC_IND <> S.INSTITUTN_SRVC_IND or 
    T.INSTITUTN_SRVC_RSN <> S.INSTITUTN_SRVC_RSN or 
    T.NSFA_INST_ADJ <> S.NSFA_INST_ADJ or 
    T.NSF_INST_SRVC_IND <> S.NSF_INST_SRVC_IND or 
    T.NSF_INST_SRVC_RSN <> S.NSF_INST_SRVC_RSN or 
    T.NSFA_1ST_INST_ADJ <> S.NSFA_1ST_INST_ADJ or 
    T.NSF_1_INS_SRVC_IND <> S.NSF_1_INS_SRVC_IND or 
    T.NSF_1_INS_SRVC_RSN <> S.NSF_1_INS_SRVC_RSN or 
    T.NSFA_1ST_CAR_ADJ <> S.NSFA_1ST_CAR_ADJ or 
    T.NSF_1_CAR_SRVC_IND <> S.NSF_1_CAR_SRVC_IND or 
    T.NSF_1_CAR_SRVC_RSN <> S.NSF_1_CAR_SRVC_RSN or 
    T.SRVC_IND_CD <> S.SRVC_IND_CD or 
    T.SRVC_IND_REASON <> S.SRVC_IND_REASON or 
    T.LOCK_IN_FLG <> S.LOCK_IN_FLG or 
    T.SSF_HECS_CODE <> S.SSF_HECS_CODE or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.BUSINESS_UNIT,
    T.SEL_GROUP,
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.DESCR,
    T.DESCRSHORT, 
    T.DESCR254A,
    T.PRIORITY, 
    T.TUIT_CALC_GROUP,
    T.FA_CALC_GROUP,
    T.TRANS_FEE_CD, 
    T.PRO_RATA_ADJ, 
    T.PRO_RATA_SRVC_IND,
    T.PRO_RATA_SRVC_RSN,
    T.ACCREDIT_AGNCY_ADJ, 
    T.ACCR_AGEN_SRVC_IND, 
    T.ACCR_AGEN_SRVC_RSN, 
    T.STATE_ADJ,
    T.STATE_SRVC_IND, 
    T.STATE_SRVC_RSN, 
    T.FED_REFUND_ADJ, 
    T.FEDERAL_SRVC_IND, 
    T.FEDERAL_SRVC_RSN, 
    T.INSTITUTION_ADJ,
    T.INSTITUTN_SRVC_IND, 
    T.INSTITUTN_SRVC_RSN, 
    T.NSFA_INST_ADJ,
    T.NSF_INST_SRVC_IND,
    T.NSF_INST_SRVC_RSN,
    T.NSFA_1ST_INST_ADJ,
    T.NSF_1_INS_SRVC_IND, 
    T.NSF_1_INS_SRVC_RSN, 
    T.NSFA_1ST_CAR_ADJ, 
    T.NSF_1_CAR_SRVC_IND, 
    T.NSF_1_CAR_SRVC_RSN, 
    T.SRVC_IND_CD,
    T.SRVC_IND_REASON,
    T.LOCK_IN_FLG,
    T.SSF_HECS_CODE,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.BUSINESS_UNIT,
    S.SEL_GROUP,
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.DESCR,
    S.DESCRSHORT, 
    S.DESCR254A,
    S.PRIORITY, 
    S.TUIT_CALC_GROUP,
    S.FA_CALC_GROUP,
    S.TRANS_FEE_CD, 
    S.PRO_RATA_ADJ, 
    S.PRO_RATA_SRVC_IND,
    S.PRO_RATA_SRVC_RSN,
    S.ACCREDIT_AGNCY_ADJ, 
    S.ACCR_AGEN_SRVC_IND, 
    S.ACCR_AGEN_SRVC_RSN, 
    S.STATE_ADJ,
    S.STATE_SRVC_IND, 
    S.STATE_SRVC_RSN, 
    S.FED_REFUND_ADJ, 
    S.FEDERAL_SRVC_IND, 
    S.FEDERAL_SRVC_RSN, 
    S.INSTITUTION_ADJ,
    S.INSTITUTN_SRVC_IND, 
    S.INSTITUTN_SRVC_RSN, 
    S.NSFA_INST_ADJ,
    S.NSF_INST_SRVC_IND,
    S.NSF_INST_SRVC_RSN,
    S.NSFA_1ST_INST_ADJ,
    S.NSF_1_INS_SRVC_IND, 
    S.NSF_1_INS_SRVC_RSN, 
    S.NSFA_1ST_CAR_ADJ, 
    S.NSF_1_CAR_SRVC_IND, 
    S.NSF_1_CAR_SRVC_RSN, 
    S.SRVC_IND_CD,
    S.SRVC_IND_REASON,
    S.LOCK_IN_FLG,
    S.SSF_HECS_CODE,
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

strMessage01    := '# of PS_SEL_GROUP_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SEL_GROUP_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_SEL_GROUP_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_SEL_GROUP_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_SEL_GROUP_TBL';
update AMSTG_OWNER.PS_SEL_GROUP_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select BUSINESS_UNIT, SEL_GROUP, EFFDT
   from AMSTG_OWNER.PS_SEL_GROUP_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SEL_GROUP_TBL') = 'Y'
  minus
 select BUSINESS_UNIT, SEL_GROUP, EFFDT
   from SYSADM.PS_SEL_GROUP_TBL@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SEL_GROUP_TBL') = 'Y'
  and SEL_GROUP <> 'UG-PTIS'
   ) S
 where T.BUSINESS_UNIT = S.BUSINESS_UNIT
   and T.SEL_GROUP = S.SEL_GROUP
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SEL_GROUP_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SEL_GROUP_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_SEL_GROUP_TBL'
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

END AM_PS_SEL_GROUP_TBL_P;
/
