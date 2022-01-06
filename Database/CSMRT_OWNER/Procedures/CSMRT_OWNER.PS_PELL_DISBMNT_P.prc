CREATE OR REPLACE PROCEDURE             "PS_PELL_DISBMNT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_PELL_DISBMNT from PeopleSoft table PS_PELL_DISBMNT.
--
-- V01  SMT-xxxx 04/04/2017,    Jim Doucette
--                              Converted from PS_PELL_DISBMNT.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_PELL_DISBMNT';
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
 where TABLE_NAME = 'PS_PELL_DISBMNT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_PELL_DISBMNT@SASOURCE S)
 where TABLE_NAME = 'PS_PELL_DISBMNT'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_PELL_DISBMNT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_PELL_DISBMNT';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_PELL_DISBMNT T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(AID_YEAR),'-') AID_YEAR,
nvl(trim(PELL_ORIG_ID),'-') PELL_ORIG_ID,
nvl(PELL_DISB_SEQ_NBR,0) PELL_DISB_SEQ_NBR,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(ITEM_TYPE),'-') ITEM_TYPE,
nvl(trim(DISBURSEMENT_ID),'-') DISBURSEMENT_ID,
nvl(PELL_DISB_AMT,0) PELL_DISB_AMT,
to_date(to_char(case when PELL_DISB_DT < '01-JAN-1800' then NULL else PELL_DISB_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PELL_DISB_DT,
nvl(trim(PELL_DISB_STATUS),'-') PELL_DISB_STATUS,
to_date(to_char(case when PELL_DISB_STAT_DT < '01-JAN-1800' then NULL else PELL_DISB_STAT_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PELL_DISB_STAT_DT,
nvl(trim(AWARD_PERIOD),'-') AWARD_PERIOD,
nvl(PELL_YTD_DSB_AMT,0) PELL_YTD_DSB_AMT,
nvl(trim(ACTION_CODE),'-') ACTION_CODE,
nvl(PELL_PAYPR_NBR,0) PELL_PAYPR_NBR,
to_date(to_char(case when PELL_PAYPR_STRT_DT < '01-JAN-1800' then NULL else PELL_PAYPR_STRT_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PELL_PAYPR_STRT_DT,
to_date(to_char(case when PELL_PAYPR_END_DT < '01-JAN-1800' then NULL else PELL_PAYPR_END_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PELL_PAYPR_END_DT,
nvl(PELL_PAY_PERIODS,0) PELL_PAY_PERIODS,
nvl(PELL_PAYPR_AMOUNT,0) PELL_PAYPR_AMOUNT,
nvl(trim(PELL_PAYPR_ACD_CAL),'-') PELL_PAYPR_ACD_CAL,
nvl(trim(PELL_PAYPR_PMT_MTH),'-') PELL_PAYPR_PMT_MTH,
nvl(PELL_PAYPR_COA,0) PELL_PAYPR_COA,
nvl(trim(PELL_PAYPR_ENRL_ST),'-') PELL_PAYPR_ENRL_ST,
nvl(trim(PELL_PAYPR_WEEKS),'-') PELL_PAYPR_WEEKS,
nvl(trim(WEEKS_PROG_ACADYR),'-') WEEKS_PROG_ACADYR,
nvl(trim(PELL_PAYPR_HOURS),'-') PELL_PAYPR_HOURS,
nvl(trim(HRS_CREDITS_ACADYR),'-') HRS_CREDITS_ACADYR,
nvl(trim(PG_ED_USE_FLAG_1),'-') PG_ED_USE_FLAG_1,
nvl(trim(PG_ED_USE_FLAG_2),'-') PG_ED_USE_FLAG_2,
nvl(trim(PG_ED_USE_FLAG_3),'-') PG_ED_USE_FLAG_3,
nvl(trim(PG_ED_USE_FLAG_4),'-') PG_ED_USE_FLAG_4,
nvl(trim(PG_ED_USE_FLAG_5),'-') PG_ED_USE_FLAG_5,
nvl(trim(PG_ED_USE_FLAG_6),'-') PG_ED_USE_FLAG_6,
nvl(trim(PG_ED_USE_FLAG_7),'-') PG_ED_USE_FLAG_7,
nvl(trim(PG_ED_USE_FLAG_8),'-') PG_ED_USE_FLAG_8,
nvl(trim(PG_ED_USE_FLAG_9),'-') PG_ED_USE_FLAG_9,
nvl(trim(PG_ED_USE_FLAG_10),'-') PG_ED_USE_FLAG_10,
nvl(PELL_RFMS_DISB_SEQ,0) PELL_RFMS_DISB_SEQ,
nvl(PELL_PREV_DISB_REG,0) PELL_PREV_DISB_REG,
nvl(trim(PELL_DISB_TYPE),'-') PELL_DISB_TYPE,
nvl(PELL_COD_DISB_NUM,0) PELL_COD_DISB_NUM,
nvl(PELL_COD_DISB_SEQ,0) PELL_COD_DISB_SEQ,
nvl(PELL_COD_DISB_AMT,0) PELL_COD_DISB_AMT,
to_date(to_char(case when PELL_ACT_DISB_DT < '01-JAN-1800' then NULL else PELL_ACT_DISB_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PELL_ACT_DISB_DT,
nvl(trim(SFA_COD_ENR_SCHLCD),'-') SFA_COD_ENR_SCHLCD
  from SYSADM.PS_PELL_DISBMNT@SASOURCE S
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PELL_DISBMNT')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
   on (
T.EMPLID = S.EMPLID and
T.INSTITUTION = S.INSTITUTION and
T.AID_YEAR = S.AID_YEAR and
T.PELL_ORIG_ID = S.PELL_ORIG_ID and
T.PELL_DISB_SEQ_NBR = S.PELL_DISB_SEQ_NBR and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.ACAD_CAREER = S.ACAD_CAREER,
T.ITEM_TYPE = S.ITEM_TYPE,
T.DISBURSEMENT_ID = S.DISBURSEMENT_ID,
T.PELL_DISB_AMT = S.PELL_DISB_AMT,
T.PELL_DISB_DT = S.PELL_DISB_DT,
T.PELL_DISB_STATUS = S.PELL_DISB_STATUS,
T.PELL_DISB_STAT_DT = S.PELL_DISB_STAT_DT,
T.AWARD_PERIOD = S.AWARD_PERIOD,
T.PELL_YTD_DSB_AMT = S.PELL_YTD_DSB_AMT,
T.ACTION_CODE = S.ACTION_CODE,
T.PELL_PAYPR_NBR = S.PELL_PAYPR_NBR,
T.PELL_PAYPR_STRT_DT = S.PELL_PAYPR_STRT_DT,
T.PELL_PAYPR_END_DT = S.PELL_PAYPR_END_DT,
T.PELL_PAY_PERIODS = S.PELL_PAY_PERIODS,
T.PELL_PAYPR_AMOUNT = S.PELL_PAYPR_AMOUNT,
T.PELL_PAYPR_ACD_CAL = S.PELL_PAYPR_ACD_CAL,
T.PELL_PAYPR_PMT_MTH = S.PELL_PAYPR_PMT_MTH,
T.PELL_PAYPR_COA = S.PELL_PAYPR_COA,
T.PELL_PAYPR_ENRL_ST = S.PELL_PAYPR_ENRL_ST,
T.PELL_PAYPR_WEEKS = S.PELL_PAYPR_WEEKS,
T.WEEKS_PROG_ACADYR = S.WEEKS_PROG_ACADYR,
T.PELL_PAYPR_HOURS = S.PELL_PAYPR_HOURS,
T.HRS_CREDITS_ACADYR = S.HRS_CREDITS_ACADYR,
T.PG_ED_USE_FLAG_1 = S.PG_ED_USE_FLAG_1,
T.PG_ED_USE_FLAG_2 = S.PG_ED_USE_FLAG_2,
T.PG_ED_USE_FLAG_3 = S.PG_ED_USE_FLAG_3,
T.PG_ED_USE_FLAG_4 = S.PG_ED_USE_FLAG_4,
T.PG_ED_USE_FLAG_5 = S.PG_ED_USE_FLAG_5,
T.PG_ED_USE_FLAG_6 = S.PG_ED_USE_FLAG_6,
T.PG_ED_USE_FLAG_7 = S.PG_ED_USE_FLAG_7,
T.PG_ED_USE_FLAG_8 = S.PG_ED_USE_FLAG_8,
T.PG_ED_USE_FLAG_9 = S.PG_ED_USE_FLAG_9,
T.PG_ED_USE_FLAG_10 = S.PG_ED_USE_FLAG_10,
T.PELL_RFMS_DISB_SEQ = S.PELL_RFMS_DISB_SEQ,
T.PELL_PREV_DISB_REG = S.PELL_PREV_DISB_REG,
T.PELL_DISB_TYPE = S.PELL_DISB_TYPE,
T.PELL_COD_DISB_NUM = S.PELL_COD_DISB_NUM,
T.PELL_COD_DISB_SEQ = S.PELL_COD_DISB_SEQ,
T.PELL_COD_DISB_AMT = S.PELL_COD_DISB_AMT,
T.PELL_ACT_DISB_DT = S.PELL_ACT_DISB_DT,
T.SFA_COD_ENR_SCHLCD = S.SFA_COD_ENR_SCHLCD,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.ACAD_CAREER <> S.ACAD_CAREER or
T.ITEM_TYPE <> S.ITEM_TYPE or
T.DISBURSEMENT_ID <> S.DISBURSEMENT_ID or
T.PELL_DISB_AMT <> S.PELL_DISB_AMT or
nvl(trim(T.PELL_DISB_DT),0) <> nvl(trim(S.PELL_DISB_DT),0) or
T.PELL_DISB_STATUS <> S.PELL_DISB_STATUS or
nvl(trim(T.PELL_DISB_STAT_DT),0) <> nvl(trim(S.PELL_DISB_STAT_DT),0) or
T.AWARD_PERIOD <> S.AWARD_PERIOD or
T.PELL_YTD_DSB_AMT <> S.PELL_YTD_DSB_AMT or
T.ACTION_CODE <> S.ACTION_CODE or
T.PELL_PAYPR_NBR <> S.PELL_PAYPR_NBR or
nvl(trim(T.PELL_PAYPR_STRT_DT),0) <> nvl(trim(S.PELL_PAYPR_STRT_DT),0) or
nvl(trim(T.PELL_PAYPR_END_DT),0) <> nvl(trim(S.PELL_PAYPR_END_DT),0) or
T.PELL_PAY_PERIODS <> S.PELL_PAY_PERIODS or
T.PELL_PAYPR_AMOUNT <> S.PELL_PAYPR_AMOUNT or
T.PELL_PAYPR_ACD_CAL <> S.PELL_PAYPR_ACD_CAL or
T.PELL_PAYPR_PMT_MTH <> S.PELL_PAYPR_PMT_MTH or
T.PELL_PAYPR_COA <> S.PELL_PAYPR_COA or
T.PELL_PAYPR_ENRL_ST <> S.PELL_PAYPR_ENRL_ST or
T.PELL_PAYPR_WEEKS <> S.PELL_PAYPR_WEEKS or
T.WEEKS_PROG_ACADYR <> S.WEEKS_PROG_ACADYR or
T.PELL_PAYPR_HOURS <> S.PELL_PAYPR_HOURS or
T.HRS_CREDITS_ACADYR <> S.HRS_CREDITS_ACADYR or
T.PG_ED_USE_FLAG_1 <> S.PG_ED_USE_FLAG_1 or
T.PG_ED_USE_FLAG_2 <> S.PG_ED_USE_FLAG_2 or
T.PG_ED_USE_FLAG_3 <> S.PG_ED_USE_FLAG_3 or
T.PG_ED_USE_FLAG_4 <> S.PG_ED_USE_FLAG_4 or
T.PG_ED_USE_FLAG_5 <> S.PG_ED_USE_FLAG_5 or
T.PG_ED_USE_FLAG_6 <> S.PG_ED_USE_FLAG_6 or
T.PG_ED_USE_FLAG_7 <> S.PG_ED_USE_FLAG_7 or
T.PG_ED_USE_FLAG_8 <> S.PG_ED_USE_FLAG_8 or
T.PG_ED_USE_FLAG_9 <> S.PG_ED_USE_FLAG_9 or
T.PG_ED_USE_FLAG_10 <> S.PG_ED_USE_FLAG_10 or
T.PELL_RFMS_DISB_SEQ <> S.PELL_RFMS_DISB_SEQ or
T.PELL_PREV_DISB_REG <> S.PELL_PREV_DISB_REG or
T.PELL_DISB_TYPE <> S.PELL_DISB_TYPE or
T.PELL_COD_DISB_NUM <> S.PELL_COD_DISB_NUM or
T.PELL_COD_DISB_SEQ <> S.PELL_COD_DISB_SEQ or
T.PELL_COD_DISB_AMT <> S.PELL_COD_DISB_AMT or
nvl(trim(T.PELL_ACT_DISB_DT),0) <> nvl(trim(S.PELL_ACT_DISB_DT),0) or
T.SFA_COD_ENR_SCHLCD <> S.SFA_COD_ENR_SCHLCD or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.INSTITUTION,
T.AID_YEAR,
T.PELL_ORIG_ID,
T.PELL_DISB_SEQ_NBR,
T.SRC_SYS_ID,
T.ACAD_CAREER,
T.ITEM_TYPE,
T.DISBURSEMENT_ID,
T.PELL_DISB_AMT,
T.PELL_DISB_DT,
T.PELL_DISB_STATUS,
T.PELL_DISB_STAT_DT,
T.AWARD_PERIOD,
T.PELL_YTD_DSB_AMT,
T.ACTION_CODE,
T.PELL_PAYPR_NBR,
T.PELL_PAYPR_STRT_DT,
T.PELL_PAYPR_END_DT,
T.PELL_PAY_PERIODS,
T.PELL_PAYPR_AMOUNT,
T.PELL_PAYPR_ACD_CAL,
T.PELL_PAYPR_PMT_MTH,
T.PELL_PAYPR_COA,
T.PELL_PAYPR_ENRL_ST,
T.PELL_PAYPR_WEEKS,
T.WEEKS_PROG_ACADYR,
T.PELL_PAYPR_HOURS,
T.HRS_CREDITS_ACADYR,
T.PG_ED_USE_FLAG_1,
T.PG_ED_USE_FLAG_2,
T.PG_ED_USE_FLAG_3,
T.PG_ED_USE_FLAG_4,
T.PG_ED_USE_FLAG_5,
T.PG_ED_USE_FLAG_6,
T.PG_ED_USE_FLAG_7,
T.PG_ED_USE_FLAG_8,
T.PG_ED_USE_FLAG_9,
T.PG_ED_USE_FLAG_10,
T.PELL_RFMS_DISB_SEQ,
T.PELL_PREV_DISB_REG,
T.PELL_DISB_TYPE,
T.PELL_COD_DISB_NUM,
T.PELL_COD_DISB_SEQ,
T.PELL_COD_DISB_AMT,
T.PELL_ACT_DISB_DT,
T.SFA_COD_ENR_SCHLCD,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.EMPLID,
S.INSTITUTION,
S.AID_YEAR,
S.PELL_ORIG_ID,
S.PELL_DISB_SEQ_NBR,
'CS90',
S.ACAD_CAREER,
S.ITEM_TYPE,
S.DISBURSEMENT_ID,
S.PELL_DISB_AMT,
S.PELL_DISB_DT,
S.PELL_DISB_STATUS,
S.PELL_DISB_STAT_DT,
S.AWARD_PERIOD,
S.PELL_YTD_DSB_AMT,
S.ACTION_CODE,
S.PELL_PAYPR_NBR,
S.PELL_PAYPR_STRT_DT,
S.PELL_PAYPR_END_DT,
S.PELL_PAY_PERIODS,
S.PELL_PAYPR_AMOUNT,
S.PELL_PAYPR_ACD_CAL,
S.PELL_PAYPR_PMT_MTH,
S.PELL_PAYPR_COA,
S.PELL_PAYPR_ENRL_ST,
S.PELL_PAYPR_WEEKS,
S.WEEKS_PROG_ACADYR,
S.PELL_PAYPR_HOURS,
S.HRS_CREDITS_ACADYR,
S.PG_ED_USE_FLAG_1,
S.PG_ED_USE_FLAG_2,
S.PG_ED_USE_FLAG_3,
S.PG_ED_USE_FLAG_4,
S.PG_ED_USE_FLAG_5,
S.PG_ED_USE_FLAG_6,
S.PG_ED_USE_FLAG_7,
S.PG_ED_USE_FLAG_8,
S.PG_ED_USE_FLAG_9,
S.PG_ED_USE_FLAG_10,
S.PELL_RFMS_DISB_SEQ,
S.PELL_PREV_DISB_REG,
S.PELL_DISB_TYPE,
S.PELL_COD_DISB_NUM,
S.PELL_COD_DISB_SEQ,
S.PELL_COD_DISB_AMT,
S.PELL_ACT_DISB_DT,
S.SFA_COD_ENR_SCHLCD,
'N',
'S',
sysdate,
sysdate,
1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_PELL_DISBMNT rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PELL_DISBMNT',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_PELL_DISBMNT';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_PELL_DISBMNT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_PELL_DISBMNT';
update CSSTG_OWNER.PS_PELL_DISBMNT T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, PELL_ORIG_ID, PELL_DISB_SEQ_NBR
   from CSSTG_OWNER.PS_PELL_DISBMNT T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PELL_DISBMNT') = 'Y'
  minus
 select EMPLID, INSTITUTION, AID_YEAR, PELL_ORIG_ID, PELL_DISB_SEQ_NBR
   from SYSADM.PS_PELL_DISBMNT@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PELL_DISBMNT') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.PELL_ORIG_ID = S.PELL_ORIG_ID
   and T.PELL_DISB_SEQ_NBR = S.PELL_DISB_SEQ_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_PELL_DISBMNT rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PELL_DISBMNT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_PELL_DISBMNT'
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

END PS_PELL_DISBMNT_P;
/
