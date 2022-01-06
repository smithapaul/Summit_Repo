CREATE OR REPLACE PROCEDURE             "PS_PELL_ORIG_DTL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_PELL_ORIG_DTL from PeopleSoft table PS_PELL_ORIG_DTL.
--
-- V01  SMT-xxxx 04/04/2017,    Jim Doucette
--                              Converted from PS_PELL_ORIG_DTL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_PELL_ORIG_DTL';
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
 where TABLE_NAME = 'PS_PELL_ORIG_DTL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_PELL_ORIG_DTL@SASOURCE S)
 where TABLE_NAME = 'PS_PELL_ORIG_DTL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_PELL_ORIG_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_PELL_ORIG_DTL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_PELL_ORIG_DTL T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(AID_YEAR),'-') AID_YEAR,
nvl(trim(PELL_ORIG_ID),'-') PELL_ORIG_ID,
nvl(PELL_ORIG_SEQ_NBR,0) PELL_ORIG_SEQ_NBR,
ORIG_SSN ORIG_SSN,
NAME_CD NAME_CD,
ISIR_TXN_NBR ISIR_TXN_NBR,
PELL_EFC PELL_EFC,
PELL_ID_ATTENDED PELL_ID_ATTENDED,
PELL_BDGT_COA PELL_BDGT_COA,
VERIF_STATUS_CODE VERIF_STATUS_CODE,
ACADEMIC_CALENDAR ACADEMIC_CALENDAR,
PELL_ENROLL_STAT PELL_ENROLL_STAT,
HOURS_CRD_PD HOURS_CRD_PD,
HRS_CREDITS_ACADYR HRS_CREDITS_ACADYR,
PELL_AWARD_AMT PELL_AWARD_AMT,
PELL_PMT_METH PELL_PMT_METH,
PELL_PAY_PERIODS PELL_PAY_PERIODS,
INCARCERATED_CODE INCARCERATED_CODE,
to_date(to_char(case when PELL_ENRLMNT_DT < '01-JAN-1800' then NULL else PELL_ENRLMNT_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PELL_ENRLMNT_DT,
SSN SSN,
ACTION_CODE ACTION_CODE,
WEEKS_USED_TO_CALC WEEKS_USED_TO_CALC,
WEEKS_PROG_ACADYR WEEKS_PROG_ACADYR,
LOW_TUITION_FLAG LOW_TUITION_FLAG,
SECONDARY_EFC_FLAG SECONDARY_EFC_FLAG,
PELL_SCHED_AWARD PELL_SCHED_AWARD,
PREV_ISIR_TXN_NBR PREV_ISIR_TXN_NBR,
PREV_PELL_EFC PREV_PELL_EFC,
PREV_SEC_EFC_FLAG PREV_SEC_EFC_FLAG,
PREV_PELL_BDGT_COA PREV_PELL_BDGT_COA,
PG_ED_USE_FLAG_1 PG_ED_USE_FLAG_1,
PG_ED_USE_FLAG_2 PG_ED_USE_FLAG_2,
PG_ED_USE_FLAG_3 PG_ED_USE_FLAG_3,
PG_ED_USE_FLAG_4 PG_ED_USE_FLAG_4,
PG_ED_USE_FLAG_5 PG_ED_USE_FLAG_5,
PG_ED_USE_FLAG_6 PG_ED_USE_FLAG_6,
PG_ED_USE_FLAG_7 PG_ED_USE_FLAG_7,
PG_ED_USE_FLAG_8 PG_ED_USE_FLAG_8,
PG_ED_USE_FLAG_9 PG_ED_USE_FLAG_9,
PG_ED_USE_FLAG_10 PG_ED_USE_FLAG_10,
PELL_MANUAL_OVRD PELL_MANUAL_OVRD,
SFA_ADDL_PELL_ELIG SFA_ADDL_PELL_ELIG,
SFA_COD_CITZN_STAT SFA_COD_CITZN_STAT,
SFA_ATB_CD SFA_ATB_CD,
SFA_ATB_TST_ADM_CD SFA_ATB_TST_ADM_CD,
SFA_ATB_TST_CD SFA_ATB_TST_CD,
SFA_ATB_STATE_CD SFA_ATB_STATE_CD,
to_date(to_char(case when SFA_ATB_COMP_DT < '01-JAN-1800' then NULL else SFA_ATB_COMP_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SFA_ATB_COMP_DT
  from SYSADM.PS_PELL_ORIG_DTL@SASOURCE S
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PELL_ORIG_DTL')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
   on (
T.EMPLID = S.EMPLID and
T.INSTITUTION = S.INSTITUTION and
T.AID_YEAR = S.AID_YEAR and
T.PELL_ORIG_ID = S.PELL_ORIG_ID and
T.PELL_ORIG_SEQ_NBR = S.PELL_ORIG_SEQ_NBR and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.ORIG_SSN = S.ORIG_SSN,
T.NAME_CD = S.NAME_CD,
T.ISIR_TXN_NBR = S.ISIR_TXN_NBR,
T.PELL_EFC = S.PELL_EFC,
T.PELL_ID_ATTENDED = S.PELL_ID_ATTENDED,
T.PELL_BDGT_COA = S.PELL_BDGT_COA,
T.VERIF_STATUS_CODE = S.VERIF_STATUS_CODE,
T.ACADEMIC_CALENDAR = S.ACADEMIC_CALENDAR,
T.PELL_ENROLL_STAT = S.PELL_ENROLL_STAT,
T.HOURS_CRD_PD = S.HOURS_CRD_PD,
T.HRS_CREDITS_ACADYR = S.HRS_CREDITS_ACADYR,
T.PELL_AWARD_AMT = S.PELL_AWARD_AMT,
T.PELL_PMT_METH = S.PELL_PMT_METH,
T.PELL_PAY_PERIODS = S.PELL_PAY_PERIODS,
T.INCARCERATED_CODE = S.INCARCERATED_CODE,
T.PELL_ENRLMNT_DT = S.PELL_ENRLMNT_DT,
T.SSN = S.SSN,
T.ACTION_CODE = S.ACTION_CODE,
T.WEEKS_USED_TO_CALC = S.WEEKS_USED_TO_CALC,
T.WEEKS_PROG_ACADYR = S.WEEKS_PROG_ACADYR,
T.LOW_TUITION_FLAG = S.LOW_TUITION_FLAG,
T.SECONDARY_EFC_FLAG = S.SECONDARY_EFC_FLAG,
T.PELL_SCHED_AWARD = S.PELL_SCHED_AWARD,
T.PREV_ISIR_TXN_NBR = S.PREV_ISIR_TXN_NBR,
T.PREV_PELL_EFC = S.PREV_PELL_EFC,
T.PREV_SEC_EFC_FLAG = S.PREV_SEC_EFC_FLAG,
T.PREV_PELL_BDGT_COA = S.PREV_PELL_BDGT_COA,
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
T.PELL_MANUAL_OVRD = S.PELL_MANUAL_OVRD,
T.SFA_ADDL_PELL_ELIG = S.SFA_ADDL_PELL_ELIG,
T.SFA_COD_CITZN_STAT = S.SFA_COD_CITZN_STAT,
T.SFA_ATB_CD = S.SFA_ATB_CD,
T.SFA_ATB_TST_ADM_CD = S.SFA_ATB_TST_ADM_CD,
T.SFA_ATB_TST_CD = S.SFA_ATB_TST_CD,
T.SFA_ATB_STATE_CD = S.SFA_ATB_STATE_CD,
T.SFA_ATB_COMP_DT = S.SFA_ATB_COMP_DT,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
nvl(trim(T.ORIG_SSN),0) <> nvl(trim(S.ORIG_SSN),0) or
nvl(trim(T.NAME_CD),0) <> nvl(trim(S.NAME_CD),0) or
nvl(trim(T.ISIR_TXN_NBR),0) <> nvl(trim(S.ISIR_TXN_NBR),0) or
nvl(trim(T.PELL_EFC),0) <> nvl(trim(S.PELL_EFC),0) or
nvl(trim(T.PELL_ID_ATTENDED),0) <> nvl(trim(S.PELL_ID_ATTENDED),0) or
nvl(trim(T.PELL_BDGT_COA),0) <> nvl(trim(S.PELL_BDGT_COA),0) or
nvl(trim(T.VERIF_STATUS_CODE),0) <> nvl(trim(S.VERIF_STATUS_CODE),0) or
nvl(trim(T.ACADEMIC_CALENDAR),0) <> nvl(trim(S.ACADEMIC_CALENDAR),0) or
nvl(trim(T.PELL_ENROLL_STAT),0) <> nvl(trim(S.PELL_ENROLL_STAT),0) or
nvl(trim(T.HOURS_CRD_PD),0) <> nvl(trim(S.HOURS_CRD_PD),0) or
nvl(trim(T.HRS_CREDITS_ACADYR),0) <> nvl(trim(S.HRS_CREDITS_ACADYR),0) or
nvl(trim(T.PELL_AWARD_AMT),0) <> nvl(trim(S.PELL_AWARD_AMT),0) or
nvl(trim(T.PELL_PMT_METH),0) <> nvl(trim(S.PELL_PMT_METH),0) or
nvl(trim(T.PELL_PAY_PERIODS),0) <> nvl(trim(S.PELL_PAY_PERIODS),0) or
nvl(trim(T.INCARCERATED_CODE),0) <> nvl(trim(S.INCARCERATED_CODE),0) or
nvl(trim(T.PELL_ENRLMNT_DT),0) <> nvl(trim(S.PELL_ENRLMNT_DT),0) or
nvl(trim(T.SSN),0) <> nvl(trim(S.SSN),0) or
nvl(trim(T.ACTION_CODE),0) <> nvl(trim(S.ACTION_CODE),0) or
nvl(trim(T.WEEKS_USED_TO_CALC),0) <> nvl(trim(S.WEEKS_USED_TO_CALC),0) or
nvl(trim(T.WEEKS_PROG_ACADYR),0) <> nvl(trim(S.WEEKS_PROG_ACADYR),0) or
nvl(trim(T.LOW_TUITION_FLAG),0) <> nvl(trim(S.LOW_TUITION_FLAG),0) or
nvl(trim(T.SECONDARY_EFC_FLAG),0) <> nvl(trim(S.SECONDARY_EFC_FLAG),0) or
nvl(trim(T.PELL_SCHED_AWARD),0) <> nvl(trim(S.PELL_SCHED_AWARD),0) or
nvl(trim(T.PREV_ISIR_TXN_NBR),0) <> nvl(trim(S.PREV_ISIR_TXN_NBR),0) or
nvl(trim(T.PREV_PELL_EFC),0) <> nvl(trim(S.PREV_PELL_EFC),0) or
nvl(trim(T.PREV_SEC_EFC_FLAG),0) <> nvl(trim(S.PREV_SEC_EFC_FLAG),0) or
nvl(trim(T.PREV_PELL_BDGT_COA),0) <> nvl(trim(S.PREV_PELL_BDGT_COA),0) or
nvl(trim(T.PG_ED_USE_FLAG_1),0) <> nvl(trim(S.PG_ED_USE_FLAG_1),0) or
nvl(trim(T.PG_ED_USE_FLAG_2),0) <> nvl(trim(S.PG_ED_USE_FLAG_2),0) or
nvl(trim(T.PG_ED_USE_FLAG_3),0) <> nvl(trim(S.PG_ED_USE_FLAG_3),0) or
nvl(trim(T.PG_ED_USE_FLAG_4),0) <> nvl(trim(S.PG_ED_USE_FLAG_4),0) or
nvl(trim(T.PG_ED_USE_FLAG_5),0) <> nvl(trim(S.PG_ED_USE_FLAG_5),0) or
nvl(trim(T.PG_ED_USE_FLAG_6),0) <> nvl(trim(S.PG_ED_USE_FLAG_6),0) or
nvl(trim(T.PG_ED_USE_FLAG_7),0) <> nvl(trim(S.PG_ED_USE_FLAG_7),0) or
nvl(trim(T.PG_ED_USE_FLAG_8),0) <> nvl(trim(S.PG_ED_USE_FLAG_8),0) or
nvl(trim(T.PG_ED_USE_FLAG_9),0) <> nvl(trim(S.PG_ED_USE_FLAG_9),0) or
nvl(trim(T.PG_ED_USE_FLAG_10),0) <> nvl(trim(S.PG_ED_USE_FLAG_10),0) or
nvl(trim(T.PELL_MANUAL_OVRD),0) <> nvl(trim(S.PELL_MANUAL_OVRD),0) or
nvl(trim(T.SFA_ADDL_PELL_ELIG),0) <> nvl(trim(S.SFA_ADDL_PELL_ELIG),0) or
nvl(trim(T.SFA_COD_CITZN_STAT),0) <> nvl(trim(S.SFA_COD_CITZN_STAT),0) or
nvl(trim(T.SFA_ATB_CD),0) <> nvl(trim(S.SFA_ATB_CD),0) or
nvl(trim(T.SFA_ATB_TST_ADM_CD),0) <> nvl(trim(S.SFA_ATB_TST_ADM_CD),0) or
nvl(trim(T.SFA_ATB_TST_CD),0) <> nvl(trim(S.SFA_ATB_TST_CD),0) or
nvl(trim(T.SFA_ATB_STATE_CD),0) <> nvl(trim(S.SFA_ATB_STATE_CD),0) or
nvl(trim(T.SFA_ATB_COMP_DT),0) <> nvl(trim(S.SFA_ATB_COMP_DT),0) or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.INSTITUTION,
T.AID_YEAR,
T.PELL_ORIG_ID,
T.PELL_ORIG_SEQ_NBR,
T.SRC_SYS_ID,
T.ORIG_SSN,
T.NAME_CD,
T.ISIR_TXN_NBR,
T.PELL_EFC,
T.PELL_ID_ATTENDED,
T.PELL_BDGT_COA,
T.VERIF_STATUS_CODE,
T.ACADEMIC_CALENDAR,
T.PELL_ENROLL_STAT,
T.HOURS_CRD_PD,
T.HRS_CREDITS_ACADYR,
T.PELL_AWARD_AMT,
T.PELL_PMT_METH,
T.PELL_PAY_PERIODS,
T.INCARCERATED_CODE,
T.PELL_ENRLMNT_DT,
T.SSN,
T.ACTION_CODE,
T.WEEKS_USED_TO_CALC,
T.WEEKS_PROG_ACADYR,
T.LOW_TUITION_FLAG,
T.SECONDARY_EFC_FLAG,
T.PELL_SCHED_AWARD,
T.PREV_ISIR_TXN_NBR,
T.PREV_PELL_EFC,
T.PREV_SEC_EFC_FLAG,
T.PREV_PELL_BDGT_COA,
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
T.PELL_MANUAL_OVRD,
T.SFA_ADDL_PELL_ELIG,
T.SFA_COD_CITZN_STAT,
T.SFA_ATB_CD,
T.SFA_ATB_TST_ADM_CD,
T.SFA_ATB_TST_CD,
T.SFA_ATB_STATE_CD,
T.SFA_ATB_COMP_DT,
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
S.PELL_ORIG_SEQ_NBR,
'CS90',
S.ORIG_SSN,
S.NAME_CD,
S.ISIR_TXN_NBR,
S.PELL_EFC,
S.PELL_ID_ATTENDED,
S.PELL_BDGT_COA,
S.VERIF_STATUS_CODE,
S.ACADEMIC_CALENDAR,
S.PELL_ENROLL_STAT,
S.HOURS_CRD_PD,
S.HRS_CREDITS_ACADYR,
S.PELL_AWARD_AMT,
S.PELL_PMT_METH,
S.PELL_PAY_PERIODS,
S.INCARCERATED_CODE,
S.PELL_ENRLMNT_DT,
S.SSN,
S.ACTION_CODE,
S.WEEKS_USED_TO_CALC,
S.WEEKS_PROG_ACADYR,
S.LOW_TUITION_FLAG,
S.SECONDARY_EFC_FLAG,
S.PELL_SCHED_AWARD,
S.PREV_ISIR_TXN_NBR,
S.PREV_PELL_EFC,
S.PREV_SEC_EFC_FLAG,
S.PREV_PELL_BDGT_COA,
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
S.PELL_MANUAL_OVRD,
S.SFA_ADDL_PELL_ELIG,
S.SFA_COD_CITZN_STAT,
S.SFA_ATB_CD,
S.SFA_ATB_TST_ADM_CD,
S.SFA_ATB_TST_CD,
S.SFA_ATB_STATE_CD,
S.SFA_ATB_COMP_DT,
'N',
'S',
sysdate,
sysdate,
1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_PELL_ORIG_DTL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PELL_ORIG_DTL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_PELL_ORIG_DTL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_PELL_ORIG_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_PELL_ORIG_DTL';
update CSSTG_OWNER.PS_PELL_ORIG_DTL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, PELL_ORIG_ID, PELL_ORIG_SEQ_NBR
   from CSSTG_OWNER.PS_PELL_ORIG_DTL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PELL_ORIG_DTL') = 'Y'
  minus
 select EMPLID, INSTITUTION, AID_YEAR, PELL_ORIG_ID, PELL_ORIG_SEQ_NBR
   from SYSADM.PS_PELL_ORIG_DTL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PELL_ORIG_DTL') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.PELL_ORIG_ID = S.PELL_ORIG_ID
   and T.PELL_ORIG_SEQ_NBR = S.PELL_ORIG_SEQ_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_PELL_ORIG_DTL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PELL_ORIG_DTL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_PELL_ORIG_DTL'
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

END PS_PELL_ORIG_DTL_P;
/
