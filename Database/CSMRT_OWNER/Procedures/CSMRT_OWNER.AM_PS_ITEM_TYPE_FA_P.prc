DROP PROCEDURE CSMRT_OWNER.AM_PS_ITEM_TYPE_FA_P
/

--
-- AM_PS_ITEM_TYPE_FA_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_ITEM_TYPE_FA_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ITEM_TYPE_FA from PeopleSoft table PS_ITEM_TYPE_FA.
--
-- V01  SMT-xxxx 04/18/2017,    Jim Doucette
--                              Converted from PS_ITEM_TYPE_FA.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_ITEM_TYPE_FA';
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
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_ITEM_TYPE_FA'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ITEM_TYPE_FA@AMSOURCE S)
 where TABLE_NAME = 'PS_ITEM_TYPE_FA'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_ITEM_TYPE_FA';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_ITEM_TYPE_FA'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into AMSTG_OWNER.PS_T_ITEM_TYPE_FA
select /*+ full(S) */
nvl(trim(SETID),'-') SETID,
nvl(trim(ITEM_TYPE),'-') ITEM_TYPE,
nvl(trim(AID_YEAR),'-') AID_YEAR,
nvl(EFFDT, to_date('01-JAN-1900')) EFFDT,
'CS90' SRC_SYS_ID,
EFF_STATUS,
DESCR,
DESCRSHORT,
PAY_ACROSS_TERMS,
FA_SOURCE,
FEDERAL_ID,
LOAN_PROGRAM,
TITLE_IV,
AWARD_LETTER_PRINT,
PRINT_LTR_OPTION,
FAN_PRINT_SPECS,
FIN_AID_TYPE,
FED_OR_INST,
PACKAGE_LIMIT_RULE,
NEED_BASED,
MEET_NEED_COST,
DISBURSE_METHOD,
AGGREGATE_AREA,
FA_ROUND_OPTION,
ROUND_DIRECTION,
REMAINDER_RULE,
FEE_REMAINDER_RULE,
TRUNCATE_FEE,
LOAN_INTEREST_ATTR,
PASS_ANTICIP_AID,
ANTCP_AID_EXP_DAYS,
PACKAGING_FEED,
EQUITY_AWARD,
INST_OVRAWRD_RULE,
FED_OVRAWRD_RULE,
OVRAWD_TOLERANCE,
REPORT_CODE,
AWARD_MSG_CD,
SIGNATURE_RELEASE,
AUTO_CANCEL_TYPE,
MANUAL_AUTH_IND,
LINE_REASON_CD,
PARENT_ITEM_TYPE,
SELECTION_CRITERIA,
METHODOLOGY,
SELF_HELP_AWARD,
LOCK_AWARD_FLAG,
AWARD_PERIOD,
ONE_INSTANCE_ITEM,
MISSING_TERM_SPLIT,
DISB_PROTECTION,
SEQUENCE_OVERRIDE,
INCLUDE_IN_TSCRPT,
FA_SS_ACCEPT,
FA_SS_DECLINE,
FA_SS_REDUX,
SFA_MPN_REQUIRED,
SFA_LNTRANSIT_FLAG,
SFA_NO_RPKG_FLAG,
SFA_ASG_ACAD_LEVEL,
SFA_ADDL_PELL_ELIG,
substr(to_char(trim(DESCRLONG)),1,4000) DESCRLONG,
to_number(ORA_ROWSCN) SRC_SCN
  from SYSADM.PS_ITEM_TYPE_FA@AMSOURCE S
;
strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_ITEM_TYPE_FA'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_ITEM_TYPE_FA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_ITEM_TYPE_FA';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_ITEM_TYPE_FA T
using (select /*+ full(S) */
nvl(trim(SETID),'-') SETID,
nvl(trim(ITEM_TYPE),'-') ITEM_TYPE,
nvl(trim(AID_YEAR),'-') AID_YEAR,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(EFF_STATUS),'-') EFF_STATUS,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(DESCRSHORT),'-') DESCRSHORT,
nvl(trim(PAY_ACROSS_TERMS),'-') PAY_ACROSS_TERMS,
nvl(trim(FA_SOURCE),'-') FA_SOURCE,
nvl(trim(FEDERAL_ID),'-') FEDERAL_ID,
nvl(trim(LOAN_PROGRAM),'-') LOAN_PROGRAM,
nvl(trim(TITLE_IV),'-') TITLE_IV,
nvl(trim(AWARD_LETTER_PRINT),'-') AWARD_LETTER_PRINT,
nvl(trim(PRINT_LTR_OPTION),'-') PRINT_LTR_OPTION,
nvl(trim(FAN_PRINT_SPECS),'-') FAN_PRINT_SPECS,
nvl(trim(FIN_AID_TYPE),'-') FIN_AID_TYPE,
nvl(trim(FED_OR_INST),'-') FED_OR_INST,
nvl(trim(PACKAGE_LIMIT_RULE),'-') PACKAGE_LIMIT_RULE,
nvl(trim(NEED_BASED),'-') NEED_BASED,
nvl(trim(MEET_NEED_COST),'-') MEET_NEED_COST,
nvl(trim(DISBURSE_METHOD),'-') DISBURSE_METHOD,
nvl(trim(AGGREGATE_AREA),'-') AGGREGATE_AREA,
nvl(trim(FA_ROUND_OPTION),'-') FA_ROUND_OPTION,
nvl(trim(ROUND_DIRECTION),'-') ROUND_DIRECTION,
nvl(trim(REMAINDER_RULE),'-') REMAINDER_RULE,
nvl(trim(FEE_REMAINDER_RULE),'-') FEE_REMAINDER_RULE,
nvl(trim(TRUNCATE_FEE),'-') TRUNCATE_FEE,
nvl(trim(LOAN_INTEREST_ATTR),'-') LOAN_INTEREST_ATTR,
nvl(trim(PASS_ANTICIP_AID),'-') PASS_ANTICIP_AID,
nvl(ANTCP_AID_EXP_DAYS,0) ANTCP_AID_EXP_DAYS,
nvl(trim(PACKAGING_FEED),'-') PACKAGING_FEED,
nvl(trim(EQUITY_AWARD),'-') EQUITY_AWARD,
nvl(trim(INST_OVRAWRD_RULE),'-') INST_OVRAWRD_RULE,
nvl(trim(FED_OVRAWRD_RULE),'-') FED_OVRAWRD_RULE,
nvl(OVRAWD_TOLERANCE,0) OVRAWD_TOLERANCE,
nvl(trim(REPORT_CODE),'-') REPORT_CODE,
nvl(trim(AWARD_MSG_CD),'-') AWARD_MSG_CD,
nvl(trim(SIGNATURE_RELEASE),'-') SIGNATURE_RELEASE,
nvl(trim(AUTO_CANCEL_TYPE),'-') AUTO_CANCEL_TYPE,
nvl(trim(MANUAL_AUTH_IND),'-') MANUAL_AUTH_IND,
nvl(trim(LINE_REASON_CD),'-') LINE_REASON_CD,
nvl(trim(PARENT_ITEM_TYPE),'-') PARENT_ITEM_TYPE,
nvl(trim(SELECTION_CRITERIA),'-') SELECTION_CRITERIA,
nvl(trim(METHODOLOGY),'-') METHODOLOGY,
nvl(trim(SELF_HELP_AWARD),'-') SELF_HELP_AWARD,
nvl(trim(LOCK_AWARD_FLAG),'-') LOCK_AWARD_FLAG,
nvl(trim(AWARD_PERIOD),'-') AWARD_PERIOD,
nvl(trim(ONE_INSTANCE_ITEM),'-') ONE_INSTANCE_ITEM,
nvl(trim(MISSING_TERM_SPLIT),'-') MISSING_TERM_SPLIT,
nvl(trim(DISB_PROTECTION),'-') DISB_PROTECTION,
nvl(trim(SEQUENCE_OVERRIDE),'-') SEQUENCE_OVERRIDE,
nvl(trim(INCLUDE_IN_TSCRPT),'-') INCLUDE_IN_TSCRPT,
nvl(trim(FA_SS_ACCEPT),'-') FA_SS_ACCEPT,
nvl(trim(FA_SS_DECLINE),'-') FA_SS_DECLINE,
nvl(trim(FA_SS_REDUX),'-') FA_SS_REDUX,
nvl(trim(SFA_MPN_REQUIRED),'-') SFA_MPN_REQUIRED,
SFA_LNTRANSIT_FLAG SFA_LNTRANSIT_FLAG,
SFA_NO_RPKG_FLAG SFA_NO_RPKG_FLAG,
SFA_ASG_ACAD_LEVEL SFA_ASG_ACAD_LEVEL,
SFA_ADDL_PELL_ELIG SFA_ADDL_PELL_ELIG,
DESCRLONG DESCRLONG
from AMSTG_OWNER.PS_T_ITEM_TYPE_FA S
where SRC_SCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ITEM_TYPE_FA') ) S
   on (
T.SETID = S.SETID and
T.ITEM_TYPE = S.ITEM_TYPE and
T.AID_YEAR = S.AID_YEAR and
T.EFFDT = S.EFFDT and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.EFF_STATUS = S.EFF_STATUS,
T.DESCR = S.DESCR,
T.DESCRSHORT = S.DESCRSHORT,
T.PAY_ACROSS_TERMS = S.PAY_ACROSS_TERMS,
T.FA_SOURCE = S.FA_SOURCE,
T.FEDERAL_ID = S.FEDERAL_ID,
T.LOAN_PROGRAM = S.LOAN_PROGRAM,
T.TITLE_IV = S.TITLE_IV,
T.AWARD_LETTER_PRINT = S.AWARD_LETTER_PRINT,
T.PRINT_LTR_OPTION = S.PRINT_LTR_OPTION,
T.FAN_PRINT_SPECS = S.FAN_PRINT_SPECS,
T.FIN_AID_TYPE = S.FIN_AID_TYPE,
T.FED_OR_INST = S.FED_OR_INST,
T.PACKAGE_LIMIT_RULE = S.PACKAGE_LIMIT_RULE,
T.NEED_BASED = S.NEED_BASED,
T.MEET_NEED_COST = S.MEET_NEED_COST,
T.DISBURSE_METHOD = S.DISBURSE_METHOD,
T.AGGREGATE_AREA = S.AGGREGATE_AREA,
T.FA_ROUND_OPTION = S.FA_ROUND_OPTION,
T.ROUND_DIRECTION = S.ROUND_DIRECTION,
T.REMAINDER_RULE = S.REMAINDER_RULE,
T.FEE_REMAINDER_RULE = S.FEE_REMAINDER_RULE,
T.TRUNCATE_FEE = S.TRUNCATE_FEE,
T.LOAN_INTEREST_ATTR = S.LOAN_INTEREST_ATTR,
T.PASS_ANTICIP_AID = S.PASS_ANTICIP_AID,
T.ANTCP_AID_EXP_DAYS = S.ANTCP_AID_EXP_DAYS,
T.PACKAGING_FEED = S.PACKAGING_FEED,
T.EQUITY_AWARD = S.EQUITY_AWARD,
T.INST_OVRAWRD_RULE = S.INST_OVRAWRD_RULE,
T.FED_OVRAWRD_RULE = S.FED_OVRAWRD_RULE,
T.OVRAWD_TOLERANCE = S.OVRAWD_TOLERANCE,
T.REPORT_CODE = S.REPORT_CODE,
T.AWARD_MSG_CD = S.AWARD_MSG_CD,
T.SIGNATURE_RELEASE = S.SIGNATURE_RELEASE,
T.AUTO_CANCEL_TYPE = S.AUTO_CANCEL_TYPE,
T.MANUAL_AUTH_IND = S.MANUAL_AUTH_IND,
T.LINE_REASON_CD = S.LINE_REASON_CD,
T.PARENT_ITEM_TYPE = S.PARENT_ITEM_TYPE,
T.SELECTION_CRITERIA = S.SELECTION_CRITERIA,
T.METHODOLOGY = S.METHODOLOGY,
T.SELF_HELP_AWARD = S.SELF_HELP_AWARD,
T.LOCK_AWARD_FLAG = S.LOCK_AWARD_FLAG,
T.AWARD_PERIOD = S.AWARD_PERIOD,
T.ONE_INSTANCE_ITEM = S.ONE_INSTANCE_ITEM,
T.MISSING_TERM_SPLIT = S.MISSING_TERM_SPLIT,
T.DISB_PROTECTION = S.DISB_PROTECTION,
T.SEQUENCE_OVERRIDE = S.SEQUENCE_OVERRIDE,
T.INCLUDE_IN_TSCRPT = S.INCLUDE_IN_TSCRPT,
T.FA_SS_ACCEPT = S.FA_SS_ACCEPT,
T.FA_SS_DECLINE = S.FA_SS_DECLINE,
T.FA_SS_REDUX = S.FA_SS_REDUX,
T.SFA_MPN_REQUIRED = S.SFA_MPN_REQUIRED,
T.SFA_LNTRANSIT_FLAG = S.SFA_LNTRANSIT_FLAG,
T.SFA_NO_RPKG_FLAG = S.SFA_NO_RPKG_FLAG,
T.SFA_ASG_ACAD_LEVEL = S.SFA_ASG_ACAD_LEVEL,
T.SFA_ADDL_PELL_ELIG = S.SFA_ADDL_PELL_ELIG,
T.DESCRLONG = S.DESCRLONG,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.EFF_STATUS <> S.EFF_STATUS or
T.DESCR <> S.DESCR or
T.DESCRSHORT <> S.DESCRSHORT or
T.PAY_ACROSS_TERMS <> S.PAY_ACROSS_TERMS or
T.FA_SOURCE <> S.FA_SOURCE or
T.FEDERAL_ID <> S.FEDERAL_ID or
T.LOAN_PROGRAM <> S.LOAN_PROGRAM or
T.TITLE_IV <> S.TITLE_IV or
T.AWARD_LETTER_PRINT <> S.AWARD_LETTER_PRINT or
T.PRINT_LTR_OPTION <> S.PRINT_LTR_OPTION or
T.FAN_PRINT_SPECS <> S.FAN_PRINT_SPECS or
T.FIN_AID_TYPE <> S.FIN_AID_TYPE or
T.FED_OR_INST <> S.FED_OR_INST or
T.PACKAGE_LIMIT_RULE <> S.PACKAGE_LIMIT_RULE or
T.NEED_BASED <> S.NEED_BASED or
T.MEET_NEED_COST <> S.MEET_NEED_COST or
T.DISBURSE_METHOD <> S.DISBURSE_METHOD or
T.AGGREGATE_AREA <> S.AGGREGATE_AREA or
T.FA_ROUND_OPTION <> S.FA_ROUND_OPTION or
T.ROUND_DIRECTION <> S.ROUND_DIRECTION or
T.REMAINDER_RULE <> S.REMAINDER_RULE or
T.FEE_REMAINDER_RULE <> S.FEE_REMAINDER_RULE or
T.TRUNCATE_FEE <> S.TRUNCATE_FEE or
T.LOAN_INTEREST_ATTR <> S.LOAN_INTEREST_ATTR or
T.PASS_ANTICIP_AID <> S.PASS_ANTICIP_AID or
T.ANTCP_AID_EXP_DAYS <> S.ANTCP_AID_EXP_DAYS or
T.PACKAGING_FEED <> S.PACKAGING_FEED or
T.EQUITY_AWARD <> S.EQUITY_AWARD or
T.INST_OVRAWRD_RULE <> S.INST_OVRAWRD_RULE or
T.FED_OVRAWRD_RULE <> S.FED_OVRAWRD_RULE or
T.OVRAWD_TOLERANCE <> S.OVRAWD_TOLERANCE or
T.REPORT_CODE <> S.REPORT_CODE or
T.AWARD_MSG_CD <> S.AWARD_MSG_CD or
T.SIGNATURE_RELEASE <> S.SIGNATURE_RELEASE or
T.AUTO_CANCEL_TYPE <> S.AUTO_CANCEL_TYPE or
T.MANUAL_AUTH_IND <> S.MANUAL_AUTH_IND or
T.LINE_REASON_CD <> S.LINE_REASON_CD or
T.PARENT_ITEM_TYPE <> S.PARENT_ITEM_TYPE or
T.SELECTION_CRITERIA <> S.SELECTION_CRITERIA or
T.METHODOLOGY <> S.METHODOLOGY or
T.SELF_HELP_AWARD <> S.SELF_HELP_AWARD or
T.LOCK_AWARD_FLAG <> S.LOCK_AWARD_FLAG or
T.AWARD_PERIOD <> S.AWARD_PERIOD or
T.ONE_INSTANCE_ITEM <> S.ONE_INSTANCE_ITEM or
T.MISSING_TERM_SPLIT <> S.MISSING_TERM_SPLIT or
T.DISB_PROTECTION <> S.DISB_PROTECTION or
T.SEQUENCE_OVERRIDE <> S.SEQUENCE_OVERRIDE or
T.INCLUDE_IN_TSCRPT <> S.INCLUDE_IN_TSCRPT or
T.FA_SS_ACCEPT <> S.FA_SS_ACCEPT or
T.FA_SS_DECLINE <> S.FA_SS_DECLINE or
T.FA_SS_REDUX <> S.FA_SS_REDUX or
T.SFA_MPN_REQUIRED <> S.SFA_MPN_REQUIRED or
nvl(trim(T.SFA_LNTRANSIT_FLAG),0) <> nvl(trim(S.SFA_LNTRANSIT_FLAG),0) or
nvl(trim(T.SFA_NO_RPKG_FLAG),0) <> nvl(trim(S.SFA_NO_RPKG_FLAG),0) or
nvl(trim(T.SFA_ASG_ACAD_LEVEL),0) <> nvl(trim(S.SFA_ASG_ACAD_LEVEL),0) or
nvl(trim(T.SFA_ADDL_PELL_ELIG),0) <> nvl(trim(S.SFA_ADDL_PELL_ELIG),0) or
nvl(trim(T.DESCRLONG),0) <> nvl(trim(S.DESCRLONG),0) or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.SETID,
T.ITEM_TYPE,
T.AID_YEAR,
T.EFFDT,
T.SRC_SYS_ID,
T.EFF_STATUS,
T.DESCR,
T.DESCRSHORT,
T.PAY_ACROSS_TERMS,
T.FA_SOURCE,
T.FEDERAL_ID,
T.LOAN_PROGRAM,
T.TITLE_IV,
T.AWARD_LETTER_PRINT,
T.PRINT_LTR_OPTION,
T.FAN_PRINT_SPECS,
T.FIN_AID_TYPE,
T.FED_OR_INST,
T.PACKAGE_LIMIT_RULE,
T.NEED_BASED,
T.MEET_NEED_COST,
T.DISBURSE_METHOD,
T.AGGREGATE_AREA,
T.FA_ROUND_OPTION,
T.ROUND_DIRECTION,
T.REMAINDER_RULE,
T.FEE_REMAINDER_RULE,
T.TRUNCATE_FEE,
T.LOAN_INTEREST_ATTR,
T.PASS_ANTICIP_AID,
T.ANTCP_AID_EXP_DAYS,
T.PACKAGING_FEED,
T.EQUITY_AWARD,
T.INST_OVRAWRD_RULE,
T.FED_OVRAWRD_RULE,
T.OVRAWD_TOLERANCE,
T.REPORT_CODE,
T.AWARD_MSG_CD,
T.SIGNATURE_RELEASE,
T.AUTO_CANCEL_TYPE,
T.MANUAL_AUTH_IND,
T.LINE_REASON_CD,
T.PARENT_ITEM_TYPE,
T.SELECTION_CRITERIA,
T.METHODOLOGY,
T.SELF_HELP_AWARD,
T.LOCK_AWARD_FLAG,
T.AWARD_PERIOD,
T.ONE_INSTANCE_ITEM,
T.MISSING_TERM_SPLIT,
T.DISB_PROTECTION,
T.SEQUENCE_OVERRIDE,
T.INCLUDE_IN_TSCRPT,
T.FA_SS_ACCEPT,
T.FA_SS_DECLINE,
T.FA_SS_REDUX,
T.SFA_MPN_REQUIRED,
T.SFA_LNTRANSIT_FLAG,
T.SFA_NO_RPKG_FLAG,
T.SFA_ASG_ACAD_LEVEL,
T.SFA_ADDL_PELL_ELIG,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID,
T.DESCRLONG
)
values (
S.SETID,
S.ITEM_TYPE,
S.AID_YEAR,
S.EFFDT,
'CS90',
S.EFF_STATUS,
S.DESCR,
S.DESCRSHORT,
S.PAY_ACROSS_TERMS,
S.FA_SOURCE,
S.FEDERAL_ID,
S.LOAN_PROGRAM,
S.TITLE_IV,
S.AWARD_LETTER_PRINT,
S.PRINT_LTR_OPTION,
S.FAN_PRINT_SPECS,
S.FIN_AID_TYPE,
S.FED_OR_INST,
S.PACKAGE_LIMIT_RULE,
S.NEED_BASED,
S.MEET_NEED_COST,
S.DISBURSE_METHOD,
S.AGGREGATE_AREA,
S.FA_ROUND_OPTION,
S.ROUND_DIRECTION,
S.REMAINDER_RULE,
S.FEE_REMAINDER_RULE,
S.TRUNCATE_FEE,
S.LOAN_INTEREST_ATTR,
S.PASS_ANTICIP_AID,
S.ANTCP_AID_EXP_DAYS,
S.PACKAGING_FEED,
S.EQUITY_AWARD,
S.INST_OVRAWRD_RULE,
S.FED_OVRAWRD_RULE,
S.OVRAWD_TOLERANCE,
S.REPORT_CODE,
S.AWARD_MSG_CD,
S.SIGNATURE_RELEASE,
S.AUTO_CANCEL_TYPE,
S.MANUAL_AUTH_IND,
S.LINE_REASON_CD,
S.PARENT_ITEM_TYPE,
S.SELECTION_CRITERIA,
S.METHODOLOGY,
S.SELF_HELP_AWARD,
S.LOCK_AWARD_FLAG,
S.AWARD_PERIOD,
S.ONE_INSTANCE_ITEM,
S.MISSING_TERM_SPLIT,
S.DISB_PROTECTION,
S.SEQUENCE_OVERRIDE,
S.INCLUDE_IN_TSCRPT,
S.FA_SS_ACCEPT,
S.FA_SS_DECLINE,
S.FA_SS_REDUX,
S.SFA_MPN_REQUIRED,
S.SFA_LNTRANSIT_FLAG,
S.SFA_NO_RPKG_FLAG,
S.SFA_ASG_ACAD_LEVEL,
S.SFA_ADDL_PELL_ELIG,
'N',
'S',
sysdate,
sysdate,
1234,
S.DESCRLONG);



strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_ITEM_TYPE_FA rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ITEM_TYPE_FA',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ITEM_TYPE_FA';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_ITEM_TYPE_FA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_ITEM_TYPE_FA';
update AMSTG_OWNER.PS_ITEM_TYPE_FA T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, ITEM_TYPE, AID_YEAR, EFFDT
   from AMSTG_OWNER.PS_ITEM_TYPE_FA T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ITEM_TYPE_FA') = 'Y'
  minus
 select nvl(trim(SETID),'-'), nvl(trim(ITEM_TYPE),'-'), nvl(trim(AID_YEAR),'-'), EFFDT
   from SYSADM.PS_ITEM_TYPE_FA@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ITEM_TYPE_FA') = 'Y'
   ) S
 where T.SETID = S.SETID
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.AID_YEAR = S.AID_YEAR
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ITEM_TYPE_FA rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ITEM_TYPE_FA',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ITEM_TYPE_FA'
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

END AM_PS_ITEM_TYPE_FA_P;
/
