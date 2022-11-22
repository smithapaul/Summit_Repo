DROP PROCEDURE CSMRT_OWNER.AM_PS_ISIR_COMPUTED_P
/

--
-- AM_PS_ISIR_COMPUTED_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_ISIR_COMPUTED_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ISIR_COMPUTED from PeopleSoft table PS_ISIR_COMPUTED.
--
-- V01  SMT-xxxx 04/11/2017,    Jim Doucette
--                              Converted from PS_ISIR_COMPUTED.SQL
--VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_ISIR_COMPUTED';
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
 where TABLE_NAME = 'PS_ISIR_COMPUTED'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ISIR_COMPUTED@AMSOURCE S)
 where TABLE_NAME = 'PS_ISIR_COMPUTED'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_ISIR_COMPUTED';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_ISIR_COMPUTED';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_ISIR_COMPUTED T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(AID_YEAR),'-') AID_YEAR, 
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL 
               else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
    nvl(EFFSEQ,0) EFFSEQ, 
    nvl(trim(TABLE_ID),'-') TABLE_ID, 
    nvl(PRIMARY_EFC,0) PRIMARY_EFC, 
    nvl(SECONDARY_EFC,0) SECONDARY_EFC, 
    nvl(PELL_SPCL_PRM_EFC,0) PELL_SPCL_PRM_EFC, 
    nvl(PELL_SPCL_SEC_EFC,0) PELL_SPCL_SEC_EFC, 
    nvl(trim(PELL_PAID_EFC_TYPE),'-') PELL_PAID_EFC_TYPE, 
    nvl(trim(VALID_EFC_CALC),'-') VALID_EFC_CALC, 
    nvl(trim(AUTO_ZERO_EFC),'-') AUTO_ZERO_EFC, 
    nvl(trim(FORMULA_TYPE),'-') FORMULA_TYPE, 
    nvl(trim(PRM_ALT_MONTH_1),'-') PRM_ALT_MONTH_1, 
    nvl(trim(PRM_ALT_MONTH_2),'-') PRM_ALT_MONTH_2, 
    nvl(trim(PRM_ALT_MONTH_3),'-') PRM_ALT_MONTH_3, 
    nvl(trim(PRM_ALT_MONTH_4),'-') PRM_ALT_MONTH_4, 
    nvl(trim(PRM_ALT_MONTH_5),'-') PRM_ALT_MONTH_5, 
    nvl(trim(PRM_ALT_MONTH_6),'-') PRM_ALT_MONTH_6, 
    nvl(trim(PRM_ALT_MONTH_7),'-') PRM_ALT_MONTH_7, 
    nvl(trim(PRM_ALT_MONTH_8),'-') PRM_ALT_MONTH_8, 
    nvl(trim(PRM_ALT_MONTH_10),'-') PRM_ALT_MONTH_10, 
    nvl(trim(PRM_ALT_MONTH_11),'-') PRM_ALT_MONTH_11, 
    nvl(trim(PRM_ALT_MONTH_12),'-') PRM_ALT_MONTH_12, 
    nvl(trim(SEC_ALT_MONTH_1),'-') SEC_ALT_MONTH_1, 
    nvl(trim(SEC_ALT_MONTH_2),'-') SEC_ALT_MONTH_2, 
    nvl(trim(SEC_ALT_MONTH_3),'-') SEC_ALT_MONTH_3, 
    nvl(trim(SEC_ALT_MONTH_4),'-') SEC_ALT_MONTH_4, 
    nvl(trim(SEC_ALT_MONTH_5),'-') SEC_ALT_MONTH_5, 
    nvl(trim(SEC_ALT_MONTH_6),'-') SEC_ALT_MONTH_6, 
    nvl(trim(SEC_ALT_MONTH_7),'-') SEC_ALT_MONTH_7, 
    nvl(trim(SEC_ALT_MONTH_8),'-') SEC_ALT_MONTH_8, 
    nvl(trim(SEC_ALT_MONTH_10),'-') SEC_ALT_MONTH_10, 
    nvl(trim(SEC_ALT_MONTH_11),'-') SEC_ALT_MONTH_11, 
    nvl(trim(SEC_ALT_MONTH_12),'-') SEC_ALT_MONTH_12, 
    nvl(TOTAL_INCOME,0) TOTAL_INCOME, 
    nvl(ALWNC_AGAINST_TI,0) ALWNC_AGAINST_TI, 
    nvl(STATE_TAX_ALWNC,0) STATE_TAX_ALWNC, 
    nvl(EMPLOYMENT_ALWNC,0) EMPLOYMENT_ALWNC, 
    nvl(INC_PROTECTN_ALWNC,0) INC_PROTECTN_ALWNC, 
    nvl(AVAILABLE_INCOME,0) AVAILABLE_INCOME, 
    nvl(DESCRTN_NET_WORTH,0) DESCRTN_NET_WORTH, 
    nvl(AST_PROTECTN_ALWNC,0) AST_PROTECTN_ALWNC, 
    nvl(CONTRIB_FROM_ASSET,0) CONTRIB_FROM_ASSET, 
    nvl(ADJ_AVAILABLE_INC,0) ADJ_AVAILABLE_INC, 
    nvl(TOTAL_PAR_CONTRIB,0) TOTAL_PAR_CONTRIB, 
    nvl(TOTAL_STU_CONTRIB,0) TOTAL_STU_CONTRIB, 
    nvl(ADJ_PAR_CONTRIB,0) ADJ_PAR_CONTRIB, 
    nvl(DEP_STU_I_CONTRIB,0) DEP_STU_I_CONTRIB, 
    nvl(DEP_STU_A_CONTRIB,0) DEP_STU_A_CONTRIB, 
    nvl(STU_TOTAL_INC,0) STU_TOTAL_INC, 
    nvl(CONTRIB_AVAIL_INC,0) CONTRIB_AVAIL_INC, 
    nvl(FICA_TAX_PD,0) FICA_TAX_PD, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    to_date(to_char(case when EFC_CALC_DT < '01-JAN-1800' then NULL 
                    else EFC_CALC_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFC_CALC_DT, 
    nvl(BUDGET_DURATION,0) BUDGET_DURATION, 
    nvl(ADJ_EFC_AMT,0) ADJ_EFC_AMT, 
    nvl(WEEKLY_PC,0) WEEKLY_PC, 
    nvl(WEEKLY_SC,0) WEEKLY_SC, 
    nvl(PRORATED_EFC,0) PRORATED_EFC, 
    nvl(trim(SECONDARY_EFC_TP),'-') SECONDARY_EFC_TP, 
    nvl(EFC_NET_WORTH,0) EFC_NET_WORTH, 
    nvl(STU_ALLOW_VS_TI,0) STU_ALLOW_VS_TI, 
    nvl(STU_DISC_NET_WORTH,0) STU_DISC_NET_WORTH, 
    nvl(SEC_NET_WORTH,0) SEC_NET_WORTH, 
    nvl(SEC_ALLOW_VS_TI,0) SEC_ALLOW_VS_TI, 
    nvl(SEC_SDNW,0) SEC_SDNW, 
    nvl(PAID_EFC,0) PAID_EFC, 
    nvl(ISIR_CALC_SC,0) ISIR_CALC_SC, 
    nvl(ISIR_CALC_PC,0) ISIR_CALC_PC, 
    nvl(ISIR_CALC_EFC,0) ISIR_CALC_EFC, 
    nvl(SFA_SIG_REJ_EFC,0) SFA_SIG_REJ_EFC, 
    nvl(SFA_PRIMARY_PC,0) SFA_PRIMARY_PC, 
    nvl(SFA_PRIMARY_SCA,0) SFA_PRIMARY_SCA, 
    nvl(SFA_PRIMARY_SIC,0) SFA_PRIMARY_SIC
  from SYSADM.PS_ISIR_COMPUTED@AMSOURCE S 
 where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ISIR_COMPUTED')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.INSTITUTION = S.INSTITUTION and 
    T.AID_YEAR = S.AID_YEAR and 
    T.EFFDT = S.EFFDT and 
    T.EFFSEQ = S.EFFSEQ and 
    T.TABLE_ID = S.TABLE_ID and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.PRIMARY_EFC = S.PRIMARY_EFC,
    T.SECONDARY_EFC = S.SECONDARY_EFC,
    T.PELL_SPCL_PRM_EFC = S.PELL_SPCL_PRM_EFC,
    T.PELL_SPCL_SEC_EFC = S.PELL_SPCL_SEC_EFC,
    T.PELL_PAID_EFC_TYPE = S.PELL_PAID_EFC_TYPE,
    T.VALID_EFC_CALC = S.VALID_EFC_CALC,
    T.AUTO_ZERO_EFC = S.AUTO_ZERO_EFC,
    T.FORMULA_TYPE = S.FORMULA_TYPE,
    T.PRM_ALT_MONTH_1 = S.PRM_ALT_MONTH_1,
    T.PRM_ALT_MONTH_2 = S.PRM_ALT_MONTH_2,
    T.PRM_ALT_MONTH_3 = S.PRM_ALT_MONTH_3,
    T.PRM_ALT_MONTH_4 = S.PRM_ALT_MONTH_4,
    T.PRM_ALT_MONTH_5 = S.PRM_ALT_MONTH_5,
    T.PRM_ALT_MONTH_6 = S.PRM_ALT_MONTH_6,
    T.PRM_ALT_MONTH_7 = S.PRM_ALT_MONTH_7,
    T.PRM_ALT_MONTH_8 = S.PRM_ALT_MONTH_8,
    T.PRM_ALT_MONTH_10 = S.PRM_ALT_MONTH_10,
    T.PRM_ALT_MONTH_11 = S.PRM_ALT_MONTH_11,
    T.PRM_ALT_MONTH_12 = S.PRM_ALT_MONTH_12,
    T.SEC_ALT_MONTH_1 = S.SEC_ALT_MONTH_1,
    T.SEC_ALT_MONTH_2 = S.SEC_ALT_MONTH_2,
    T.SEC_ALT_MONTH_3 = S.SEC_ALT_MONTH_3,
    T.SEC_ALT_MONTH_4 = S.SEC_ALT_MONTH_4,
    T.SEC_ALT_MONTH_5 = S.SEC_ALT_MONTH_5,
    T.SEC_ALT_MONTH_6 = S.SEC_ALT_MONTH_6,
    T.SEC_ALT_MONTH_7 = S.SEC_ALT_MONTH_7,
    T.SEC_ALT_MONTH_8 = S.SEC_ALT_MONTH_8,
    T.SEC_ALT_MONTH_10 = S.SEC_ALT_MONTH_10,
    T.SEC_ALT_MONTH_11 = S.SEC_ALT_MONTH_11,
    T.SEC_ALT_MONTH_12 = S.SEC_ALT_MONTH_12,
    T.TOTAL_INCOME = S.TOTAL_INCOME,
    T.ALWNC_AGAINST_TI = S.ALWNC_AGAINST_TI,
    T.STATE_TAX_ALWNC = S.STATE_TAX_ALWNC,
    T.EMPLOYMENT_ALWNC = S.EMPLOYMENT_ALWNC,
    T.INC_PROTECTN_ALWNC = S.INC_PROTECTN_ALWNC,
    T.AVAILABLE_INCOME = S.AVAILABLE_INCOME,
    T.DESCRTN_NET_WORTH = S.DESCRTN_NET_WORTH,
    T.AST_PROTECTN_ALWNC = S.AST_PROTECTN_ALWNC,
    T.CONTRIB_FROM_ASSET = S.CONTRIB_FROM_ASSET,
    T.ADJ_AVAILABLE_INC = S.ADJ_AVAILABLE_INC,
    T.TOTAL_PAR_CONTRIB = S.TOTAL_PAR_CONTRIB,
    T.TOTAL_STU_CONTRIB = S.TOTAL_STU_CONTRIB,
    T.ADJ_PAR_CONTRIB = S.ADJ_PAR_CONTRIB,
    T.DEP_STU_I_CONTRIB = S.DEP_STU_I_CONTRIB,
    T.DEP_STU_A_CONTRIB = S.DEP_STU_A_CONTRIB,
    T.STU_TOTAL_INC = S.STU_TOTAL_INC,
    T.CONTRIB_AVAIL_INC = S.CONTRIB_AVAIL_INC,
    T.FICA_TAX_PD = S.FICA_TAX_PD,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.EFC_CALC_DT = S.EFC_CALC_DT,
    T.BUDGET_DURATION = S.BUDGET_DURATION,
    T.ADJ_EFC_AMT = S.ADJ_EFC_AMT,
    T.WEEKLY_PC = S.WEEKLY_PC,
    T.WEEKLY_SC = S.WEEKLY_SC,
    T.PRORATED_EFC = S.PRORATED_EFC,
    T.SECONDARY_EFC_TP = S.SECONDARY_EFC_TP,
    T.EFC_NET_WORTH = S.EFC_NET_WORTH,
    T.STU_ALLOW_VS_TI = S.STU_ALLOW_VS_TI,
    T.STU_DISC_NET_WORTH = S.STU_DISC_NET_WORTH,
    T.SEC_NET_WORTH = S.SEC_NET_WORTH,
    T.SEC_ALLOW_VS_TI = S.SEC_ALLOW_VS_TI,
    T.SEC_SDNW = S.SEC_SDNW,
    T.PAID_EFC = S.PAID_EFC,
    T.ISIR_CALC_SC = S.ISIR_CALC_SC,
    T.ISIR_CALC_PC = S.ISIR_CALC_PC,
    T.ISIR_CALC_EFC = S.ISIR_CALC_EFC,
    T.SFA_SIG_REJ_EFC = S.SFA_SIG_REJ_EFC,
    T.SFA_PRIMARY_PC = S.SFA_PRIMARY_PC,
    T.SFA_PRIMARY_SCA = S.SFA_PRIMARY_SCA,
    T.SFA_PRIMARY_SIC = S.SFA_PRIMARY_SIC,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.PRIMARY_EFC <> S.PRIMARY_EFC or 
    T.SECONDARY_EFC <> S.SECONDARY_EFC or 
    T.PELL_SPCL_PRM_EFC <> S.PELL_SPCL_PRM_EFC or 
    T.PELL_SPCL_SEC_EFC <> S.PELL_SPCL_SEC_EFC or 
    T.PELL_PAID_EFC_TYPE <> S.PELL_PAID_EFC_TYPE or 
    T.VALID_EFC_CALC <> S.VALID_EFC_CALC or 
    T.AUTO_ZERO_EFC <> S.AUTO_ZERO_EFC or 
    T.FORMULA_TYPE <> S.FORMULA_TYPE or 
    T.PRM_ALT_MONTH_1 <> S.PRM_ALT_MONTH_1 or 
    T.PRM_ALT_MONTH_2 <> S.PRM_ALT_MONTH_2 or 
    T.PRM_ALT_MONTH_3 <> S.PRM_ALT_MONTH_3 or 
    T.PRM_ALT_MONTH_4 <> S.PRM_ALT_MONTH_4 or 
    T.PRM_ALT_MONTH_5 <> S.PRM_ALT_MONTH_5 or 
    T.PRM_ALT_MONTH_6 <> S.PRM_ALT_MONTH_6 or 
    T.PRM_ALT_MONTH_7 <> S.PRM_ALT_MONTH_7 or 
    T.PRM_ALT_MONTH_8 <> S.PRM_ALT_MONTH_8 or 
    T.PRM_ALT_MONTH_10 <> S.PRM_ALT_MONTH_10 or 
    T.PRM_ALT_MONTH_11 <> S.PRM_ALT_MONTH_11 or 
    T.PRM_ALT_MONTH_12 <> S.PRM_ALT_MONTH_12 or 
    T.SEC_ALT_MONTH_1 <> S.SEC_ALT_MONTH_1 or 
    T.SEC_ALT_MONTH_2 <> S.SEC_ALT_MONTH_2 or 
    T.SEC_ALT_MONTH_3 <> S.SEC_ALT_MONTH_3 or 
    T.SEC_ALT_MONTH_4 <> S.SEC_ALT_MONTH_4 or 
    T.SEC_ALT_MONTH_5 <> S.SEC_ALT_MONTH_5 or 
    T.SEC_ALT_MONTH_6 <> S.SEC_ALT_MONTH_6 or 
    T.SEC_ALT_MONTH_7 <> S.SEC_ALT_MONTH_7 or 
    T.SEC_ALT_MONTH_8 <> S.SEC_ALT_MONTH_8 or 
    T.SEC_ALT_MONTH_10 <> S.SEC_ALT_MONTH_10 or 
    T.SEC_ALT_MONTH_11 <> S.SEC_ALT_MONTH_11 or 
    T.SEC_ALT_MONTH_12 <> S.SEC_ALT_MONTH_12 or 
    T.TOTAL_INCOME <> S.TOTAL_INCOME or 
    T.ALWNC_AGAINST_TI <> S.ALWNC_AGAINST_TI or 
    T.STATE_TAX_ALWNC <> S.STATE_TAX_ALWNC or 
    T.EMPLOYMENT_ALWNC <> S.EMPLOYMENT_ALWNC or 
    T.INC_PROTECTN_ALWNC <> S.INC_PROTECTN_ALWNC or 
    T.AVAILABLE_INCOME <> S.AVAILABLE_INCOME or 
    T.DESCRTN_NET_WORTH <> S.DESCRTN_NET_WORTH or 
    T.AST_PROTECTN_ALWNC <> S.AST_PROTECTN_ALWNC or 
    T.CONTRIB_FROM_ASSET <> S.CONTRIB_FROM_ASSET or 
    T.ADJ_AVAILABLE_INC <> S.ADJ_AVAILABLE_INC or 
    T.TOTAL_PAR_CONTRIB <> S.TOTAL_PAR_CONTRIB or 
    T.TOTAL_STU_CONTRIB <> S.TOTAL_STU_CONTRIB or 
    T.ADJ_PAR_CONTRIB <> S.ADJ_PAR_CONTRIB or 
    T.DEP_STU_I_CONTRIB <> S.DEP_STU_I_CONTRIB or 
    T.DEP_STU_A_CONTRIB <> S.DEP_STU_A_CONTRIB or 
    T.STU_TOTAL_INC <> S.STU_TOTAL_INC or 
    T.CONTRIB_AVAIL_INC <> S.CONTRIB_AVAIL_INC or 
    T.FICA_TAX_PD <> S.FICA_TAX_PD or 
    T.CURRENCY_CD <> S.CURRENCY_CD or 
    nvl(trim(T.EFC_CALC_DT),0) <> nvl(trim(S.EFC_CALC_DT),0) or 
    T.BUDGET_DURATION <> S.BUDGET_DURATION or 
    T.ADJ_EFC_AMT <> S.ADJ_EFC_AMT or 
    T.WEEKLY_PC <> S.WEEKLY_PC or 
    T.WEEKLY_SC <> S.WEEKLY_SC or 
    T.PRORATED_EFC <> S.PRORATED_EFC or 
    T.SECONDARY_EFC_TP <> S.SECONDARY_EFC_TP or 
    T.EFC_NET_WORTH <> S.EFC_NET_WORTH or 
    T.STU_ALLOW_VS_TI <> S.STU_ALLOW_VS_TI or 
    T.STU_DISC_NET_WORTH <> S.STU_DISC_NET_WORTH or 
    T.SEC_NET_WORTH <> S.SEC_NET_WORTH or 
    T.SEC_ALLOW_VS_TI <> S.SEC_ALLOW_VS_TI or 
    T.SEC_SDNW <> S.SEC_SDNW or 
    T.PAID_EFC <> S.PAID_EFC or 
    T.ISIR_CALC_SC <> S.ISIR_CALC_SC or 
    T.ISIR_CALC_PC <> S.ISIR_CALC_PC or 
    T.ISIR_CALC_EFC <> S.ISIR_CALC_EFC or 
    T.SFA_SIG_REJ_EFC <> S.SFA_SIG_REJ_EFC or 
    T.SFA_PRIMARY_PC <> S.SFA_PRIMARY_PC or 
    T.SFA_PRIMARY_SCA <> S.SFA_PRIMARY_SCA or 
    T.SFA_PRIMARY_SIC <> S.SFA_PRIMARY_SIC or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.INSTITUTION,
    T.AID_YEAR, 
    T.EFFDT,
    T.EFFSEQ, 
    T.TABLE_ID, 
    T.SRC_SYS_ID, 
    T.PRIMARY_EFC,
    T.SECONDARY_EFC,
    T.PELL_SPCL_PRM_EFC,
    T.PELL_SPCL_SEC_EFC,
    T.PELL_PAID_EFC_TYPE, 
    T.VALID_EFC_CALC, 
    T.AUTO_ZERO_EFC,
    T.FORMULA_TYPE, 
    T.PRM_ALT_MONTH_1,
    T.PRM_ALT_MONTH_2,
    T.PRM_ALT_MONTH_3,
    T.PRM_ALT_MONTH_4,
    T.PRM_ALT_MONTH_5,
    T.PRM_ALT_MONTH_6,
    T.PRM_ALT_MONTH_7,
    T.PRM_ALT_MONTH_8,
    T.PRM_ALT_MONTH_10, 
    T.PRM_ALT_MONTH_11, 
    T.PRM_ALT_MONTH_12, 
    T.SEC_ALT_MONTH_1,
    T.SEC_ALT_MONTH_2,
    T.SEC_ALT_MONTH_3,
    T.SEC_ALT_MONTH_4,
    T.SEC_ALT_MONTH_5,
    T.SEC_ALT_MONTH_6,
    T.SEC_ALT_MONTH_7,
    T.SEC_ALT_MONTH_8,
    T.SEC_ALT_MONTH_10, 
    T.SEC_ALT_MONTH_11, 
    T.SEC_ALT_MONTH_12, 
    T.TOTAL_INCOME, 
    T.ALWNC_AGAINST_TI, 
    T.STATE_TAX_ALWNC,
    T.EMPLOYMENT_ALWNC, 
    T.INC_PROTECTN_ALWNC, 
    T.AVAILABLE_INCOME, 
    T.DESCRTN_NET_WORTH,
    T.AST_PROTECTN_ALWNC, 
    T.CONTRIB_FROM_ASSET, 
    T.ADJ_AVAILABLE_INC,
    T.TOTAL_PAR_CONTRIB,
    T.TOTAL_STU_CONTRIB,
    T.ADJ_PAR_CONTRIB,
    T.DEP_STU_I_CONTRIB,
    T.DEP_STU_A_CONTRIB,
    T.STU_TOTAL_INC,
    T.CONTRIB_AVAIL_INC,
    T.FICA_TAX_PD,
    T.CURRENCY_CD,
    T.EFC_CALC_DT,
    T.BUDGET_DURATION,
    T.ADJ_EFC_AMT,
    T.WEEKLY_PC,
    T.WEEKLY_SC,
    T.PRORATED_EFC, 
    T.SECONDARY_EFC_TP, 
    T.EFC_NET_WORTH,
    T.STU_ALLOW_VS_TI,
    T.STU_DISC_NET_WORTH, 
    T.SEC_NET_WORTH,
    T.SEC_ALLOW_VS_TI,
    T.SEC_SDNW, 
    T.PAID_EFC, 
    T.ISIR_CALC_SC, 
    T.ISIR_CALC_PC, 
    T.ISIR_CALC_EFC,
    T.SFA_SIG_REJ_EFC,
    T.SFA_PRIMARY_PC, 
    T.SFA_PRIMARY_SCA,
    T.SFA_PRIMARY_SIC,
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
    S.EFFDT,
    S.EFFSEQ, 
    S.TABLE_ID, 
    'CS90', 
    S.PRIMARY_EFC,
    S.SECONDARY_EFC,
    S.PELL_SPCL_PRM_EFC,
    S.PELL_SPCL_SEC_EFC,
    S.PELL_PAID_EFC_TYPE, 
    S.VALID_EFC_CALC, 
    S.AUTO_ZERO_EFC,
    S.FORMULA_TYPE, 
    S.PRM_ALT_MONTH_1,
    S.PRM_ALT_MONTH_2,
    S.PRM_ALT_MONTH_3,
    S.PRM_ALT_MONTH_4,
    S.PRM_ALT_MONTH_5,
    S.PRM_ALT_MONTH_6,
    S.PRM_ALT_MONTH_7,
    S.PRM_ALT_MONTH_8,
    S.PRM_ALT_MONTH_10, 
    S.PRM_ALT_MONTH_11, 
    S.PRM_ALT_MONTH_12, 
    S.SEC_ALT_MONTH_1,
    S.SEC_ALT_MONTH_2,
    S.SEC_ALT_MONTH_3,
    S.SEC_ALT_MONTH_4,
    S.SEC_ALT_MONTH_5,
    S.SEC_ALT_MONTH_6,
    S.SEC_ALT_MONTH_7,
    S.SEC_ALT_MONTH_8,
    S.SEC_ALT_MONTH_10, 
    S.SEC_ALT_MONTH_11, 
    S.SEC_ALT_MONTH_12, 
    S.TOTAL_INCOME, 
    S.ALWNC_AGAINST_TI, 
    S.STATE_TAX_ALWNC,
    S.EMPLOYMENT_ALWNC, 
    S.INC_PROTECTN_ALWNC, 
    S.AVAILABLE_INCOME, 
    S.DESCRTN_NET_WORTH,
    S.AST_PROTECTN_ALWNC, 
    S.CONTRIB_FROM_ASSET, 
    S.ADJ_AVAILABLE_INC,
    S.TOTAL_PAR_CONTRIB,
    S.TOTAL_STU_CONTRIB,
    S.ADJ_PAR_CONTRIB,
    S.DEP_STU_I_CONTRIB,
    S.DEP_STU_A_CONTRIB,
    S.STU_TOTAL_INC,
    S.CONTRIB_AVAIL_INC,
    S.FICA_TAX_PD,
    S.CURRENCY_CD,
    S.EFC_CALC_DT,
    S.BUDGET_DURATION,
    S.ADJ_EFC_AMT,
    S.WEEKLY_PC,
    S.WEEKLY_SC,
    S.PRORATED_EFC, 
    S.SECONDARY_EFC_TP, 
    S.EFC_NET_WORTH,
    S.STU_ALLOW_VS_TI,
    S.STU_DISC_NET_WORTH, 
    S.SEC_NET_WORTH,
    S.SEC_ALLOW_VS_TI,
    S.SEC_SDNW, 
    S.PAID_EFC, 
    S.ISIR_CALC_SC, 
    S.ISIR_CALC_PC, 
    S.ISIR_CALC_EFC,
    S.SFA_SIG_REJ_EFC,
    S.SFA_PRIMARY_PC, 
    S.SFA_PRIMARY_SCA,
    S.SFA_PRIMARY_SIC,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ISIR_COMPUTED rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ISIR_COMPUTED',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ISIR_COMPUTED';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_ISIR_COMPUTED';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_ISIR_COMPUTED';
update AMSTG_OWNER.PS_ISIR_COMPUTED T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, EFFDT, EFFSEQ, TABLE_ID
   from AMSTG_OWNER.PS_ISIR_COMPUTED T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ISIR_COMPUTED') = 'Y'
  minus
 select EMPLID, INSTITUTION, AID_YEAR, EFFDT, EFFSEQ, TABLE_ID
   from SYSADM.PS_ISIR_COMPUTED@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ISIR_COMPUTED') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.EFFDT = S.EFFDT
   and T.EFFSEQ = S.EFFSEQ
   and T.TABLE_ID = S.TABLE_ID
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ISIR_COMPUTED rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ISIR_COMPUTED',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ISIR_COMPUTED'
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

END AM_PS_ISIR_COMPUTED_P;
/
