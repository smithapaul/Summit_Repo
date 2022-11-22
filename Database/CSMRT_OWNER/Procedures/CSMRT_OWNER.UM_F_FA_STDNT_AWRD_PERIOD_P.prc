DROP PROCEDURE CSMRT_OWNER.UM_F_FA_STDNT_AWRD_PERIOD_P
/

--
-- UM_F_FA_STDNT_AWRD_PERIOD_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_FA_STDNT_AWRD_PERIOD_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_FA_STDNT_AWRD_PERIOD.
--
--V01   SMT-xxxx 07/26/2018,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_AWRD_PERIOD';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_AWRD_PERIOD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_AWRD_PERIOD';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_AWRD_PERIOD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_AWRD_PERIOD');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_AWRD_PERIOD disable constraint PK_UM_F_FA_STDNT_AWRD_PERIOD';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_AWRD_PERIOD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_AWRD_PERIOD';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_FA_STDNT_AWRD_PERIOD
  with X as (
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D')
SELECT /*+ PARALLEL(8) INLINE */
       A.INSTITUTION   INSTITUTION_CD,
       A.AID_YEAR,
       A.EMPLID PERSON_ID,
       A.AWARD_PERIOD,
       A.SRC_SYS_ID,
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       BASE_WEEKS,
       BDGT_DURATION_FED,
       BDGT_DURATION_INST,
       EFC_STATUS,
       X1.XLATLONGNAME EFC_STATUS_LD,
       FANLTR_STATUS,
       X2.XLATLONGNAME FANLTR_STATUS_LD,
       FANLTR_STATUS_PREH,
       FED_EFC,
       FED_NEED,
       FED_NEED_BASE_AID,
       FED_OVRAWD_AMT,
       FED_OVRAWD_COA,
       FED_PARENT_CONTRB,
       FED_SPECIAL_AID,
       FED_STDNT_CONTRB,
       FED_TOTAL_AID,
       FED_UNMET_COA,
       FED_UNMET_NEED,
       FED_YEAR_COA,
       ISIR_CALC_EFC,
       ISIR_CALC_SC,
       ISIR_CALC_PC,
       INST_CALC_EFC,
       INST_CALC_SC,
       INST_CALC_PC,
       INST_EFC,
       INST_EFC_OVERIDE,
       INST_NEED,
       INST_NEED_BASE_AID,
       INST_OVRAWD_AMT,
       INST_OVRAWD_COA,
       INST_PARENT_CONTRB,
       INST_SPECIAL_AID,
       INST_STDNT_CONTRB,
       INST_TOTAL_AID,
       INST_UNMET_COA,
       INST_UNMET_NEED,
       INST_YEAR_COA,
       PELL_YEAR_COA,
       PRORATED_EFC,
       PRORATED_PAR_CNTRB,
       PRORATED_STU_CNTRB,
       SFA_PELLYR_COA_LHT,
       VET_ED_BENEFIT,
       VET_ED_FAN_PRINT,
       WEEKS_ENROLLED,
       WEEKLY_PC,
       WEEKLY_SC,
       COMMENTS,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID
   FROM CSSTG_OWNER.PS_STDNT_AWD_PER A
   left outer join X X1
     on A.EFC_STATUS = X1.FIELDVALUE
    and A.SRC_SYS_ID = X1.SRC_SYS_ID
    and X1.FIELDNAME = 'EFC_STATUS'
    and X1.X_ORDER = 1
   left outer join X X2
     on A.FANLTR_STATUS = X2.FIELDVALUE
    and A.SRC_SYS_ID = X2.SRC_SYS_ID
    and X2.FIELDNAME = 'FANLTR_STATUS'
    and X2.X_ORDER = 1
   left outer join CSMRT_OWNER.PS_D_INSTITUTION I
    on A.INSTITUTION = I.INSTITUTION_CD
   and A.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on A.EMPLID = P.PERSON_ID
   and A.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  WHERE A.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_AWRD_PERIOD rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_AWRD_PERIOD',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_AWRD_PERIOD',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_AWRD_PERIOD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_AWRD_PERIOD enable constraint PK_UM_F_FA_STDNT_AWRD_PERIOD';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_AWRD_PERIOD');

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

END UM_F_FA_STDNT_AWRD_PERIOD_P;
/
