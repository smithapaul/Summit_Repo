DROP PROCEDURE CSMRT_OWNER.UM_F_FA_STDNT_PELL_ORIG_P
/

--
-- UM_F_FA_STDNT_PELL_ORIG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_FA_STDNT_PELL_ORIG_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_FA_STDNT_PELL_ORIG.
--
 --V01  SMT-xxxx 07/06/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_PELL_ORIG';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_PELL_ORIG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_PELL_ORIG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_PELL_ORIG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_PELL_ORIG');

----alter table UM_F_FA_STDNT_PELL_ORIG disable constraint PK_UM_F_FA_STDNT_PELL_ORIG;
--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_PELL_ORIG disable constraint PK_UM_F_FA_STDNT_PELL_ORIG';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_PELL_ORIG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_PELL_ORIG';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_FA_STDNT_PELL_ORIG
with XL as (select /*+ materialize */
                   FIELDNAME, FIELDVALUE, SRC_SYS_ID, XLATLONGNAME, XLATSHORTNAME
              from UM_D_XLATITEM
             where SRC_SYS_ID = 'CS90')
select /*+ PARALLEL(8) INLINE */
       O.INSTITUTION INSTITUTION_CD,
       O.AID_YEAR,
       O.EMPLID PERSON_ID,
       O.PELL_ORIG_ID,
       D.PELL_ORIG_SEQ_NBR,
       O.SRC_SYS_ID,
       I.INSTITUTION_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       O.TIV_SCHOOL_CODE,
       O.PELL_TRANS_STAT,
       nvl(X1.XLATSHORTNAME,'-') PELL_TRANS_STAT_SD,
       nvl(X1.XLATLONGNAME,'-') PELL_TRANS_STAT_LD,
       O.PELL_TRANS_STAT_DT,
       O.UPDATE_PELL_ORG,
       O.PELL_MANUAL_OVRD,
       O.PELL_ORIG_STATUS,
       nvl(X2.XLATSHORTNAME,'-') PELL_ORIG_STATUS_SD,
       nvl(X2.XLATLONGNAME,'-') PELL_ORIG_STATUS_LD,
       O.PELL_ORIG_STAT_DT,
       O.PELL_MRR_STATUS,
       nvl(X3.XLATSHORTNAME,'-') PELL_MRR_STATUS_SD,
       nvl(X3.XLATLONGNAME,'-') PELL_MRR_STATUS_LD,
       O.PELL_MRR_STAT_DT,
       D.ORIG_SSN,
       D.NAME_CD,
       D.ISIR_TXN_NBR,
       D.PELL_EFC,
       D.PELL_ID_ATTENDED,
       D.PELL_BDGT_COA,
       D.VERIF_STATUS_CODE,
       D.PELL_ENROLL_STAT,
       D.PELL_AWARD_AMT,
       D.PELL_ENRLMNT_DT,
       D.SSN,
       D.ACTION_CODE,
       nvl(X4.XLATSHORTNAME,'-') ACTION_CODE_SD,
       nvl(X4.XLATLONGNAME,'-') ACTION_CODE_LD,
       D.PELL_SCHED_AWARD,
       D.PG_ED_USE_FLAG_1,
       D.PG_ED_USE_FLAG_2,
       D.PG_ED_USE_FLAG_3,
       D.PG_ED_USE_FLAG_4,
       D.PG_ED_USE_FLAG_5,
       D.PG_ED_USE_FLAG_6,
       D.PG_ED_USE_FLAG_7,
       D.PG_ED_USE_FLAG_8,
       D.PG_ED_USE_FLAG_9,
       D.PG_ED_USE_FLAG_10,
       D.SFA_ADDL_PELL_ELIG,
       D.SFA_COD_CITZN_STAT,
       D.SFA_ATB_CD,
       'N' LOAD_ERROR, 'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM, 1234 BATCH_SID
  from CSSTG_OWNER.PS_PELL_ORIGINATN O
  join CSSTG_OWNER.PS_PELL_ORIG_DTL D
    on O.EMPLID = D.EMPLID
   and O.INSTITUTION = D.INSTITUTION
   and O.AID_YEAR = D.AID_YEAR
   and O.PELL_ORIG_ID = D.PELL_ORIG_ID
   and O.SRC_SYS_ID = D.SRC_SYS_ID
   and D.DATA_ORIGIN <> 'D'
  join CSMRT_OWNER.PS_D_INSTITUTION I
    on O.INSTITUTION = I.INSTITUTION_CD
   and O.SRC_SYS_ID = I.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on O.EMPLID = P.PERSON_ID
   and O.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join XL X1
    on X1.FIELDNAME = 'PELL_TRANS_STAT'
   and X1.FIELDVALUE = O.PELL_TRANS_STAT
   and X1.SRC_SYS_ID = O.SRC_SYS_ID
  left outer join XL X2
    on X2.FIELDNAME = 'PELL_ORIG_STATUS'
   and X2.FIELDVALUE = O.PELL_ORIG_STATUS
   and X2.SRC_SYS_ID = O.SRC_SYS_ID
  left outer join XL X3
    on X3.FIELDNAME = 'PELL_MRR_STATUS'
   and X3.FIELDVALUE = O.PELL_MRR_STATUS
   and X3.SRC_SYS_ID = O.SRC_SYS_ID
  left outer join XL X4
    on X4.FIELDNAME = 'ACTION_CODE'
   and X4.FIELDVALUE = D.ACTION_CODE
   and X4.SRC_SYS_ID = D.SRC_SYS_ID
--  left outer join XL X5
--    on X5.FIELDNAME = 'LN_BOOK_STAT'
--   and X5.FIELDVALUE = O.LN_BOOK_STAT
--   and X5.SRC_SYS_ID = O.SRC_SYS_ID
 where O.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_PELL_ORIG rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_PELL_ORIG',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );



strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_PELL_ORIG',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_PELL_ORIG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
--alter table UM_F_FA_STDNT_PELL_ORIG enable constraint PK_UM_F_FA_STDNT_PELL_ORIG;

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_PELL_ORIG enable constraint PK_UM_F_FA_STDNT_PELL_ORIG';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_PELL_ORIG');

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

END UM_F_FA_STDNT_PELL_ORIG_P;
/
