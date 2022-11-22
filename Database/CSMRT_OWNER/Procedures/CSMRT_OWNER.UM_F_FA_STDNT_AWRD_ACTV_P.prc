DROP PROCEDURE CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV_P
/

--
-- UM_F_FA_STDNT_AWRD_ACTV_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_FA_STDNT_AWRD_ACTV_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads mart table UM_F_FA_STDNT_AWRD_ACTV.
--
 --V01  SMT-xxxx 07/12/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_AWRD_ACTV';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_AWRD_ACTV');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV disable constraint PK_UM_F_FA_STDNT_AWRD_ACTV';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_FA_STDNT_AWRD_ACTV
  with X as (
select /*+ inline */
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D')
select F.INSTITUTION INSTITUTION_CD,
       F.ACAD_CAREER ACAD_CAR_CD,
       F.AID_YEAR,
       F.EMPLID PERSON_ID,
       F.ITEM_TYPE,
       F.ACTION_DTTM,
       F.SRC_SYS_ID,
       nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       nvl(T.ITEM_TYPE_SID,2147483646) ITEM_TYPE_SID,
       F.DISBURSEMENT_PLAN,
       F.SPLIT_CODE,
       F.DISBURSEMENT_ID,
       F.OPRID,
       F.AWARD_DISB_ACTION,
       nvl(X1.XLATLONGNAME,'') AWARD_DISB_ACTION_LD,
       F.OFFER_AMOUNT,
       F.ACCEPT_AMOUNT,
       F.AUTHORIZED_AMOUNT,
       F.DISB_AMOUNT,
       F.CURRENCY_CD,
       F.BUSINESS_UNIT,
       F.ADJUST_REASON_CD,
       D.DESCR50 ADJUST_REASON_LD,
       F.ADJUST_AMOUNT,
       F.LOAN_ADJUST_CD,
       nvl(X2.XLATLONGNAME,'') LOAN_ADJUST_LD,
       F.DISB_TO_DATE,
       F.AUTH_TO_DATE,
       F.PKG_APP_DATA_USED,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID
  from CSSTG_OWNER.PS_STDNT_AWRD_ACTV F
  left outer join CSSTG_OWNER.PS_AWD_ADJ_RSN_TBL D
    on F.INSTITUTION = D.INSTITUTION
   and F.AID_YEAR = D.AID_YEAR
   and F.ADJUST_REASON_CD = D.ADJUST_REASON_CD
   and F.SRC_SYS_ID = D.SRC_SYS_ID
   and D.DATA_ORIGIN <> 'D'
  left outer join PS_D_INSTITUTION I
    on F.INSTITUTION = I.INSTITUTION_CD
   and F.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_CAR C
    on F.INSTITUTION = C.INSTITUTION_CD
   and F.ACAD_CAREER = C.ACAD_CAR_CD
   and F.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join PS_D_PERSON P
    on F.EMPLID = P.PERSON_ID
   and F.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join UM_D_FA_ITEM_TYPE T
    on F.INSTITUTION = T.INSTITUTION_CD
   and F.ITEM_TYPE = T.ITEM_TYPE
   and F.AID_YEAR = T.AID_YEAR
   and F.SRC_SYS_ID = T.SRC_SYS_ID
   and T.DATA_ORIGIN <> 'D'
  left outer join X X1
    on F.AWARD_DISB_ACTION = X1.FIELDVALUE
   and F.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'AWARD_DISB_ACTION'
   and X1.X_ORDER = 1
  left outer join X X2
    on F.LOAN_ADJUST_CD = X2.FIELDVALUE
   and F.SRC_SYS_ID = X2.SRC_SYS_ID
   and X2.FIELDNAME = 'LOAN_ADJUST_CD'
   and X2.X_ORDER = 1
 where F.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_AWRD_ACTV rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_AWRD_ACTV',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_AWRD_ACTV',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV enable constraint PK_UM_F_FA_STDNT_AWRD_ACTV';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_AWRD_ACTV');

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

END UM_F_FA_STDNT_AWRD_ACTV_P;
/
