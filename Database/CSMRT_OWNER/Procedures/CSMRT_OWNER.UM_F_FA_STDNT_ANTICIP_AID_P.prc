DROP PROCEDURE CSMRT_OWNER.UM_F_FA_STDNT_ANTICIP_AID_P
/

--
-- UM_F_FA_STDNT_ANTICIP_AID_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_FA_STDNT_ANTICIP_AID_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads mart table UM_F_FA_STDNT_ANTICIP_AID.
--
 --V01  SMT-xxxx 07/12/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_ANTICIP_AID';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_ANTICIP_AID';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_ANTICIP_AID';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_ANTICIP_AID';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_ANTICIP_AID');

----alter table UM_F_FA_STDNT_ANTICIP_AID disable constraint PK_UM_F_FA_STDNT_ANT_AID;
--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_ANTICIP_AID disable constraint PK_UM_F_FA_STDNT_ANT_AID';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_ANTICIP_AID';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_ANTICIP_AID';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_FA_STDNT_ANTICIP_AID
with PL as (
select /*+ inline parallel(8) */
       INSTITUTION, AID_YEAR, ACAD_CAREER, DISBURSEMENT_PLAN, SRC_SYS_ID,
       DESCR
  from CSSTG_OWNER.PS_DISB_PLAN_TBL
 where DATA_ORIGIN <> 'D')
select /*+ inline parallel(8) */
       A.INSTITUTION INSTITUTION_CD,  -- SID
       A.ACAD_CAREER ACAD_CAR_CD,     -- SID
       A.AID_YEAR,
       A.EMPLID PERSON_ID,            -- SID
       A.ITEM_TYPE,                   -- SID
       A.DISBURSEMENT_PLAN,           -- XLAT? See UM_F_FA_STDNT_AWARDS
       A.DISBURSEMENT_ID,
       nvl(A.AS_OF_DTTM,to_date('01-JAN-1900')), -- Apr 2018
       A.SRC_SYS_ID,
       nvl(B.INSTITUTION_SID,2147483646) INSTITUTION_SID,
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       nvl(Y.ITEM_TYPE_SID,2147483646) ITEM_TYPE_SID,
       nvl(T.TERM_SID,2147483646) TERM_SID,
       nvl(PL.DESCR,'-') DISBURSEMENT_PLAN_LD,
       NET_AWARD_AMT,
       DISB_APPLY_DT,
       DISB_EXPIRE_DT,
       CURRENCY_CD,
       A.LOAD_ERROR,
       A.DATA_ORIGIN,
       A.CREATED_EW_DTTM,
       A.LASTUPD_EW_DTTM,
       A.BATCH_SID
  from CSSTG_OWNER.PS_ANTICIPATED_AID A
  left outer join CSMRT_OWNER.PS_D_INSTITUTION B
    on A.INSTITUTION = B.INSTITUTION_CD
   and A.SRC_SYS_ID = B.SRC_SYS_ID
   and B.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
    on A.INSTITUTION = C.INSTITUTION_CD
   and A.ACAD_CAREER = C.ACAD_CAR_CD
   and A.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on A.EMPLID = P.PERSON_ID
   and A.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_ITEM_TYPE Y
    on A.INSTITUTION = Y.SETID
   and A.ITEM_TYPE = Y.ITEM_TYPE_ID
   and A.SRC_SYS_ID = Y.SRC_SYS_ID
   and Y.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_TERM T
    on A.INSTITUTION = T.INSTITUTION_CD
   and A.ACAD_CAREER = T.ACAD_CAR_CD
   and A.STRM = T.TERM_CD
   and A.SRC_SYS_ID = T.SRC_SYS_ID
   and T.DATA_ORIGIN <> 'D'
  left outer join PL
    on A.INSTITUTION = PL.INSTITUTION
   and A.AID_YEAR = PL.AID_YEAR
   and A.ACAD_CAREER = PL.ACAD_CAREER
   and A.DISBURSEMENT_PLAN = PL.DISBURSEMENT_PLAN
 where A.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_ANTICIP_AID rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_ANTICIP_AID',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );



strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_ANTICIP_AID',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_ANTICIP_AID';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
--alter table UM_F_FA_STDNT_ANTICIP_AID enable constraint PK_UM_F_FA_STDNT_ANT_AID;

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_ANTICIP_AID enable constraint PK_UM_F_FA_STDNT_ANT_AID';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_ANTICIP_AID');

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

END UM_F_FA_STDNT_ANTICIP_AID_P;
/
